"""
Sincroniza facturas desde la API de Dux Software hacia Supabase.

Flujo:  Dux (API REST)  ->  mapear()  ->  RPC upsert_factura()  ->  Supabase

Pensado para correrse seguido (es idempotente: el upsert se hace sobre
id_factura, así que volver a correrlo NO duplica y deja todo actualizado).
Recorre TODAS las sucursales de la empresa, porque en Dux la sucursal es
un parámetro de la consulta y NO viene dentro de cada factura: la inyectamos
nosotros (campo id_sucursal) según con qué sucursal la pedimos.

Variables de entorno (poné un archivo .env, NO lo subas al repo):
    DUX_TOKEN              token de la API de Dux (panel: Integraciones y API)
    DUX_BASE              base URL de la API (default: WSERP de Dux)
    DUX_ID_EMPRESA        id de empresa en Dux (default: 3526)
    SUPABASE_URL          https://<proyecto>.supabase.co
    SUPABASE_SERVICE_KEY  service_role key (permite RPC de escritura)

Uso:
    # Sincronizar junio 2026 (todas las sucursales) contra Supabase:
    python3 sync_facturas.py --desde 2026-06-01 --hasta 2026-06-30

    # Validar el mapeo SIN tocar la base ni la red, usando un JSON guardado
    # (sirve para ver errores de mapeo/casteo antes de subir nada):
    python3 sync_facturas.py --dry-run --file facturas.json
"""

import os
import re
import sys
import json
import time
import logging
import argparse
from datetime import datetime, timezone, date, timedelta

# --- defaults tomados de credenciales.md (se pueden pisar por entorno) --------
DUX_BASE_DEFAULT = "https://erp.duxsoftware.com.ar/WSERP/rest/services"
ID_EMPRESA_DEFAULT = "3526"

log = logging.getLogger("dux.sync")


def empresas_env():
    """Lista de id_empresa desde DUX_ID_EMPRESA (soporta varias: '3526,1234')."""
    crudo = os.environ.get("DUX_ID_EMPRESA", ID_EMPRESA_DEFAULT)
    return [e.strip() for e in crudo.split(",") if e.strip()]


MESES = {
    "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
    "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
}
_FECHA_RE = re.compile(
    r"([A-Za-z]{3}) (\d{1,2}), (\d{4}) (\d{1,2}):(\d{2}):(\d{2}) (AM|PM)"
)


def parse_dux_date(s):
    """Convierte la fecha de Dux ('Jun 5, 2026 3:00:00 AM') a ISO 8601 UTC.

    Dux entrega las fechas en UTC (por eso un comprobante del día 5 figura a
    las 03:00 AM = medianoche en Argentina, UTC-3). Las dejamos como UTC y el
    trigger facturas_set_fecha_base() ya las baja a hora local para fecha_base.
    """
    if not s:
        return None
    m = _FECHA_RE.match(s.strip())
    if not m:
        return s  # formato inesperado: que lo intente castear Postgres
    mon, day, year, hh, mm, ss, ap = m.groups()
    hh = int(hh) % 12
    if ap == "PM":
        hh += 12
    dt = datetime(int(year), MESES[mon], int(day), hh, int(mm), int(ss),
                  tzinfo=timezone.utc)
    return dt.isoformat()


def nombre_cliente(f):
    """En Dux el nombre viene partido: apellido_razon_soc + nombre."""
    apellido = (f.get("apellido_razon_soc") or "").strip()
    nombre = (f.get("nombre") or "").strip()
    if apellido and nombre:
        return f"{apellido}, {nombre}"
    return apellido or nombre or None


def mapear(f, id_sucursal=None):
    """Convierte el JSON crudo de Dux al shape que espera upsert_factura().

    Claves de la izquierda  -> las que lee la función SQL (normalizadas).
    Claves de la derecha     -> las reales que devuelve Dux (snake_case).
    Los montos se dejan tal cual (string) para no perder precisión; el SQL
    los castea a numeric (soporta hasta notación científica tipo 1.0069e7).
    """
    items = [
        {
            "nro_linea": i + 1,
            "sku": it.get("cod_item"),
            "nombre_producto": it.get("item"),
            "cantidad": it.get("ctd"),
            "precio_unitario": it.get("precio_uni"),
            "descuento_pct": it.get("porc_desc") or 0,
            "iva_pct": it.get("porc_iva") or 0,
            "costo_unitario": it.get("costo"),
            "moneda": it.get("moneda"),
            "cotizacion_moneda": it.get("cotizacion_moneda"),
            "id_lista_precio_venta": it.get("id_lista_precio_venta"),
        }
        for i, it in enumerate(f.get("detalles") or [])
    ]

    # Los pagos vienen anidados: grupos de cobro -> movimientos. Los aplanamos.
    pagos = []
    for g_idx, grupo in enumerate(f.get("detalles_cobro") or [], start=1):
        pdv = grupo.get("numero_punto_de_venta")
        for l_idx, mov in enumerate(grupo.get("detalles_mov_cobro") or [], start=1):
            pagos.append({
                "nro_grupo": g_idx,
                "nro_linea": l_idx,
                "punto_venta": str(pdv) if pdv is not None else None,
                "numero_comprobante": grupo.get("numero_comprobante"),
                "caja": grupo.get("caja"),
                "cajero": grupo.get("personal"),
                "tipo_pago": mov.get("tipo_de_valor"),
                "referencia_pago": mov.get("referencia"),
                "monto_pago": mov.get("monto"),
            })

    return {
        "id_factura": f.get("id"),
        "id_empresa": f.get("id_empresa"),
        "id_cliente": f.get("id_cliente"),
        "punto_venta": f.get("nro_pto_vta"),
        "tipo_comprobante": f.get("tipo_comp"),
        "letra_comprobante": f.get("letra_comp"),
        "numero_comprobante": f.get("nro_comp"),
        "fecha_comprobante": parse_dux_date(f.get("fecha_comp")),
        "fecha_registro": parse_dux_date(f.get("fecha_registro")),
        "cliente_nombre": nombre_cliente(f),
        "cliente_cuit": f.get("cuit") or (
            str(f["nro_doc"]) if f.get("nro_doc") is not None else None
        ),
        "monto_exento": f.get("monto_exento"),
        "monto_gravado": f.get("monto_gravado"),
        "monto_iva": f.get("monto_iva"),
        "monto_descuento": f.get("monto_desc"),
        "monto_total": f.get("total"),
        "anulada": f.get("anulada_boolean", False),
        "id_vendedor": f.get("id_vendedor"),
        "url_factura": f.get("url_factura"),
        "id_sucursal": id_sucursal,
        "items": items,
        "pagos": pagos,
    }


# ---------------------------------------------------------------------------
# Validación local: simula los casts que hace el SQL para detectar errores
# ANTES de mandar nada a la base.
# ---------------------------------------------------------------------------
def _cast_int(v):
    if v is None or v == "":
        return None
    int(str(v))  # lanza si no castea (igual de estricto que ::int de Postgres)


def _cast_num(v):
    if v is None or v == "":
        return None
    from decimal import Decimal
    Decimal(str(v))  # soporta '1.0069e7', '0.0', etc.


def _cast_ts(v):
    if v is None or v == "":
        return None
    s = str(v).replace("Z", "+00:00")
    try:
        datetime.fromisoformat(s)
    except ValueError:
        # puede ser un formato crudo de Dux que dejamos pasar a Postgres
        if not _FECHA_RE.match(str(v).strip()):
            raise


def validar(p):
    """Devuelve lista de problemas (campo, valor, error) para una factura mapeada."""
    errores = []

    def chk(fn, campo, valor):
        try:
            fn(valor)
        except Exception as e:
            errores.append((campo, valor, str(e)))

    for c in ("id_factura", "id_empresa", "id_cliente", "numero_comprobante",
              "id_vendedor", "id_sucursal"):
        chk(_cast_int, c, p.get(c))
    for c in ("monto_exento", "monto_gravado", "monto_iva", "monto_descuento",
              "monto_total"):
        chk(_cast_num, c, p.get(c))
    for c in ("fecha_comprobante", "fecha_registro"):
        chk(_cast_ts, c, p.get(c))

    for it in p.get("items", []):
        n = it.get("nro_linea")
        chk(_cast_int, f"item[{n}].nro_linea", it.get("nro_linea"))
        chk(_cast_int, f"item[{n}].id_lista_precio_venta", it.get("id_lista_precio_venta"))
        for c in ("cantidad", "precio_unitario", "descuento_pct", "iva_pct",
                  "costo_unitario", "cotizacion_moneda"):
            chk(_cast_num, f"item[{n}].{c}", it.get(c))

    for pg in p.get("pagos", []):
        tag = f"pago[{pg.get('nro_grupo')}/{pg.get('nro_linea')}]"
        chk(_cast_int, f"{tag}.numero_comprobante", pg.get("numero_comprobante"))
        chk(_cast_num, f"{tag}.monto_pago", pg.get("monto_pago"))

    return errores


# ---------------------------------------------------------------------------
# Acceso a Dux (imports perezosos: así el --dry-run corre sin httpx/supabase)
# ---------------------------------------------------------------------------
def _dux_client():
    import httpx
    token = os.environ["DUX_TOKEN"]
    base = os.environ.get("DUX_BASE", DUX_BASE_DEFAULT)
    # Dux WSERP autentica con el token crudo en el header 'authorization'.
    return httpx.Client(
        base_url=base,
        headers={"accept": "application/json", "authorization": token},
        timeout=60.0,
    )


def _get(dux, path, params, intentos=6):
    """GET con reintentos y backoff ante 429/5xx (respeta Retry-After) y
    pacing fijo entre llamadas, porque Dux tiene un rate limit estricto.
    Dux pide >=5s por request; en tandas largas eso igual se corta, así que
    el default es 7s. Ajustable con DUX_RATE_DELAY (segundos entre requests)."""
    espera = float(os.environ.get("DUX_RATE_DELAY", "7.0"))
    r = None
    for intento in range(1, intentos + 1):
        r = dux.get(path, params=params)
        if r.status_code == 429 or r.status_code >= 500:
            ra = r.headers.get("Retry-After", "")
            if ra.isdigit():
                pausa = min(float(ra), 300.0)
            else:
                pausa = min(espera * (2 ** (intento - 1)), 120.0)
            log.warning("Dux %s en %s; espero %.0fs y reintento (%s/%s)",
                        r.status_code, path, pausa, intento, intentos)
            time.sleep(pausa)
            continue
        r.raise_for_status()
        time.sleep(espera)  # pacing para no gatillar el rate limit
        return r
    r.raise_for_status()  # se agotaron los reintentos: propaga el último error
    return r


def fetch_sucursales(dux, id_empresa):
    """Devuelve [{'id': int, 'sucursal': str}] de la empresa."""
    r = _get(dux, "/sucursales", {"idEmpresa": id_empresa})
    data = r.json()
    lista = data.get("results", data) if isinstance(data, dict) else data
    out = []
    for s in lista:
        sid = s.get("id") or s.get("id_sucursal") or s.get("idSucursal")
        if sid is not None:
            out.append({"id": sid, "sucursal": s.get("sucursal") or s.get("nombre")})
    return out


def fetch_facturas(dux, id_empresa, id_sucursal, desde, hasta):
    """Trae todas las facturas del rango para UNA sucursal, paginando.

    Formato de fecha de Dux: YYYY-MM-DD (ej. 2026-06-01), según credenciales.md.
    """
    facturas = []
    offset, limit = 0, 50
    while True:
        r = _get(dux, "/facturas", {
            "idEmpresa": id_empresa,
            "idSucursal": id_sucursal,
            "fechaDesde": desde,
            "fechaHasta": hasta,
            "offset": offset,
            "limit": limit,
        })
        data = r.json()
        if isinstance(data, list) and data and isinstance(data[0], dict) and "results" in data[0]:
            data = data[0]  # algunas respuestas vienen como [{paging, results}]
        lote = data.get("results", data) if isinstance(data, dict) else data
        if not lote:
            break
        facturas.extend(lote)
        offset += limit
    return facturas


def cargar_local(path):
    """Lee un JSON guardado (export de Dux) y devuelve la lista de facturas."""
    raw = json.load(open(path, encoding="utf-8"))
    if isinstance(raw, dict):
        raw = [raw]
    facturas = []
    for page in raw:
        if isinstance(page, dict) and "results" in page:
            facturas.extend(page["results"])
        elif isinstance(page, dict):
            facturas.append(page)
    return facturas


# ---------------------------------------------------------------------------
def run_dry(facturas, id_sucursal):
    ok = items = pagos = 0
    con_error = 0
    for f in facturas:
        p = mapear(f, id_sucursal)
        errs = validar(p)
        if errs:
            con_error += 1
            print(f"\n✗ factura {p.get('id_factura')} "
                  f"({p.get('tipo_comprobante')} {p.get('letra_comprobante')} "
                  f"{p.get('numero_comprobante')}):")
            for campo, valor, msg in errs:
                print(f"    - {campo} = {valor!r}: {msg}")
        else:
            ok += 1
            items += len(p["items"])
            pagos += len(p["pagos"])
    print(f"\n[dry-run] {len(facturas)} facturas mapeadas | "
          f"{ok} sin errores, {con_error} con errores de casteo | "
          f"{items} items, {pagos} pagos en total.")
    return con_error


def get_supabase():
    from supabase import create_client
    return create_client(os.environ["SUPABASE_URL"],
                         os.environ["SUPABASE_SERVICE_KEY"])


# ---------------------------------------------------------------------------
# Watermark por (empresa, sucursal): tabla sync_state en Supabase.
# Permite el sync incremental y alimenta /status del servicio.
# ---------------------------------------------------------------------------
def leer_watermark(sb, id_empresa, id_sucursal):
    """Devuelve la última fecha 'hasta' sincronizada (str YYYY-MM-DD) o None."""
    r = (sb.table("sync_state")
           .select("ultima_fecha_hasta")
           .eq("id_empresa", int(id_empresa))
           .eq("id_sucursal", int(id_sucursal))
           .execute())
    if r.data:
        return r.data[0].get("ultima_fecha_hasta")
    return None


def escribir_watermark(sb, id_empresa, id_sucursal, sucursal, hasta,
                       ok, error, ultimo_error=None):
    sb.table("sync_state").upsert({
        "id_empresa": int(id_empresa),
        "id_sucursal": int(id_sucursal),
        "sucursal": sucursal,
        "ultima_fecha_hasta": hasta,
        "ultima_corrida": datetime.now(timezone.utc).isoformat(),
        "facturas_ok": ok,
        "facturas_error": error,
        "ultimo_error": ultimo_error,
    }, on_conflict="id_empresa,id_sucursal").execute()


def leer_estado(sb):
    """Todas las filas de sync_state (para /status)."""
    return (sb.table("sync_state").select("*")
              .order("id_empresa").order("id_sucursal").execute()).data


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def _enviar(sb, raw_batch, sid, nombre):
    """Manda un lote de facturas CRUDAS (tal cual Dux) al RPC del ERP.
    El mapeo lo hace el SQL (sync_facturas_dux): recibe los detalles/cobros
    crudos + la sucursal, y upsertea idempotentemente. Devuelve facturas_procesadas."""
    rpc = os.environ.get("SYNC_RPC", "sync_facturas_dux")
    r = sb.rpc(rpc, {
        "p_data": raw_batch,
        "p_id_sucursal": int(sid) if sid is not None else None,
        "p_nombre_sucursal": nombre,
    }).execute()
    data = r.data or {}
    return int(data.get("facturas_procesadas", 0)) if isinstance(data, dict) else 0


def _sync_sucursal(sb, dux, id_empresa, suc, desde, hasta):
    """Trae las facturas crudas de una sucursal y las manda en lotes al RPC.
    Si un lote falla, reintenta factura por factura para aislar la culpable
    (la próxima corrida igual reintenta el resto gracias a la ventana móvil)."""
    sid = suc["id"]
    nombre = suc.get("sucursal")
    crudas = fetch_facturas(dux, id_empresa, sid, desde, hasta)
    log.info("emp %s suc %s (%s): %s facturas traídas %s..%s",
             id_empresa, sid, nombre, len(crudas), desde, hasta)
    batch = int(os.environ.get("DUX_BATCH_SIZE", "50"))
    ok = error = 0
    ultimo_error = None
    # Si no hay facturas en el rango, NO registramos la sucursal: así no se
    # llena public.sucursales con sucursales vacías. sync_facturas_dux solo
    # crea la sucursal cuando procesa al menos una factura.
    for lote in _chunks(crudas, batch):
        try:
            ok += _enviar(sb, lote, sid, nombre)
        except Exception as e:
            log.warning("lote de %s facturas falló (emp %s suc %s): %s; "
                        "reintento individual", len(lote), id_empresa, sid, e)
            for f in lote:
                try:
                    ok += _enviar(sb, [f], sid, nombre)
                except Exception as e2:
                    error += 1
                    ultimo_error = str(e2)
                    log.error("factura %s (emp %s suc %s): %s",
                              f.get("id"), id_empresa, sid, e2)
    return ok, error, ultimo_error, len(crudas)


def run_sync(desde, hasta, sucursal_filtro=None, sb=None, dux=None):
    """Sync de un rango EXPLÍCITO (backfill manual). Idempotente."""
    sb = sb or get_supabase()
    propio = dux is None
    dux = dux or _dux_client()
    resumen = {"ok": 0, "error": 0, "sucursales": []}
    try:
        for emp in empresas_env():
            sucs = fetch_sucursales(dux, emp)
            if sucursal_filtro:
                sucs = [s for s in sucs if str(s["id"]) == str(sucursal_filtro)]
            for suc in sucs:
                ok, error, ult, traidas = _sync_sucursal(sb, dux, emp, suc, desde, hasta)
                resumen["ok"] += ok
                resumen["error"] += error
                resumen["sucursales"].append({
                    "empresa": emp, "sucursal": suc["id"], "nombre": suc.get("sucursal"),
                    "desde": desde, "hasta": hasta, "traidas": traidas,
                    "ok": ok, "error": error, "ultimo_error": ult,
                })
    finally:
        if propio:
            dux.close()
    log.info("backfill %s..%s: %s ok, %s error", desde, hasta,
             resumen["ok"], resumen["error"])
    return resumen


def run_incremental(ventana_dias=None, sb=None, dux=None):
    """Sync incremental con ventana móvil. Para cada (empresa, sucursal):
      desde = watermark - VENTANA_DIAS   (re-chequea lo reciente: anulaciones/cambios)
      hasta = hoy (UTC)
    Si no hay watermark, hace backfill desde DUX_BACKFILL_DESDE.
    Avanza el watermark sólo si la sucursal se trajo sin error fatal."""
    ventana = (ventana_dias if ventana_dias is not None
               else int(os.environ.get("DUX_VENTANA_DIAS", "35")))
    backfill = os.environ.get("DUX_BACKFILL_DESDE", "2024-01-01")
    sb = sb or get_supabase()
    propio = dux is None
    dux = dux or _dux_client()
    hasta = datetime.now(timezone.utc).date().isoformat()

    resumen = {"inicio": datetime.now(timezone.utc).isoformat(),
               "ventana_dias": ventana, "ok": 0, "error": 0, "sucursales": []}
    try:
        for emp in empresas_env():
            try:
                sucs = fetch_sucursales(dux, emp)
            except Exception as e:
                log.error("no pude listar sucursales de empresa %s: %s", emp, e)
                resumen["sucursales"].append({"empresa": emp, "error_fatal": str(e)})
                continue
            for suc in sucs:
                sid = suc["id"]
                # Leer watermark con tolerancia: si sync_state no está disponible
                # (p. ej. schema cache de PostgREST recién creado), NO abortamos:
                # degradamos a una ventana acotada desde hoy y seguimos.
                wm, wm_error = None, False
                try:
                    wm = leer_watermark(sb, emp, sid)
                except Exception as e:
                    wm_error = True
                    log.warning("watermark no disponible (emp %s suc %s): %s", emp, sid, e)
                if wm:
                    desde = (date.fromisoformat(str(wm)[:10]) -
                             timedelta(days=ventana)).isoformat()
                elif wm_error:
                    desde = (date.fromisoformat(hasta) -
                             timedelta(days=ventana)).isoformat()
                else:
                    desde = backfill
                try:
                    ok, error, ult, traidas = _sync_sucursal(sb, dux, emp, suc, desde, hasta)
                    try:
                        escribir_watermark(sb, emp, sid, suc.get("sucursal"),
                                           hasta, ok, error, ult)
                    except Exception as e:
                        log.warning("no pude escribir watermark (emp %s suc %s): %s",
                                    emp, sid, e)
                    resumen["ok"] += ok
                    resumen["error"] += error
                    resumen["sucursales"].append({
                        "empresa": emp, "sucursal": sid, "nombre": suc.get("sucursal"),
                        "desde": desde, "hasta": hasta, "traidas": traidas,
                        "ok": ok, "error": error, "ultimo_error": ult,
                    })
                except Exception as e:
                    log.exception("fallo sync emp %s suc %s", emp, sid)
                    resumen["sucursales"].append({
                        "empresa": emp, "sucursal": sid, "error_fatal": str(e),
                    })
    finally:
        if propio:
            dux.close()
    resumen["fin"] = datetime.now(timezone.utc).isoformat()
    log.info("incremental: %s ok, %s error", resumen["ok"], resumen["error"])
    return resumen


def run_full_resync(sb=None, dux=None):
    """Re-sync COMPLETO del histórico (todas las sucursales, sin watermark).
    Idempotente: solo pisa lo que cambió. Es la red de seguridad para captar
    ediciones/anulaciones de facturas VIEJAS: Dux no tiene filtro "modificado
    desde", así que la única forma confiable de detectarlas es re-leer todo
    cada tanto. La ventana móvil del incremental capta lo reciente; esto, lo viejo."""
    desde = os.environ.get("DUX_BACKFILL_DESDE", "2024-01-01")
    hasta = datetime.now(timezone.utc).date().isoformat()
    log.info("FULL re-sync %s..%s (todas las sucursales)", desde, hasta)
    res = run_sync(desde, hasta, sb=sb, dux=dux)
    res["full_resync"] = True
    return res


def cargar_env(path=".env"):
    """Lee un .env simple (KEY=VALOR) sin depender de python-dotenv."""
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as fh:
        for linea in fh:
            linea = linea.strip()
            if not linea or linea.startswith("#") or "=" not in linea:
                continue
            k, v = linea.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


def main():
    cargar_env()
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    ap = argparse.ArgumentParser(description="Sync de facturas Dux -> Supabase")
    ap.add_argument("--incremental", action="store_true",
                    help="sync incremental con ventana móvil (usa watermark)")
    ap.add_argument("--full-resync", action="store_true",
                    help="re-sync COMPLETO del histórico (capta ediciones/anulaciones viejas)")
    ap.add_argument("--desde", help="fecha desde (YYYY-MM-DD) para backfill manual")
    ap.add_argument("--hasta", help="fecha hasta (YYYY-MM-DD) para backfill manual")
    ap.add_argument("--sucursal", help="limitar a una sola sucursal (id)")
    ap.add_argument("--dry-run", action="store_true",
                    help="no toca la base: solo mapea y valida")
    ap.add_argument("--file", help="JSON local a usar en --dry-run")
    args = ap.parse_args()

    if args.dry_run:
        if not args.file:
            ap.error("--dry-run requiere --file con un JSON de Dux")
        facturas = cargar_local(args.file)
        sys.exit(1 if run_dry(facturas, args.sucursal) else 0)

    if args.incremental:
        res = run_incremental()
        sys.exit(1 if res["error"] else 0)

    if args.full_resync:
        res = run_full_resync()
        sys.exit(1 if res["error"] else 0)

    if not (args.desde and args.hasta):
        ap.error("indicá --incremental, o --desde y --hasta (o --dry-run --file)")
    res = run_sync(args.desde, args.hasta, args.sucursal)
    sys.exit(1 if res["error"] else 0)


if __name__ == "__main__":
    main()
