"""
Microservicio de sincronización Ferrepro -> Supabase.

Un solo proceso liviano con jobs programados (APScheduler):
  - incremental: cada SYNC_INTERVAL_MIN min. Ventana móvil (últimos N días):
    facturas nuevas + ediciones/anulaciones RECIENTES.
  - full re-sync: cron FULL_RESYNC_CRON (semanal por defecto). Re-lee TODO el
    histórico: capta ediciones/anulaciones VIEJAS (Dux no tiene filtro
    "modificado desde", así que re-leer todo es la única forma confiable).
  - productos: Tiendanube + OpenAI embeddings + RPC ferrepro_upsert_producto.
  - meta: Meta Ads insights diarios por account/campaign/adset/ad.

FastAPI expone /health, /status y endpoints on-demand.

El sync de Dux es secuencial a propósito: el rate limit de Dux es global por
token (1 request cada 5s). Productos y Meta tienen locks propios.

Correr:  uvicorn service:app --host 0.0.0.0 --port 8000
"""
import os
import logging
import threading
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import sync_facturas as core
import sync_meta
import sync_productos
import notifier

core.cargar_env()
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("dux.service")

_locks = {
    "dux": threading.Lock(),
    "productos": threading.Lock(),
    "meta": threading.Lock(),
}
scheduler = BackgroundScheduler(timezone="UTC")

JOBS = {
    "incremental": core.run_incremental,
    "full": core.run_full_resync,
    "productos": sync_productos.run_productos,
    "productos_reconcile": sync_productos.run_reconcile,
    "meta": sync_meta.run_meta,
}
JOB_GROUP = {
    "incremental": "dux",
    "full": "dux",
    "productos": "productos",
    "productos_reconcile": "productos",
    "meta": "meta",
}
_ultima = {k: None for k in JOBS}   # resumen de la última corrida de cada tipo


def _alertar_si_errores(tipo: str, trigger: str, res: dict):
    """Manda alerta a Telegram si la corrida terminó con errores (no fatal)."""
    fatales = [s for s in res.get("sucursales", []) if s.get("error_fatal")]
    n_err = res.get("error", 0)
    if not (n_err or fatales):
        return
    partes = []
    for s in fatales:
        partes.append(f"emp {s.get('empresa')} suc {s.get('sucursal')}: "
                      f"{s.get('error_fatal')}")
    for s in res.get("sucursales", []):
        if s.get("error") and s.get("ultimo_error"):
            partes.append(f"emp {s.get('empresa')} suc {s.get('sucursal')} "
                          f"({s['error']} fallidas): {s['ultimo_error']}")
    partes.extend(res.get("errores", [])[:10])
    for n in res.get("niveles", []):
        if n.get("error"):
            partes.append(f"{n.get('level')}: {n.get('error')}")
    notifier.notify_error(
        f"{tipo} terminó con errores",
        detalle="\n".join(partes) or None,
        contexto={"trigger": trigger, "errores": n_err,
                  "sucursales_con_fallo": len(fatales)},
    )


def _run(tipo: str, trigger: str):
    """Corre un job garantizando exclusión mutua por grupo."""
    lock = _locks[JOB_GROUP[tipo]]
    if not lock.acquire(blocking=False):
        log.warning("%s: ya hay una corrida en curso; se omite '%s'", tipo, trigger)
        return {"skipped": True, "motivo": "ya_en_curso"}
    try:
        log.info("arranca %s (%s)", tipo, trigger)
        res = JOBS[tipo]()
        res["tipo"] = tipo
        res["trigger"] = trigger
        _ultima[tipo] = res
        _alertar_si_errores(tipo, trigger, res)
        return res
    except Exception as e:                 # nunca dejar caer el scheduler
        log.exception("%s falló (%s)", tipo, trigger)
        _ultima[tipo] = {
            "tipo": tipo, "trigger": trigger, "error_fatal": str(e),
            "fin": datetime.now(timezone.utc).isoformat(),
        }
        notifier.notify_error(
            f"{tipo} CRASH",
            detalle=traceback.format_exc(),
            contexto={"trigger": trigger},
        )
        return _ultima[tipo]
    finally:
        lock.release()


def _lanzar(tipo: str, trigger: str):
    """Dispara un job en background (no bloquea el request)."""
    if _locks[JOB_GROUP[tipo]].locked():
        return {"status": "ya_en_curso"}
    threading.Thread(target=_run, args=(tipo, trigger), daemon=True).start()
    return {"status": "disparado", "tipo": tipo}


@asynccontextmanager
async def lifespan(app: FastAPI):
    intervalo = int(os.environ.get("SYNC_INTERVAL_MIN", "30"))
    scheduler.add_job(_run, "interval", minutes=intervalo,
                      args=["incremental", "scheduler"], id="incremental",
                      max_instances=1, coalesce=True)
    log.info("scheduler incremental: cada %s min", intervalo)

    cron = os.environ.get("FULL_RESYNC_CRON", "0 6 * * 1").strip()
    if cron:
        scheduler.add_job(_run, CronTrigger.from_crontab(cron, timezone="UTC"),
                          args=["full", "scheduler"], id="full",
                          max_instances=1, coalesce=True)
        log.info("scheduler full re-sync: cron '%s' (UTC)", cron)
    else:
        log.info("full re-sync deshabilitado (FULL_RESYNC_CRON vacío)")

    productos_horas = int(os.environ.get("PRODUCTOS_SYNC_INTERVAL_HOURS", "6"))
    if productos_horas > 0:
        scheduler.add_job(_run, "interval", hours=productos_horas,
                          args=["productos", "scheduler"], id="productos",
                          max_instances=1, coalesce=True)
        log.info("scheduler productos: cada %s h", productos_horas)

    productos_reconcile_cron = os.environ.get("PRODUCTOS_RECONCILE_CRON", "0 7 * * *").strip()
    if productos_reconcile_cron:
        scheduler.add_job(_run, CronTrigger.from_crontab(productos_reconcile_cron, timezone="UTC"),
                          args=["productos_reconcile", "scheduler"], id="productos_reconcile",
                          max_instances=1, coalesce=True)
        log.info("scheduler productos reconcile: cron '%s' (UTC)", productos_reconcile_cron)

    meta_horas = int(os.environ.get("META_SYNC_INTERVAL_HOURS", "24"))
    if meta_horas > 0:
        scheduler.add_job(_run, "interval", hours=meta_horas,
                          args=["meta", "scheduler"], id="meta",
                          max_instances=1, coalesce=True)
        log.info("scheduler meta: cada %s h", meta_horas)

    scheduler.start()
    if os.environ.get("SYNC_ON_START", "false").lower() == "true":
        threading.Thread(target=_run, args=("incremental", "startup"),
                         daemon=True).start()
    yield
    scheduler.shutdown(wait=False)


app = FastAPI(title="Dux -> Supabase sync", lifespan=lifespan)


@app.get("/health")
def health():
    """Liveness para el orquestador (Docker/k8s/load balancer)."""
    return {"status": "ok", "corriendo": any(l.locked() for l in _locks.values())}


@app.get("/status")
def status():
    """Últimas corridas en memoria + watermark de facturas por sucursal."""
    estado = {
        "corriendo": any(l.locked() for l in _locks.values()),
        "locks": {k: v.locked() for k, v in _locks.items()},
        "ultimas": _ultima,
        "ultima_incremental": _ultima["incremental"],
        "ultima_full": _ultima["full"],
    }
    try:
        estado["sucursales"] = core.leer_estado(core.get_supabase())
    except Exception as e:
        estado["sucursales_error"] = str(e)
    return estado


@app.post("/sync")
def sync():
    """Dispara un sync INCREMENTAL on-demand (ventana móvil)."""
    return _lanzar("incremental", "manual")


@app.post("/resync")
def resync():
    """Dispara un RE-SYNC COMPLETO on-demand (tarda; capta ediciones viejas)."""
    return _lanzar("full", "manual")


@app.post("/productos/sync")
def productos_sync():
    """Dispara sync de productos Tiendanube on-demand."""
    return _lanzar("productos", "manual")


@app.post("/productos/reconcile")
def productos_reconcile():
    """Dispara reconciliación de productos publicados on-demand."""
    return _lanzar("productos_reconcile", "manual")


@app.post("/meta/sync")
def meta_sync():
    """Dispara sync de Meta Ads on-demand."""
    return _lanzar("meta", "manual")
