"""
Sincroniza métricas de Meta Ads hacia public.meta_insights_daily.

Reemplaza los workflows n8n:
  - Campaign y Account - MetaAds FerrePro
  - Ad y Adset- MetaAds FerrePro
"""
import argparse
import calendar
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import sync_facturas as core

log = logging.getLogger("ferrepro.meta")

FIELDS = {
    "account": "date_start,impressions,clicks,spend,reach,cpc,cpm,ctr,frequency,actions,cost_per_action_type",
    "campaign": "campaign_id,campaign_name,date_start,date_stop,impressions,clicks,spend,reach,cpc,cpm,ctr,cpp,frequency,actions,cost_per_action_type",
    "adset": "adset_id,adset_name,campaign_id,campaign_name,date_start,date_stop,impressions,clicks,spend,reach,actions,cost_per_action_type,cpc,cpm,ctr,cpp,frequency,action_values",
    "ad": "ad_id,ad_name,campaign_id,campaign_name,adset_id,adset_name,date_start,date_stop,impressions,clicks,spend,reach,frequency,cpc,cpm,ctr,cpp,actions,action_values,cost_per_action_type",
}


def _num(v):
    if v in (None, ""):
        return None
    try:
        return float(str(v).replace(",", "."))
    except ValueError:
        return None


def _int(v):
    if v in (None, ""):
        return None
    try:
        return int(str(v))
    except ValueError:
        return None


def _date(v):
    return str(v)[:10] if v else None


def _add_months(iso_date, delta):
    y, m, d = map(int, iso_date.split("-"))
    m += delta
    y += (m - 1) // 12
    m = (m - 1) % 12 + 1
    return f"{y:04d}-{m:02d}-{min(d, calendar.monthrange(y, m)[1]):02d}"


def rango_default():
    tz = ZoneInfo(os.environ.get("META_TIMEZONE", "America/Argentina/Tucuman"))
    hasta = datetime.now(tz).date().isoformat()
    desde = _add_months(hasta, -int(os.environ.get("META_BACKFILL_MONTHS", "37")))
    return desde, hasta


def validar_rango(desde, hasta):
    minimo, hoy = rango_default()
    if desde < minimo:
        raise ValueError(
            f"Meta permite hasta {os.environ.get('META_BACKFILL_MONTHS', '37')} meses: "
            f"desde mínimo {minimo}. Pediste {desde}..{hasta}."
        )
    if hasta > hoy:
        raise ValueError(f"hasta no puede ser futuro: máximo {hoy}. Pediste {hasta}.")


def _meta_get(client, url, params=None):
    espera = float(os.environ.get("META_RATE_DELAY", "0.5"))
    for intento in range(1, 6):
        r = client.get(url, params=params)
        if r.status_code == 429 or r.status_code >= 500:
            pausa = min(espera * (2 ** (intento - 1)), 60.0)
            log.warning("Meta %s; espero %.1fs (%s/5)", r.status_code, pausa, intento)
            time.sleep(pausa)
            continue
        r.raise_for_status()
        time.sleep(espera)
        return r.json()
    r.raise_for_status()


def fetch_level(client, level, desde, hasta):
    version = os.environ.get("META_API_VERSION", "v24.0")
    account_id = os.environ["META_AD_ACCOUNT_ID"]
    url = f"https://graph.facebook.com/{version}/act_{account_id}/insights"
    params = {
        "fields": FIELDS[level],
        "level": level,
        "time_increment": "1",
        "time_range": json.dumps({"since": desde, "until": hasta}),
        "limit": os.environ.get("META_INSIGHTS_LIMIT", "500"),
    }
    while url:
        data = _meta_get(client, url, params=params)
        yield from data.get("data") or []
        url = (data.get("paging") or {}).get("next")
        params = None


def transformar(level, r):
    if level == "account":
        entity_id, entity_name = "account", "Account"
    else:
        entity_id = str(r.get(f"{level}_id"))
        entity_name = r.get(f"{level}_name")
    return {
        "level": level,
        "entity_id": entity_id,
        "entity_name": entity_name,
        "date_start": _date(r.get("date_start")),
        "date_stop": _date(r.get("date_stop") or r.get("date_start")),
        "impressions": _int(r.get("impressions")),
        "clicks": _int(r.get("clicks")),
        "reach": _int(r.get("reach")),
        "spend": _num(r.get("spend")),
        "cpc": _num(r.get("cpc")),
        "cpm": _num(r.get("cpm")),
        "ctr": _num(r.get("ctr")),
        "frequency": _num(r.get("frequency")),
        "cpp": _num(r.get("cpp")),
        "actions": r.get("actions") if isinstance(r.get("actions"), list) else [],
        "cost_per_action_type": r.get("cost_per_action_type") if isinstance(r.get("cost_per_action_type"), list) else [],
        "raw": r,
    }


def _chunks(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def run_meta(desde=None, hasta=None, sb=None):
    """Sync Meta Ads daily insights. Idempotente por (level, entity_id, date_start)."""
    import httpx

    desde, hasta = (desde, hasta) if desde and hasta else rango_default()
    validar_rango(desde, hasta)
    sb = sb or core.get_supabase()
    table = os.environ.get("META_INSIGHTS_TABLE", "meta_insights_daily")
    resumen = {"inicio": datetime.now(timezone.utc).isoformat(), "desde": desde, "hasta": hasta, "ok": 0, "error": 0, "niveles": []}
    with httpx.Client(headers={"Authorization": f"Bearer {os.environ['META_ACCESS_TOKEN']}"}, timeout=90.0) as client:
        for level in ("account", "campaign", "adset", "ad"):
            rows, error = [], None
            try:
                rows = [transformar(level, r) for r in fetch_level(client, level, desde, hasta)]
                for lote in _chunks(rows, 500):
                    sb.table(table).upsert(lote, on_conflict="level,entity_id,date_start").execute()
                resumen["ok"] += len(rows)
            except Exception as e:
                error = str(e)
                resumen["error"] += 1
                log.exception("Meta level %s falló", level)
            resumen["niveles"].append({"level": level, "ok": len(rows), "error": error})
    resumen["fin"] = datetime.now(timezone.utc).isoformat()
    return resumen


def _self_check():
    assert _add_months("2024-03-31", -1) == "2024-02-29"
    try:
        validar_rango("2021-01-01", "2026-06-27")
        raise AssertionError("rango viejo no falló")
    except ValueError:
        pass
    row = transformar("campaign", {"campaign_id": 12, "campaign_name": "Camp", "date_start": "2026-01-01", "spend": "10.5"})
    assert row["entity_id"] == "12"
    assert row["spend"] == 10.5


def main():
    core.cargar_env()
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    ap = argparse.ArgumentParser(description="Sync Meta Ads Ferrepro")
    ap.add_argument("--desde")
    ap.add_argument("--hasta")
    ap.add_argument("--self-check", action="store_true")
    args = ap.parse_args()
    if args.self_check:
        _self_check()
        print("ok")
        return
    try:
        res = run_meta(args.desde, args.hasta)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        sys.exit(2)
    print(json.dumps(res, ensure_ascii=False, indent=2))
    sys.exit(1 if res.get("error") else 0)


if __name__ == "__main__":
    main()
