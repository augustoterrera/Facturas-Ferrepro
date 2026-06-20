"""
Microservicio de sincronización Dux -> Supabase.

Un solo proceso liviano con DOS jobs programados (APScheduler):
  - incremental: cada SYNC_INTERVAL_MIN min. Ventana móvil (últimos N días):
    facturas nuevas + ediciones/anulaciones RECIENTES.
  - full re-sync: cron FULL_RESYNC_CRON (semanal por defecto). Re-lee TODO el
    histórico: capta ediciones/anulaciones VIEJAS (Dux no tiene filtro
    "modificado desde", así que re-leer todo es la única forma confiable).

FastAPI expone /health, /status, /sync (incremental on-demand) y /resync (full).

El sync es SECUENCIAL a propósito: el rate limit de Dux es global por token
(1 request cada 5s). Un único lock evita que dos corridas (de cualquier tipo)
se pisen.

Correr:  uvicorn service:app --host 0.0.0.0 --port 8000
"""
import os
import logging
import threading
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

import sync_facturas as core

core.cargar_env()
logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("dux.service")

_lock = threading.Lock()
_ultima = {"incremental": None, "full": None}   # resumen de la última de cada tipo
scheduler = BackgroundScheduler(timezone="UTC")

JOBS = {
    "incremental": core.run_incremental,
    "full": core.run_full_resync,
}


def _run(tipo: str, trigger: str):
    """Corre un job (incremental|full) garantizando exclusión mutua."""
    if not _lock.acquire(blocking=False):
        log.warning("%s: ya hay una corrida en curso; se omite '%s'", tipo, trigger)
        return {"skipped": True, "motivo": "ya_en_curso"}
    try:
        log.info("arranca %s (%s)", tipo, trigger)
        res = JOBS[tipo]()
        res["tipo"] = tipo
        res["trigger"] = trigger
        _ultima[tipo] = res
        return res
    except Exception as e:                 # nunca dejar caer el scheduler
        log.exception("%s falló (%s)", tipo, trigger)
        _ultima[tipo] = {
            "tipo": tipo, "trigger": trigger, "error_fatal": str(e),
            "fin": datetime.now(timezone.utc).isoformat(),
        }
        return _ultima[tipo]
    finally:
        _lock.release()


def _lanzar(tipo: str, trigger: str):
    """Dispara un job en background (no bloquea el request)."""
    if _lock.locked():
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
    return {"status": "ok", "corriendo": _lock.locked()}


@app.get("/status")
def status():
    """Última corrida de cada tipo (en memoria) + watermark por sucursal."""
    estado = {
        "corriendo": _lock.locked(),
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
