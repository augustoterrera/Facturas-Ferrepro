# Sync de facturas Dux → Supabase

Microservicio que sincroniza las facturas de **Dux Software** hacia **Supabase**,
por sucursal, de forma incremental y desatendida.

## Arquitectura

Un solo proceso (`service.py`):

- **APScheduler** corre un sync incremental cada `SYNC_INTERVAL_MIN` minutos.
- **FastAPI** expone `/health`, `/status` y `/sync`.
- `sync_facturas.py` es el core reutilizable (también se usa como CLI).

```
Dux API ──(httpx, 1 req/5s)──> [lotes Dux crudo] ──> RPC sync_facturas_dux() ──> Supabase
                                                              │
                                                      tabla sync_state (watermark)
```

> El mapeo Dux→tablas lo hace la función `sync_facturas_dux` **en el SQL**
> (recibe el JSON crudo + la sucursal). El microservicio solo trae los datos
> y los manda en lotes; no transforma nada del lado de Python.

> **Por qué un solo worker secuencial:** el rate limit de Dux es **1 request
> cada 5 segundos, global por token**. Paralelizar no sirve (todos los workers
> comparten el cupo). "Escalable" acá = sync **incremental** (no re-bajar todo)
> + resiliencia (reintentos, idempotencia) + operación desatendida.

## Dos niveles de sincronización

| Job | Cadencia | Qué cubre |
|---|---|---|
| **Incremental** | cada `SYNC_INTERVAL_MIN` (30 min) | facturas nuevas + ediciones/anulaciones **recientes** (ventana móvil de `DUX_VENTANA_DIAS`) |
| **Re-sync completo** | cron `FULL_RESYNC_CRON` (lunes 06:00 UTC) | re-lee **todo** el histórico → capta ediciones/anulaciones **viejas** |

> **Por qué hace falta el re-sync completo:** la API de Dux no tiene filtro
> "modificado desde" ni campo de última modificación. Si te editan/anulan una
> factura vieja, su `fecha_comp` no cambia y la ventana móvil no la vuelve a
> mirar. Re-leer todo (idempotente) es la única forma confiable de detectarlo.
> El **borrado físico** no se cubre (la API deja de devolverla y el sync nunca
> borra) — eso requeriría un paso de reconciliación aparte.

## Cómo funciona el sync incremental

Para cada `(empresa, sucursal)` se guarda un *watermark* en `sync_state`:

- **Primera corrida** (sin watermark): trae desde `DUX_BACKFILL_DESDE` hasta hoy.
- **Corridas siguientes**: trae desde `watermark − DUX_VENTANA_DIAS` hasta hoy,
  así re-chequea lo reciente y captura **anulaciones/cambios**.
- El upsert es sobre `id_factura` → **idempotente**: re-procesar no duplica.

## Setup

1. **Base de datos** — en el SQL Editor de Supabase corré, en orden,
   [`db/migrations/001_schema.sql`](db/migrations/001_schema.sql) (esquema completo:
   tablas, índices, funciones, trigger) y luego
   [`db/migrations/002_sync_facturas_dux.sql`](db/migrations/002_sync_facturas_dux.sql)
   (función cargadora que usa el micro). Ambos son re-ejecutables.
   > Inventario de objetos y cómo reconstruir la base están en [`db/README.md`](db/README.md).

2. **Credenciales** — copiá `.env.example` a `.env` y completá:
   ```bash
   cp .env.example .env
   ```
   `DUX_TOKEN`, `SUPABASE_URL`, `SUPABASE_SERVICE_KEY` son obligatorias.

## Correr

### Con Docker (recomendado para el server)
```bash
docker compose up -d --build
docker compose logs -f          # ver el sync en vivo
```

### Local (sin Docker)
```bash
pip install -r requirements.txt
uvicorn service:app --host 0.0.0.0 --port 8000
```

### CLI (sin levantar el servicio)
```bash
# Validar el mapeo contra un JSON guardado (no toca la base ni la red):
python sync_facturas.py --dry-run --file facturas.json

# Backfill manual de un rango (la primera carga histórica):
python sync_facturas.py --desde 2024-01-01 --hasta 2026-06-30

# Una corrida incremental puntual:
python sync_facturas.py --incremental
```

## Endpoints

| Método | Ruta | Qué hace |
|---|---|---|
| GET | `/health` | Liveness (lo usa el healthcheck del contenedor). |
| GET | `/status` | Última corrida (incremental y full) + watermark por sucursal. |
| POST | `/sync` | Dispara un sync **incremental** on-demand (no bloquea). |
| POST | `/resync` | Dispara un **re-sync completo** on-demand (tarda; capta ediciones viejas). |

```bash
curl localhost:8000/health
curl localhost:8000/status
curl -X POST localhost:8000/sync
```

## Configuración (variables de entorno)

| Variable | Default | Descripción |
|---|---|---|
| `DUX_TOKEN` | — | Token de la API de Dux (**obligatoria**). |
| `DUX_BASE` | WSERP de Dux | Base URL de la API. |
| `DUX_ID_EMPRESA` | `3526` | Empresa(s); varias separadas por coma. |
| `DUX_RATE_DELAY` | `7` | Segundos entre requests (Dux exige ≥5). |
| `DUX_BATCH_SIZE` | `50` | Facturas por RPC (`sync_facturas_dux` acepta lotes). |
| `SYNC_RPC` | `sync_facturas_dux` | Nombre del RPC de carga en tu base. |
| `SUPABASE_URL` | — | URL del proyecto (**obligatoria**). |
| `SUPABASE_SERVICE_KEY` | — | service_role key (**obligatoria**). |
| `DUX_VENTANA_DIAS` | `35` | Re-chequeo de los últimos N días por corrida. |
| `DUX_BACKFILL_DESDE` | `2024-01-01` | Desde cuándo trae sin watermark. |
| `SYNC_INTERVAL_MIN` | `30` | Cadencia del incremental (minutos). |
| `FULL_RESYNC_CRON` | `0 6 * * 1` | Cron (UTC) del re-sync completo. Vacío = desactivado. |
| `SYNC_ON_START` | `false` | `true` = sync al arrancar. |
| `LOG_LEVEL` | `INFO` | Nivel de logging. |

## Operación / notas

- **Carga histórica inicial:** hacé un backfill manual una vez
  (`--desde ... --hasta ...`); después el servicio mantiene la ventana móvil.
  Un full histórico son ~5.846 facturas ÷ 50 × 7s ≈ **14 min por sucursal**.
- **Anulaciones viejas:** la ventana móvil capta cambios dentro de los últimos
  `DUX_VENTANA_DIAS`. Para anulaciones de comprobantes más viejos, corré un
  backfill del período cada tanto.
- **Secretos:** `.env` y `credenciales.md` están en `.gitignore` — no los subas.
