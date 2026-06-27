# Base de datos

Base: **Supabase / PostgreSQL**. Schema: `public`.

Acá vive la *receta* para construir la estructura, **no los datos**. Los datos
viven en Supabase y se regeneran sincronizando desde Dux con el microservicio.
Nunca se versionan dumps ni `facturas.json`.

> ⚠️ La base `public` está **compartida con otros proyectos** (chatbots, CRM,
> catálogo, `documents`/pgvector, n8n, etc.). Estas migraciones contienen **solo
> los objetos del proyecto facturas** — el resto pertenece a otros sistemas y no
> debe versionarse acá.

## Migraciones

| Archivo | Qué crea |
|---------|----------|
| [`migrations/001_schema.sql`](migrations/001_schema.sql) | Extensión `pg_trgm`, función `parse_erp_timestamptz`, tablas `sucursales`, `facturas`, `factura_items`, `factura_pagos`, `sync_state`, el trigger `facturas_set_fecha_base` y todos los índices. |
| [`migrations/002_sync_facturas_dux.sql`](migrations/002_sync_facturas_dux.sql) | La función cargadora `sync_facturas_dux(...)` que usa el microservicio. |
| [`migrations/003_productos_meta.sql`](migrations/003_productos_meta.sql) | Productos Tiendanube/RAG, reconciliación y métricas Meta Ads. |

## Objetos del proyecto facturas

```
parse_erp_timestamptz(text)        función helper (fechas del ERP)
sucursales                         tabla   (FK ← facturas.id_sucursal)
facturas                           tabla   (cabecera) + trigger fecha_base
factura_items                      tabla   (líneas;  FK → facturas)
factura_pagos                      tabla   (cobros;  FK → facturas)
sync_state                         tabla   (watermark del sync incremental)
sync_facturas_dux(...)             función cargadora (la llama el micro)
ferrepro_productos                 tabla   (productos Tiendanube + embedding)
ferrepro_variantes                 tabla   (variantes Tiendanube)
ferrepro_sync_state                tabla   (cursor incremental productos)
ferrepro_upsert_producto(...)      función cargadora de productos
ferrepro_reconciliar(...)          función de reconciliación de productos
meta_insights_daily                tabla   (insights diarios Meta Ads)
```

## Reconstruir la base desde cero

En orden, sobre una base limpia:

```bash
psql "$SUPABASE_DB_URL" -f db/migrations/001_schema.sql
psql "$SUPABASE_DB_URL" -f db/migrations/002_sync_facturas_dux.sql
psql "$SUPABASE_DB_URL" -f db/migrations/003_productos_meta.sql
```

O desde el **SQL Editor** del dashboard de Supabase: pegá y ejecutá primero
`001_schema.sql` y después `002_sync_facturas_dux.sql`.
Si también reconstruís productos/Meta, ejecutá luego `003_productos_meta.sql`.

Ambos archivos son re-ejecutables (`create ... if not exists` / `or replace`),
así que correrlos de nuevo sobre una base existente no rompe nada.

## Notas

- `001_schema.sql` deja comentadas algunas **redundancias** detectadas en la base
  real (índices duplicados en `factura_items`/`factura_pagos` y dos índices
  parciales casi equivalentes en `facturas`). Se replicó la versión limpia; ver
  los comentarios en el archivo si querés alinear 1:1 con producción.
- Si en el futuro agregás objetos del proyecto facturas, sumá un
  `003_*.sql`, `004_*.sql`, etc. con el cambio incremental.
