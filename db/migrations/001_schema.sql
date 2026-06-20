-- ============================================================
--  001 — Esquema base del proyecto facturas
--  Base: Supabase / PostgreSQL   ·   schema: public
-- ============================================================
--  OJO: la base `public` está COMPARTIDA con otros proyectos
--  (chatbots, CRM, catálogo, documents/pgvector, etc.). Este archivo
--  contiene SOLO los objetos del proyecto facturas.
--
--  Orden (respeta dependencias):
--    extensión → parse_erp_timestamptz → sucursales → facturas
--    → trigger fecha_base → factura_items → factura_pagos → sync_state
--  Después corré 002_sync_facturas_dux.sql.
--
--  Todo es re-ejecutable (if not exists / or replace).
-- ============================================================


-- ------------------------------------------------------------
--  Extensiones
-- ------------------------------------------------------------
-- pg_trgm: requerido por el índice GIN trigram sobre factura_items.nombre_producto.
create extension if not exists pg_trgm;


-- ------------------------------------------------------------
--  Helper: parsea fechas crudas del ERP ("Jun 5, 2026 3:00:00 AM")
--  a timestamptz, interpretándolas en horario de Argentina.
-- ------------------------------------------------------------
create or replace function public.parse_erp_timestamptz(p_text text)
  returns timestamptz
  language sql
  immutable
as $$
  select case
    when p_text is null or btrim(p_text) = '' then null
    else (
      to_timestamp(p_text, 'Mon FMDD, YYYY FMHH12:MI:SS AM')::timestamp
      at time zone 'America/Argentina/Buenos_Aires'
    )
  end
$$;


-- ------------------------------------------------------------
--  sucursales  (referenciada por facturas.id_sucursal)
-- ------------------------------------------------------------
create table if not exists public.sucursales (
  id      integer primary key,
  nombre  text    not null,
  activa  boolean not null default true
);


-- ------------------------------------------------------------
--  facturas  (cabecera)
-- ------------------------------------------------------------
create table if not exists public.facturas (
  id_factura          bigint  primary key,
  id_empresa          integer not null,
  id_cliente          integer,
  punto_venta         text,
  tipo_comprobante    text,
  letra_comprobante   text,
  numero_comprobante  integer,
  fecha_comprobante   timestamptz,
  fecha_registro      timestamptz,
  cliente_nombre      text,
  cliente_cuit        text,
  monto_exento        numeric,
  monto_gravado       numeric,
  monto_iva           numeric,
  monto_descuento     numeric,
  monto_total         numeric not null,
  anulada             boolean default false,
  id_vendedor         integer,
  url_factura         text,
  raw                 jsonb,
  inserted_at         timestamptz default now(),
  fecha_base          date,                                   -- la setea el trigger
  id_sucursal         integer references public.sucursales(id)
);

-- fecha_base = primera fecha disponible (comprobante > registro > inserción).
-- Es la columna sobre la que se filtran los reportes.
create or replace function public.facturas_set_fecha_base()
  returns trigger
  language plpgsql
as $$
begin
  new.fecha_base := coalesce(
    new.fecha_comprobante::date,
    new.fecha_registro::date,
    new.inserted_at::date,
    now()::date
  );
  return new;
end;
$$;

drop trigger if exists trg_facturas_set_fecha_base on public.facturas;
create trigger trg_facturas_set_fecha_base
  before insert or update of fecha_comprobante, fecha_registro, inserted_at
  on public.facturas
  for each row execute function public.facturas_set_fecha_base();

-- Índices de facturas
create index if not exists idx_facturas_fecha                       on public.facturas (fecha_comprobante);
create index if not exists idx_facturas_empresa_fecha               on public.facturas (id_empresa, fecha_comprobante);
create index if not exists idx_facturas_cliente                     on public.facturas (id_cliente);
create index if not exists idx_facturas_fecha_base                  on public.facturas (fecha_base);
create index if not exists idx_facturas_empresa_fecha_base          on public.facturas (id_empresa, fecha_base);
create index if not exists idx_facturas_vendedor                    on public.facturas (id_vendedor);
create index if not exists idx_facturas_sucursal                    on public.facturas (id_sucursal);
create index if not exists idx_facturas_empresa_sucursal_fecha_base on public.facturas (id_empresa, id_sucursal, fecha_base);
-- parciales (solo no anuladas) para los reportes habituales:
create index if not exists idx_facturas_empresa_fecha_base_anulada     on public.facturas (id_empresa, fecha_base) where (anulada = false);
create index if not exists idx_facturas_empresa_fecha_base_no_anuladas on public.facturas (id_empresa, fecha_base) where (coalesce(anulada, false) = false);
create index if not exists idx_facturas_fecha_base_no_anuladas         on public.facturas (fecha_base) where (coalesce(anulada, false) = false);
create index if not exists idx_facturas_sucursal_fecha_base            on public.facturas (id_sucursal, fecha_base) where (coalesce(anulada, false) = false);
-- nota: idx_facturas_empresa_fecha_base_anulada e _no_anuladas son casi
-- equivalentes; quedan replicadas tal cual están en la base. Se pueden podar.


-- ------------------------------------------------------------
--  factura_items  (líneas; reemplazo total por factura)
-- ------------------------------------------------------------
create table if not exists public.factura_items (
  id_factura            bigint  not null references public.facturas(id_factura) on delete cascade,
  nro_linea             integer not null,
  sku                   text    not null,
  nombre_producto       text    not null,
  cantidad              numeric not null,
  precio_unitario       numeric not null,
  descuento_pct         numeric default 0,
  iva_pct               numeric default 0,
  costo_unitario        numeric,
  moneda                text,
  cotizacion_moneda     numeric,
  id_lista_precio_venta integer,
  subtotal_linea        numeric,
  iva_linea             numeric,
  total_linea           numeric,
  costo_total_linea     numeric,
  margen_linea          numeric,
  margen_pct_linea      numeric,
  raw                   jsonb,
  primary key (id_factura, nro_linea)
);

create index if not exists idx_factura_items_id_factura        on public.factura_items (id_factura);
create index if not exists idx_factura_items_sku               on public.factura_items (sku);
create index if not exists idx_factura_items_sku_id_factura    on public.factura_items (sku, id_factura);
create index if not exists idx_factura_items_nombre_producto   on public.factura_items (nombre_producto);
create index if not exists idx_factura_items_nombre_trgm       on public.factura_items using gin (nombre_producto gin_trgm_ops);
-- nota: en la base hay además idx_items_sku / idx_items_id_factura /
-- idx_items_nombre_producto, duplicados de los de arriba. No se replican acá.


-- ------------------------------------------------------------
--  factura_pagos  (cobros; reemplazo total por factura)
-- ------------------------------------------------------------
create table if not exists public.factura_pagos (
  id_factura         bigint  not null references public.facturas(id_factura) on delete cascade,
  nro_grupo          integer not null,
  nro_linea          integer not null,
  punto_venta        text,
  numero_comprobante integer,
  caja               text,
  cajero             text,
  tipo_pago          text    not null,
  referencia_pago    text,
  monto_pago         numeric not null,
  raw                jsonb,
  primary key (id_factura, nro_grupo, nro_linea)
);

create index if not exists idx_factura_pagos_id_factura           on public.factura_pagos (id_factura);
create index if not exists idx_factura_pagos_tipo_pago            on public.factura_pagos (tipo_pago);
create index if not exists idx_factura_pagos_id_factura_tipo_pago on public.factura_pagos (id_factura, tipo_pago);


-- ------------------------------------------------------------
--  sync_state  (watermark del sync incremental, por empresa+sucursal)
--    Lo escribe run_incremental() en sync_facturas.py y lo lee /status.
--    La próxima corrida arranca en ultima_fecha_hasta - DUX_VENTANA_DIAS.
-- ------------------------------------------------------------
create table if not exists public.sync_state (
  id_empresa          integer not null,
  id_sucursal         integer not null,
  sucursal            text,
  ultima_fecha_hasta  date,
  ultima_corrida      timestamptz,
  facturas_ok         integer default 0,
  facturas_error      integer default 0,
  ultimo_error        text,
  primary key (id_empresa, id_sucursal)
);
