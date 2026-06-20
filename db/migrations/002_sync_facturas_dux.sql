-- ============================================================
--  002 — Función cargadora de facturas usada por el microservicio
--  Base: Supabase / PostgreSQL
-- ============================================================
--  Esta es la fuente versionada de public.sync_facturas_dux. Es
--  idempotente (CREATE OR REPLACE), así que correrla de nuevo es seguro.
--
--  Contrato:
--    sync_facturas_dux(p_data, p_id_sucursal, p_nombre_sucursal)
--      p_data            -> factura CRUDA de Dux, o array de facturas,
--                           o {"facturas":[...]}
--      p_id_sucursal     -> id de la sucursal (se inyecta; Dux no lo trae)
--      p_nombre_sucursal -> nombre (crea/actualiza public.sucursales)
--    return jsonb -> {ok, id_sucursal, nombre_sucursal,
--                     facturas_procesadas, items_insertados, pagos_insertados}
--
--  Idempotente a nivel datos: cabecera ON CONFLICT (id_factura) DO UPDATE;
--                             items y pagos con DELETE + INSERT (reemplazo total).
--
--  Depende de objetos creados en 001_schema.sql (correlo ANTES):
--    - tablas public.sucursales / facturas / factura_items / factura_pagos
--    - func   public.parse_erp_timestamptz(text)
--    - trigger facturas_set_fecha_base() sobre facturas
-- ============================================================

CREATE OR REPLACE FUNCTION public.sync_facturas_dux(p_data jsonb, p_id_sucursal integer DEFAULT NULL::integer, p_nombre_sucursal text DEFAULT NULL::text)
 RETURNS jsonb
 LANGUAGE plpgsql
 SECURITY DEFINER
 SET search_path TO 'public'
AS $function$
declare
  v_factura jsonb;
  v_payload jsonb;
  v_facturas_count integer := 0;
  v_items_insertados integer := 0;
  v_pagos_insertados integer := 0;
  v_rows integer := 0;
  v_id_factura bigint;
begin
  /*
    1) Crear o actualizar la sucursal automáticamente.
    Esto evita el error:
    insert or update on table "facturas" violates foreign key constraint "facturas_id_sucursal_fkey"
  */
  if p_id_sucursal is not null then
    insert into public.sucursales (
      id,
      nombre,
      activa
    )
    values (
      p_id_sucursal,
      coalesce(
        nullif(trim(p_nombre_sucursal), ''),
        'Sucursal ' || p_id_sucursal::text
      ),
      true
    )
    on conflict (id) do update
    set
      nombre = coalesce(
        nullif(trim(excluded.nombre), ''),
        public.sucursales.nombre
      ),
      activa = true;
  end if;

  /*
    2) Normalizar payload recibido.
    Acepta:
    - Array directo de facturas
    - Objeto con propiedad facturas
    - Objeto único de factura
  */
  v_payload := case
    when jsonb_typeof(p_data) = 'array' then p_data
    when jsonb_typeof(p_data) = 'object' and jsonb_typeof(p_data->'facturas') = 'array' then p_data->'facturas'
    when jsonb_typeof(p_data) = 'object' and (p_data ? 'id') then jsonb_build_array(p_data)
    else '[]'::jsonb
  end;

  /*
    3) Recorrer facturas
  */
  for v_factura in
    select value
    from jsonb_array_elements(
      case
        when jsonb_typeof(v_payload) = 'array' then v_payload
        else '[]'::jsonb
      end
    )
  loop
    v_id_factura := nullif(v_factura->>'id', '')::bigint;

    if v_id_factura is null then
      continue;
    end if;

    insert into public.facturas (
      id_factura,
      id_empresa,
      id_cliente,
      punto_venta,
      tipo_comprobante,
      letra_comprobante,
      numero_comprobante,
      fecha_comprobante,
      fecha_registro,
      cliente_nombre,
      cliente_cuit,
      monto_exento,
      monto_gravado,
      monto_iva,
      monto_descuento,
      monto_total,
      anulada,
      id_vendedor,
      id_sucursal,
      url_factura,
      raw
    )
    values (
      v_id_factura,
      (v_factura->>'id_empresa')::integer,
      nullif(v_factura->>'id_cliente', '')::integer,
      nullif(v_factura->>'nro_pto_vta', ''),
      nullif(v_factura->>'tipo_comp', ''),
      nullif(v_factura->>'letra_comp', ''),
      nullif(v_factura->>'nro_comp', '')::integer,
      public.parse_erp_timestamptz(v_factura->>'fecha_comp'),
      public.parse_erp_timestamptz(v_factura->>'fecha_registro'),
      nullif(v_factura->>'apellido_razon_soc', ''),
      coalesce(nullif(v_factura->>'cuit', ''), nullif(v_factura->>'nro_doc', '')),
      nullif(v_factura->>'monto_exento', '')::numeric,
      nullif(v_factura->>'monto_gravado', '')::numeric,
      nullif(v_factura->>'monto_iva', '')::numeric,
      nullif(v_factura->>'monto_desc', '')::numeric,
      coalesce(nullif(v_factura->>'total', '')::numeric, 0),
      coalesce((v_factura->>'anulada_boolean')::boolean, false),
      nullif(v_factura->>'id_vendedor', '')::integer,
      p_id_sucursal,
      nullif(v_factura->>'url_factura', ''),
      v_factura
    )
    on conflict (id_factura) do update
    set
      id_empresa         = excluded.id_empresa,
      id_cliente         = excluded.id_cliente,
      punto_venta        = excluded.punto_venta,
      tipo_comprobante   = excluded.tipo_comprobante,
      letra_comprobante  = excluded.letra_comprobante,
      numero_comprobante = excluded.numero_comprobante,
      fecha_comprobante  = excluded.fecha_comprobante,
      fecha_registro     = excluded.fecha_registro,
      cliente_nombre     = excluded.cliente_nombre,
      cliente_cuit       = excluded.cliente_cuit,
      monto_exento       = excluded.monto_exento,
      monto_gravado      = excluded.monto_gravado,
      monto_iva          = excluded.monto_iva,
      monto_descuento    = excluded.monto_descuento,
      monto_total        = excluded.monto_total,
      anulada            = excluded.anulada,
      id_vendedor        = excluded.id_vendedor,
      id_sucursal        = excluded.id_sucursal,
      url_factura        = excluded.url_factura,
      raw                = excluded.raw;

    v_facturas_count := v_facturas_count + 1;

    /*
      4) Reemplazar items de la factura
    */
    delete from public.factura_items
    where id_factura = v_id_factura;

    insert into public.factura_items (
      id_factura,
      nro_linea,
      sku,
      nombre_producto,
      cantidad,
      precio_unitario,
      descuento_pct,
      iva_pct,
      costo_unitario,
      moneda,
      cotizacion_moneda,
      id_lista_precio_venta,
      subtotal_linea,
      iva_linea,
      total_linea,
      costo_total_linea,
      margen_linea,
      margen_pct_linea,
      raw
    )
    select
      v_id_factura,
      row_number() over (),
      coalesce(nullif(d->>'cod_item', ''), 'SIN-SKU'),
      coalesce(nullif(d->>'item', ''), 'SIN NOMBRE'),
      coalesce(nullif(d->>'ctd', '')::numeric, 0),
      coalesce(nullif(d->>'precio_uni', '')::numeric, 0),
      coalesce(nullif(d->>'porc_desc', '')::numeric, 0),
      coalesce(nullif(d->>'porc_iva', '')::numeric, 0),
      nullif(d->>'costo', '')::numeric,
      nullif(d->>'moneda', ''),
      nullif(d->>'cotizacion_moneda', '')::numeric,
      nullif(d->>'id_lista_precio_venta', '')::integer,
      (
        coalesce(nullif(d->>'ctd', '')::numeric, 0)
        * coalesce(nullif(d->>'precio_uni', '')::numeric, 0)
        * (1 - coalesce(nullif(d->>'porc_desc', '')::numeric, 0) / 100)
      ),
      (
        (
          coalesce(nullif(d->>'ctd', '')::numeric, 0)
          * coalesce(nullif(d->>'precio_uni', '')::numeric, 0)
          * (1 - coalesce(nullif(d->>'porc_desc', '')::numeric, 0) / 100)
        ) * (coalesce(nullif(d->>'porc_iva', '')::numeric, 0) / 100)
      ),
      (
        (
          coalesce(nullif(d->>'ctd', '')::numeric, 0)
          * coalesce(nullif(d->>'precio_uni', '')::numeric, 0)
          * (1 - coalesce(nullif(d->>'porc_desc', '')::numeric, 0) / 100)
        )
        +
        (
          (
            coalesce(nullif(d->>'ctd', '')::numeric, 0)
            * coalesce(nullif(d->>'precio_uni', '')::numeric, 0)
            * (1 - coalesce(nullif(d->>'porc_desc', '')::numeric, 0) / 100)
          ) * (coalesce(nullif(d->>'porc_iva', '')::numeric, 0) / 100)
        )
      ),
      (
        coalesce(nullif(d->>'ctd', '')::numeric, 0)
        * coalesce(nullif(d->>'costo', '')::numeric, 0)
      ),
      (
        (
          coalesce(nullif(d->>'ctd', '')::numeric, 0)
          * coalesce(nullif(d->>'precio_uni', '')::numeric, 0)
          * (1 - coalesce(nullif(d->>'porc_desc', '')::numeric, 0) / 100)
        )
        -
        (
          coalesce(nullif(d->>'ctd', '')::numeric, 0)
          * coalesce(nullif(d->>'costo', '')::numeric, 0)
        )
      ),
      case
        when (
          coalesce(nullif(d->>'ctd', '')::numeric, 0)
          * coalesce(nullif(d->>'precio_uni', '')::numeric, 0)
          * (1 - coalesce(nullif(d->>'porc_desc', '')::numeric, 0) / 100)
        ) > 0
        then
          (
            (
              (
                coalesce(nullif(d->>'ctd', '')::numeric, 0)
                * coalesce(nullif(d->>'precio_uni', '')::numeric, 0)
                * (1 - coalesce(nullif(d->>'porc_desc', '')::numeric, 0) / 100)
              )
              -
              (
                coalesce(nullif(d->>'ctd', '')::numeric, 0)
                * coalesce(nullif(d->>'costo', '')::numeric, 0)
              )
            )
            /
            (
              coalesce(nullif(d->>'ctd', '')::numeric, 0)
              * coalesce(nullif(d->>'precio_uni', '')::numeric, 0)
              * (1 - coalesce(nullif(d->>'porc_desc', '')::numeric, 0) / 100)
            )
          )
        else null
      end,
      d
    from jsonb_array_elements(
      case
        when jsonb_typeof(v_factura->'detalles') = 'array' then v_factura->'detalles'
        else '[]'::jsonb
      end
    ) d;

    get diagnostics v_rows = row_count;
    v_items_insertados := v_items_insertados + v_rows;

    /*
      5) Reemplazar pagos de la factura
    */
    delete from public.factura_pagos
    where id_factura = v_id_factura;

    insert into public.factura_pagos (
      id_factura,
      nro_grupo,
      nro_linea,
      punto_venta,
      numero_comprobante,
      caja,
      cajero,
      tipo_pago,
      referencia_pago,
      monto_pago,
      raw
    )
    select
      v_id_factura,
      g.ord as nro_grupo,
      m.ord as nro_linea,
      nullif(g.grupo->>'numero_punto_de_venta', ''),
      nullif(g.grupo->>'numero_comprobante', '')::integer,
      nullif(g.grupo->>'caja', ''),
      nullif(g.grupo->>'personal', ''),
      coalesce(nullif(m.mov->>'tipo_de_valor', ''), 'SIN_TIPO'),
      nullif(m.mov->>'referencia', ''),
      coalesce(nullif(m.mov->>'monto', '')::numeric, 0),
      m.mov
    from jsonb_array_elements(
      case
        when jsonb_typeof(v_factura->'detalles_cobro') = 'array' then v_factura->'detalles_cobro'
        else '[]'::jsonb
      end
    ) with ordinality as g(grupo, ord)
    cross join lateral jsonb_array_elements(
      case
        when jsonb_typeof(g.grupo->'detalles_mov_cobro') = 'array' then g.grupo->'detalles_mov_cobro'
        else '[]'::jsonb
      end
    ) with ordinality as m(mov, ord);

    get diagnostics v_rows = row_count;
    v_pagos_insertados := v_pagos_insertados + v_rows;
  end loop;

  return jsonb_build_object(
    'ok', true,
    'id_sucursal', p_id_sucursal,
    'nombre_sucursal', p_nombre_sucursal,
    'facturas_procesadas', v_facturas_count,
    'items_insertados', v_items_insertados,
    'pagos_insertados', v_pagos_insertados
  );
end;
$function$
;
