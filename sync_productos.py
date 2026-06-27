"""
Sincroniza productos de Tiendanube hacia Supabase para Ferrepro.

Reemplaza los workflows n8n:
  - RAG - FerrePro
  - Ferrepro · Reconciliación
"""
import argparse
import html
import json
import logging
import os
import re
import sys
import time
import urllib.parse
from datetime import datetime, timedelta, timezone

import sync_facturas as core

log = logging.getLogger("ferrepro.productos")

FIELDS = (
    "id,name,description,brand,handle,canonical_url,published,is_kit,"
    "has_stock,tags,categories,images,variants,created_at,updated_at"
)


def _env_bool(name, default=False):
    return os.environ.get(name, str(default)).strip().lower() in ("1", "true", "yes", "si")


def _tiendanube_headers():
    token = os.environ["TIENDANUBE_ACCESS_TOKEN"]
    return {
        "Authentication": f"bearer {token}",
        "User-Agent": os.environ.get("TIENDANUBE_USER_AGENT", "facturas-ferrepro"),
    }


def _tn_base():
    version = os.environ.get("TIENDANUBE_API_VERSION", "2025-03")
    store_id = os.environ["TIENDANUBE_STORE_ID"]
    return f"https://api.tiendanube.com/{version}/{store_id}"


def _link_next(link):
    for part in (link or "").split(","):
        if 'rel="next"' in part:
            return part.split(";", 1)[0].strip().strip("<>")
    return None


def _get(client, url, params=None):
    espera = float(os.environ.get("TIENDANUBE_RATE_DELAY", "0.6"))
    r = client.get(url, params=params)
    r.raise_for_status()
    time.sleep(espera)
    return r


def _paginar_productos(client, url):
    while url:
        r = _get(client, url)
        data = r.json()
        if isinstance(data, list):
            yield from data
        url = _link_next(r.headers.get("link"))


def _es(v):
    if isinstance(v, dict):
        return v.get("es") or next(iter(v.values()), None)
    return v


def _txt(value):
    return re.sub(r"\s+", " ", re.sub(r"<[^>]*>", " ", html.unescape(str(value or "")))).strip()


def _lista(value):
    return value if isinstance(value, list) else []


def _blank_none(value):
    return None if value == "" else value


def _tags(tags):
    if isinstance(tags, list):
        return [str(_es(t) or "").strip() for t in tags if str(_es(t) or "").strip()]
    if isinstance(tags, str):
        return [t.strip() for t in tags.split(",") if t.strip()]
    value = _es(tags) if isinstance(tags, dict) else None
    return _tags(value) if value is not None else []


def transformar(p):
    name = _es(p.get("name"))
    description = _txt(_es(p.get("description")))
    categories = _lista(p.get("categories"))
    images = _lista(p.get("images"))
    raw_variants = _lista(p.get("variants"))
    cats = [c for c in (_es(c.get("name")) for c in categories if isinstance(c, dict)) if c]
    cat_ids = [c.get("id") for c in categories if isinstance(c, dict) and c.get("id") is not None]
    tags = _tags(p.get("tags"))
    variants = [
        {
            "id": v.get("id"),
            "sku": v.get("sku") or None,
            "barcode": v.get("barcode") or None,
            "values": [_es(x) for x in _lista(v.get("values"))],
            "price": _blank_none(v.get("price")),
            "promotional_price": _blank_none(v.get("promotional_price")),
            "stock": _blank_none(v.get("stock")),
            "stock_management": v.get("stock_management"),
            "weight": _blank_none(v.get("weight")),
            "position": _blank_none(v.get("position")),
        }
        for v in raw_variants if isinstance(v, dict)
    ]
    skus = [v["sku"] for v in variants if v.get("sku")]
    search_text = " . ".join(
        str(x) for x in (name, p.get("brand"), description, " ".join(tags), " ".join(cats), " ".join(skus))
        if x
    )[:8000]
    return {
        "id": p.get("id"),
        "name": name,
        "description": description,
        "brand": p.get("brand") or None,
        "handle": _es(p.get("handle")),
        "canonical_url": p.get("canonical_url") or None,
        "published": p.get("published"),
        "is_kit": p.get("is_kit"),
        "has_stock": p.get("has_stock"),
        "tags": tags,
        "category_ids": cat_ids,
        "category_names": cats,
        "primary_image": (images[0] or {}).get("src") if images and isinstance(images[0], dict) else None,
        "tn_created_at": p.get("created_at"),
        "tn_updated_at": p.get("updated_at"),
        "raw": p,
        "search_text": search_text,
        "variants": variants,
    }


def _embedding(client, text):
    key = os.environ["OPENAI_API_KEY"]
    r = client.post(
        "https://api.openai.com/v1/embeddings",
        headers={"Authorization": f"Bearer {key}"},
        json={"model": os.environ.get("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small"), "input": text},
        timeout=60.0,
    )
    r.raise_for_status()
    return r.json()["data"][0]["embedding"]


def _leer_cursor(sb):
    table = os.environ.get("PRODUCTOS_SYNC_STATE_TABLE", "ferrepro_sync_state")
    state_id = int(os.environ.get("PRODUCTOS_SYNC_STATE_ID", "1"))
    r = sb.table(table).select("last_run_at").eq("id", state_id).execute()
    return r.data[0].get("last_run_at") if r.data else None


def _avanzar_cursor(sb, run_started_at):
    table = os.environ.get("PRODUCTOS_SYNC_STATE_TABLE", "ferrepro_sync_state")
    state_id = int(os.environ.get("PRODUCTOS_SYNC_STATE_ID", "1"))
    sb.table(table).upsert({
        "id": state_id,
        "last_run_at": run_started_at,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }, on_conflict="id").execute()


def _url_inicial(last_run_at=None):
    params = {"per_page": "200", "fields": FIELDS}
    if _env_bool("TIENDANUBE_ONLY_PUBLISHED", True):
        params["published"] = "true"
    if last_run_at:
        overlap = int(os.environ.get("TIENDANUBE_OVERLAP_MINUTES", "5"))
        since = datetime.fromisoformat(last_run_at.replace("Z", "+00:00")) - timedelta(minutes=overlap)
        params["updated_at_min"] = since.isoformat()
    return f"{_tn_base()}/products?{urllib.parse.urlencode(params)}"


def run_productos(sb=None):
    """Sync incremental de productos Tiendanube -> RPC ferrepro_upsert_producto."""
    import httpx

    run_started_at = datetime.now(timezone.utc).isoformat()
    sb = sb or core.get_supabase()
    resumen = {"inicio": run_started_at, "ok": 0, "error": 0, "errores": []}
    rpc = os.environ.get("PRODUCTOS_UPSERT_RPC", "ferrepro_upsert_producto")
    if not os.environ.get("OPENAI_API_KEY", "").strip():
        raise RuntimeError("falta OPENAI_API_KEY para generar embeddings de productos")
    with httpx.Client(headers=_tiendanube_headers(), timeout=60.0) as tn, httpx.Client(timeout=60.0) as ai:
        for raw in _paginar_productos(tn, _url_inicial(_leer_cursor(sb))):
            payload = transformar(raw)
            try:
                emb = _embedding(ai, payload["search_text"])
                sb.rpc(rpc, {
                    "p_payload": payload,
                    "p_embedding": json.dumps(emb) if emb is not None else None,
                }).execute()
                resumen["ok"] += 1
            except Exception as e:
                resumen["error"] += 1
                resumen["errores"].append(f"{payload.get('id')}: {e}")
                log.exception("producto %s falló", payload.get("id"))
    if resumen["error"] == 0:
        _avanzar_cursor(sb, run_started_at)
    resumen["fin"] = datetime.now(timezone.utc).isoformat()
    return resumen


def run_reconcile(sb=None):
    """Borra productos que ya no figuran publicados en Tiendanube."""
    import httpx

    sb = sb or core.get_supabase()
    url = f"{_tn_base()}/products?per_page=200&fields=id&published=true"
    ids = []
    with httpx.Client(headers=_tiendanube_headers(), timeout=60.0) as client:
        for p in _paginar_productos(client, url):
            if p.get("id") is not None:
                ids.append(p["id"])
    deleted = sb.rpc(os.environ.get("PRODUCTOS_RECONCILE_RPC", "ferrepro_reconciliar"), {"p_ids": ids}).execute().data
    return {
        "inicio": datetime.now(timezone.utc).isoformat(),
        "ok": len(ids),
        "error": 0,
        "publicados": len(ids),
        "borrados": deleted,
        "fin": datetime.now(timezone.utc).isoformat(),
    }


def _self_check():
    p = transformar({
        "id": 1,
        "name": {"es": "Taladro"},
        "description": {"es": "<p>Potente&nbsp;&amp; liviano</p>"},
        "tags": "herramienta, oferta",
        "categories": [{"id": 2, "name": {"es": "Ferreteria"}}],
        "images": [{"src": "https://x/img.jpg"}],
        "variants": [{"id": 9, "sku": "T-1", "price": "10", "stock": 3}],
    })
    assert p["name"] == "Taladro"
    assert p["description"] == "Potente & liviano"
    assert p["tags"] == ["herramienta", "oferta"]
    assert p["variants"][0]["sku"] == "T-1"


def main():
    core.cargar_env()
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
    ap = argparse.ArgumentParser(description="Sync productos Ferrepro")
    ap.add_argument("--reconcile", action="store_true")
    ap.add_argument("--self-check", action="store_true")
    args = ap.parse_args()
    if args.self_check:
        _self_check()
        print("ok")
        return
    res = run_reconcile() if args.reconcile else run_productos()
    print(json.dumps(res, ensure_ascii=False, indent=2))
    sys.exit(1 if res.get("error") else 0)


if __name__ == "__main__":
    main()
