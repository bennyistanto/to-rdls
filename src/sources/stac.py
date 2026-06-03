"""STAC source adapter — walks a STAC Catalog/Collection/Item tree from a
   root URL and writes raw documents to disk, preserving the hierarchy and
   the (item, parent_collection) association needed downstream.

Reads only; no auth required. Designed for static GitHub-Pages-hosted
catalogs such as climate-risk-stac.
"""

from __future__ import annotations

import json
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@dataclass
class StacSourceConfig:
    name: str
    catalog_url: str  # e.g. https://.../catalog.json
    rate_limit: float = 0.2  # seconds between requests (GitHub Pages is cheap)
    timeout: int = 30
    max_retries: int = 3
    user_agent: str = "Mozilla/5.0 (rdls-stac-crawler)"


# ---------------------------------------------------------------------------
# Crawler
# ---------------------------------------------------------------------------

def _fetch_json(url: str, cfg: StacSourceConfig) -> Dict[str, Any]:
    last_err: Optional[Exception] = None
    for attempt in range(cfg.max_retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": cfg.user_agent})
            with urllib.request.urlopen(req, timeout=cfg.timeout) as r:
                return json.load(r)
        except Exception as e:
            last_err = e
            time.sleep(cfg.rate_limit * (attempt + 1))
    raise RuntimeError(f"failed after {cfg.max_retries} retries: {url} ({last_err})")


def iter_collections(raw_root: Path) -> Iterable[Dict[str, Any]]:
    """Yield each cached Collection document. Use for Collection-per-record
    translation modes (e.g. CoCliCo) where Items are rendering variants."""
    collections_root = raw_root / "01_raw" / "collections"
    if not collections_root.exists():
        return
    for path in sorted(collections_root.glob("*.json")):
        try:
            yield json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            continue


def crawl_catalog(
    cfg: StacSourceConfig,
    out_root: Path,
    on_progress: Optional[callable] = None,
    *,
    crawl_items: bool = True,
) -> Dict[str, int]:
    """Walk a STAC catalog tree from `cfg.catalog_url`, writing each
    Collection and Item to disk:

        out_root/01_raw/catalog.json
        out_root/01_raw/collections/{collection_id}.json
        out_root/01_raw/items/{collection_id}/{item_id}.json

    Returns a count dict: {catalogs, collections, items, errors}.
    """
    items_dir = out_root / "01_raw" / "items"
    collections_dir = out_root / "01_raw" / "collections"
    items_dir.mkdir(parents=True, exist_ok=True)
    collections_dir.mkdir(parents=True, exist_ok=True)

    counts = {"catalogs": 0, "collections": 0, "items": 0, "errors": 0}

    visited: set = set()

    def walk(url: str, parent_collection_id: Optional[str]) -> None:
        if url in visited:
            return
        visited.add(url)
        try:
            doc = _fetch_json(url, cfg)
        except Exception as e:
            counts["errors"] += 1
            print(f"    FETCH FAIL ({type(e).__name__}): {url} -- {e}", flush=True)
            return
        time.sleep(cfg.rate_limit)
        typ = doc.get("type")
        # Always stamp the source URL on the document so downstream stages
        # can resolve relative `links[]` to absolute URLs without re-fetching.
        doc["_source_url"] = url
        if typ == "Catalog":
            counts["catalogs"] += 1
            if doc.get("id") == cfg.name or url == cfg.catalog_url:
                (out_root / "01_raw" / "catalog.json").write_text(
                    json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8"
                )
            for l in doc.get("links", []):
                if l.get("rel") in ("child", "item"):
                    walk(urllib.parse.urljoin(url, l["href"]), None)
        elif typ == "Collection":
            counts["collections"] += 1
            coll_id = doc.get("id") or "unknown_collection"
            (collections_dir / f"{coll_id}.json").write_text(
                json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8"
            )
            for l in doc.get("links", []):
                rel = l.get("rel")
                if rel == "child":
                    walk(urllib.parse.urljoin(url, l["href"]), coll_id)
                elif rel == "item" and crawl_items:
                    walk(urllib.parse.urljoin(url, l["href"]), coll_id)
        elif typ == "Feature":  # STAC Item
            counts["items"] += 1
            item_id = doc.get("id") or "unknown_item"
            cid = parent_collection_id or "_loose"
            (items_dir / cid).mkdir(exist_ok=True, parents=True)
            (items_dir / cid / f"{item_id}.json").write_text(
                json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8"
            )
        if on_progress and (counts["items"] % 25 == 0 or counts["items"] in (1, 5)):
            on_progress(counts)

    walk(cfg.catalog_url, None)
    return counts


# ---------------------------------------------------------------------------
# Iter helpers (for the translator stage)
# ---------------------------------------------------------------------------

def iter_items_with_collection(raw_root: Path) -> Iterable[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """Yield (item_doc, parent_collection_doc) for every Item under
    `raw_root/01_raw/items/<collection_id>/*.json`. The parent collection is
    loaded from `raw_root/01_raw/collections/<collection_id>.json`."""
    items_root = raw_root / "01_raw" / "items"
    collections_root = raw_root / "01_raw" / "collections"
    if not items_root.exists():
        return
    for coll_dir in sorted(items_root.iterdir()):
        if not coll_dir.is_dir():
            continue
        coll_id = coll_dir.name
        coll_path = collections_root / f"{coll_id}.json"
        coll_doc = {}
        if coll_path.exists():
            try:
                coll_doc = json.loads(coll_path.read_text(encoding="utf-8"))
            except Exception:
                coll_doc = {}
        for item_path in sorted(coll_dir.glob("*.json")):
            try:
                item_doc = json.loads(item_path.read_text(encoding="utf-8"))
            except Exception:
                continue
            yield item_doc, coll_doc
