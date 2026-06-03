r"""
rdls_stac_pipeline_v10.py
=========================

End-to-end STAC -> RDLS v1.0 ingestion pipeline.

Stages (per catalog):
    01_raw/                      crawled STAC documents
        catalog.json
        collections/{coll_id}.json
        items/{coll_id}/{item_id}.json
    v1.0/02_translated/          one base RDLS v1.0 record per Item
    v1.0/03_validated/           3-layer audit (audit.py)
      high/ medium/ low/ invalid/
    v1.0/reports/                validation_summary.csv

No LLM, no extraction cascade — STAC properties carry RDLS concepts directly
(risk data type / subcategory / scenarios / analysis type / etc.), so each
Item produces exactly one base RDLS record via a direct field-mapped builder.

Usage:
    set PYTHONPATH=C:\Users\benny\OneDrive\Documents\Github\to-rdls
    C:/Users/benny/miniforge3/envs/to-rdls/python.exe scripts/rdls_stac_pipeline_v10.py
"""

from __future__ import annotations

import csv
import json
import sys
import time
from collections import Counter
from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR))

OUTPUT_DIR = PROJECT_DIR / "output" / "stac"
SCHEMA_PATH = PROJECT_DIR.parent / "rdl-standard" / "schema" / "rdls_schema.json"
CODELISTS_DIR = PROJECT_DIR.parent / "rdl-standard" / "schema" / "codelists"

# --- Catalogs to run ---
# mode: "item_per_record" (default) - each STAC Item becomes one RDLS record
#       "collection_per_record"     - each STAC Collection becomes one record
#                                     (use for catalogs where Items are
#                                     rendering variants of the same dataset)
CATALOGS = [
    {
        "name":        "climate-risk-stac",
        "catalog_url": "https://climate-risk-data.github.io/climate-risk-stac/stac/catalog.json",
        "org_slug":    "crstac",
        "rate_limit":  0.2,
        "mode":        "item_per_record",
    },
    {
        "name":        "coclico",
        "catalog_url": "https://storage.googleapis.com/coclico-data-public/coclico/coclico-stac/catalog.json",
        "org_slug":    "coclico",
        "rate_limit":  0.2,
        "mode":        "collection_per_record",
    },
]

# --- Imports ---
from src.utils import load_json, write_json
from src.sources.stac import StacSourceConfig, crawl_catalog, iter_items_with_collection, iter_collections
from src.translate_stac_v10 import build_record_stac_v10
from src.translate_stac_coclico_v10 import build_record_collection_v10
from src.audit import (
    CodelistRegistry, ValidationResult,
    validate_layer1_schema, validate_layer2_codelists, validate_layer3_semantic,
)


def quiet_validate(data: dict, schema: dict, registry: CodelistRegistry) -> ValidationResult:
    result = ValidationResult()
    validate_layer1_schema(data, schema, result)
    validate_layer2_codelists(data, registry, result)
    validate_layer3_semantic(data, registry, result)
    return result


def tier_of(result: ValidationResult) -> str:
    if result.errors:
        return "invalid"
    w = len(result.warnings)
    if w == 0:
        return "high"
    if w <= 3:
        return "medium"
    return "low"


def progress(label: str, counts: dict):
    print(f"  [{label}] catalogs={counts['catalogs']} collections={counts['collections']} items={counts['items']}", flush=True)


def main() -> int:
    print("=" * 60)
    print("  STEP 0: load schema + codelists")
    print("=" * 60)
    if not SCHEMA_PATH.exists():
        print(f"ERROR: schema not found: {SCHEMA_PATH}", file=sys.stderr)
        return 1
    schema = load_json(str(SCHEMA_PATH))
    registry = CodelistRegistry(CODELISTS_DIR)

    grand_totals = Counter()

    for cfg_entry in CATALOGS:
        name = cfg_entry["name"]
        mode = cfg_entry.get("mode", "item_per_record")
        cfg = StacSourceConfig(
            name=name,
            catalog_url=cfg_entry["catalog_url"],
            rate_limit=cfg_entry.get("rate_limit", 0.2),
        )
        cat_root = OUTPUT_DIR / name
        cat_root.mkdir(parents=True, exist_ok=True)

        print("\n" + "=" * 60)
        print(f"  CATALOG: {name}  (mode={mode})")
        print("=" * 60)

        # ---- Stage 1: crawl ----
        print("\n[1] crawl STAC tree -> 01_raw")
        crawl_items_flag = (mode == "item_per_record")
        existing_check_dir = (
            cat_root / "01_raw" / "items"
            if crawl_items_flag
            else cat_root / "01_raw" / "collections"
        )
        if existing_check_dir.exists() and any(existing_check_dir.rglob("*.json")):
            print(f"  skipping crawl (existing 01_raw); delete it to force re-crawl")
            counts = {
                "catalogs":   sum(1 for _ in (cat_root / "01_raw").glob("catalog.json")),
                "collections": sum(1 for _ in (cat_root / "01_raw" / "collections").glob("*.json")),
                "items":       sum(1 for _ in (cat_root / "01_raw" / "items").rglob("*.json")) if (cat_root / "01_raw" / "items").exists() else 0,
                "errors": 0,
            }
        else:
            counts = crawl_catalog(
                cfg, cat_root,
                on_progress=lambda c: progress(name, c),
                crawl_items=crawl_items_flag,
            )
        print(f"  catalogs={counts['catalogs']}  collections={counts['collections']}  items={counts['items']}  errors={counts['errors']}")

        # ---- Stage 2: translate ----
        translated_dir = cat_root / "v1.0" / "02_translated"
        translated_dir.mkdir(parents=True, exist_ok=True)
        existing = {p.stem for p in translated_dir.glob("*.json")}
        n_translated = 0
        n_skipped = 0
        if mode == "item_per_record":
            print("\n[2] translate Items -> v1.0 base records")
            for item, coll in iter_items_with_collection(cat_root):
                rec = build_record_stac_v10(
                    item, coll,
                    org_slug=cfg_entry.get("org_slug", "crstac"),
                    catalog_name=name,
                )
                if rec is None:
                    n_skipped += 1
                    continue
                if rec["id"] in existing:
                    continue
                write_json(str(translated_dir / f"{rec['id']}.json"), {"datasets": [rec]})
                n_translated += 1
        else:
            print("\n[2] translate Collections -> v1.0 base records")
            for coll in iter_collections(cat_root):
                rec = build_record_collection_v10(
                    coll,
                    org_slug=cfg_entry.get("org_slug", "coclico"),
                    catalog_name=name,
                )
                if rec is None:
                    n_skipped += 1
                    continue
                if rec["id"] in existing:
                    continue
                write_json(str(translated_dir / f"{rec['id']}.json"), {"datasets": [rec]})
                n_translated += 1
        print(f"  translated: {n_translated}  skipped: {n_skipped}")

        # ---- Stage 3: validate + tier ----
        print("\n[3] validate (3-layer audit) + tier")
        validated_dir = cat_root / "v1.0" / "03_validated"
        for tier in ("high", "medium", "low", "invalid"):
            (validated_dir / tier).mkdir(parents=True, exist_ok=True)

        reports_dir = cat_root / "v1.0" / "reports"
        reports_dir.mkdir(parents=True, exist_ok=True)

        tier_counter: Counter = Counter()
        rows: list[list] = []
        for tr_path in sorted(translated_dir.glob("*.json")):
            wrapped = load_json(str(tr_path))
            record = wrapped["datasets"][0]
            result = quiet_validate(record, schema, registry)
            tier = tier_of(result)
            tier_counter[tier] += 1
            grand_totals[tier] += 1
            out_path = validated_dir / tier / tr_path.name
            write_json(str(out_path), wrapped)
            rows.append([
                record.get("id", ""),
                tier,
                "true" if result.is_valid else "false",
                len(result.errors),
                len(result.warnings),
            ])

        csv_path = reports_dir / "validation_summary.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["rdls_id", "tier", "is_valid", "error_count", "warning_count"])
            for row in rows:
                w.writerow(row)
        print(f"  tiers: {dict(tier_counter)}")
        print(f"  report: {csv_path}")

    print("\n" + "=" * 60)
    print("  GRAND TOTAL")
    print("=" * 60)
    print(f"  tiers: {dict(grand_totals)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
