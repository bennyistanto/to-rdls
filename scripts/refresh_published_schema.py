"""
refresh_published_schema.py - fetch the BAKED RDLS v1.0 schema for validation.
=============================================================================

WHY THIS EXISTS
---------------
The source schema in `rdl-standard/schema/rdls_schema.json` ships the hazard
constraint blocks as EMPTY placeholders:

    "conditional_hazard_type_to_process": {
        "$comment": "populated dynamically by config_inited in docs/conf.py"
    }

Those conditionals (type -> allowed process, type -> allowed intensity_measure)
are only baked in when the RDLS docs site builds the PUBLISHED schema at
https://docs.riskdatalibrary.org/en/1__0__0/rdls_schema.json - which is exactly
what JKAN and every downstream consumer validates against.

If we validate against the source schema, we SILENTLY SKIP every hazard
type/process pairing rule and pass records that real consumers reject. This
script closes that gap by caching the published (baked) schema locally and
verifying it is in sync with the codelists you just resynced.

WHAT IT DOES
------------
1. Download the published schema (docs.riskdatalibrary.org).
2. Verify it is in sync with the local rdl-standard codelist CSVs:
     - hazard_type enum            == closed/hazard_type.csv
     - climate_scenario enum       == closed/climate_scenario.csv
     - type -> process conditionals == process_type.csv (Hazard column)
   If ANY check fails, abort without writing (the website is stale vs your
   resync - do not adopt it).
3. Write the verified schema to schema/rdls_schema_v1.0.json (our canonical
   validation schema, conditionals baked in).

Usage:
    python scripts/refresh_published_schema.py
    python scripts/refresh_published_schema.py --offline temp/published_rdls_schema.json
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
import urllib.request
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PUBLISHED_URL = "https://docs.riskdatalibrary.org/en/1__0__0/rdls_schema.json"
CODELISTS_DIR = PROJECT_ROOT.parent / "rdl-standard" / "schema" / "codelists"
OUT_PATH = PROJECT_ROOT / "schema" / "rdls_schema_v1.0.json"


def fetch_published(url: str) -> dict:
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 (rdls-validator)"})
    with urllib.request.urlopen(req, timeout=60) as r:
        return json.loads(r.read().decode("utf-8"))


def csv_codes(sub: str, name: str) -> list[str]:
    p = CODELISTS_DIR / sub / f"{name}.csv"
    with p.open(encoding="utf-8") as f:
        return [(r.get("Code") or r.get("code") or "").strip()
                for r in csv.DictReader(f) if (r.get("Code") or r.get("code"))]


def find_simplehazard_type_enum(schema: dict) -> list[str]:
    return schema["$defs"]["SimpleHazard"]["properties"]["type"]["enum"]


def find_scenario_enum(schema: dict):
    def walk(o):
        if isinstance(o, dict):
            sc = (o.get("properties") or {}).get("scenario", {})
            if "enum" in sc:
                return sc["enum"]
            for v in o.values():
                r = walk(v)
                if r:
                    return r
        elif isinstance(o, list):
            for v in o:
                r = walk(v)
                if r:
                    return r
        return None
    return walk(schema) or []


def find_type_process(schema: dict) -> dict:
    out = {}
    def walk(o):
        if isinstance(o, dict):
            if "if" in o and "then" in o:
                t = o["if"].get("properties", {}).get("type", {}).get("const")
                pe = o["then"].get("properties", {}).get("process", {}).get("enum")
                if t and pe:
                    out[t] = set(pe)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(schema)
    return out


def csv_type_process() -> dict:
    out: dict = {}
    with (CODELISTS_DIR / "closed" / "process_type.csv").open(encoding="utf-8") as f:
        for r in csv.DictReader(f):
            proc = (r.get("Code") or "").strip()
            for hz in (r.get("Hazard") or "").split(","):
                hz = hz.strip()
                if hz and proc:
                    out.setdefault(hz, set()).add(proc)
    return out


def verify_sync(schema: dict) -> list[str]:
    """Return a list of human-readable mismatch messages (empty == in sync)."""
    problems = []

    pub_ht = set(find_simplehazard_type_enum(schema))
    csv_ht = set(csv_codes("closed", "hazard_type"))
    if pub_ht != csv_ht:
        problems.append(
            f"hazard_type mismatch: published-only={sorted(pub_ht - csv_ht)} "
            f"csv-only={sorted(csv_ht - pub_ht)}"
        )

    pub_sc = set(find_scenario_enum(schema))
    csv_sc = set(csv_codes("closed", "climate_scenario"))
    if pub_sc != csv_sc:
        problems.append(
            f"climate_scenario mismatch: published-only={sorted(pub_sc - csv_sc)} "
            f"csv-only={sorted(csv_sc - pub_sc)}"
        )

    pub_tp = find_type_process(schema)
    csv_tp = csv_type_process()
    for t in set(pub_tp) | set(csv_tp):
        if pub_tp.get(t, set()) != csv_tp.get(t, set()):
            problems.append(
                f"type->process mismatch for '{t}': "
                f"published-only={sorted(pub_tp.get(t, set()) - csv_tp.get(t, set()))} "
                f"csv-only={sorted(csv_tp.get(t, set()) - pub_tp.get(t, set()))}"
            )
    return problems


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--offline", metavar="PATH",
                    help="Use a local copy of the published schema instead of downloading")
    ap.add_argument("--force", action="store_true",
                    help="Write even if sync verification fails (NOT recommended)")
    args = ap.parse_args()

    if not CODELISTS_DIR.exists():
        print(f"ERROR: codelists dir not found: {CODELISTS_DIR}")
        return 1

    if args.offline:
        print(f"Loading published schema from local file: {args.offline}")
        schema = json.loads(Path(args.offline).read_text(encoding="utf-8"))
    else:
        print(f"Downloading published schema: {PUBLISHED_URL}")
        try:
            schema = fetch_published(PUBLISHED_URL)
        except Exception as e:
            print(f"ERROR: download failed: {e}")
            print("  Re-run with --offline <path-to-published-schema.json> if you have a copy.")
            return 1

    # Sanity: must actually contain baked conditionals
    tp_rules = len(schema["$defs"].get("conditional_hazard_type_to_process", {}).get("allOf", []))
    imt_rules = len(schema["$defs"].get("conditional_hazard_type_to_intensity_measure", {}).get("allOf", []))
    print(f"  baked rules: type->process={tp_rules}, type->IMT={imt_rules}")
    if tp_rules == 0:
        print("ERROR: fetched schema has EMPTY conditionals - this is the source schema, not the published one.")
        return 1

    problems = verify_sync(schema)
    if problems:
        print("\n*** SYNC CHECK FAILED - published schema is NOT in sync with your resynced CSVs ***")
        for p in problems:
            print(f"  - {p}")
        if not args.force:
            print("\nAborting (use --force to override). The docs site likely has not")
            print("rebuilt since your codelist resync. Wait for rebuild or build locally.")
            return 1
        print("\n--force set: writing anyway.")
    else:
        print("  sync check: PASS (hazard_type, climate_scenario, type->process all match resynced CSVs)")

    OUT_PATH.write_text(json.dumps(schema, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\nWrote baked validation schema: {OUT_PATH}")
    print(f"  size: {OUT_PATH.stat().st_size:,} bytes")
    return 0


if __name__ == "__main__":
    sys.exit(main())
