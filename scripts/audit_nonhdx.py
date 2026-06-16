"""Exhaustive per-file audit of every output record EXCEPT hdx: runs the 5
validation layers AND every reviewer-derived rule, reporting findings file-by-file
so nothing is missed. Read-only - reports, does not modify.

Checks per record:
  L1-L5  : src.audit.validate (schema, codelist, semantic, media_type, MDE rules)
  RES-RAW: resource.description is a bare code/slug (no spaces) or == a slug title
  RES-WRAP: resource.description is a short label not prefixed with the dataset title
  TITLE-TAG: title ends with a [tag-code]
  GED4ALL: asset_type.scheme==GED4ALL but id not a taxonomy_ged4all code (allow 'bui')
  LOSS-FLAT: loss impact_and_losses uses flat impact_type/... instead of nested impact
  VULN-FLAT: vulnerability function uses flat impact_type/... instead of nested impact
  MEAS-MISSING: a measurement object lacks quantity_kind
  HAZ-SCOPE: title/description names a hazard/flood-process absent from the structured block
  LEAK: review-commentary phrases leaked into a human field

Usage:
    python scripts/audit_nonhdx.py [--details] [--root output]
"""
from __future__ import annotations
import csv, re, sys
from pathlib import Path
from collections import Counter, defaultdict

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from src.utils import load_json, sanitize_text
from src.audit import (CodelistRegistry, ValidationResult, validate_layer1_schema,
    validate_layer2_codelists, validate_layer3_semantic, validate_layer4_consistency, validate_layer5_mde_rules)

SCHEMA = load_json(str(ROOT / "schema/rdls_schema_v1.0.json"))
CLDIR = ROOT.parent / "rdl-standard" / "schema" / "codelists"
REG = CodelistRegistry(CLDIR)
BACKUP = re.compile(r"_v0\.?3\.json$", re.I)
SLUG = re.compile(r"^[\w\-.]+$")
SENTENCE = re.compile(r"[.!?]\s")
TITLE_TAG = re.compile(r"\[[A-Za-z0-9_\-]+\]\s*$")
LEAK = re.compile(r"flag for .{0,40}review|inherited from the v0\.3|not stated in the source|"
                  r"series-level review|v0\.3 classification", re.I)
GED4ALL_CODES = {r["Code"].strip() for r in
                 csv.DictReader(open(CLDIR / "closed" / "taxonomy_ged4all.csv", encoding="utf-8"))
                 if r.get("Code")} | {"bui"}  # bui is pending in the codelist

HAZ_KW = {r"\bflood": "flood", r"\bcyclone|\btyphoon|\bhurricane": "strong_wind",
          r"\bearthquake|ground shaking|seismic": "earthquake", r"\btsunami": "tsunami",
          r"\bdrought": "drought", r"\blandslide": "landslide", r"\bwildfire": "wildfire",
          r"volcan|ashfall|\blahar": "volcanic"}
FLOODPROC_KW = {r"\bfluvial|riverine": "fluvial_flood", r"\bpluvial|surface water": "pluvial_flood",
                r"coastal flood|storm surge|\bsurge": "coastal_flood"}

TARGETS = ["output/upload", "output/desinventar/metadata", "output/gca/metadata",
    "output/google/metadata", "output/india-gobs/metadata", "output/jrc-drmkc/metadata",
    "output/mdg/metadata", "output/nismod/icra", "output/nismod/sdk",
    "output/tomorrow-cities/metadata", "output/vulnerability-datasets/metadata", "output/wbg-ufra",
    "output/stac/climate-risk-stac/v1.0/03_validated/high", "output/stac/coclico/v1.0/03_validated/high"]
TARGETS += [str(p) for p in sorted(Path("output/geonode").glob("*/v1.0/07_validated/high"))]


def struct_hazards(rec):
    types, procs = set(), set()
    def add(h):
        if isinstance(h, dict):
            if h.get("type"): types.add(h["type"])
            if h.get("process"): procs.add(h["process"])
    for es in rec.get("hazard", {}).get("event_sets", []) or []:
        for h in es.get("hazards", []) or []:
            add(h); add(h.get("trigger"))
        for ev in es.get("events", []) or []:
            add(ev.get("hazard"))
    for l in rec.get("loss", {}).get("losses", []) or []:
        add(l.get("hazard"))
    for arr in (rec.get("vulnerability", {}).get("functions") or {}).values():
        if isinstance(arr, list):
            for fn in arr:
                add(fn.get("hazard_primary")); add(fn.get("hazard_secondary"))
    return types, procs


def audit_record(rec):
    finds = []
    # L1-L5
    r = ValidationResult()
    validate_layer1_schema(rec, SCHEMA, r); validate_layer2_codelists(rec, REG, r)
    validate_layer3_semantic(rec, REG, r); validate_layer4_consistency(rec, r); validate_layer5_mde_rules(rec, r)
    for e in r.errors:
        finds.append((f"L-{e['layer']}", f"{e['path']}: {e['message'][:80]}"))
    # surface the event_set-vs-events process coverage warning (RULE 11) as a finding
    for w in r.warnings:
        if "event_set declares processes" in w["message"] or "not declared in the event_set" in w["message"]:
            finds.append(("EVENTSET-PROC", f"{w['path']}: {w['message'][:90]}"))
    title = sanitize_text(rec.get("title") or "").strip()
    # TITLE-TAG
    if title and TITLE_TAG.search(title):
        finds.append(("TITLE-TAG", title[:60]))
    # LEAK (human fields)
    for fld in [rec.get("description", "")] + [r2.get("description", "") for r2 in rec.get("resources", []) or []] \
               + [l.get("description", "") for l in rec.get("loss", {}).get("losses", []) or []]:
        if fld and LEAK.search(str(fld)):
            finds.append(("LEAK", str(fld)[:70])); break
    # resources
    for res in rec.get("resources", []) or []:
        if not isinstance(res, dict):
            continue
        d = (res.get("description") or "").strip()
        rt = (res.get("title") or "").strip()
        if not d:
            finds.append(("RES-RAW", f"{res.get('id')}: empty description"))
        elif " " not in d and SLUG.match(d):
            finds.append(("RES-RAW", f"{res.get('id')}: bare code '{d[:30]}'"))
        elif d == rt and " " not in rt and SLUG.match(rt):
            finds.append(("RES-RAW", f"{res.get('id')}: desc==slug-title '{d[:30]}'"))
        elif (len(d) <= 80 and not SENTENCE.search(d) and title
              and not d.lower().startswith(title.lower()[:25])):
            finds.append(("RES-WRAP", f"{res.get('id')}: short label not title-prefixed '{d[:40]}'"))
    # exposure asset_type GED4ALL + measurement
    for e in rec.get("exposure", []) if isinstance(rec.get("exposure"), list) else []:
        at = e.get("asset_type")
        if isinstance(at, dict) and at.get("scheme") == "GED4ALL" and at.get("id") not in GED4ALL_CODES:
            finds.append(("GED4ALL", f"{e.get('category')}: id '{at.get('id')}' not a taxonomy_ged4all code"))
        for m in e.get("metrics", []) or []:
            meas = m.get("measurement")
            if isinstance(meas, dict) and not meas.get("quantity_kind"):
                finds.append(("MEAS-MISSING", f"exposure metric {m.get('id')} measurement has no quantity_kind"))
    # loss flat / vuln flat
    for i, l in enumerate(rec.get("loss", {}).get("losses", []) or []):
        ial = l.get("impact_and_losses", {})
        if isinstance(ial, dict) and any(k in ial for k in ("impact_type", "impact_modelling", "impact_metric")) and "impact" not in ial:
            finds.append(("LOSS-FLAT", f"losses[{i}] flat impact_*"))
    for ft, arr in (rec.get("vulnerability", {}).get("functions") or {}).items():
        if isinstance(arr, list):
            for i, fn in enumerate(arr):
                if isinstance(fn, dict) and any(k in fn for k in ("impact_type", "impact_metric", "impact_modelling")) and "impact" not in fn:
                    finds.append(("VULN-FLAT", f"functions.{ft}[{i}] flat impact_*"))
    # hazard scope
    types, procs = struct_hazards(rec)
    if types:
        txt = (str(rec.get("title", "")) + " . " + str(rec.get("description", ""))).lower()
        for l in rec.get("loss", {}).get("losses", []) or []:
            txt += " . " + str(l.get("description") or "")
        ment = {v for pat, v in HAZ_KW.items() if re.search(pat, txt)}
        miss_t = ment - types
        miss_p = set()
        if "flood" in types or "flood" in ment:
            mp = {v for pat, v in FLOODPROC_KW.items() if re.search(pat, txt)}
            miss_p = mp - procs
        if miss_t or miss_p:
            finds.append(("HAZ-SCOPE", f"text mentions types {sorted(miss_t)} / procs {sorted(miss_p)} not in block"))
    return finds


def main():
    details = "--details" in sys.argv
    # positional args (non-flag) override the default TARGETS, so the same auditor
    # can run on HDX: python scripts/audit_nonhdx.py output/hdx/v1.0/dist/high
    global TARGETS
    pos = [a for a in sys.argv[1:] if not a.startswith("--")]
    if pos:
        TARGETS = pos
    by_cat = Counter()
    by_folder = defaultdict(Counter)
    examples = defaultdict(list)
    total = 0
    for t in TARGETS:
        d = Path(t)
        if not d.exists():
            continue
        for f in d.rglob("*.json"):
            if BACKUP.search(f.name):
                continue
            try:
                raw = load_json(str(f))
            except Exception:
                continue
            recs = raw["datasets"] if isinstance(raw, dict) and isinstance(raw.get("datasets"), list) else [raw]
            for rec in recs:
                if not isinstance(rec, dict):
                    continue
                total += 1
                for cat, msg in audit_record(rec):
                    by_cat[cat] += 1
                    by_folder[t][cat] += 1
                    if len(examples[cat]) < 12:
                        examples[cat].append(f"{f.name}: {msg}")
    print(f"audited records (non-HDX): {total}\n")
    print("=== findings by category ===")
    if not by_cat:
        print("   NONE - clean across all checks")
    for cat, n in by_cat.most_common():
        print(f"   {n:>5}  {cat}")
    if details:
        print("\n=== examples per category ===")
        for cat in by_cat:
            print(f"\n[{cat}]")
            for ex in examples[cat]:
                print(f"   {ex}")
        print("\n=== folders with findings ===")
        for folder, cc in by_folder.items():
            print(f"   {folder}: {dict(cc)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
