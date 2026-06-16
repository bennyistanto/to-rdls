"""Fix the data-quality bugs the upload-vs-canonical comparison + sweep exposed
(the 5-layer validator misses these because the values are well-formed):

  A. JUNK details   - remove a `details` field that is a placeholder concatenation
                      ("No information provided None None", standalone None/null/nan).
  B. AREA-as-currency - exposure metric measured as currency on a dataset whose
                      title says "square met..." (area in square metres). -> quantity_kind
                      'area', unit 'square_metre'. Also fix temporal.central_year from a
                      "(YYYY)" in the title when present (the icpac family had 2021 vs 2019).
  C. INDEX-as-currency - exposure metric with dimension 'index' measured as currency on a
                      dataset whose title says index/severity. -> quantity_kind 'index',
                      drop the (monetary) unit.

NOT touched: building_replacement_value / loss / GDP etc. (currency is correct there).
Validates each changed record (5 layers); only writes if it still passes.

Usage:
    python scripts/fix_measurement_and_junk.py <dir> [...] [--dry-run]
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path
from collections import Counter

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from src.utils import load_json
from src.audit import (CodelistRegistry, ValidationResult, validate_layer1_schema,
    validate_layer2_codelists, validate_layer3_semantic, validate_layer4_consistency, validate_layer5_mde_rules)

SCHEMA = load_json(str(ROOT / "schema/rdls_schema_v1.0.json"))
REG = CodelistRegistry(ROOT.parent / "rdl-standard" / "schema" / "codelists")
BACKUP = re.compile(r"_v0\.?3\.json$", re.I)
JUNK = re.compile(r"No information provided|None None|\bNaN\b|^\s*(None|null|nan|N/?A)\s*$", re.I)
SQM = re.compile(r"square met|in square met|\bm2\b|sq\.?\s*m", re.I)
INDEXISH = re.compile(r"\bindex\b|severity|\bscore\b", re.I)
YEAR = re.compile(r"\((19|20)\d\d\)")
# unambiguous additional measurement corrections (only applied where qk==currency):
CROPAREA = re.compile(r"cropland|crop-land|grazing land|growing area|agricultural area|arable land", re.I)
PERCENT = re.compile(r"\bin %|\bpercent\b|\(%\)|\bshare of\b", re.I)
BATHY = re.compile(r"bathymetr", re.I)


def validate(rec):
    v = ValidationResult()
    validate_layer1_schema(rec, SCHEMA, v); validate_layer2_codelists(rec, REG, v)
    validate_layer3_semantic(rec, REG, v); validate_layer4_consistency(rec, v); validate_layer5_mde_rules(rec, v)
    return v


def fix_record(rec, stats: Counter):
    changed = 0
    title = str(rec.get("title") or "")
    desc = str(rec.get("description") or "")
    text = title + " . " + desc
    # A. junk details
    if isinstance(rec.get("details"), str) and JUNK.search(rec["details"]):
        rec.pop("details", None); stats["junk_details_removed"] += 1; changed += 1
    # B/C. exposure measurement
    if isinstance(rec.get("exposure"), list):
        is_sqm = bool(SQM.search(text))
        is_index = bool(INDEXISH.search(text))
        for e in rec["exposure"]:
            for m in e.get("metrics", []) or []:
                meas = m.get("measurement")
                if not isinstance(meas, dict) or meas.get("quantity_kind") != "currency":
                    continue
                if is_sqm:
                    meas["quantity_kind"] = "area"; meas["unit"] = "square_metre"
                    stats["currency_to_area_sqm"] += 1; changed += 1
                elif PERCENT.search(text):
                    meas["quantity_kind"] = "dimensionless_ratio"; meas["unit"] = "percent"
                    stats["currency_to_percent"] += 1; changed += 1
                elif CROPAREA.search(text):
                    # land extent -> area; unit unknown (not "square meters") so drop the USD unit
                    meas["quantity_kind"] = "area"; meas.pop("unit", None)
                    stats["currency_to_area_land"] += 1; changed += 1
                elif BATHY.search(text):
                    meas["quantity_kind"] = "length"; meas["unit"] = "metre"
                    stats["currency_to_length_depth"] += 1; changed += 1
                elif m.get("dimension") == "index" and is_index:
                    meas["quantity_kind"] = "index"; meas.pop("unit", None)
                    stats["currency_to_index"] += 1; changed += 1
        # fix temporal year for square-meters datasets from "(YYYY)" in title
        if is_sqm:
            y = YEAR.search(title)
            if y:
                yr = int(y.group(0).strip("()"))
                t = rec.setdefault("temporal", {})
                if isinstance(t, dict) and t.get("central_year") != yr:
                    t["central_year"] = yr; stats["temporal_year_fixed"] += 1; changed += 1
    return changed


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry-run" in sys.argv
    files = []
    for a in args:
        p = Path(a)
        if p.is_dir():
            files += [f for f in p.rglob("*.json") if not BACKUP.search(f.name)]
        else:
            files += [f for f in Path().glob(a) if not BACKUP.search(f.name)]
    files = sorted(set(files))
    stats = Counter(); nfiles = 0; failed = []
    for f in files:
        try:
            doc = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        recs = doc["datasets"] if isinstance(doc, dict) and isinstance(doc.get("datasets"), list) else [doc]
        changed = 0
        for rec in recs:
            if isinstance(rec, dict):
                c = fix_record(rec, stats)
                if c:
                    v = validate(rec)
                    if v.errors:
                        failed.append((f.name, [e["message"][:60] for e in v.errors[:2]]))
                        continue
                    changed += c
        if changed:
            nfiles += 1
            if not dry:
                f.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{'(dry-run) ' if dry else ''}files changed: {nfiles}")
    for k, n in stats.most_common():
        print(f"   {n:>5}x  {k}")
    if failed:
        print(f"VALIDATION-FAILED (not written): {len(failed)}")
        for nm, errs in failed[:8]:
            print(f"   {nm}: {errs}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
