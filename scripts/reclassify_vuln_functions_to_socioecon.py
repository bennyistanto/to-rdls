"""Reclassify mis-placed vulnerability FUNCTIONS to socio_economic INDICATORS.

A vulnerability function (damage/fragility curve) MUST carry impact.{type,
modelling,metric,measurement.quantity_kind} (enforced by the RDLS Metadata
Editor; see src/audit.validate_layer5_mde_rules). Several translators routed
index/indicator/"characteristics" datasets into functions.vulnerability[] with
a DEFAULT hazard_primary (flood/wd:m) and NO impact - i.e. they are not damage
functions at all. Filling impact would fabricate a damage curve; the honest fix
is to move them to vulnerability.socio_economic[] (the correct container for
indices/indicators), with fields drawn from the record itself.

A function is reclassified iff it lacks impact.type (cannot be a real function).
Functions that DO have impact are left untouched. reference_year is derived from
temporal / id / title / version / referenced_by; records with no derivable year
are reported and NOT changed (no fabrication).

Usage:
    python scripts/reclassify_vuln_functions_to_socioecon.py <dir> [--dry-run]
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from src.utils import slugify, sanitize_text
BACKUP = re.compile(r"_v0\.?3\.json$", re.I)


def derive_year(rec):
    t = rec.get("temporal") or {}
    for k in ("central_year", "start", "end"):
        if t.get(k):
            m = re.search(r"(19|20)\d\d", str(t[k]))
            if m: return int(m.group(0))
    for fld in (rec.get("id", ""), rec.get("title", ""), str(rec.get("version", ""))):
        m = re.search(r"(19|20)\d\d", fld)
        if m: return int(m.group(0))
    for ref in rec.get("referenced_by", []) or []:
        dp = ref.get("date_published", "")
        m = re.search(r"(19|20)\d\d", str(dp))
        if m: return int(m.group(0))
    # resource file names / URLs frequently carry the year (e.g.
    # "INFORM v3.2021.xlsx", "INFORM_Honduras_25_OCT_2021.xlsx")
    for r in rec.get("resources", []) or []:
        for fld in (r.get("title", ""), r.get("download_url", ""), r.get("access_url", "")):
            m = re.search(r"(19|20)\d\d", str(fld))
            if m: return int(m.group(0))
    return None


def strip_source_suffix(desc):
    return re.sub(r"\s*\[Source:.*?\]\s*$", "", desc or "").strip()


def reclassify(rec):
    """Return (n_moved, needs_year_flag)."""
    vuln = rec.get("vulnerability")
    if not isinstance(vuln, dict):
        return 0, False
    funcs = vuln.get("functions") or {}
    moved = []
    for func_type in list(funcs.keys()):
        arr = funcs.get(func_type)
        if not isinstance(arr, list):
            continue
        keep = []
        for fn in arr:
            if isinstance(fn, dict) and not (isinstance(fn.get("impact"), dict) and fn["impact"].get("type")):
                moved.append((func_type, fn))      # impact-less -> reclassify
            else:
                keep.append(fn)
        if keep:
            funcs[func_type] = keep
        else:
            del funcs[func_type]
    if not moved:
        return 0, False
    year = derive_year(rec)
    if year is None:
        return 0, True   # cannot reclassify without a reference_year - flag, do not fabricate

    socio = vuln.get("socio_economic") or []
    base = len(socio)
    title = sanitize_text(rec.get("title") or "")
    desc = strip_source_suffix(rec.get("description") or "") or title
    for k, (func_type, fn) in enumerate(moved, 1):
        socio.append({
            "id": f"se_{base + k}",
            "indicator_name": title or f"indicator {base + k}",
            "indicator_code": slugify(title, 40) or f"indicator_{base + k}",
            "description": desc[:500] if desc else title,
            "reference_year": year,
        })
    vuln["socio_economic"] = socio
    if not funcs:
        vuln.pop("functions", None)
    return len(moved), False


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry-run" in sys.argv
    if not args:
        print("Usage: python scripts/reclassify_vuln_functions_to_socioecon.py <dir> [--dry-run]"); return 1
    d = Path(args[0])
    files = [f for f in sorted(d.rglob("*.json")) if not BACKUP.search(f.name)]
    nfiles = nmoved = 0
    need_year = []
    for f in files:
        doc = json.loads(f.read_text(encoding="utf-8"))
        recs = doc["datasets"] if isinstance(doc, dict) and isinstance(doc.get("datasets"), list) else [doc]
        changed = 0
        for rec in recs:
            if not isinstance(rec, dict):
                continue
            m, flag = reclassify(rec)
            changed += m
            if flag:
                need_year.append(f.name)
        if changed:
            nfiles += 1; nmoved += changed
            if not dry:
                f.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{'(dry-run) ' if dry else ''}{d}: files changed={nfiles}, functions->socio_economic={nmoved}")
    if need_year:
        print(f"  NEEDS MANUAL reference_year (NOT changed): {sorted(set(need_year))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
