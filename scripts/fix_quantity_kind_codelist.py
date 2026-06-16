"""Canonicalise non-codelist quantity_kind values in measurement objects.

quantity_kind.csv is an OPEN codelist in RDLS, but the RDLS Metadata Editor
drives a FIXED dropdown from it (and the dependent unit dropdown). A
quantity_kind outside the codelist is not recognised, so the measurement -- and
with it the whole exposure/loss/vulnerability block -- fails to load in the
editor (the "exposure block is not recognised" symptom). See
src/audit.py:validate_layer3_semantic RULE 3, which now enforces membership.

quantity_kind.csv is an OPEN codelist and the editor ACCEPTS custom relevant
values, so a novel value such as `power` (installed generation capacity in
megawatt) is a legitimate extension and is left UNTOUCHED. This fixer only
rewrites the obsolete v0.3 terms / unit-as-quantity_kind mistakes that have a
canonical v1.0 codelist code (these are what fail to load in the editor's
dropdown):

  monetary -> currency            (v0.3->v1.0 rename; unit USD/etc stays valid)
  weight   -> mass                (v0.3->v1.0 rename; unit kilogram/etc stays valid)
  percent  -> dimensionless_ratio (percent is a UNIT, not a quantity_kind;
                                   unit is set to the only valid code, `percent`)
  boolean  -> count               (per-asset 0/1 flag counted as discrete items;
                                   team decision, no RDLS boolean quantity_kind)

Covers measurements in exposure metrics, loss impact_and_losses, and
vulnerability function impact -- the same three locations the validator checks.

Usage:
    python scripts/fix_quantity_kind_codelist.py <dir-or-glob> [<dir> ...] [--dry-run]
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path
from collections import Counter

BACKUP = re.compile(r"_v0\.?3\.json$", re.I)

# quantity_kind value -> (canonical quantity_kind, forced unit or None to keep)
# `power` is deliberately ABSENT: it is a valid novel open-codelist value (kept).
RENAME = {
    "monetary": ("currency", None),
    "weight": ("mass", None),
    "percent": ("dimensionless_ratio", "percent"),
    "boolean": ("count", None),
}


def iter_measurements(rec):
    """Yield measurement dicts in exposure / loss / vulnerability blocks."""
    exp = rec.get("exposure")
    if isinstance(exp, list):
        for e in exp:
            for m in e.get("metrics", []) or []:
                meas = m.get("measurement")
                if isinstance(meas, dict):
                    yield meas
    loss = rec.get("loss")
    if isinstance(loss, dict):
        for l in loss.get("losses", []) or []:
            meas = (l.get("impact_and_losses") or {}).get("measurement")
            if isinstance(meas, dict):
                yield meas
    vuln = rec.get("vulnerability")
    if isinstance(vuln, dict):
        for arr in (vuln.get("functions") or {}).values():
            if isinstance(arr, list):
                for fn in arr:
                    meas = (fn.get("impact") or {}).get("measurement")
                    if isinstance(meas, dict):
                        yield meas


def fix_record(rec, fixed: Counter):
    changed = 0
    for meas in iter_measurements(rec):
        qk = meas.get("quantity_kind")
        if not qk:
            continue
        if qk in RENAME:
            new_qk, forced_unit = RENAME[qk]
            meas["quantity_kind"] = new_qk
            if forced_unit is not None:
                meas["unit"] = forced_unit
            fixed[f"{qk}->{new_qk}"] += 1
            changed += 1
        # everything else left as-is; reporting happens in the caller via a sweep
    return changed


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry-run" in sys.argv
    if not args:
        print("Usage: python scripts/fix_quantity_kind_codelist.py <dir> [...] [--dry-run]")
        return 1

    files = []
    for a in args:
        p = Path(a)
        if p.is_dir():
            files += [f for f in p.rglob("*.json") if not BACKUP.search(f.name)]
        else:  # treat as glob relative to cwd
            files += [f for f in Path().glob(a) if not BACKUP.search(f.name)]
    files = sorted(set(files))

    fixed = Counter()
    kept = Counter()
    nfiles = 0
    for f in files:
        try:
            doc = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        recs = doc["datasets"] if isinstance(doc, dict) and isinstance(doc.get("datasets"), list) else [doc]
        changed = 0
        for rec in recs:
            if isinstance(rec, dict):
                changed += fix_record(rec, fixed)
                # report any other out-of-RENAME custom value (kept as a valid
                # open-codelist extension, e.g. `power`) for visibility
                for meas in iter_measurements(rec):
                    qk = meas.get("quantity_kind")
                    if qk and qk not in RENAME and qk not in _CANONICAL:
                        kept[qk] += 1
        if changed:
            nfiles += 1
            if not dry:
                f.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"{'(dry-run) ' if dry else ''}files changed: {nfiles}")
    for k, n in fixed.most_common():
        print(f"  fixed  {n:>5}x  {k}")
    if kept:
        print("KEPT as valid custom open-codelist values (not rewritten):")
        for k, n in kept.most_common():
            print(f"  keep   {n:>5}x  quantity_kind={k!r}")
    return 0


# Canonical RDLS quantity_kind codelist codes (anything outside this AND outside
# RENAME is a custom open-codelist value we KEEP and merely report).
_CANONICAL = {
    "area", "length", "count", "currency", "time", "volume",
    "mass", "mass_per_area", "dimensionless_ratio", "index", "energy",
}

if __name__ == "__main__":
    sys.exit(main())
