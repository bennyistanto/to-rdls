"""Strip the broken templated statistics paragraph from HeiGIT road-surface
descriptions (HDX export artifact where the stats computed to 0.0 / nan%):

  "Roughly 0.0 million km of roads are mapped in OSM ... approximately 0.0 and 0.0
   (in million kms), corressponding to nan% and nan% respectively ... 0.0 million km
   or nan% of road surface information is missing ... (corressponding to nan% of
   total missing information on road surface)"

Only records whose description actually contains 'nan%' are touched (so any record
with real, valid stats is left alone). The rest of the description (methodology,
attribute list, attribution) is preserved. Validates each record (5 layers).

Usage:
    python scripts/fix_heigit_broken_stats.py <dir> [...] [--dry-run]
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
# the templated broken-stats block (numbers vary; ends at the parenthesised clause)
BLOCK = re.compile(
    r"\s*Roughly\s+[\d.]+\s*million km of roads are mapped in OSM.*?"
    r"total missing information on road surface\)", re.S | re.I)


def validate(rec):
    v = ValidationResult()
    validate_layer1_schema(rec, SCHEMA, v); validate_layer2_codelists(rec, REG, v)
    validate_layer3_semantic(rec, REG, v); validate_layer4_consistency(rec, v); validate_layer5_mde_rules(rec, v)
    return v


def clean(desc):
    new = BLOCK.sub("", desc)
    new = re.sub(r"[ \t]{2,}", " ", new)          # collapse double spaces left behind
    new = re.sub(r"\s+([.)])", r"\1", new)        # tidy stray space before . or )
    return new.strip()


def fix_record(rec, stats):
    d = rec.get("description")
    if not isinstance(d, str):
        return 0
    m = BLOCK.search(d)
    # strip the templated stats block only when it is broken/uninformative
    # (nan% percentages, or "0.0 million km" rounding artifacts for small regions)
    if m and ("nan%" in m.group(0) or "0.0 million km" in m.group(0)):
        new = clean(d)
        if new != d:
            rec["description"] = new
            stats["stripped"] += 1
            if "nan%" in new or "0.0 million km" in new:
                stats["residual_after_strip"] += 1
            return 1
    return 0


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
            if isinstance(rec, dict) and fix_record(rec, stats):
                v = validate(rec)
                if v.errors:
                    failed.append((f.name, [e["message"][:50] for e in v.errors[:2]])); continue
                changed += 1
        if changed:
            nfiles += 1
            if not dry:
                f.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{'(dry-run) ' if dry else ''}files changed: {nfiles}   ({dict(stats)})")
    if failed:
        print(f"VALIDATION-FAILED (not written): {len(failed)}")
        for nm, e in failed[:6]:
            print(f"   {nm}: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
