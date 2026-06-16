"""Set asset_type.id = "bui" for generic buildings exposure classified under the
GED4ALL scheme (reviewer: the internalised taxonomy_ged4all codelist lacks a
general "buildings without properties" code; use "bui", which the team will add).

Scope: exposure items with category="buildings" AND asset_type.scheme="GED4ALL"
whose asset_type.id is a human label ("built_up_surface_area", "buildings", ...)
rather than the GED4ALL code. The human label is preserved in title/description.
Other categories (population/infrastructure/economic) are NOT touched - the
reviewer's note covered only buildings, and general GED4ALL codes for those don't
exist in the codelist yet.

Usage:
    python scripts/fix_ged4all_buildings_code.py <dir> [...] [--dry-run]
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path
from collections import Counter

BACKUP = re.compile(r"_v0\.?3\.json$", re.I)


def fix_record(rec, stats: Counter):
    changed = 0
    exp = rec.get("exposure")
    if not isinstance(exp, list):
        return 0
    for e in exp:
        at = e.get("asset_type")
        if (isinstance(at, dict) and e.get("category") == "buildings"
                and at.get("scheme") == "GED4ALL" and at.get("id") != "bui"):
            stats[at.get("id")] += 1
            at["id"] = "bui"
            changed += 1
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
    stats = Counter(); nfiles = 0
    for f in files:
        try:
            doc = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        recs = doc["datasets"] if isinstance(doc, dict) and isinstance(doc.get("datasets"), list) else [doc]
        changed = sum(fix_record(rec, stats) for rec in recs if isinstance(rec, dict))
        if changed:
            nfiles += 1
            if not dry:
                f.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{'(dry-run) ' if dry else ''}files changed: {nfiles}   asset_type.id -> 'bui':")
    for k, n in stats.most_common():
        print(f"   {n:>5}x  {k!r} -> 'bui'")
    return 0


if __name__ == "__main__":
    sys.exit(main())
