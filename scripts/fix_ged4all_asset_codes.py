"""Set asset_type.id to the correct GED4ALL code (taxonomy_ged4all.csv) for
exposure items classified under scheme="GED4ALL". The codes ARE in the closed
codelist - this maps our human-label ids to them. The human label is preserved
in asset_type.title/description.

Mapping uses real taxonomy_ged4all codes (verified against
rdl-standard/schema/codelists/closed/taxonomy_ged4all.csv). Ids with no clean
single GED4ALL code (multimodal networks, mixed points-of-interest) are LEFT
UNCHANGED and reported. "bui" (generic buildings) is applied by
fix_ged4all_buildings_code.py; the reviewer is adding it to the codelist.

Usage:
    python scripts/fix_ged4all_asset_codes.py <dir> [...] [--dry-run]
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path
from collections import Counter

BACKUP = re.compile(r"_v0\.?3\.json$", re.I)

# current asset_type.id -> GED4ALL code (taxonomy_ged4all.csv)
MAP = {
    "population": "sei-pop", "rural_population": "sei-pop", "total_population": "sei-pop",
    "roads": "trs-rod", "road_network": "trs-rod",
    "railways": "trs-rwy",
    "power_plants": "pwr-plt", "power_generation_facilities": "pwr-plt",
    "power_transmission_lines": "pwr-lin", "power_distribution_lines": "pwr-lin",
    "electricity_transmission_lines": "pwr-lin", "electricity_distribution_lines": "pwr-lin",
    "power_infrastructure": "pwr-lin", "electricity_low_voltage_infrastructure": "pwr-lin",
    "gridded_gdp": "sei-eco",
}
# ids deliberately NOT mapped (no clean single GED4ALL code) - reported for review
UNMAPPED_NOTE = {"multimodal_transport_network", "points_of_interest"}


def fix_record(rec, stats: Counter, unmapped: Counter):
    changed = 0
    exp = rec.get("exposure")
    if not isinstance(exp, list):
        return 0
    for e in exp:
        at = e.get("asset_type")
        if not (isinstance(at, dict) and at.get("scheme") == "GED4ALL"):
            continue
        cur = at.get("id")
        if cur in MAP and cur != MAP[cur]:
            stats[f"{cur} -> {MAP[cur]}"] += 1
            at["id"] = MAP[cur]
            changed += 1
        elif cur in UNMAPPED_NOTE:
            unmapped[cur] += 1
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
    stats = Counter(); unmapped = Counter(); nfiles = 0
    for f in files:
        try:
            doc = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        recs = doc["datasets"] if isinstance(doc, dict) and isinstance(doc.get("datasets"), list) else [doc]
        changed = sum(fix_record(rec, stats, unmapped) for rec in recs if isinstance(rec, dict))
        if changed:
            nfiles += 1
            if not dry:
                f.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{'(dry-run) ' if dry else ''}files changed: {nfiles}")
    for k, n in stats.most_common():
        print(f"   {n:>5}x  {k}")
    if unmapped:
        print("LEFT UNCHANGED (no clean single GED4ALL code - needs review):")
        for k, n in unmapped.most_common():
            print(f"   {n:>5}x  {k!r}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
