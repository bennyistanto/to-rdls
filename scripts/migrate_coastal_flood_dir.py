"""Migrate coastal_flood-as-hazard-type across all v1.0 records in a directory.

2026-06 codelist change: coastal_flood is no longer a hazard type. It is a
PROCESS under type=flood. This applies the faithful migration to every hazard
object (event_sets[].hazards[], events[].hazard, loss.losses[].hazard,
vulnerability.functions.*.hazard_primary/secondary):

    type == coastal_flood  ->  type=flood, process=coastal_flood
    intensity_measure: keep if present, else default wd:m (flood standard)

Skips *_v03.json / *_v0.3.json backups. Read+write; prints an audit and the
count. Does NOT touch any hazard whose type != coastal_flood.

Usage:
    python scripts/migrate_coastal_flood_dir.py <dir> [--dry-run]
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
BACKUP_RE = re.compile(r"_v0\.?3\.json$", re.I)


def fix_hazard(h, *, is_trigger=False):
    """Migrate one hazard object. is_trigger=True => SimpleHazard (type+process
    only, never add intensity_measure). Preserves an existing intensity_measure;
    only defaults wd:m for a full hazard that lacks one."""
    if not isinstance(h, dict) or h.get("type") != "coastal_flood":
        return None
    before = f"type=coastal_flood process={h.get('process')}"
    h["type"] = "flood"
    h["process"] = "coastal_flood"
    if not is_trigger and not h.get("intensity_measure"):
        h["intensity_measure"] = "wd:m"
    return before


def migrate_record(rec):
    changes = []
    for i, es in enumerate((rec.get("hazard") or {}).get("event_sets") or []):
        for j, h in enumerate(es.get("hazards") or []):
            if fix_hazard(h): changes.append(f"hazard.es[{i}].hazards[{j}]")
            if isinstance(h.get("trigger"), dict) and fix_hazard(h["trigger"], is_trigger=True):
                changes.append(f"hazard.es[{i}].hazards[{j}].trigger")
        for j, ev in enumerate(es.get("events") or []):
            evh = ev.get("hazard")
            if fix_hazard(evh): changes.append(f"hazard.es[{i}].events[{j}].hazard")
            if isinstance(evh, dict) and isinstance(evh.get("trigger"), dict) and fix_hazard(evh["trigger"], is_trigger=True):
                changes.append(f"hazard.es[{i}].events[{j}].hazard.trigger")
    for i, l in enumerate((rec.get("loss") or {}).get("losses") or []):
        lh = l.get("hazard")
        if fix_hazard(lh): changes.append(f"loss.losses[{i}].hazard")
        if isinstance(lh, dict) and isinstance(lh.get("trigger"), dict) and fix_hazard(lh["trigger"], is_trigger=True):
            changes.append(f"loss.losses[{i}].hazard.trigger")
    funcs = (rec.get("vulnerability") or {}).get("functions") or {}
    for kind, arr in funcs.items():
        if isinstance(arr, list):
            for i, vf in enumerate(arr):
                for slot in ("hazard_primary", "hazard_secondary"):
                    hh = vf.get(slot)
                    if fix_hazard(hh): changes.append(f"vuln.{kind}[{i}].{slot}")
                    if isinstance(hh, dict) and isinstance(hh.get("trigger"), dict) and fix_hazard(hh["trigger"], is_trigger=True):
                        changes.append(f"vuln.{kind}[{i}].{slot}.trigger")
    return changes


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry-run" in sys.argv
    if not args:
        print("Usage: python scripts/migrate_coastal_flood_dir.py <dir> [--dry-run]"); return 1
    d = Path(args[0])
    files = [f for f in sorted(d.rglob("*.json")) if not BACKUP_RE.search(f.name)]
    total_files = total_hz = 0
    for f in files:
        doc = json.loads(f.read_text(encoding="utf-8"))
        recs = doc["datasets"] if isinstance(doc, dict) and "datasets" in doc else [doc]
        fchanges = []
        for rec in recs:
            fchanges += migrate_record(rec)
        if fchanges:
            total_files += 1; total_hz += len(fchanges)
            print(f"{'[dry] ' if dry else ''}{f.name}: {len(fchanges)} hazard(s) -> flood/coastal_flood")
            if not dry:
                f.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"\n{'(dry-run) ' if dry else ''}files changed: {total_files}, hazard objects migrated: {total_hz}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
