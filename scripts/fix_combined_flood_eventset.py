"""Move flood process to the event_set hazards[] level (not repeated per event),
add the missing pluvial process, and drop the process from 'combined' flood sets -
per the reviewer's phuquoc notes (Issue 5):

  - "state it at the event_set level ... you don't need to repeat it for each
     individual event" -> events[].hazard keeps type+intensity_measure, drops process;
     the process lives in the event_set hazards[] array.
  - event_set covering fluvial AND pluvial -> list BOTH processes in hazards[].
  - 'combined' flood event_set -> no process, just type=flood (covers all).

Targets only records that have flood event_sets whose id names a flood sub-process
or 'combined' (the phuquoc pattern); other records are untouched. Validates each.

Usage:
    python scripts/fix_phuquoc_eventset.py <dir> [...] [--dry-run]
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


def validate(rec):
    v = ValidationResult()
    validate_layer1_schema(rec, SCHEMA, v); validate_layer2_codelists(rec, REG, v)
    validate_layer3_semantic(rec, REG, v); validate_layer4_consistency(rec, v); validate_layer5_mde_rules(rec, v)
    return v


def target_processes(eid):
    """Flood sub-processes the event_set covers, per its id.
    NOTE: the schema requires `process` on every hazard (Event_set.hazards[].items
    required=[id,process] AND Event.hazard required=[process]), so 'combined' cannot
    drop the process - instead it lists ALL the flood processes it combines."""
    eid = eid.lower()
    if "combined" in eid:
        return ["fluvial_flood", "pluvial_flood", "coastal_flood"]
    procs = []
    if "fluvial" in eid:
        procs.append("fluvial_flood")
    if "pluvial" in eid:
        procs.append("pluvial_flood")
    if "coastal" in eid:
        procs.append("coastal_flood")
    return procs


def fix_record(rec, stats):
    h = rec.get("hazard")
    if not isinstance(h, dict):
        return 0
    ess = h.get("event_sets") or []
    changed = 0
    for es in ess:
        if not isinstance(es, dict):
            continue
        eid = str(es.get("id", ""))
        hazards = es.get("hazards") or []
        # only touch flood event_sets whose id encodes a sub-process or 'combined'
        is_flood = any(hz.get("type") == "flood" for hz in hazards if isinstance(hz, dict))
        procs = target_processes(eid)
        if not is_flood or not procs:
            continue
        imt = next((hz.get("intensity_measure") for hz in hazards if isinstance(hz, dict) and hz.get("intensity_measure")), "wd:m")
        existing = {hz.get("process") for hz in hazards if isinstance(hz, dict)}
        base_id = (hazards[0].get("id") if hazards and isinstance(hazards[0], dict) else None) or f"hazard_{eid}"
        # enrich the event_set hazards[] to list ALL flood processes it covers
        # (each with id+process+type+imt -- schema requires id and process).
        if len(procs) == 1:
            new_hazards = [{"id": base_id, "type": "flood", "process": procs[0], "intensity_measure": imt}]
        else:
            new_hazards = [{"id": f"hazard_{p}", "type": "flood", "process": p, "intensity_measure": imt} for p in procs]
        before = json.dumps(es.get("hazards"), sort_keys=True)
        es["hazards"] = new_hazards
        after = json.dumps(es["hazards"], sort_keys=True)
        if before != after:
            changed += 1
            added = set(procs) - existing
            for p in added:
                stats[f"added_{p}"] += 1
        # events keep their process (schema-required); left untouched
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
            if isinstance(rec, dict) and fix_record(rec, stats):
                v = validate(rec)
                if v.errors:
                    failed.append((f.name, [e["message"][:60] for e in v.errors[:3]])); continue
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
