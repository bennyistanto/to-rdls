"""Complete each event_set's hazards[] so it declares every (type, process) its
events actually use. RULE 11 reverse case: an event uses a process the set-level
hazards[] doesn't list, leaving the set scope incomplete.

This is fact-based (the events ALREADY carry these processes - we only surface them
at the set level); it does NOT invent any new event. New hazards[] entries mirror
the type + intensity_measure of a matching event. Validates each changed record.

Usage:
    python scripts/fix_eventset_hazards_coverage.py <dir> [...] [--dry-run]
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


def fix_record(rec, stats):
    changed = 0
    for es in (rec.get("hazard", {}) or {}).get("event_sets", []) or []:
        if not isinstance(es, dict):
            continue
        hazards = es.get("hazards") or []
        declared = {(h.get("type"), h.get("process")) for h in hazards if isinstance(h, dict)}
        # collect (type, process) actually used by events, with a sample hazard to mirror
        used = {}
        for ev in es.get("events", []) or []:
            h = ev.get("hazard")
            if isinstance(h, dict) and h.get("type") and h.get("process"):
                used.setdefault((h["type"], h["process"]), h)
        for (typ, proc), sample in used.items():
            if (typ, proc) not in declared:
                entry = {"id": f"hazard_{typ}_{proc}", "type": typ, "process": proc}
                if sample.get("intensity_measure"):
                    entry["intensity_measure"] = sample["intensity_measure"]
                hazards.append(entry)
                declared.add((typ, proc))
                stats[f"{typ}/{proc}"] += 1
                changed += 1
        if hazards:
            es["hazards"] = hazards
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
                    failed.append((f.name, [e["message"][:60] for e in v.errors[:2]])); continue
                changed += 1
        if changed:
            nfiles += 1
            if not dry:
                f.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{'(dry-run) ' if dry else ''}files changed: {nfiles}   hazards[] entries added: {dict(stats)}")
    if failed:
        print(f"VALIDATION-FAILED (not written): {len(failed)}")
        for nm, e in failed[:6]:
            print(f"   {nm}: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
