"""Add a stable `id` to every hazard object that lacks one.

The JKAN consumer (rdl-jkan mappers.make_hazard) does `hazard["id"]` unconditionally
on loss.hazard, event.hazard, event_set.hazards[], vulnerability hazard_primary/
hazard_secondary and their triggers - so a missing id crashes ingestion with
KeyError: 'id'. The JSON Schema only REQUIRES id on event_set.hazards[].items, so
every other hazard object validly omits it - which is exactly what broke JKAN.

id convention: "hazard_<type>" (matches existing event_set.hazards[] ids like
"hazard_flood"); a trigger gets "trigger_<type>". JKAN needs presence, not
uniqueness, and the schema enforces no hazard-id uniqueness, so a type-based id is
sufficient and stable. No other field is touched.

Usage:
    python scripts/fix_hazard_ids.py <dir> [...] [--dry-run]
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


def ensure_id(h, stats, is_trigger=False):
    """Add id to a hazard dict (and its trigger) if missing. Returns #added."""
    n = 0
    if not isinstance(h, dict) or not h.get("type"):
        return 0
    if "id" not in h:
        prefix = "trigger_" if is_trigger else "hazard_"
        h["id"] = prefix + str(h["type"])
        # keep id first for readability
        h_items = [("id", h["id"])] + [(k, v) for k, v in h.items() if k != "id"]
        h.clear(); h.update(h_items)
        stats["trigger" if is_trigger else "hazard"] += 1
        n += 1
    if isinstance(h.get("trigger"), dict):
        n += ensure_id(h["trigger"], stats, is_trigger=True)
    return n


def fix_record(rec, stats):
    changed = 0
    for l in (rec.get("loss") or {}).get("losses", []) or []:
        if isinstance(l, dict):
            changed += ensure_id(l.get("hazard"), stats)
    for es in (rec.get("hazard") or {}).get("event_sets", []) or []:
        if not isinstance(es, dict):
            continue
        for h in es.get("hazards", []) or []:
            changed += ensure_id(h, stats)
        for ev in es.get("events", []) or []:
            if isinstance(ev, dict):
                changed += ensure_id(ev.get("hazard"), stats)
    for arr in (rec.get("vulnerability") or {}).get("functions", {}).values():
        if isinstance(arr, list):
            for fn in arr:
                if isinstance(fn, dict):
                    changed += ensure_id(fn.get("hazard_primary"), stats)
                    changed += ensure_id(fn.get("hazard_secondary"), stats)
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
                        failed.append((f.name, [e["message"][:50] for e in v.errors[:2]])); continue
                    changed += c
        if changed:
            nfiles += 1
            if not dry:
                f.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{'(dry-run) ' if dry else ''}files changed: {nfiles}   ids added: {dict(stats)}")
    if failed:
        print(f"VALIDATION-FAILED (not written): {len(failed)}")
        for nm, e in failed[:6]:
            print(f"   {nm}: {e}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
