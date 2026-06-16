"""Reconcile event_set.analysis_type with the occurrence its events actually use.

RDLS semantic rule 5: an event's occurrence sub-object (probabilistic /
empirical / deterministic) must match the event_set analysis_type. Some
generated records carry a mismatch (e.g. analysis_type=probabilistic but events
use occurrence.empirical, with an EMPTY probabilistic placeholder).

This script, per event_set:
  1. Drops an EMPTY probabilistic placeholder from each event's occurrence when
     the event also has a real occurrence of another type. "Empty" =
     probabilistic with no return_period, no event_rate, and no
     probability.value (only e.g. a bare `span`).
  2. If, after cleanup, every event in the set has exactly ONE occurrence type
     and it is consistent across the set, sets analysis_type to that type.
  3. Leaves the set untouched (and reports it) if events are mixed/ambiguous.

Fact-based: analysis_type is set to match the occurrence the events genuinely
carry; nothing is invented. Skips *_v03 backups.

Usage:
    python scripts/reconcile_analysis_occurrence.py <dir> [--dry-run]
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path

BACKUP_RE = re.compile(r"_v0\.?3\.json$", re.I)


def prob_is_empty(prob: dict) -> bool:
    if not isinstance(prob, dict):
        return True
    if prob.get("return_period") is not None:
        return False
    if prob.get("event_rate") is not None:
        return False
    p = prob.get("probability")
    if isinstance(p, dict) and p.get("value") is not None:
        return False
    return True


def reconcile_event_set(es: dict):
    changes = []
    events = es.get("events") or []
    # 1. drop empty probabilistic placeholders that coexist with another type
    for ev in events:
        occ = ev.get("occurrence")
        if not isinstance(occ, dict):
            continue
        if "probabilistic" in occ and prob_is_empty(occ["probabilistic"]) and len(occ) > 1:
            occ.pop("probabilistic")
            changes.append("dropped empty probabilistic placeholder")
    # 2. collect occurrence types per event
    types = set()
    for ev in events:
        occ = ev.get("occurrence")
        if isinstance(occ, dict):
            for k in ("probabilistic", "empirical", "deterministic"):
                if k in occ:
                    types.add(k)
    # 3. if uniform single type and analysis_type differs, fix it
    if len(types) == 1:
        only = next(iter(types))
        if es.get("analysis_type") != only:
            changes.append(f"analysis_type {es.get('analysis_type')} -> {only}")
            es["analysis_type"] = only
    elif len(types) > 1:
        changes.append(f"AMBIGUOUS: events use multiple occurrence types {sorted(types)} (left untouched)")
    return changes


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry-run" in sys.argv
    if not args:
        print("Usage: python scripts/reconcile_analysis_occurrence.py <dir> [--dry-run]"); return 1
    d = Path(args[0])
    files = [f for f in sorted(d.rglob("*.json")) if not BACKUP_RE.search(f.name)]
    nfiles = 0; ambiguous = []
    for f in files:
        doc = json.loads(f.read_text(encoding="utf-8"))
        recs = doc["datasets"] if isinstance(doc, dict) and "datasets" in doc else [doc]
        fchanges = []
        for rec in recs:
            for es in (rec.get("hazard") or {}).get("event_sets") or []:
                cs = reconcile_event_set(es)
                fchanges += cs
                if any("AMBIGUOUS" in c for c in cs):
                    ambiguous.append((f.name, [c for c in cs if "AMBIGUOUS" in c]))
        if fchanges and not all("AMBIGUOUS" in c for c in fchanges):
            nfiles += 1
            if not dry:
                f.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{'(dry-run) ' if dry else ''}files changed: {nfiles}")
    if ambiguous:
        print(f"AMBIGUOUS event_sets (NOT changed) in {len(ambiguous)} file(s):")
        for fn, cs in ambiguous[:10]:
            print(f"   {fn}: {cs}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
