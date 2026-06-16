"""Flag records whose TITLE/DESCRIPTION mention hazards (or flood sub-processes)
that are NOT represented in the structured hazard block. Produces a REVIEW LIST
only - it never edits data (these need per-record judgment against the source).

Catches the class the reviewer found:
  - Pacific AAL: text says multi-hazard (cyclone, flood, earthquake, tsunami) but
    the hazard block lists only 'flood'.
  - PhuQuoc: id/description says fluvial+pluvial+coastal flood but only 'fluvial'.

Heuristic (keyword -> canonical hazard_type / flood process). False positives are
expected; output is for a human to triage, not to auto-apply.

Usage:
    python scripts/detect_hazard_scope_mismatch.py <dir> [...] [--limit N]
"""
from __future__ import annotations
import re, sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from src.utils import load_json

BACKUP = re.compile(r"_v0\.?3\.json$", re.I)

# keyword (regex, word-ish) -> canonical hazard_type
HAZARD_KW = {
    r"\bflood": "flood",
    r"\bearthquake|\bseismic|ground shaking|ground motion": "earthquake",
    r"\btsunami": "tsunami",
    r"\bcyclone|\btyphoon|\bhurricane": "strong_wind",
    r"\bdrought": "drought",
    r"\blandslide": "landslide",
    r"\bwildfire|bushfire": "wildfire",
    r"volcan|ashfall|\blahar": "volcanic",
    r"heatwave|extreme heat|extreme cold|heat stress": "extreme_temperature",
}
# flood sub-process keywords
FLOOD_PROC_KW = {
    r"\bfluvial|riverine|river flood": "fluvial_flood",
    r"\bpluvial|surface water|rainfall flood": "pluvial_flood",
    r"coastal flood|storm surge|\bsurge|coastal/surge": "coastal_flood",
}


def _struct_hazards(rec):
    """Set of hazard types + flood processes present in the structured blocks."""
    types, procs = set(), set()
    def add(h):
        if isinstance(h, dict):
            if h.get("type"): types.add(h["type"])
            if h.get("process"): procs.add(h["process"])
    for es in rec.get("hazard", {}).get("event_sets", []) or []:
        for h in es.get("hazards", []) or []:
            add(h)
            add(h.get("trigger"))
        for ev in es.get("events", []) or []:
            add(ev.get("hazard"))
            if isinstance(ev.get("hazard"), dict):
                add(ev["hazard"].get("trigger"))
    for l in rec.get("loss", {}).get("losses", []) or []:
        add(l.get("hazard"))
    vuln = rec.get("vulnerability", {})
    for arr in (vuln.get("functions") or {}).values():
        if isinstance(arr, list):
            for fn in arr:
                if isinstance(fn, dict):
                    add(fn.get("hazard_primary")); add(fn.get("hazard_secondary"))
    return types, procs


def _mentioned(text, kwmap):
    found = set()
    low = text.lower()
    for pat, canon in kwmap.items():
        if re.search(pat, low):
            found.add(canon)
    return found


def check(rec):
    text = f"{rec.get('title','')} . {rec.get('description','')}"
    for l in rec.get("loss", {}).get("losses", []) or []:
        text += " . " + (l.get("description") or "")
    types, procs = _struct_hazards(rec)
    if not types:
        return None  # no hazard block to compare against
    ment_types = _mentioned(text, HAZARD_KW)
    missing_types = ment_types - types
    # flood-process check only when flood is in scope
    missing_procs = set()
    if "flood" in types or "flood" in ment_types:
        ment_procs = _mentioned(text, FLOOD_PROC_KW)
        missing_procs = ment_procs - procs
    if missing_types or missing_procs:
        return (sorted(missing_types), sorted(missing_procs), sorted(types), sorted(procs))
    return None


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    limit = None
    for a in sys.argv:
        if a.startswith("--limit"):
            try: limit = int(a.split("=")[1]) if "=" in a else int(sys.argv[sys.argv.index(a)+1])
            except Exception: limit = None
    files = []
    for a in args:
        p = Path(a)
        if p.is_dir():
            files += [f for f in p.rglob("*.json") if not BACKUP.search(f.name)]
        else:
            files += [f for f in Path().glob(a) if not BACKUP.search(f.name)]
    files = sorted(set(files))

    hits = []
    for f in files:
        try:
            raw = load_json(str(f))
        except Exception:
            continue
        recs = raw["datasets"] if isinstance(raw, dict) and isinstance(raw.get("datasets"), list) else [raw]
        for rec in recs:
            if not isinstance(rec, dict):
                continue
            r = check(rec)
            if r:
                hits.append((f.name, *r))
    print(f"records with possible hazard-scope mismatch: {len(hits)}")
    print("(text mentions a hazard/flood-process absent from the structured block - REVIEW, not auto-fix)\n")
    for name, mt, mp, t, p in hits[:(limit or 60)]:
        bits = []
        if mt: bits.append(f"types missing: {mt}")
        if mp: bits.append(f"flood procs missing: {mp}")
        print(f"  {name}\n     {'; '.join(bits)}  (have types={t} procs={p})")
    if len(hits) > (limit or 60):
        print(f"  ... +{len(hits)-(limit or 60)} more")
    return 0


if __name__ == "__main__":
    sys.exit(main())
