"""Make every resource description self-describing by prefixing the dataset title:
a short label like "OGC WMS: geonode Service" becomes "<dataset title> (OGC WMS:
geonode Service)" (reviewer, kir AAL line 37-40; applies to all resources in all
datasets).

Only SHORT LABELS are wrapped - a description that is already a full readable
sentence, or that already starts with the dataset title (e.g. the format-derived
ones from fix_resource_descriptions.py: "<title> (GeoPackage)"), is left as-is, so
this is safe to run after that fixer and is idempotent.

"Short label" = non-empty, <= 80 chars, single line, no sentence punctuation
(". "/"! "/"? " mid-text), and does not already begin with the dataset title.

Usage:
    python scripts/wrap_resource_descriptions.py <dir> [...] [--dry-run]
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path
from collections import Counter

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from src.utils import sanitize_text

BACKUP = re.compile(r"_v0\.?3\.json$", re.I)
SENTENCE = re.compile(r"[.!?]\s")


def is_short_label(d, title):
    if not d or len(d) > 80 or "\n" in d:
        return False
    if SENTENCE.search(d):
        return False
    if title and d.lower().startswith(title.lower()[:25]):
        return False
    return True


def fix_record(rec, stats: Counter, examples: list):
    title = sanitize_text(rec.get("title") or "").strip()
    if not title:
        return 0
    changed = 0
    for r in rec.get("resources", []) or []:
        if not isinstance(r, dict):
            continue
        d = (r.get("description") or "").strip()
        if is_short_label(d, title):
            new = f"{title} ({d})"[:500]
            if new != d:
                if len(examples) < 12:
                    examples.append(f"'{d}' -> '{new[:75]}'")
                r["description"] = new
                stats["wrapped"] += 1
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
    stats = Counter(); examples = []; nfiles = 0
    for f in files:
        try:
            doc = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        recs = doc["datasets"] if isinstance(doc, dict) and isinstance(doc.get("datasets"), list) else [doc]
        changed = sum(fix_record(rec, stats, examples) for rec in recs if isinstance(rec, dict))
        if changed:
            nfiles += 1
            if not dry:
                f.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{'(dry-run) ' if dry else ''}files changed: {nfiles}   descriptions wrapped: {stats['wrapped']}")
    for ex in examples:
        print(f"   {ex}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
