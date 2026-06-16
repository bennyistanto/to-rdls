"""Strip trailing dataset tag-codes from `title` (e.g. "CoCliCo - Flood Maps
[cfhp_all]" -> "CoCliCo - Flood Maps").

The reviewer: a title should not carry source tag-codes like [cfhp_all]; the code
already lives in the record id/slug, so it is dropped (not duplicated into the
description). ONLY a bracketed token at the very END of the title is removed -
a mid-title bracket (e.g. "Somalia Displacement - [IDPs] - Emergency...") is an
acronym in the dataset's real name and is left untouched.

Usage:
    python scripts/fix_title_tag_codes.py <dir> [...] [--dry-run]
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path

BACKUP = re.compile(r"_v0\.?3\.json$", re.I)
# trailing " [token]" where token is a code: letters/digits/_/-, no spaces inside
TRAILING_TAG = re.compile(r"\s*\[[A-Za-z0-9_\-]+\]\s*$")


def fix_record(rec, changes: list):
    t = rec.get("title")
    if not isinstance(t, str):
        return 0
    new = TRAILING_TAG.sub("", t)
    # clean a dangling separator left where the tag used to be (" - ", ":", "|")
    new = re.sub(r"[\s\-:|·•]+$", "", new).strip()
    if new and new != t:
        rec["title"] = new
        changes.append((t, new))
        return 1
    return 0


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry-run" in sys.argv
    if not args:
        print("Usage: python scripts/fix_title_tag_codes.py <dir> [...] [--dry-run]")
        return 1
    files = []
    for a in args:
        p = Path(a)
        if p.is_dir():
            files += [f for f in p.rglob("*.json") if not BACKUP.search(f.name)]
        else:
            files += [f for f in Path().glob(a) if not BACKUP.search(f.name)]
    files = sorted(set(files))

    changes = []
    nfiles = 0
    for f in files:
        try:
            doc = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        recs = doc["datasets"] if isinstance(doc, dict) and isinstance(doc.get("datasets"), list) else [doc]
        changed = sum(fix_record(rec, changes) for rec in recs if isinstance(rec, dict))
        if changed:
            nfiles += 1
            if not dry:
                f.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"{'(dry-run) ' if dry else ''}titles changed: {nfiles}")
    for old, new in changes[:40]:
        print(f"   '{old}'  ->  '{new}'")
    if len(changes) > 40:
        print(f"   ... +{len(changes)-40} more")
    return 0


if __name__ == "__main__":
    sys.exit(main())
