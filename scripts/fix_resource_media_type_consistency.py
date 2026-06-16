"""Fix resources flagged by the Layer-4 media_type/URL consistency check.

For every resource whose declared media_type/format contradicts the type its
URL DEFINITIVELY implies, set media_type to the URL-derived value and drop a
now-conflicting free-text `format`. Uses the SAME canonical derivation as
src/audit.validate_layer4_consistency, so the fixer and the check agree by
construction. Skips *_v03 backups. Inner-archive contents stay documented in
the resource description (untouched).

Usage:
    python scripts/fix_resource_media_type_consistency.py <dir> [--dry-run]
"""
from __future__ import annotations
import json, re, sys
from collections import Counter
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from src.utils import (media_type_from_url, media_types_compatible,
                       format_label_to_media_type, normalize_media_type,
                       ZIP_INCOMPATIBLE_SINGLE_FILE_TYPES)
BACKUP = re.compile(r"_v0\.?3\.json$", re.I)


FILL_ACCESS_PAGES = False  # set via --fill-access-pages


def fix_record(rec: dict, tally: Counter) -> int:
    n = 0
    for r in rec.get("resources", []) or []:
        if not isinstance(r, dict):
            continue
        expected = None
        for k in ("download_url", "access_url"):
            expected = media_type_from_url(r.get(k))
            if expected:
                break
        if not expected or expected == "text/html":
            # Optional remediation for access-only resources: a resource with
            # no media_type/format and no download_url, whose access_url is a
            # web page (no determinable data file), is a link to an HTML page.
            # text/html is the literal content type that URL serves - honest,
            # not a data-format assumption. Opt-in via --fill-access-pages.
            if (FILL_ACCESS_PAGES and not r.get("media_type") and not r.get("format")
                    and not r.get("download_url") and r.get("access_url")):
                r["media_type"] = "text/html"
                tally[("(access-page)", "text/html")] += 1
                n += 1
            continue
        has_mt = bool(r.get("media_type"))
        has_fmt = bool(r.get("format"))
        if not has_mt and not has_fmt:
            # MISSING entirely -> fill from the definitive URL (fixes the
            # schema anyOf error). Only when the URL is definitive.
            r["media_type"] = expected
            tally[("(missing)", expected)] += 1
            n += 1
            continue
        eff = r.get("media_type") or format_label_to_media_type(r.get("format"))
        if eff is None:
            # format present but free-text we cannot map (e.g. "Geodatabase")
            # -> schema anyOf already satisfied; do not touch.
            continue
        if expected == "application/zip":
            flagged = normalize_media_type(eff) in ZIP_INCOMPATIBLE_SINGLE_FILE_TYPES
        else:
            flagged = not media_types_compatible(eff, expected)
        if flagged:
            tally[(normalize_media_type(r.get("media_type") or f"fmt:{r.get('format')}"), expected)] += 1
            r["media_type"] = expected
            r.pop("format", None)
            n += 1
    return n


def main():
    global FILL_ACCESS_PAGES
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry-run" in sys.argv
    FILL_ACCESS_PAGES = "--fill-access-pages" in sys.argv
    if not args:
        print("Usage: python scripts/fix_resource_media_type_consistency.py <dir> [--dry-run]"); return 1
    d = Path(args[0])
    files = [f for f in sorted(d.rglob("*.json")) if not BACKUP.search(f.name)]
    nfiles = nres = 0
    tally: Counter = Counter()
    for f in files:
        try:
            doc = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        recs = doc["datasets"] if isinstance(doc, dict) and isinstance(doc.get("datasets"), list) else [doc]
        changed = sum(fix_record(rec, tally) for rec in recs if isinstance(rec, dict))
        if changed:
            nfiles += 1; nres += changed
            if not dry:
                f.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{'(dry-run) ' if dry else ''}{d}: files changed={nfiles}, resources fixed={nres}")
    for (decl, exp), n in tally.most_common(8):
        print(f"    {n:>6}x  {decl} -> {exp}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
