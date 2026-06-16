"""Make unreadable resource descriptions readable, deriving ONLY from facts
already in the record (dataset title + the resource's format). No invention.

The reviewer flagged resource descriptions that are bare codes/filenames, e.g.
    "title": "ki_island_aal_tc_eq", "description": "ki_island_aal_tc_eq"
A description is treated as "unreadable" and replaced when it is:
  - empty, OR
  - a bare code/slug (no internal spaces, matches ^[\\w\\-.]+$), OR
  - identical to the resource title AND that title is itself a bare code/slug.
Descriptions that read as prose (contain spaces, e.g. "OGC WMS: geonode Service",
"Zipped Shapefile", "DTM Somali - Bay region (Drought) - R33") are LEFT ALONE.

Replacement = the dataset title (a human-readable fact) plus the resource's
format label in parentheses when derivable from media_type/format, e.g.
    "Annual Average Loss from Tropical Cyclones and Earthquakes for Kiribati (GeoPackage)"

Usage:
    python scripts/fix_resource_descriptions.py <dir> [...] [--dry-run]
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path
from collections import Counter

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from src.utils import sanitize_text

BACKUP = re.compile(r"_v0\.?3\.json$", re.I)
SLUG = re.compile(r"^[\w\-.]+$")  # no spaces -> code/filename-like

# media_type -> short human format label (definitive only; unknown -> no suffix)
_MEDIA_LABEL = {
    "text/csv": "CSV",
    "application/json": "JSON",
    "application/geo+json": "GeoJSON",
    "application/vnd.shp": "Shapefile",
    "application/geopackage+sqlite3": "GeoPackage",
    "application/x-netcdf": "NetCDF",
    "application/netcdf": "NetCDF",
    "application/zip": "ZIP archive",
    "application/pdf": "PDF",
    "application/vnd.google-earth.kml+xml": "KML",
    "application/vnd.google-earth.kmz": "KMZ",
    "image/tiff; application=geotiff": "GeoTIFF",
    "application/geotiff": "GeoTIFF",
    "application/x-geotiff": "GeoTIFF",
    "application/vnd.ms-excel": "Excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "Excel",
    "application/x-parquet": "Parquet",
    "text/plain": "Text",
    "text/html": "Web page",
    "application/xml": "XML",
    "text/xml": "XML",
}


def _is_unreadable(desc, title):
    d = (desc or "").strip()
    t = (title or "").strip()
    if not d:
        return True
    if " " not in d and SLUG.match(d):
        return True
    if d == t and " " not in t and SLUG.match(t):
        return True
    return False


def _format_label(res):
    fmt = (res.get("format") or "").strip()
    if fmt:
        return fmt
    mt = (res.get("media_type") or "").strip().lower()
    # normalise spacing around ';' (image/tiff;application=geotiff vs '; application=')
    mt = re.sub(r"\s*;\s*", "; ", mt)
    return _MEDIA_LABEL.get(mt, "")


def fix_record(rec, stats: Counter, examples: list):
    title = sanitize_text(rec.get("title") or "").strip()
    if not title:
        return 0  # no readable dataset fact to derive from
    changed = 0
    for res in rec.get("resources", []) or []:
        if not isinstance(res, dict):
            continue
        if _is_unreadable(res.get("description"), res.get("title")):
            label = _format_label(res)
            new = f"{title} ({label})" if label else title
            new = new[:500]
            if new and new != (res.get("description") or ""):
                if len(examples) < 12:
                    examples.append(f"'{res.get('description')}' -> '{new[:70]}'")
                res["description"] = new
                stats["fixed"] += 1
                changed += 1
    return changed


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry-run" in sys.argv
    if not args:
        print("Usage: python scripts/fix_resource_descriptions.py <dir> [...] [--dry-run]")
        return 1
    files = []
    for a in args:
        p = Path(a)
        if p.is_dir():
            files += [f for f in p.rglob("*.json") if not BACKUP.search(f.name)]
        else:
            files += [f for f in Path().glob(a) if not BACKUP.search(f.name)]
    files = sorted(set(files))

    stats = Counter()
    examples = []
    nfiles = 0
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

    print(f"{'(dry-run) ' if dry else ''}files changed: {nfiles}   resource descriptions rewritten: {stats['fixed']}")
    for ex in examples:
        print(f"   {ex}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
