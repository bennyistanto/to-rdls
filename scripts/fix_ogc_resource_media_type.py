"""Add media_type to OGC service resources that lack media_type AND format.

Root cause: src/translate_geonode_v10.py:_pick_media_type returned ("","") for
OGC services, so WMS/WFS/WCS resources were emitted with neither media_type nor
format -> they fail the v1.0 schema anyOf rule (media_type OR format required).

Fix is FACT-BASED, derived from each resource's own metadata:
  - conforms_to -> serviceType/ogc/{wms|wfs|wcs}
  - download_url/access_url `format=` / `outputFormat=` parameter

Mapping (confirmed against media_type.csv open codelist and the 3 combos that
actually occur in the geonode output):
  WMS (GetMap, format=image/png)        -> image/png
  WFS (GetFeature, outputFormat=json)   -> application/geo+json   (GeoServer JSON = GeoJSON)
  WCS (coverage, raster)                -> image/tiff;application=geotiff

Only touches resources that (a) lack BOTH media_type and format AND (b) carry an
OGC serviceType conforms_to. Everything else is left intact. Skips v0.3 backups.

Usage:
    python scripts/fix_ogc_resource_media_type.py <dir> [--dry-run]
"""
from __future__ import annotations
import json, re, sys, urllib.parse
from pathlib import Path

BACKUP_RE = re.compile(r"_v0\.?3\.json$", re.I)


def ogc_media_type(resource: dict):
    """Return the correct media_type for an OGC service resource, or None if
    the resource is not an OGC service / already has media_type|format."""
    if resource.get("media_type") or resource.get("format"):
        return None
    ct = resource.get("conforms_to", "") or ""
    m = re.search(r"serviceType/ogc/(\w+)", ct)
    if not m:
        return None
    svc = m.group(1).lower()
    url = resource.get("download_url", "") or resource.get("access_url", "") or ""
    fmt = re.search(r"(?:outputFormat|format)=([^&]+)", url)
    fmt_val = urllib.parse.unquote(fmt.group(1)).lower() if fmt else ""
    if svc == "wms":
        # GetMap renders an image; honour the format param, default png
        if "tiff" in fmt_val or "geotiff" in fmt_val:
            return "image/tiff;application=geotiff"
        return "image/png"
    if svc == "wfs":
        # GeoServer GetFeature application/json => GeoJSON
        if "json" in fmt_val:
            return "application/geo+json"
        if "csv" in fmt_val:
            return "text/csv"
        return "application/gml+xml"
    if svc == "wcs":
        # Coverage service => raster GeoTIFF
        return "image/tiff;application=geotiff"
    return None


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry-run" in sys.argv
    if not args:
        print("Usage: python scripts/fix_ogc_resource_media_type.py <dir> [--dry-run]"); return 1
    d = Path(args[0])
    files = [f for f in sorted(d.rglob("*.json")) if not BACKUP_RE.search(f.name)]
    nfiles = nres = 0
    from collections import Counter
    tally = Counter()
    for f in files:
        doc = json.loads(f.read_text(encoding="utf-8"))
        recs = doc["datasets"] if isinstance(doc, dict) and "datasets" in doc else [doc]
        changed = 0
        for rec in recs:
            for r in rec.get("resources", []):
                mt = ogc_media_type(r)
                if mt:
                    r["media_type"] = mt
                    changed += 1; tally[mt] += 1
        if changed:
            nfiles += 1; nres += changed
            if not dry:
                f.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"{'(dry-run) ' if dry else ''}files changed: {nfiles}, OGC resources fixed: {nres}")
    for mt, n in tally.most_common():
        print(f"   {n:>5}x  {mt}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
