"""
Post-conversion enrichment for RDLS v1.0 JSON files.

Run this on EVERY newly converted file before considering the conversion done.
Applies all mechanical fixes established during the batch conversion sessions.

Usage:
    python scripts/post_convert_enrich.py "output/new-collection/**/*.json"
    python scripts/post_convert_enrich.py output/some/file.json
    python scripts/post_convert_enrich.py output/new-collection/  # all *.json in folder

What this script fixes automatically:
    1. unit=count   - measurement.unit="count" where quantity_kind="count" and unit absent
    2. URI fix      - broken GED4ALL gemproducts URL (404) replaced with wiki URL
    3. scheme fix   - invalid scheme="Custom" removed (not a valid classification_scheme.csv code)
    4. license fix  - "Commercial" and TODO license strings removed from lineage.sources
    5. IM fix       - intensity_measure="wd:m" on flood/coastal_flood losses with TODO IM
    6. format fix   - removes resource.format when resource.media_type is present (never both);
                      adds resource.conforms_to URI when access_modality is OGC_API/STAC/WMS/WFS/WCS

What still requires manual work per dataset (printed as warnings):
    - asset_type.id = "GED4ALL" or "Custom" -> needs meaningful per-item identifier
    - asset_type with no id at all
    - asset_type with no title
    - asset_type with no description
    - GED4ALL scheme assignment -> dataset-specific (see note below)

NOTE on GED4ALL scheme - NOT automated here:
    Generic IDs like "exposure_buildings" and "exposure_population" appear in both
    GED4ALL datasets (ICRA, SDK, TC) and custom-taxonomy datasets (FIAT, GFRT).
    Automated restoration cannot distinguish between them. Always set scheme in the
    dataset-specific enrichment script (follow temp/enrich_wbgufra_asset_type.py).

    GED4ALL 10 categories (apply scheme="GED4ALL" + uri when dataset uses GED4ALL):
        1. Complete Buildings  2. Simplified Buildings  3. Road Network
        4. Railway Network     5. Bridges               6. Pipelines and Storage Tanks
        7. Power Grids         8. Energy Generation     9. Crops/Livestock/Forestry
        10. Socio-Economic (population, GDP, economic indicators)
    Absent for: land_cover, mining, landuse, custom taxonomies (FIAT, GFRT, etc.)
    URI when scheme=GED4ALL: https://wiki.openstreetmap.org/wiki/GED4ALL
    asset_type.id must be a per-item label (e.g. "buildings", "population", "roads")
    NOT the scheme name ("GED4ALL" or "Custom")

media_type vs format rule (Fix 6 - automated):
    media_type and format are mutually exclusive. When media_type is present,
    format is automatically removed. When media_type is absent, format is kept.
    If format has no IANA media type code, keep format and leave media_type absent.
    Common IANA codes: GeoTIFF=image/tiff;application=geotiff,
                       SHP=application/x-shapefile, GPKG=application/geopackage+sqlite3,
                       GDB=application/x-filegdb

conforms_to rule (Fix 6 - automated):
    When access_modality is OGC_API/STAC/WMS/WFS/WCS, conforms_to is set from the
    conforms_to.csv codelist. Mapping used:
        OGC_API -> http://www.opengis.net/doc/IS/ogcapi-features-1/1.0.1
        STAC    -> https://api.stacspec.org/v1.0.0/
        WMS     -> http://www.opengis.net/def/serviceType/ogc/wms
        WFS     -> http://www.opengis.net/def/serviceType/ogc/wfs
        WCS     -> http://www.opengis.net/def/serviceType/ogc/wcs
    Resources already having conforms_to are left unchanged.

Backup/rename convention (must be done BEFORE running this script):
    Original v0.3 -> <stem>_v03.json (backup)
    v1.0 takes the original filename (no _v10 or _rev suffix)

See also: .claude/v1.0-reference.md#post-conversion-checklist for the full checklist.
"""

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent

OLD_GED4ALL_URI = "https://www.globalquakemodel.org/gemproducts/ged4all"
NEW_GED4ALL_URI = "https://wiki.openstreetmap.org/wiki/GED4ALL"

TODO_LICENSE_MARKER = "TODO"
COMMERCIAL_LICENSE = "Commercial"

# Fix 6: conforms_to URIs keyed by access_modality value (from conforms_to.csv)
# Only access modalities that map to a formal OGC/STAC standard are listed here.
# REST, API, file_download, download_page, dashboard have no standard URI.
CONFORMS_TO_MAP = {
    "OGC_API": "http://www.opengis.net/doc/IS/ogcapi-features-1/1.0.1",
    "STAC":    "https://api.stacspec.org/v1.0.0/",
    "WMS":     "http://www.opengis.net/def/serviceType/ogc/wms",
    "WFS":     "http://www.opengis.net/def/serviceType/ogc/wfs",
    "WCS":     "http://www.opengis.net/def/serviceType/ogc/wcs",
}

def fix_file(path: Path) -> dict:
    """Apply all mechanical enrichment fixes. Returns dict with counts of changes."""
    data = json.loads(path.read_text(encoding="utf-8"))
    counts = {
        "unit_count": 0,
        "uri_fix": 0,
        "scheme_custom_removed": 0,
        "license_removed": 0,
        "im_fix": 0,
        "format_removed": 0,
        "conforms_to_added": 0,
    }
    changed = False
    warnings = []

    datasets = data if isinstance(data, list) else data.get("datasets", [])
    for ds in datasets:
        if not isinstance(ds, dict):
            continue

        # Exposure: fix asset_type issues + unit=count
        for exp in ds.get("exposure", []):
            if not isinstance(exp, dict):
                continue
            eid = exp.get("id", "")

            at = exp.get("asset_type")
            if isinstance(at, dict):
                # Fix 2: broken GED4ALL URI
                if at.get("uri") == OLD_GED4ALL_URI:
                    at["uri"] = NEW_GED4ALL_URI
                    counts["uri_fix"] += 1
                    changed = True

                # Fix 3: scheme="Custom" (not a valid classification_scheme.csv code)
                if at.get("scheme") == "Custom":
                    del at["scheme"]
                    counts["scheme_custom_removed"] += 1
                    changed = True

                # Warnings for items still needing manual attention
                # GED4ALL scheme is NOT auto-set here: generic IDs (exposure_buildings,
                # exposure_population) appear in both GED4ALL and custom-taxonomy datasets.
                # Always assign scheme in the dataset-specific enrichment script.
                at_id = at.get("id")
                if at_id in ("GED4ALL", "Custom", None):
                    warnings.append(
                        f"  exposure.{eid}: asset_type.id={at_id!r} - "
                        f"needs meaningful per-item identifier"
                    )
                if not at.get("title"):
                    warnings.append(f"  exposure.{eid}: asset_type.title missing")
                if not at.get("description"):
                    warnings.append(f"  exposure.{eid}: asset_type.description missing")
            elif exp.get("category"):
                # exposure has no asset_type at all
                warnings.append(
                    f"  exposure.{eid}: no asset_type object - "
                    f"needs id/title/description/scheme"
                )

            # Fix unit=count for qk=count metrics
            for m in exp.get("metrics", []):
                meas = m.get("measurement")
                if isinstance(meas, dict):
                    if meas.get("quantity_kind") == "count" and not meas.get("unit"):
                        meas["unit"] = "count"
                        counts["unit_count"] += 1
                        changed = True

        # Fix 4: remove Commercial/TODO licenses from lineage.sources
        for src in ds.get("lineage", {}).get("sources", []):
            if not isinstance(src, dict):
                continue
            lic = src.get("license", "")
            if lic == COMMERCIAL_LICENSE or (isinstance(lic, str) and TODO_LICENSE_MARKER in lic):
                del src["license"]
                counts["license_removed"] += 1
                changed = True

        # Fix 5: set wd:m on flood/coastal_flood losses with TODO intensity_measure
        # NOTE: only fires when IM is a TODO string - never when IM is simply absent.
        # Observational/empirical loss datasets (e.g. DesInventar) legitimately have
        # no IM - do NOT add wd:m to them.
        for loss in ds.get("loss", {}).get("losses", []):
            if not isinstance(loss, dict):
                continue
            haz = loss.get("hazard")
            if isinstance(haz, dict):
                htype = haz.get("type", "")
                im = haz.get("intensity_measure", "")
                if (
                    htype in ("flood", "coastal_flood")
                    and isinstance(im, str)
                    and TODO_LICENSE_MARKER in im
                ):
                    haz["intensity_measure"] = "wd:m"
                    counts["im_fix"] += 1
                    changed = True

        # Fix 6: media_type/format mutual exclusion + conforms_to for OGC/STAC resources
        for r in ds.get("resources", []):
            if not isinstance(r, dict):
                continue

            # Remove format when media_type is present (they are mutually exclusive)
            if "media_type" in r and "format" in r:
                del r["format"]
                counts["format_removed"] += 1
                changed = True

            # Add conforms_to when access_modality maps to a formal standard
            am = r.get("access_modality", "")
            if am in CONFORMS_TO_MAP and "conforms_to" not in r:
                r["conforms_to"] = CONFORMS_TO_MAP[am]
                counts["conforms_to_added"] += 1
                changed = True

    if changed:
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    return {"counts": counts, "warnings": warnings, "changed": changed}


def resolve_files(args: list[str]) -> list[Path]:
    """Resolve CLI args to a list of JSON paths, excluding _v03 backups."""
    files = []
    root = PROJECT_ROOT
    for arg in args:
        p = Path(arg)
        if not p.is_absolute():
            p = root / p
        if p.is_dir():
            files.extend(
                f for f in sorted(p.glob("*.json")) if "_v03" not in f.name
            )
        elif "*" in str(arg) or "?" in str(arg):
            files.extend(
                f for f in sorted(root.glob(arg)) if "_v03" not in f.name
            )
        elif p.exists():
            if "_v03" not in p.name:
                files.append(p)
        else:
            print(f"WARNING: not found: {arg}")
    return files


def main():
    if len(sys.argv) < 2:
        print(__doc__)
        sys.exit(1)

    files = resolve_files(sys.argv[1:])
    if not files:
        print("No matching files found.")
        sys.exit(1)

    print(f"\n=== Post-conversion enrichment: {len(files)} file(s) ===\n")

    total = {k: 0 for k in ("unit_count", "uri_fix", "scheme_custom_removed",
                             "license_removed", "im_fix", "format_removed",
                             "conforms_to_added")}
    all_warnings = []

    for path in files:
        try:
            result = fix_file(path)
        except Exception as e:
            print(f"  ERROR {path.name}: {e}")
            continue

        c = result["counts"]
        parts = []
        if c["unit_count"]:
            parts.append(f"unit=count x{c['unit_count']}")
        if c["uri_fix"]:
            parts.append(f"URI x{c['uri_fix']}")
        if c["scheme_custom_removed"]:
            parts.append(f"Custom-removed x{c['scheme_custom_removed']}")
        if c["license_removed"]:
            parts.append(f"license-removed x{c['license_removed']}")
        if c["im_fix"]:
            parts.append(f"IM=wd:m x{c['im_fix']}")
        if c["format_removed"]:
            parts.append(f"format-removed x{c['format_removed']}")
        if c["conforms_to_added"]:
            parts.append(f"conforms_to x{c['conforms_to_added']}")
        status = ", ".join(parts) if parts else "no changes needed"
        print(f"  {path.name}: {status}")

        for k in total:
            total[k] += c[k]

        if result["warnings"]:
            all_warnings.append((path.name, result["warnings"]))

    print(f"\nTotal: unit_count={total['unit_count']} uri={total['uri_fix']} "
          f"custom-removed={total['scheme_custom_removed']} license={total['license_removed']} "
          f"im={total['im_fix']} format-removed={total['format_removed']} "
          f"conforms_to={total['conforms_to_added']}")

    if all_warnings:
        print("\n=== Manual attention required ===\n")
        for fname, warns in all_warnings:
            print(f"  {fname}:")
            for w in warns:
                print(w)
        print(
            "\nFor each item above, create a dataset-specific enrichment script in temp/\n"
            "following the pattern in temp/enrich_wbgufra_asset_type.py.\n"
            "See .claude/v1.0-reference.md -> Post-conversion checklist for the full guide."
        )
    else:
        print("\nNo manual fixes needed for asset_type fields.")

    # Reminder for checks that cannot be automated
    print("\n=== Reminders (manual checks always required) ===")
    print("  [ ] asset_type: id/title/description/scheme/uri -> dataset-specific enrichment script")
    print("  [ ] GED4ALL scheme: set in specific script (NOT auto-detected by this script)")
    print("  [ ] media_type: if format has no IANA code, keep format and leave media_type absent")
    print("      Check rdl-standard/schema/codelists/open/media_type.csv for available codes")
    print("  [ ] conforms_to: OGC_API maps to Features 1.0.1 - override if resource is Coverages/Tiles")
    print("  [ ] Backup/rename: original v0.3 -> <stem>_v03.json, v1.0 keeps original name")
    print("  [ ] Layer 1 validation: python scripts/validate_v1.0.py <file>")


if __name__ == "__main__":
    main()
