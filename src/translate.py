"""
Generic RDLS record builder.

Translates source metadata into RDLS v0.3 JSON records.
Handles format mapping, license mapping, attribution/resource building,
and the main record assembly. Source-independent (uses field maps).
"""

import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from .utils import (
    as_list, load_yaml, looks_like_url, parse_hdx_temporal,
    sanitize_text, slugify_token, split_semicolon_list,
)
from .spatial import country_name_to_iso3, infer_spatial, load_spatial_config


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_format_config(yaml_path: Union[str, Path]) -> Dict[str, Any]:
    """Load format mapping config (aliases, skip formats, service patterns)."""
    cfg = load_yaml(yaml_path)
    # Compile service URL patterns
    service_patterns = []
    for entry in cfg.get("service_url_patterns", []):
        service_patterns.append((
            re.compile(entry["pattern"], re.IGNORECASE),
            entry["format"],
            entry["modality"],
        ))
    # Build service_formats dict: UPPER_KEY -> (format, modality)
    svc_fmts = {}
    for key, val in cfg.get("service_formats", {}).items():
        svc_fmts[key.upper()] = (val["format"], val["modality"])
    return {
        "format_aliases": cfg.get("format_aliases", {}),
        "skip_formats": set(cfg.get("skip_formats", [])),
        "zip_inner_formats": cfg.get("zip_inner_formats", []),
        "service_url_patterns": service_patterns,
        "service_formats": svc_fmts,
    }


def load_license_config(yaml_path: Union[str, Path]) -> Dict[str, Any]:
    """Load license mapping config."""
    return load_yaml(yaml_path)


# ---------------------------------------------------------------------------
# Format mapping
# ---------------------------------------------------------------------------

def detect_service_url(
    url: str,
    patterns: List[Tuple["re.Pattern", str, str]],
) -> Optional[Tuple[str, str]]:
    """Detect service URLs and return (data_format, access_modality) or None."""
    if not url:
        return None
    for pat, fmt, modality in patterns:
        if pat.search(url):
            return (fmt, modality)
    return None


def infer_format_from_name(name: str, url: str = "") -> Optional[str]:
    """Infer RDLS data_format from filename keywords (for ZIP/unknown formats)."""
    text = f"{name} {url}".lower()
    hints = [
        ("geotiff", "GeoTIFF (tif)"), ("geotif", "GeoTIFF (tif)"),
        (".tif", "GeoTIFF (tif)"), ("shapefile", "Shapefile (shp)"),
        (".shp", "Shapefile (shp)"), ("geopackage", "GeoPackage (gpkg)"),
        (".gpkg", "GeoPackage (gpkg)"), ("geodatabase", "File Geodatabase (gdb)"),
        (".gdb", "File Geodatabase (gdb)"),
        ("geojson", "GeoJSON (geojson)"), (".geojson", "GeoJSON (geojson)"),
        ("flatgeobuf", "FlatGeobuf (fgb)"),
        ("netcdf", "NetCDF (nc)"), (".nc.", "NetCDF (nc)"),
        ("parquet", "Parquet (parquet)"),
        ("_csv", "CSV (csv)"), (".csv", "CSV (csv)"),
        ("excel", "Excel (xlsx)"), ("_xlsx", "Excel (xlsx)"),
        (".xlsx", "Excel (xlsx)"), (".xls", "Excel (xlsx)"),
        ("_json", "JSON (json)"), (".json", "JSON (json)"),
        (".xml", "XML (xml)"), (".kml", "KML (kml)"),
        (".pdf", "PDF (pdf)"),
    ]
    for hint, rdls_fmt in hints:
        if hint in text:
            return rdls_fmt
    return None


def map_data_format(
    source_fmt: str,
    url: str = "",
    name: str = "",
    format_config: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Map a source format string to RDLS data_format enum value.

    Returns None for formats that should be skipped.
    """
    s = (source_fmt or "").strip().upper()
    if not format_config:
        return None

    aliases = format_config.get("format_aliases", {})
    skip = format_config.get("skip_formats", set())
    service_patterns = format_config.get("service_url_patterns", [])

    # Skip non-data formats
    if s in skip:
        return None

    # Direct alias lookup
    if s in aliases:
        return aliases[s]

    # ZIP/archive: infer from filename
    if s in ("ZIP", "7Z", "TAR", "GZ", "GZIP"):
        return infer_format_from_name(name, url)

    # URL extension guess
    u = (url or "").lower().split("?")[0]
    ext_map = [
        (".geojson", "GeoJSON (geojson)"), (".json", "JSON (json)"),
        (".csv", "CSV (csv)"), (".xlsx", "Excel (xlsx)"), (".xls", "Excel (xlsx)"),
        (".shp", "Shapefile (shp)"),
        (".tif", "GeoTIFF (tif)"), (".tiff", "GeoTIFF (tif)"),
        (".nc", "NetCDF (nc)"), (".pdf", "PDF (pdf)"),
        (".parquet", "Parquet (parquet)"), (".gpkg", "GeoPackage (gpkg)"),
        (".kml", "KML (kml)"), (".kmz", "KML (kml)"),
        (".xml", "XML (xml)"), (".gdb", "File Geodatabase (gdb)"),
    ]
    for ext, rdls in ext_map:
        if u.endswith(ext):
            return rdls

    # Last resort: infer from filename
    return infer_format_from_name(name, url)


# ---------------------------------------------------------------------------
# License mapping
# ---------------------------------------------------------------------------

def map_license(license_str: str, license_config: Optional[Dict[str, Any]] = None) -> str:
    """Map a source license string to RDLS license code.

    Uses pattern matching for Creative Commons variants.
    """
    raw = (license_str or "").strip()
    if not raw:
        return ""

    key = re.sub(r"\s+", " ", raw.lower().strip())

    # Pattern-based matching
    if re.search(r"\bcc0\b", key) or ("public domain" in key and "cc0" in key):
        return "CC0-1.0"
    if "odbl" in key or "open database license" in key:
        return "ODbL-1.0"
    if "pddl" in key or "public domain dedication" in key:
        return "PDDL-1.0"

    k2 = key.replace("creative commons", "cc")
    if re.search(r"\bcc\s*by\b", k2) and "sa" not in k2 and "nd" not in k2 and "nc" not in k2:
        return "CC-BY-4.0" if "4.0" in k2 or "v4" in k2 else ("CC-BY-3.0" if "3.0" in k2 else "CC-BY-4.0")
    if "by-sa" in k2 or re.search(r"\bcc\s*by\s*sa\b", k2):
        return "CC-BY-SA-4.0" if "4.0" in k2 else ("CC-BY-SA-3.0" if "3.0" in k2 else "CC-BY-SA-4.0")
    if "by-nc" in k2 or re.search(r"\bcc\s*by\s*nc\b", k2):
        return "CC-BY-NC-4.0" if "4.0" in k2 else ("CC-BY-NC-3.0" if "3.0" in k2 else "CC-BY-NC-4.0")

    # Config-based lookup
    if license_config:
        license_map = license_config.get("license_map", {})
        if key in license_map:
            return license_map[key]
        return license_config.get("default", raw)

    return raw


# ---------------------------------------------------------------------------
# Attribution building
# ---------------------------------------------------------------------------

def build_attributions(
    fields: Dict[str, Any],
    source_url: str = "",
) -> List[Dict[str, Any]]:
    """Build RDLS attributions array (publisher, creator, contact_point).

    Args:
        fields: Common field structure (from extract_hdx_fields or similar).
        source_url: URL to the dataset on the source platform.
    """
    org_name = fields.get("organization", "")
    maintainer = fields.get("maintainer", "")
    dataset_source = fields.get("dataset_source", "")

    attributions = []

    # Publisher = organization
    if org_name:
        attributions.append({
            "id": "attribution_publisher",
            "role": "publisher",
            "entity": {"name": sanitize_text(org_name)},
        })

    # Creator = dataset_source or organization
    creator_name = dataset_source or org_name
    if creator_name:
        attributions.append({
            "id": "attribution_creator",
            "role": "creator",
            "entity": {"name": sanitize_text(creator_name)},
        })

    # Contact point = maintainer or organization
    contact_name = maintainer or org_name
    if contact_name:
        attr = {
            "id": "attribution_contact_point",
            "role": "contact_point",
            "entity": {"name": sanitize_text(contact_name)},
        }
        if source_url:
            attr["entity"]["url"] = source_url
        attributions.append(attr)

    # Ensure minimum 3 attributions
    roles_present = {a["role"] for a in attributions}
    for role in ["publisher", "creator", "contact_point"]:
        if role not in roles_present:
            attributions.append({
                "id": f"attribution_{role}",
                "role": role,
                "entity": {"name": org_name or "Unknown"},
            })

    return attributions


# ---------------------------------------------------------------------------
# Resource building
# ---------------------------------------------------------------------------

def build_resources(
    fields: Dict[str, Any],
    format_config: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """Build RDLS resources array from source resource list.

    Args:
        fields: Common field structure with 'resources' key.
                Also reads 'dataset_date' and 'data_update_frequency' for
                temporal metadata on resources.
        format_config: Format mapping config.
    """
    resources = []
    service_patterns = format_config.get("service_url_patterns", []) if format_config else []
    service_formats = format_config.get("service_formats", {}) if format_config else {}

    # Parse dataset-level temporal for resource annotation
    temporal = parse_hdx_temporal(
        fields.get("dataset_date", ""),
        fields.get("data_update_frequency", ""),
    )

    for r in fields.get("resources", []):
        r_id = r.get("id", str(uuid.uuid4())[:8])
        r_name = r.get("name", "") or r.get("description", "") or ""
        r_url = r.get("url", "") or r.get("download_url", "") or ""
        r_fmt = r.get("format", "")
        r_desc = r.get("description", "")

        r_fmt_upper = (r_fmt or "").strip().upper()

        # Check service formats first (GEOSERVICE, API, WEB APP)
        if r_fmt_upper in service_formats:
            rdls_fmt, modality = service_formats[r_fmt_upper]
            # Refine using URL patterns (e.g., ArcGIS REST)
            svc = detect_service_url(r_url, service_patterns) if service_patterns else None
            if svc:
                rdls_fmt, modality = svc
        else:
            # Check for service URL patterns
            svc = detect_service_url(r_url, service_patterns) if service_patterns else None
            if svc:
                rdls_fmt, modality = svc
            else:
                rdls_fmt = map_data_format(r_fmt, r_url, r_name, format_config)
                modality = "file_download"

        if rdls_fmt is None:
            continue  # Skip non-data resources

        resource = {
            "id": f"resource_{r_id[:8]}",
            "title": sanitize_text(r_name) or "Data resource",
            "data_format": rdls_fmt,
            "access_modality": modality,
        }
        if r_url:
            resource["url"] = r_url
        if r_desc:
            resource["description"] = sanitize_text(r_desc)

        # Add temporal metadata from dataset-level date range
        temporal_data = {k: v for k, v in temporal.items() if v}
        if temporal_data:
            resource["temporal"] = temporal_data

        resources.append(resource)

    return resources


# ---------------------------------------------------------------------------
# Details + referenced_by
# ---------------------------------------------------------------------------

def build_details(
    fields: Dict[str, Any],
) -> Tuple[Optional[str], Optional[List[Dict[str, Any]]]]:
    """Build RDLS details field and extract referenced_by from methodology URLs.

    Composites: caveats, methodology/methodology_other.
    Temporal coverage, update frequency, and last modified are excluded.

    If methodology text contains URLs, they are extracted into referenced_by
    entries and replaced with '(see referenced_by)' in the text.

    Args:
        fields: Common field structure with optional 'caveats',
                'methodology', 'methodology_other' keys.

    Returns:
        Tuple of (details_text_or_None, referenced_by_list_or_None).
    """
    parts: List[str] = []
    referenced_by: Optional[List[Dict[str, Any]]] = None

    # Caveats
    caveats = sanitize_text(fields.get("caveats", "") or "")
    if caveats:
        parts.append(f"Caveats: {caveats}")

    # Methodology
    methodology = sanitize_text(fields.get("methodology", "") or "")
    methodology_other = sanitize_text(fields.get("methodology_other", "") or "")
    meth_text = methodology_other or (
        methodology if methodology and methodology.lower() not in ("other", "") else ""
    )

    if meth_text:
        # Extract URLs -> referenced_by
        urls = re.findall(r'https?://[^\s<>"\')\]]+', meth_text)
        if urls:
            referenced_by = []
            for i, url in enumerate(urls):
                clean_url = url.rstrip(".,;:")
                meth_text = meth_text.replace(clean_url, "(see referenced_by)")
                referenced_by.append({
                    "id": f"reference_{i + 1}",
                    "name": "Methodology documentation",
                    "author_names": [],
                    "date_published": "",
                    "url": clean_url,
                    "doi": "",
                })
        parts.append(f"Methodology: {meth_text}")

    if not parts:
        return None, None

    result = ". ".join(parts)
    if result and result[-1] not in ".!?":
        result += "."
    return result, referenced_by


# ---------------------------------------------------------------------------
# Main record builder
# ---------------------------------------------------------------------------

def build_rdls_record(
    fields: Dict[str, Any],
    components: List[str],
    spatial_config: Dict[str, Any],
    format_config: Optional[Dict[str, Any]] = None,
    license_config: Optional[Dict[str, Any]] = None,
    source_base_url: str = "",
    naming_config: Optional[Dict[str, Any]] = None,
    hazard_types: Optional[List[str]] = None,
    exposure_categories: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Build a complete RDLS dataset record from common fields.

    Args:
        fields: Common field structure (from extract_hdx_fields or similar).
        components: RDLS risk_data_type components (e.g. ["hazard", "exposure"]).
        spatial_config: Loaded spatial config (from load_spatial_config).
        format_config: Format mapping config (from load_format_config).
        license_config: License mapping config (from load_license_config).
        source_base_url: Base URL of the source platform.
        naming_config: Optional naming config for structured ID generation.
                       If None, falls back to using the raw source dataset ID.
        hazard_types: Optional list of hazard types for ID item codes.
        exposure_categories: Optional list of exposure categories for ID item codes.

    Returns:
        RDLS record dict, or None if record cannot be built.
    """
    ds_id = fields.get("id", "")
    if not ds_id:
        return None

    title = sanitize_text(fields.get("title", ""))
    if not title:
        return None

    # Map components to RDLS risk_data_type
    component_map = {
        "hazard": "hazard",
        "exposure": "exposure",
        "vulnerability_proxy": "vulnerability",
        "vulnerability": "vulnerability",
        "loss_impact": "loss",
        "loss": "loss",
    }
    risk_data_type = sorted(set(
        component_map.get(c, c) for c in components
        if component_map.get(c, c) in {"hazard", "exposure", "vulnerability", "loss"}
    ))
    if not risk_data_type:
        return None

    # Build spatial
    groups = fields.get("groups", [])
    spatial = infer_spatial(
        groups=groups,
        region_map=spatial_config.get("region_to_countries", {}),
        country_fixes=spatial_config.get("country_name_fixes", {}),
        non_country_groups=spatial_config.get("non_country_groups", set()),
    )

    # Build source URL
    source_url = ""
    if source_base_url and ds_id:
        source_url = f"{source_base_url}/dataset/{ds_id}"

    # Build resources
    resources = build_resources(fields, format_config)
    if not resources:
        return None

    # Build attributions
    attributions = build_attributions(fields, source_url)

    # Build license
    license_code = map_license(
        fields.get("license_title", ""),
        license_config,
    )

    # Generate record ID
    record_id = ds_id  # fallback: raw source ID
    if naming_config:
        from .naming import build_rdls_id
        iso3_codes = spatial.get("countries", [])
        org_name = fields.get("organization", "")
        org_slug = fields.get("org_slug", "")
        record_id = build_rdls_id(
            components=risk_data_type,
            iso3_codes=iso3_codes,
            org_name=org_name,
            org_slug=org_slug,
            config=naming_config,
            title=title,
        )

    # Assemble record
    record = {
        "id": record_id,
        "title": title,
        "risk_data_type": risk_data_type,
        "attributions": attributions,
        "spatial": spatial,
        "license": license_code or "Custom",
        "resources": resources,
    }

    # Optional fields
    notes = sanitize_text(fields.get("notes", ""))
    if notes:
        record["description"] = notes

    # Details + referenced_by from caveats/methodology
    details, referenced_by = build_details(fields)
    if details:
        record["details"] = details
    if referenced_by:
        record["referenced_by"] = referenced_by

    license_url = fields.get("license_url", "")
    if license_url:
        record["license_url"] = license_url

    return record
