"""v1.0 base record builder for HDX datasets.

Builds the RDLS v1.0 dataset record (minus HEVL blocks) from raw HDX metadata.
Handles: media_type mapping (IANA), license IRI, publisher/creator/contact_point
entities, resource building, spatial inference, ID generation.

Key v1.0 differences from translate.py (v0.3):
- publisher, creator, contact_point: top-level entity fields (not in attributions)
- resources: media_type (IANA) + format (free text) are mutually exclusive
- license: IRI URL string, not codelist code
- links.href: 1__0__0
- project: object {name, url} (not a plain string)
"""

from __future__ import annotations

import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .codelists import load_codelists_v10 as _load_cl_v10
from .naming import build_rdls_id, load_naming_config
from .spatial import infer_spatial, load_spatial_config
from .utils import load_yaml, sanitize_text, sort_rdt_hevl

# Closed codelist of valid ISO3 country codes (249 codes; loaded once at import time).
# LLM can hallucinate codes like "XKX" (Kosovo) or "ANT" that are not in the schema.
# Filter all country lists against this set before writing to the record.
_VALID_COUNTRY_CODES: frozenset = _load_cl_v10().get("country", frozenset())


# ---------------------------------------------------------------------------
# Media type mapping (HDX format string -> IANA media_type code)
# Source: rdl-standard/schema/codelists/open/media_type.csv
# Keys are uppercase for case-insensitive lookup.
# ---------------------------------------------------------------------------

MEDIA_TYPE_MAP: Dict[str, str] = {
    # GeoTIFF / raster
    "GEOTIFF": "image/tiff;application=geotiff",
    "GEOTIFF (TIF)": "image/tiff;application=geotiff",
    "TIF": "image/tiff;application=geotiff",
    "TIFF": "image/tiff;application=geotiff",
    "COG": "image/tiff;application=geotiff;profile=cloud-optimized",
    "CLOUD OPTIMIZED GEOTIFF": "image/tiff;application=geotiff;profile=cloud-optimized",
    # NetCDF / HDF / Zarr
    "NETCDF": "application/netcdf",
    "NETCDF (NC)": "application/netcdf",
    "NC": "application/netcdf",
    "HDF5": "application/x-hdf5",
    "HDF5 (HDF5)": "application/x-hdf5",
    "HDF": "application/x-hdf",
    "ZARR": "application/vnd.zarr",
    # Vector
    "GEOJSON": "application/geo+json",
    "GEOJSON (GEOJSON)": "application/geo+json",
    "SHP": "application/vnd.shp",
    "SHAPEFILE": "application/vnd.shp",
    "SHAPEFILE (SHP)": "application/vnd.shp",
    "GPKG": "application/geopackage+sqlite3",
    "GEOPACKAGE": "application/geopackage+sqlite3",
    "GEOPACKAGE (GPKG)": "application/geopackage+sqlite3",
    "GDB": "application/x-filegdb",
    "FILE GEODATABASE": "application/x-filegdb",
    "FILE GEODATABASE (GDB)": "application/x-filegdb",
    "FGB": "application/vnd.flatgeobuf",
    "FLATGEOBUF": "application/vnd.flatgeobuf",
    "FLATGEOBUF (FGB)": "application/vnd.flatgeobuf",
    "KML": "application/vnd.google-earth.kml+xml",
    "KML (KML)": "application/vnd.google-earth.kml+xml",
    "KMZ": "application/vnd.google-earth.kml+xml",
    # Tabular
    "CSV": "text/csv",
    "CSV (CSV)": "text/csv",
    "XLSX": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "XLS": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "EXCEL": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "EXCEL (XLSX)": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "PARQUET": "application/vnd.apache.parquet",
    "PARQUET (PARQUET)": "application/vnd.apache.parquet",
    # Documents / other
    "JSON": "application/json",
    "JSON (JSON)": "application/json",
    "XML": "application/xml",
    "XML (XML)": "application/xml",
    "PDF": "application/pdf",
    "PDF (PDF)": "application/pdf",
    "TXT": "text/plain",
    "TEXT": "text/plain",
    "TEXT (TXT)": "text/plain",
}

# Formats that have no IANA entry: use the `format` field (free text), no media_type
FORMAT_ONLY_MAP: Dict[str, str] = {
    "GRIB": "GRIB",
    "GRIB2": "GRIB2",
    "GRIB (GRIB)": "GRIB",
    "GRIB2 (GRIB2)": "GRIB2",
    "LAS": "LAS point cloud",
    "LAZ": "LAZ compressed point cloud",
    "MBT": "MBTiles",
    "PMTILES": "PMTiles",
    "3DTILES": "3D Tiles",
}

# Service/API formats -> (None, access_modality)
# These have no meaningful media_type but must set access_modality correctly.
SERVICE_MODALITY_MAP: Dict[str, str] = {
    "WMS": "WMS",
    "WFS": "WFS",
    "WCS": "WCS",
    "OGC API": "OGC_API",
    "OGC_API": "OGC_API",
    "STAC": "STAC",
    "REST": "REST",
    "API": "API",
    "GEOSERVICE": "API",
    "WEB APP": "dashboard",
    "DASHBOARD": "dashboard",
}

# access_modality -> conforms_to URI
CONFORMS_TO_MAP: Dict[str, str] = {
    "OGC_API": "http://www.opengis.net/doc/IS/ogcapi-features-1/1.0.1",
    "STAC": "https://api.stacspec.org/v1.0.0/",
    "WMS": "http://www.opengis.net/def/serviceType/ogc/wms",
    "WFS": "http://www.opengis.net/def/serviceType/ogc/wfs",
    "WCS": "http://www.opengis.net/def/serviceType/ogc/wcs",
}


# ---------------------------------------------------------------------------
# License mapping: HDX title/id -> v1.0 IRI URL
# v1.0 requires an IRI, not a code.
# ---------------------------------------------------------------------------

LICENSE_URL_MAP: Dict[str, str] = {
    # CC0 / Public Domain
    "cc0-1.0": "https://creativecommons.org/publicdomain/zero/1.0/",
    "cc0": "https://creativecommons.org/publicdomain/zero/1.0/",
    "public domain": "https://creativecommons.org/publicdomain/zero/1.0/",
    "hdx-pddl": "https://opendatacommons.org/licenses/pddl/1-0/",
    "pddl-1.0": "https://opendatacommons.org/licenses/pddl/1-0/",
    # CC BY
    "cc-by-4.0": "https://creativecommons.org/licenses/by/4.0/",
    "cc-by-3.0": "https://creativecommons.org/licenses/by/3.0/",
    "cc-by-2.0": "https://creativecommons.org/licenses/by/2.0/",
    "creative commons attribution international (cc by)": "https://creativecommons.org/licenses/by/4.0/",
    "creative commons attribution for intergovernmental organisations": "https://creativecommons.org/licenses/by/4.0/",
    "hdx-odc-by": "https://opendatacommons.org/licenses/by/1-0/",
    "odc-by-1.0": "https://opendatacommons.org/licenses/by/1-0/",
    "open-by": "https://creativecommons.org/licenses/by/4.0/",
    # CC BY-SA
    "cc-by-sa-4.0": "https://creativecommons.org/licenses/by-sa/4.0/",
    "cc-by-sa-3.0": "https://creativecommons.org/licenses/by-sa/3.0/",
    "creative commons attribution share-alike (cc-by-sa)": "https://creativecommons.org/licenses/by-sa/4.0/",
    # CC BY-NC
    "cc-by-nc-4.0": "https://creativecommons.org/licenses/by-nc/4.0/",
    "cc-by-nc-3.0": "https://creativecommons.org/licenses/by-nc/3.0/",
    # CC BY-ND
    "cc-by-nd-4.0": "https://creativecommons.org/licenses/by-nd/4.0/",
    # CC BY-NC-SA
    "cc-by-nc-sa-4.0": "https://creativecommons.org/licenses/by-nc-sa/4.0/",
    # ODbL
    "odbl-1.0": "https://opendatacommons.org/licenses/odbl/1-0/",
    "odbl": "https://opendatacommons.org/licenses/odbl/1-0/",
    "open database license (odbl)": "https://opendatacommons.org/licenses/odbl/1-0/",
    # MIT
    "mit": "https://opensource.org/licenses/MIT",
    "mit license": "https://opensource.org/licenses/MIT",
    "hdx-mit": "https://opensource.org/licenses/MIT",
    # Apache
    "apache-2.0": "https://www.apache.org/licenses/LICENSE-2.0",
    # GPL
    "gpl-3.0": "https://www.gnu.org/licenses/gpl-3.0.en.html",
    # Other open
    "open-dc": "https://creativecommons.org/publicdomain/zero/1.0/",
}

# Fallback for "Other" / custom: use the license_url field from HDX if available
_LICENSE_FALLBACK = "https://creativecommons.org/licenses/by/4.0/"


# ---------------------------------------------------------------------------
# URL extraction helper (referenced_by)
# ---------------------------------------------------------------------------

_URL_RE = re.compile(r'https?://[^\s<>"\')\]]+')


def _extract_methodology_urls(
    texts: List[Optional[str]],
) -> Tuple[List[Optional[str]], List[Dict[str, Any]]]:
    """Extract HTTP/S URLs from methodology texts into referenced_by entries.

    Scans each text in `texts` for bare URLs, replaces each unique URL with
    "(see referenced_by)", and collects deduplicated referenced_by entries.

    Args:
        texts: List of text strings (may contain None). Modified in place.

    Returns:
        (cleaned_texts, referenced_by_list)
        referenced_by_list items follow v1.0 schema: {id, name, url} only -
        no empty author_names/doi/date_published (those would fail v1.0 validation).
    """
    seen_urls: set = set()
    referenced_by: List[Dict[str, Any]] = []
    cleaned: List[Optional[str]] = []

    for text in texts:
        if not text:
            cleaned.append(text)
            continue

        urls_found = _URL_RE.findall(text)
        for raw_url in urls_found:
            clean_url = raw_url.rstrip(".,;:")
            if clean_url not in seen_urls:
                seen_urls.add(clean_url)
                referenced_by.append({
                    "id": f"reference_{len(referenced_by) + 1}",
                    "name": "Methodology documentation",
                    "url": clean_url,
                })
            text = text.replace(raw_url, "(see referenced_by)")

        cleaned.append(text)

    return cleaned, referenced_by


# ---------------------------------------------------------------------------
# HDX date parsing
# ---------------------------------------------------------------------------

# Matches Solr range format: [2022-03-02T14:32:02 TO 2022-03-02T23:59:59]
_HDX_DATE_RE = re.compile(
    r"\[(\d{4}-\d{2}-\d{2})T[^\s]+\s+TO\s+(\d{4}-\d{2}-\d{2})T",
    re.IGNORECASE,
)


def parse_hdx_date(
    dataset_date: str,
) -> tuple[Optional[Dict[str, str]], str]:
    """Parse HDX dataset_date (Solr range format) into a temporal dict and a short
    date suffix suitable for appending to a record ID.

    Examples:
        "[2022-03-02T14:32:02 TO 2022-03-02T14:32:02]"  ->  ({"start": "2022-03-02"}, "2022-03")
        "[2015-01-01T00:00:00 TO 2015-12-31T23:59:59]"  ->  ({"start": "2015-01-01", "end": "2015-12-31"}, "2015")
        "[2010-01-01T00:00:00 TO 2020-12-31T23:59:59]"  ->  ({"start": "2010-01-01", "end": "2020-12-31"}, "2010")

    Returns:
        (temporal_dict, id_date_suffix)
        temporal_dict is None when the input is unparseable.
        id_date_suffix is "" when unparseable.
    """
    if not dataset_date:
        return None, ""

    m = _HDX_DATE_RE.search(dataset_date)
    if not m:
        return None, ""

    start_str, end_str = m.group(1), m.group(2)

    temporal: Dict[str, str] = {"start": start_str}
    if end_str != start_str:
        temporal["end"] = end_str

    # Date suffix for ID disambiguation (keeps IDs short but meaningful)
    start_year = start_str[:4]
    end_year = end_str[:4]
    start_month = start_str[:7]  # YYYY-MM
    end_month   = end_str[:7]

    if start_str == end_str:
        # Single-day event: full date precision (e.g. "20220302")
        suffix = start_str.replace("-", "")            # yyyymmdd
    elif start_month == end_month:
        # Same month, multi-day span (e.g. "202203")
        suffix = start_month.replace("-", "")          # yyyymm
    elif start_year == end_year:
        # Same calendar year (e.g. "2015")
        suffix = start_year                            # yyyy
    else:
        # Multi-year span: start year only (e.g. "2010")
        suffix = start_year                            # yyyy

    return temporal, suffix


def map_license_url(license_title: str, license_id: str = "", license_url: str = "") -> str:
    """Map HDX license info to a v1.0 IRI URL.

    Priority: license_url field > title lookup > id lookup > pattern match > fallback.
    """
    # If HDX provides a direct URL, use it
    if license_url and license_url.startswith("http"):
        return license_url

    key = re.sub(r"\s+", " ", (license_title or license_id or "").lower().strip())

    if key in LICENSE_URL_MAP:
        return LICENSE_URL_MAP[key]

    # Pattern-based CC matching
    if re.search(r"\bcc0\b", key) or "public domain" in key:
        return "https://creativecommons.org/publicdomain/zero/1.0/"
    if re.search(r"cc.?by.?nc.?sa", key):
        return "https://creativecommons.org/licenses/by-nc-sa/4.0/"
    if re.search(r"cc.?by.?nc", key):
        return "https://creativecommons.org/licenses/by-nc/4.0/"
    if re.search(r"cc.?by.?nd", key):
        return "https://creativecommons.org/licenses/by-nd/4.0/"
    if re.search(r"cc.?by.?sa", key):
        return "https://creativecommons.org/licenses/by-sa/4.0/"
    if re.search(r"\bcc.?by\b", key):
        return "https://creativecommons.org/licenses/by/4.0/"
    if "odbl" in key:
        return "https://opendatacommons.org/licenses/odbl/1-0/"

    return _LICENSE_FALLBACK


# ---------------------------------------------------------------------------
# Media type mapping
# ---------------------------------------------------------------------------

# Service URL patterns for access_modality detection
_SERVICE_URL_PATTERNS = [
    (re.compile(r"wms[?/]", re.I),    "WMS"),
    (re.compile(r"wfs[?/]", re.I),    "WFS"),
    (re.compile(r"wcs[?/]", re.I),    "WCS"),
    (re.compile(r"/ogcapi/",  re.I),   "OGC_API"),
    (re.compile(r"stac",      re.I),   "STAC"),
    (re.compile(r"arcgis/rest", re.I), "API"),
    (re.compile(r"geoserver.*ows", re.I), "WFS"),
]


def map_media_type(
    fmt: str,
    url: str = "",
) -> Tuple[Optional[str], Optional[str], str, Optional[str]]:
    """Map an HDX resource format string to v1.0 fields.

    Returns:
        (media_type, format_free_text, access_modality, conforms_to)
        Exactly one of media_type or format_free_text will be set (never both).
    """
    fmt_upper = (fmt or "").strip().upper()
    url_lower = (url or "").lower()

    # Detect service URLs first
    for pat, modality in _SERVICE_URL_PATTERNS:
        if pat.search(url_lower):
            conforms = CONFORMS_TO_MAP.get(modality)
            return None, None, modality, conforms

    # Service format strings
    if fmt_upper in SERVICE_MODALITY_MAP:
        modality = SERVICE_MODALITY_MAP[fmt_upper]
        conforms = CONFORMS_TO_MAP.get(modality)
        return None, None, modality, conforms

    # IANA media_type lookup
    if fmt_upper in MEDIA_TYPE_MAP:
        return MEDIA_TYPE_MAP[fmt_upper], None, "file_download", None

    # Format-only (no IANA entry)
    if fmt_upper in FORMAT_ONLY_MAP:
        return None, FORMAT_ONLY_MAP[fmt_upper], "file_download", None

    # Extension-based fallback from URL
    url_clean = url_lower.split("?")[0]
    ext_map = [
        (".geojson", "application/geo+json"),
        (".gpkg",    "application/geopackage+sqlite3"),
        (".tif",     "image/tiff;application=geotiff"),
        (".tiff",    "image/tiff;application=geotiff"),
        (".shp",     "application/vnd.shp"),
        (".nc",      "application/netcdf"),
        (".csv",     "text/csv"),
        (".xlsx",    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        (".xls",     "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"),
        (".json",    "application/json"),
        (".xml",     "application/xml"),
        (".pdf",     "application/pdf"),
        (".kml",     "application/vnd.google-earth.kml+xml"),
        (".kmz",     "application/vnd.google-earth.kml+xml"),
        (".parquet", "application/vnd.apache.parquet"),
        (".fgb",     "application/vnd.flatgeobuf"),
    ]
    for ext, mt in ext_map:
        if url_clean.endswith(ext):
            return mt, None, "file_download", None

    # If format string is non-empty and unrecognised, use as free-text format
    if fmt and fmt.upper() not in ("", "UNKNOWN", "N/A", "OTHER"):
        return None, fmt, "file_download", None

    return None, None, "file_download", None


# ---------------------------------------------------------------------------
# Entity building
# ---------------------------------------------------------------------------

def build_entity(
    name: str,
    url: Optional[str] = None,
    email: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a v1.0 entity object (publisher / creator / contact_point).

    Schema anyOf requires name + at least one of email or url.
    """
    entity: Dict[str, Any] = {"name": sanitize_text(name) or "Unknown"}
    if email and "@" in email:
        entity["email"] = email
    if url and url.startswith("http"):
        entity["url"] = url
    # Guarantee at least one of email/url
    if "email" not in entity and "url" not in entity and url:
        entity["url"] = url
    return entity


# ---------------------------------------------------------------------------
# Resource building (v1.0)
# ---------------------------------------------------------------------------

def build_resources_v10(hdx_resources: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Build RDLS v1.0 resources from HDX resource list.

    v1.0 rules:
    - media_type (IANA) and format (free text) are mutually exclusive
    - download_url for file_download, access_url for services
    - conforms_to added for OGC/STAC access_modality
    """
    resources = []

    for r in hdx_resources:
        r_id = (r.get("id", "") or str(uuid.uuid4()))[:8]
        r_name = sanitize_text(r.get("name", "") or r.get("description", "") or "") or "Data resource"
        r_desc = sanitize_text(r.get("description", "") or r.get("name", "") or "") or r_name
        r_fmt = r.get("format", "")
        r_url = r.get("download_url", "") or r.get("url", "") or ""

        media_type, fmt_text, modality, conforms_to = map_media_type(r_fmt, r_url)

        # Skip formats that indicate non-data resources (previews, web pages, etc.)
        if r_fmt and r_fmt.upper() in ("HTML", "WEB PAGE", "WEBPAGE", "OTHER"):
            if modality == "file_download":
                continue

        resource: Dict[str, Any] = {
            "id": f"resource_{r_id}",
            "title": r_name,
            "description": r_desc,
        }

        # Mutually exclusive: media_type OR format, never both
        if media_type:
            resource["media_type"] = media_type
        elif fmt_text:
            resource["format"] = fmt_text

        # conforms_to for OGC/STAC services
        if conforms_to:
            resource["conforms_to"] = conforms_to

        # URL placement (modality kept as local variable for logic; not output to schema)
        if r_url:
            if modality != "file_download":
                resource["access_url"] = r_url
            else:
                resource["download_url"] = r_url

        # Resource.anyOf requires download_url OR access_url - skip if neither present.
        # A resource with only id/title/description fails schema validation.
        if "download_url" not in resource and "access_url" not in resource:
            continue

        resources.append(resource)

    return resources


# ---------------------------------------------------------------------------
# v1.0 base record builder
# ---------------------------------------------------------------------------

# Canonical v1.0 field order (from rdls_template_v1.0.json)
RDLS_V10_FIELD_ORDER = [
    "id", "title", "description", "risk_data_type",
    "publisher", "version", "purpose", "project", "details",
    "contact_point", "creator",
    "spatial", "spatial_resolution", "temporal", "temporal_resolution",
    "license", "attributions", "lineage", "referenced_by", "resources",
    "hazard", "exposure", "vulnerability", "loss", "links",
]


def order_record_fields_v10(record: Dict[str, Any]) -> Dict[str, Any]:
    """Reorder record fields to match RDLS v1.0 template order."""
    ordered: Dict[str, Any] = {}
    for key in RDLS_V10_FIELD_ORDER:
        if key in record:
            ordered[key] = record[key]
    for key in record:
        if key not in ordered:
            ordered[key] = record[key]
    return ordered


def build_base_record_v10(
    hdx_meta: Dict[str, Any],
    components: List[str],
    llm_countries: Optional[List[str]] = None,
    llm_scale: Optional[str] = None,
    llm_contributing_sources: Optional[List[Dict[str, Any]]] = None,
    llm_lineage_description: Optional[str] = None,
    naming_config: Optional[Dict[str, Any]] = None,
    spatial_config: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Build the RDLS v1.0 base record from raw HDX metadata.

    Does NOT include HEVL blocks (hazard/exposure/vulnerability/loss).
    Those are added by integrate_hevl_v10() in extract_v10.py.

    Args:
        hdx_meta: Raw HDX dataset JSON.
        components: RDLS component list e.g. ["hazard", "exposure"].
        llm_countries: ISO3 country codes from LLM classification.
        llm_scale: Spatial scale from LLM classification.
        naming_config: For structured ID generation (loads from naming.yaml if None).
        spatial_config: For spatial inference (loads from spatial.yaml if None).

    Returns:
        Base record dict, or None if required fields missing.
    """
    ds_id = hdx_meta.get("id", "")
    title = sanitize_text(hdx_meta.get("title", "") or "")
    if not ds_id or not title:
        return None

    # risk_data_type
    valid_types = {"hazard", "exposure", "vulnerability", "loss"}
    rdt = sort_rdt_hevl([c for c in components if c in valid_types])
    if not rdt:
        return None

    # Resources
    resources = build_resources_v10(hdx_meta.get("resources", []))
    if not resources:
        return None

    # Spatial
    if spatial_config is None:
        spatial_config = load_spatial_config(
            Path(__file__).resolve().parent.parent / "configs" / "spatial.yaml"
        )
    groups = hdx_meta.get("groups", [])
    spatial = infer_spatial(
        groups=groups,
        region_map=spatial_config.get("region_to_countries", {}),
        country_fixes=spatial_config.get("country_name_fixes", {}),
        non_country_groups=spatial_config.get("non_country_groups", set()),
        iso3_table=spatial_config.get("iso3_table"),
    )
    # Capture countries resolved from HDX groups BEFORE adding LLM hints.
    # infer_spatial() resolves ISO3 codes from group names; returns {"scale":"global"}
    # as a fallback when no country can be resolved (e.g. "Malawi" not in lookup).
    # We track this separately so we know whether the "global" was intentional or a fallback.
    _groups_countries: List[str] = list(spatial.get("countries", []))
    _infer_global_fallback: bool = (
        spatial.get("scale") == "global" and not _groups_countries
    )

    # Merge LLM countries (supplementary - add any not already inferred from groups).
    # Only accept 3-char codes that are in the closed codelist_country (249 codes).
    # LLM sometimes returns "XKX" (Kosovo), "ANT" (Neth. Antilles), etc. which fail
    # schema validation. The closed codelist check guards against those.
    if llm_countries:
        existing = set(spatial.get("countries", []))
        extra = [
            c for c in llm_countries
            if c not in existing and len(c) == 3 and c in _VALID_COUNTRY_CODES
        ]
        if extra:
            spatial.setdefault("countries", []).extend(extra)

    # Filter any countries already in spatial (from infer_spatial()) against closed
    # codelist. infer_spatial() uses its own ISO3 lookup which should be consistent,
    # but a belt-and-braces check here prevents any stale / out-of-sync codes from
    # reaching the schema validator.
    if spatial.get("countries"):
        spatial["countries"] = [
            c for c in spatial["countries"] if c in _VALID_COUNTRY_CODES
        ]
        if not spatial["countries"]:
            spatial.pop("countries")

    # Derive scale using the best available signal:
    # 1. infer_spatial found specific countries from groups -> scale from their count
    #    (already set correctly by infer_spatial for national/regional, skip)
    # 2. infer_spatial returned global as a fallback (couldn't resolve country names):
    #    If LLM gave specific countries, infer scale from those instead.
    #    If LLM gave a specific (non-global) scale, use that.
    #    Only keep "global" if we truly have no country information.
    # 3. No scale at all -> use LLM scale or default to global.
    _all_countries = spatial.get("countries", [])
    if _infer_global_fallback:
        # infer_spatial couldn't resolve the groups - use LLM data
        if _all_countries:
            spatial["scale"] = "national" if len(_all_countries) == 1 else "regional"
        elif llm_scale and llm_scale != "global":
            spatial["scale"] = llm_scale
        # else: truly global or unknown - keep "global"
    elif "scale" not in spatial:
        # infer_spatial returned empty dict (no groups) - use LLM scale
        if llm_scale:
            spatial["scale"] = llm_scale
        else:
            spatial["scale"] = "global"

    # Schema rule: scale=global must NOT have countries.
    if spatial.get("scale") == "global":
        spatial.pop("countries", None)

    # Entities
    org = hdx_meta.get("organization", "")
    if isinstance(org, dict):
        org_name = org.get("title", "") or org.get("name", "") or "Unknown"
        org_url = ""
    else:
        org_name = str(org) or "Unknown"
        org_url = ""

    dataset_source = sanitize_text(hdx_meta.get("dataset_source", "") or "") or org_name
    hdx_url = f"https://data.humdata.org/dataset/{hdx_meta.get('name', ds_id)}"

    # publisher / creator / contact_point all use the publishing org.
    # dataset_source (often a long concatenation of contributing orgs) is
    # broken out into attributions (role=collaborator) and lineage.sources below.
    publisher = build_entity(org_name, url=org_url or hdx_url)
    creator = build_entity(org_name, url=org_url or hdx_url)
    contact_point = build_entity(org_name, url=hdx_url)

    # Attributions: contributing organizations from LLM (role=collaborator).
    # Entity schema requires name + (url OR email). Contributing sources rarely
    # carry their own URL, so we use hdx_url as a fallback reference point.
    # Deduplication against the publisher org to avoid double-listing.
    attributions: List[Dict[str, Any]] = []
    _seen_attrib_names: set = {org_name.lower().strip()}
    if llm_contributing_sources:
        for _idx, _cs in enumerate(llm_contributing_sources):
            _cs_name = (_cs.get("name", "") or "").strip()
            if not _cs_name or _cs_name.lower().strip() in _seen_attrib_names:
                continue
            _seen_attrib_names.add(_cs_name.lower().strip())
            # Use hdx_url as entity url so the anyOf (name+url | name+email) is satisfied
            attributions.append({
                "id": f"attribution_{len(attributions) + 1}",
                "entity": build_entity(_cs_name, url=hdx_url),
                "role": "collaborator",
            })

    # License
    license_url = map_license_url(
        hdx_meta.get("license_title", ""),
        hdx_meta.get("license_id", ""),
        hdx_meta.get("license_url", ""),
    )

    # Description: HDX notes + mandatory source attribution text.
    # The attribution suffix ensures description is never empty (v1.0 requires it)
    # and provides traceable provenance to the original HDX dataset page.
    # hdx_url is defined above in the Entities section.
    description = sanitize_text(hdx_meta.get("notes", "") or "")
    _source_suffix = (
        f"[Source: This metadata record was automatically extracted from the "
        f"Humanitarian Data Exchange (HDX); Original dataset: {hdx_url}]"
    )
    description = f"{description}. {_source_suffix}" if description else _source_suffix

    # Purpose - HDX methodology field describes why/how the data was collected
    # Skip generic placeholder values that add no information
    _METHODOLOGY_SKIP = {"other", "n/a", "not specified", "unknown", "none", "na", "-"}
    _raw_methodology = sanitize_text(hdx_meta.get("methodology", "") or "")
    purpose = _raw_methodology if _raw_methodology.lower().strip() not in _METHODOLOGY_SKIP else ""

    # Details - HDX caveats field contains additional limitations and notes
    _raw_caveats = sanitize_text(hdx_meta.get("caveats", "") or "")
    details = _raw_caveats if _raw_caveats.lower().strip() not in _METHODOLOGY_SKIP else ""

    # Version - HDX version field (free text; often blank)
    version = sanitize_text(hdx_meta.get("version", "") or "")

    # Lineage - describes the source data origin
    # dataset_source: the originating organisation / data creator name
    # methodology_other: extra free-text description when methodology == "Other"
    _raw_methodology_other = sanitize_text(hdx_meta.get("methodology_other", "") or "")
    _lineage_description = (
        _raw_methodology_other
        if _raw_methodology_other.lower().strip() not in _METHODOLOGY_SKIP
        else ""
    )

    # referenced_by - extract URLs embedded in methodology/caveats texts
    # Ported from v0.3 build_details(): URLs in methodology text become referenced_by entries.
    # The URL placeholder "(see referenced_by)" replaces the inline URL in the text.
    # v1.0 referenced_by items: {id, name, url} only - no empty author_names/doi/date_published.
    [purpose, _lineage_description], _referenced_by = _extract_methodology_urls(
        [purpose, _lineage_description]
    )

    # Temporal - parse HDX dataset_date (Solr range format)
    temporal, date_suffix = parse_hdx_date(hdx_meta.get("dataset_date", "") or "")

    # Record ID
    if naming_config is None:
        nc_path = Path(__file__).resolve().parent.parent / "configs" / "naming.yaml"
        if nc_path.exists():
            naming_config = load_naming_config(nc_path)

    iso3_codes = spatial.get("countries", [])
    if naming_config:
        org_slug = ""
        if isinstance(hdx_meta.get("organization"), dict):
            org_slug = hdx_meta["organization"].get("name", "")
        record_id = build_rdls_id(
            components=rdt,
            iso3_codes=iso3_codes,
            org_name=org_name,
            org_slug=org_slug,
            config=naming_config,
            title=title,
        )
    else:
        record_id = ds_id

    # Append date suffix to disambiguate recurring series (UNOSAT, FEWSNET, etc.)
    # Format: _{yyyymmdd} | _{yyyymm} | _{yyyy}  (underscore separator, no hyphens in date)
    # e.g. rdls_hzd-bgd_unosat_satellitedetectedwater_20240908
    if date_suffix:
        record_id = f"{record_id}_{date_suffix}"

    # Assemble base record
    record: Dict[str, Any] = {"id": record_id, "title": title}
    # description is always set (source attribution suffix guarantees non-empty)
    record["description"] = description
    record["risk_data_type"] = rdt
    record["publisher"] = publisher
    if version:
        record["version"] = version
    if purpose:
        record["purpose"] = purpose
    record["contact_point"] = contact_point
    record["creator"] = creator
    if attributions:
        record["attributions"] = attributions
    if details:
        record["details"] = details
    record["spatial"] = spatial
    if temporal:
        record["temporal"] = temporal
    record["license"] = license_url

    # Lineage description: LLM-provided scientific description takes priority;
    # methodology_other text (from URL extraction) used as fallback.
    _final_lineage_desc = (llm_lineage_description or "").strip() or _lineage_description

    # Lineage sources: one entry per contributing org from LLM.
    # Each entry carries per-source risk_data_type/used_in/type/description
    # (rather than copying the whole dataset's rdt to every source).
    lineage: Dict[str, Any] = {}
    if _final_lineage_desc:
        lineage["description"] = _final_lineage_desc

    if llm_contributing_sources:
        _src_list: List[Dict[str, Any]] = []
        for _sidx, _cs in enumerate(llm_contributing_sources):
            _cs_name = (_cs.get("name", "") or "").strip()
            if not _cs_name:
                continue
            _src: Dict[str, Any] = {
                "id": f"source_{_sidx + 1}",
                "name": _cs_name,
            }
            _cs_type = _cs.get("type", "")
            if _cs_type in ("dataset", "model"):
                _src["type"] = _cs_type
            _cs_used_in = _cs.get("used_in", "")
            if _cs_used_in in ("hazard", "exposure", "vulnerability", "loss"):
                _src["risk_data_type"] = [_cs_used_in]
                _src["used_in"] = _cs_used_in
            _cs_desc = (_cs.get("description", "") or "").strip()
            if _cs_desc:
                _src["description"] = _cs_desc
            _src_list.append(_src)
        if _src_list:
            lineage["sources"] = _src_list

    # Fallback: if LLM gave no contributing sources, use dataset_source name
    if not lineage.get("sources"):
        _fallback_src: Dict[str, Any] = {"id": "source_1", "name": dataset_source}
        if rdt:
            _fallback_src["risk_data_type"] = rdt
            _fallback_src["used_in"] = rdt[0]
        lineage["sources"] = [_fallback_src]

    record["lineage"] = lineage

    if _referenced_by:
        record["referenced_by"] = _referenced_by

    record["resources"] = resources
    record["links"] = [
        {
            "href": "https://docs.riskdatalibrary.org/en/1__0__0/rdls_schema.json",
            "rel": "describedby",
        },
        {"href": hdx_url, "rel": "source"},
    ]

    return record


def wrap_datasets_v10(record: Dict[str, Any]) -> Dict[str, Any]:
    """Wrap a single v1.0 record in the datasets envelope."""
    return {"datasets": [order_record_fields_v10(record)]}
