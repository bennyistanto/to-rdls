"""
GeoNode → RDLS v1.0 base-record builder.

A focused replacement for `translate_v03.build_rdls_record()` when the source
is a GeoNode portal (ISO 19115 metadata). Maps GeoNode-native fields directly
to v1.0 wherever they're more precise than the common dict (publisher /
creator / contact_point / temporal / purpose / details / lineage / version /
media_type / referenced_by).

The HEVL blocks are NOT built here — that stays with the existing extractors
plus `extract.build_*_block()` v1.0 builders. This module returns a base
record ready to be merged with HEVL by `integrate_hevl_v10()`.

Inputs:
    fields:     GeoNode common dict from extract_geonode_fields() (carries
                _geonode_* enrichment alongside the common keys).
    components: ['hazard'|'exposure'|'vulnerability'|'loss', ...] from the
                classifier output (classify_dataset).

Output:
    A v1.0 record dict (datasets[0] payload) with proper field order, or
    None if required fields are missing.
"""

from __future__ import annotations

import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .utils import load_yaml, sanitize_text, sort_rdt_hevl
from .spatial import (
    infer_spatial, load_spatial_config, country_name_to_iso3,
    load_regional_codes, expand_regional_code,
)
from .naming import build_rdls_id, load_naming_config
from .translate import _VALID_COUNTRY_CODES  # closed ISO3 codelist

# Loaded lazily; module-level cache so we don't hit YAML on every record.
_REGIONAL_LOOKUP: Optional[Dict[str, Dict[str, Any]]] = None


def _get_regional_lookup() -> Dict[str, Dict[str, Any]]:
    global _REGIONAL_LOOKUP
    if _REGIONAL_LOOKUP is None:
        path = Path(__file__).resolve().parent.parent / "configs" / "spatial_regions.yaml"
        _REGIONAL_LOOKUP = load_regional_codes(path)
    return _REGIONAL_LOOKUP
from .translate import (
    build_entity,
    map_license_url,
    order_record_fields_v10,
)


# ---------------------------------------------------------------------------
# Placeholder-string skip set
# ---------------------------------------------------------------------------

# Strings that GeoNode portals fill in when a field is unset (literal "None",
# "N/A", empty after trim, etc.). Treat as missing.
_PLACEHOLDERS = {
    "", "none", "n/a", "not specified", "unknown", "na", "-",
    "no information", "no information provided", "tbd", "todo",
    "no information provided n/a",
}

# Portal usernames that are operational accounts, not real authors. We avoid
# putting these on the record as creator/contact_point; they fall back to the
# portal org instead. Tunable in geonode.yaml when needed.
_OPERATIONAL_USERNAMES = {
    "admin", "geodatarequest", "anonymous", "pdhsupport",
    "geonode", "portaladmin",
}


def _is_placeholder(s: Optional[str]) -> bool:
    if s is None:
        return True
    return s.strip().lower() in _PLACEHOLDERS


def _clean_text(s: Optional[str]) -> str:
    """sanitize_text + collapse newlines to single space (single-paragraph rule)."""
    if not s:
        return ""
    cleaned = sanitize_text(s) or ""
    return re.sub(r"\s+", " ", cleaned.replace("\r", " ").replace("\n", " ")).strip()


# ---------------------------------------------------------------------------
# Contact → Entity mapping
# ---------------------------------------------------------------------------

def _contact_to_entity(contact: Dict[str, Any], fallback_url: str = "") -> Optional[Dict[str, Any]]:
    """Convert a GeoNode contact object (poc / metadata_author / publisher /
    originator / etc.) to a v1.0 entity. Filters operational usernames.

    GeoNode contacts shape (per `_geonode_contacts.{role}[0]`):
        {pk, username, first_name, last_name, email, link, avatar, ...}
    """
    if not isinstance(contact, dict):
        return None
    username = (contact.get("username") or "").strip().lower()
    if username in _OPERATIONAL_USERNAMES:
        return None
    fn = (contact.get("first_name") or "").strip()
    ln = (contact.get("last_name") or "").strip()
    name = " ".join(p for p in [fn, ln] if p) or contact.get("username") or ""
    name = name.strip()
    if not name or _is_placeholder(name):
        return None
    email = (contact.get("email") or "").strip()
    url = (contact.get("link") or "").strip() or fallback_url
    if email and "@" not in email:
        email = ""
    if url and not url.startswith("http"):
        url = ""
    if not email and not url:
        return None
    return build_entity(name, url=url or None, email=email or None)


def _first_real_contact(
    contacts_dict: Dict[str, Any],
    keys: List[str],
    fallback_url: str = "",
) -> Optional[Dict[str, Any]]:
    """Return the first non-operational entity found across the given keys."""
    if not isinstance(contacts_dict, dict):
        return None
    for k in keys:
        lst = contacts_dict.get(k) or []
        if not isinstance(lst, list):
            continue
        for c in lst:
            ent = _contact_to_entity(c, fallback_url=fallback_url)
            if ent:
                return ent
    return None


def _portal_entity(portal_name: str, portal_base_url: str, org_name: str = "") -> Dict[str, Any]:
    """Fallback entity using the portal as the responsible party.

    Drops operational usernames (admin, geodatarequest, etc.) from org_name
    so they don't surface as the public-facing publisher.
    """
    safe_org = (org_name or "").strip()
    if safe_org.lower() in _OPERATIONAL_USERNAMES:
        safe_org = ""
    name = safe_org or {
        "pacificdata":       "Pacific Community (SPC) - Pacific Data Hub",
        "rcmrd":             "Regional Centre for Mapping of Resources for Development",
        "icpac":             "IGAD Climate Prediction and Applications Centre (ICPAC)",
        "framefavn":         "FRAME FAVN portal",
        "eurac":             "Eurac Research",
        "resilienceacademy": "Resilience Academy (UDSM / Ifakara)",
        "fossilfuelatlas":   "Fossil Fuel Atlas portal",
        "kmap_inforac":      "INFO/RAC - UN Environment / Mediterranean Action Plan",
        "riskinfo_lk":       "RiskInfo.lk - Disaster Management Centre Sri Lanka",
        "gishub_kemenhub":   "Indonesian Ministry of Transportation - GIS Hub",
    }.get(portal_name.lower(), f"{portal_name} GeoNode portal")
    return build_entity(name, url=portal_base_url or None)


# ---------------------------------------------------------------------------
# Temporal
# ---------------------------------------------------------------------------

_YEAR_RE = re.compile(r"^(\d{4})")


def _year_of(s: str) -> Optional[str]:
    if not s:
        return None
    m = _YEAR_RE.match(s.strip())
    return m.group(1) if m else None


def _build_temporal(fields: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Prefer GeoNode temporal_extent; fall back to dataset_date as central_year."""
    gtemp = fields.get("_geonode_temporal") or {}
    start = (gtemp.get("temporal_extent_start") or "").strip()
    end = (gtemp.get("temporal_extent_end") or "").strip()
    out: Dict[str, Any] = {}
    if start:
        ys = _year_of(start)
        if ys:
            out["start"] = ys
    if end:
        ye = _year_of(end)
        if ye:
            out["end"] = ye
    if out:
        return out
    # Fallback: single dataset_date → central_year
    raw_date = fields.get("dataset_date") or gtemp.get("date") or ""
    yc = _year_of(raw_date)
    if yc:
        try:
            return {"central_year": int(yc)}
        except (TypeError, ValueError):
            return None
    return None


# ---------------------------------------------------------------------------
# Media type mapping
# ---------------------------------------------------------------------------

_MEDIA_TYPE_BY_FORMAT = {
    "GEOTIFF": "image/tiff;application=geotiff",
    "GeoTIFF": "image/tiff;application=geotiff",
    "TIF": "image/tiff",
    "TIFF": "image/tiff",
    "SHP": "application/vnd.shp",
    "SHAPEFILE": "application/vnd.shp",
    "GEOPACKAGE": "application/geopackage+sqlite3",
    "GPKG": "application/geopackage+sqlite3",
    "GEOJSON": "application/geo+json",
    "JSON": "application/json",
    "CSV": "text/csv",
    "XLSX": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "EXCEL": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "XLS": "application/vnd.ms-excel",
    "PDF": "application/pdf",
    "NETCDF": "application/x-netcdf",
    "NC": "application/x-netcdf",
    "ZIP": "application/zip",
    "KML": "application/vnd.google-earth.kml+xml",
    "KMZ": "application/vnd.google-earth.kmz",
    "GML": "application/gml+xml",
}

_CONFORMS_TO_BY_MODALITY = {
    "WMS": "http://www.opengis.net/def/serviceType/ogc/wms",
    "WFS": "http://www.opengis.net/def/serviceType/ogc/wfs",
    "WCS": "http://www.opengis.net/def/serviceType/ogc/wcs",
}


def _pick_media_type(fmt: str, link_type: str, modality: str, subtype: str) -> Tuple[str, str]:
    """Return (media_type, format) — at most one populated, per v1.0 rule.

    Strategy:
      - For OGC services, drop media_type (we use conforms_to elsewhere) and
        leave format empty.
      - For data downloads, look up in the codelist table; fall back to the
        free-text format.
    """
    fmt_norm = (fmt or "").upper().strip()
    if modality in _CONFORMS_TO_BY_MODALITY:
        # OGC services STILL need media_type OR format: the v1.0 schema anyOf
        # rule requires one even when conforms_to documents the protocol.
        # Assign the media_type the endpoint actually delivers, by protocol:
        #   WMS GetMap -> PNG (or GeoTIFF for raster), WFS GetFeature -> GeoJSON,
        #   WCS coverage -> GeoTIFF.  (Mirror of scripts/fix_ogc_resource_media_type.py.)
        svc = (modality or "").upper()
        if svc == "WMS":
            return ("image/tiff;application=geotiff" if subtype == "raster" else "image/png"), ""
        if svc == "WFS":
            return "application/geo+json", ""
        if svc == "WCS":
            return "image/tiff;application=geotiff", ""
        return "", ""
    mt = _MEDIA_TYPE_BY_FORMAT.get(fmt_norm)
    if mt:
        return mt, ""
    # Subtype hint can rescue raster/vector when format is missing.
    if not fmt_norm:
        if subtype == "raster":
            return "image/tiff;application=geotiff", ""
        if subtype == "vector":
            return "application/vnd.shp", ""
    # Fall through to free-text format.
    return "", fmt_norm.title() if fmt_norm else ""


# ---------------------------------------------------------------------------
# Resources (v1.0)
# ---------------------------------------------------------------------------

def _media_from_filename(name: str) -> Tuple[str, str]:
    """Derive (media_type, format) from a file name's extension. Document
    fallback resources are typically named with extensions (.zip / .pdf /
    .xlsx / .tif). When no explicit format is in the API, sniff it."""
    if not name:
        return "", ""
    n = name.strip().lower()
    for ext, key in (
        (".zip", "ZIP"), (".tif", "GEOTIFF"), (".tiff", "GEOTIFF"),
        (".shp", "SHP"), (".gpkg", "GEOPACKAGE"), (".geojson", "GEOJSON"),
        (".json", "JSON"), (".csv", "CSV"), (".xlsx", "XLSX"),
        (".xls", "XLS"), (".pdf", "PDF"), (".nc", "NC"),
        (".kml", "KML"), (".kmz", "KMZ"), (".gml", "GML"),
    ):
        if n.endswith(ext):
            return _MEDIA_TYPE_BY_FORMAT.get(key, ""), key.title()
    return "", ""


def build_resources_geonode_v10(
    common_resources: List[Dict[str, Any]],
    subtype: str = "",
    srid: str = "",
) -> List[Dict[str, Any]]:
    """Build v1.0 resources[] from the GeoNode common-dict resources list.

    Each common-dict resource carries (id, name, description, format, url,
    _download_url, _link_type, _modality_hint). We use _link_type +
    _modality_hint to decide download_url vs access_url + conforms_to, and
    the subtype/format to pick media_type.
    """
    resources: List[Dict[str, Any]] = []
    for idx, r in enumerate(common_resources):
        if not isinstance(r, dict):
            continue
        rid_raw = str(r.get("id") or idx)
        # Make IDs unique per dataset and short.
        rid = f"resource_{rid_raw}"[:32]
        name = _clean_text(r.get("name") or r.get("description") or "") or "Data resource"
        desc = _clean_text(r.get("description") or r.get("name") or "") or name

        url = (r.get("url") or "").strip()
        download_url = (r.get("_download_url") or "").strip()
        link_type = (r.get("_link_type") or "").strip()
        modality = (r.get("_modality_hint") or "").strip()
        fmt = (r.get("format") or "").strip()

        # GeoNode 4.x "document" fallback: extract_geonode_fields appends a
        # minimal resource with no _modality_hint and no _link_type when the
        # links array contains only thumbnails/metadata. For these we treat
        # the URL as a direct download (the resource title carries the
        # filename, e.g. "Flood Hazard Map_...zip").
        is_doc_download = (
            not modality and not link_type
            and url.endswith(("/download", "/download/"))
        )
        if is_doc_download:
            modality = "file_download"

        # GeoServer WMS/WFS/WCS endpoints expose data via download_url, the
        # service-landing page lives at url. file_download links put the
        # actual download in url itself.
        access_url = ""
        if modality == "file_download":
            primary_dl = download_url or url
            if not primary_dl:
                continue
            download = primary_dl
        else:
            # Service: prefer the catalogue page (url) as access_url and the
            # request URL (download_url) as the live data endpoint.
            access_url = url
            download = download_url

        media_type, format_text = _pick_media_type(fmt, link_type, modality, subtype)
        # If still no media_type, try sniffing from the resource title (which
        # for documents is typically the original filename, e.g. .zip / .pdf).
        if not media_type and not format_text:
            media_type, format_text = _media_from_filename(name)

        resource: Dict[str, Any] = {
            "id": rid,
            "title": name,
            "description": desc,
        }
        if media_type:
            resource["media_type"] = media_type
        elif format_text:
            resource["format"] = format_text
        if srid and srid.upper().startswith("EPSG:"):
            resource["coordinate_system"] = srid

        # Schema anyOf: at least one of access_url / download_url must be set.
        if access_url:
            resource["access_url"] = access_url
        if download:
            resource["download_url"] = download
        if "access_url" not in resource and "download_url" not in resource:
            continue

        # conforms_to for OGC services
        if modality in _CONFORMS_TO_BY_MODALITY:
            resource["conforms_to"] = _CONFORMS_TO_BY_MODALITY[modality]

        resources.append(resource)

    return resources


# ---------------------------------------------------------------------------
# Lineage
# ---------------------------------------------------------------------------

def _build_lineage(
    fields: Dict[str, Any],
    risk_data_type: List[str],
    creator_name: str,
) -> Dict[str, Any]:
    """Compose lineage from _geonode_quality + tkeywords + organization.

    description ← data_quality_statement (non-placeholder)
    sources[]   ← thesaurus keywords (controlled vocabulary references) +
                  fallback originator/organization
    """
    quality = fields.get("_geonode_quality") or {}
    desc = _clean_text(quality.get("data_quality_statement") or "")
    if _is_placeholder(desc):
        desc = ""

    sources: List[Dict[str, Any]] = []
    tkw = fields.get("_geonode_thesaurus_keywords") or []
    seen: set = set()
    for tk in tkw:
        if not isinstance(tk, dict):
            continue
        name = _clean_text(tk.get("label") or tk.get("alt_label") or "")
        if not name or name.lower() in seen:
            continue
        seen.add(name.lower())
        src: Dict[str, Any] = {"id": f"source_t{len(sources) + 1}", "name": name}
        uri = (tk.get("uri") or tk.get("about") or "").strip()
        if uri.startswith("http"):
            src["url"] = uri
        src["type"] = "dataset"
        sources.append(src)

    # Always include the originating organisation as a fallback source so the
    # provenance chain is never empty.
    if creator_name and creator_name.lower() not in seen:
        seen.add(creator_name.lower())
        src = {
            "id": f"source_{len(sources) + 1}",
            "name": creator_name,
            "type": "dataset",
        }
        if risk_data_type:
            src["risk_data_type"] = list(risk_data_type)
            src["used_in"] = risk_data_type[0]
        sources.append(src)

    lineage: Dict[str, Any] = {}
    if desc:
        lineage["description"] = desc
    if sources:
        lineage["sources"] = sources
    return lineage


# ---------------------------------------------------------------------------
# referenced_by from DOI
# ---------------------------------------------------------------------------

def _build_referenced_by(fields: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    doi = (fields.get("_geonode_doi") or "").strip()
    if doi:
        entry: Dict[str, Any] = {"id": "ref_doi", "doi": doi}
        if doi.startswith("http"):
            entry["url"] = doi
        else:
            entry["url"] = f"https://doi.org/{doi.lstrip('doi:').strip()}"
        out.append(entry)
    return out


# ---------------------------------------------------------------------------
# Spatial enrichment (port from the v0.3 pipeline's GeoNode block)
# ---------------------------------------------------------------------------

_PCRAFI_CC_TO_ISO3 = {
    "CK": "COK", "FJ": "FJI", "FM": "FSM", "KI": "KIR", "MH": "MHL",
    "NR": "NRU", "NU": "NIU", "PG": "PNG", "PW": "PLW", "SB": "SLB",
    "TL": "TLS", "TO": "TON", "TV": "TUV", "VU": "VUT", "WS": "WSM",
}


def _enrich_spatial(
    record_spatial: Dict[str, Any],
    fields: Dict[str, Any],
    spatial_config: Dict[str, Any],
    title: str = "",
) -> Dict[str, Any]:
    """Use authoritative ISO3 codes from GeoNode regions; bbox from polygon.

    When `regions` is only ['Global'] (common on ICPAC and similar portals
    that don't enforce strict region tagging), fall back to scanning the
    title and tags for recognisable country names before accepting 'global'.
    """
    spatial = dict(record_spatial)
    # Raw codes from the GeoNode `regions` field (already 3-letter from the
    # adapter).
    raw_codes = list(fields.get("_region_iso3_codes") or [])

    # Pass 1: keep valid ISO3 codes.
    region_iso3 = [
        c for c in raw_codes
        if isinstance(c, str) and len(c) == 3 and c in _VALID_COUNTRY_CODES
    ]

    # Pass 2: only expand regional / sea codes when the record has NO specific
    # ISO3 country tags. Many portals tag the same record with both a
    # specific country (e.g. "ALB") and a regional umbrella (e.g. "MES" or
    # "EUR"); the specific country is the truthful coverage and expanding to
    # all Mediterranean countries would mis-state the scope.
    regional_lookup = _get_regional_lookup()
    matched_regions: List[Dict[str, Any]] = []
    if not region_iso3:
        candidates_for_expansion: List[str] = []
        candidates_for_expansion.extend(raw_codes)
        candidates_for_expansion.extend(
            g for g in (fields.get("groups") or []) if isinstance(g, str)
        )
        seen_codes: set = set()
        for raw in candidates_for_expansion:
            if not isinstance(raw, str):
                continue
            if raw in _VALID_COUNTRY_CODES:
                continue  # already in region_iso3 above
            entry = expand_regional_code(raw, regional_lookup)
            if not entry:
                continue
            if entry["code"] in seen_codes:
                continue
            seen_codes.add(entry["code"])
            matched_regions.append(entry)
            for member in entry["countries"]:
                if member in _VALID_COUNTRY_CODES and member not in region_iso3:
                    region_iso3.append(member)

    # Apply the same filter to whatever infer_spatial gave us, in case the
    # base spatial dict already carries an invalid code.
    if spatial.get("countries"):
        spatial["countries"] = [
            c for c in spatial["countries"]
            if isinstance(c, str) and len(c) == 3 and c in _VALID_COUNTRY_CODES
        ]
        if not spatial["countries"]:
            spatial.pop("countries")
            if spatial.get("scale") in ("national", "regional"):
                spatial["scale"] = "global"

    if region_iso3:
        # If the slug title carries a PCRAFI country prefix that matches one
        # of the region codes, narrow a multi-region record to that country.
        slug_title = fields.get("_slug_title") or ""
        m = re.match(r"^([A-Z]{2})_", slug_title)
        title_iso3 = _PCRAFI_CC_TO_ISO3.get(m.group(1)) if m else None
        if title_iso3 and title_iso3 in region_iso3:
            spatial = {"scale": "national", "countries": [title_iso3]}
        elif len(region_iso3) == 1:
            spatial = {"scale": "national", "countries": region_iso3}
        else:
            # Multi-country: regional. Includes regional-sea / continental
            # expansions where 5+ members are normal (Mediterranean: 22).
            spatial = {
                "scale": "regional",
                "countries": sorted(set(region_iso3)),
            }

    # If still global, try title + tags for a country name (one country wins).
    if spatial.get("scale") == "global":
        iso3_table = spatial_config.get("iso3_table") if spatial_config else None
        country_fixes = spatial_config.get("country_name_fixes", {}) if spatial_config else {}
        # Try multi-word title tokens first (catches "South Sudan", "Dar es Salaam"
        # via lookup tables) and fall back to single tokens.
        title_text = title or fields.get("title") or ""
        candidates: List[str] = []
        # Bigrams + trigrams to catch "South Sudan", "Sri Lanka" etc.
        words = re.findall(r"[A-Za-z]+", title_text)
        for n in (3, 2, 1):
            for i in range(len(words) - n + 1):
                candidates.append(" ".join(words[i : i + n]))
        candidates.extend(fields.get("tags") or [])
        for cand in candidates:
            if not cand:
                continue
            iso3 = country_name_to_iso3(
                cand, fixes=country_fixes, iso3_table=iso3_table,
            )
            if iso3:
                spatial = {"scale": "national", "countries": [iso3]}
                break

    # bbox from GeoNode polygon
    gn_spatial = fields.get("_geonode_spatial") or {}
    bbox_poly = gn_spatial.get("bbox")
    if isinstance(bbox_poly, dict):
        coords = (bbox_poly.get("coordinates") or [[]])
        if coords and coords[0]:
            ring = coords[0]
            try:
                lons = [p[0] for p in ring]
                lats = [p[1] for p in ring]
                spatial["bbox"] = [min(lons), min(lats), max(lons), max(lats)]
            except (TypeError, IndexError):
                pass

    return spatial


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_base_record_geonode_v10(
    fields: Dict[str, Any],
    components: List[str],
    *,
    portal_name: str,
    portal_base_url: str = "",
    spatial_config: Optional[Dict[str, Any]] = None,
    license_config: Optional[Dict[str, Any]] = None,
    naming_config: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """Build a v1.0 RDLS base record from a GeoNode common-dict record.

    Args:
        fields:          Output of extract_geonode_fields() — common keys
                         plus _geonode_* enrichment.
        components:      Risk-data-type list from the classifier.
        portal_name:     Portal key (e.g. "pacificdata").
        portal_base_url: Portal root URL for fallback links.
        spatial_config:  Optional spatial.yaml dict.
        license_config:  Optional license_mapping.yaml dict.
        naming_config:   Optional naming.yaml dict for structured ID.

    Returns:
        The base record (no HEVL blocks) or None.
    """
    ds_id = str(fields.get("id") or "").strip()
    title = _clean_text(fields.get("title") or "")
    if not ds_id or not title:
        return None

    # risk_data_type (HEVL-ordered)
    valid = {"hazard", "exposure", "vulnerability", "loss"}
    rdt = sort_rdt_hevl([c for c in components if c in valid])
    if not rdt:
        return None

    # --- Spatial ---
    if spatial_config is None:
        spatial_config = load_spatial_config(
            Path(__file__).resolve().parent.parent / "configs" / "spatial.yaml"
        )
    groups = fields.get("groups") or []
    spatial = infer_spatial(
        groups=groups,
        region_map=spatial_config.get("region_to_countries", {}),
        country_fixes=spatial_config.get("country_name_fixes", {}),
        non_country_groups=spatial_config.get("non_country_groups", set()),
        iso3_table=spatial_config.get("iso3_table"),
    )
    spatial = _enrich_spatial(spatial, fields, spatial_config, title=title)
    # global must NOT carry countries
    if spatial.get("scale") == "global":
        spatial.pop("countries", None)
    iso3_codes = spatial.get("countries", [])

    # --- Entities ---
    contacts = fields.get("_geonode_contacts") or {}
    org_name = _clean_text(fields.get("organization") or "") or ""
    portal_url = portal_base_url or fields.get("_geonode_spatial", {}).get("portal_url", "") or ""
    dataset_url = (fields.get("url") or "").strip()

    publisher = _first_real_contact(contacts, ["publisher"], fallback_url=portal_url) \
        or _portal_entity(portal_name, portal_url, org_name=org_name)
    creator = _first_real_contact(
        contacts, ["originator", "principal_investigator", "metadata_author"],
        fallback_url=dataset_url or portal_url,
    ) or build_entity(org_name or publisher["name"], url=dataset_url or portal_url or None)
    contact_point = _first_real_contact(
        contacts, ["poc", "distributor", "custodian"],
        fallback_url=dataset_url or portal_url,
    ) or build_entity(org_name or publisher["name"], url=dataset_url or portal_url or None)

    # --- Description / purpose / details / version ---
    description = _clean_text(fields.get("notes") or "")
    source_suffix = (
        f"[Source: This metadata record was automatically extracted from the "
        f"GeoNode portal {portal_name}]"
    )
    description = f"{description} {source_suffix}" if description else source_suffix

    quality = fields.get("_geonode_quality") or {}
    raw_purpose = _clean_text(quality.get("purpose") or "")
    purpose = "" if _is_placeholder(raw_purpose) else raw_purpose
    raw_details = _clean_text(quality.get("supplemental_information") or "")
    details = "" if _is_placeholder(raw_details) else raw_details
    # Fall back to common-dict methodology field when supplemental info empty
    if not details:
        raw_method = _clean_text(fields.get("methodology") or "")
        if not _is_placeholder(raw_method):
            details = raw_method
    version = _clean_text(fields.get("_geonode_edition") or "") or ""

    # --- Temporal ---
    temporal = _build_temporal(fields)

    # --- License (map to v1.0 codelist URI) ---
    license_uri = map_license_url(
        fields.get("license_title", "") or "",
        license_id="",
        license_url=fields.get("license_url", "") or "",
    )

    # --- Resources ---
    common_resources = fields.get("resources") or []
    srid = (fields.get("_geonode_spatial", {}) or {}).get("srid", "") or ""
    subtype = (fields.get("_geonode_subtype") or "").strip().lower()
    resources = build_resources_geonode_v10(common_resources, subtype=subtype, srid=srid)
    if not resources:
        return None

    # --- Lineage ---
    lineage = _build_lineage(fields, rdt, creator_name=creator.get("name", ""))

    # --- referenced_by ---
    referenced_by = _build_referenced_by(fields)

    # --- ID ---
    if naming_config is None:
        nc_path = Path(__file__).resolve().parent.parent / "configs" / "naming.yaml"
        if nc_path.exists():
            naming_config = load_naming_config(nc_path)
    record_id = ds_id
    if naming_config:
        org_slug = fields.get("org_slug") or fields.get("_source_portal") or portal_name
        record_id = build_rdls_id(
            components=rdt,
            iso3_codes=iso3_codes,
            org_name=org_name or publisher.get("name", ""),
            org_slug=org_slug,
            config=naming_config,
            title=fields.get("_slug_title") or title,
        )

    # --- Assemble (template field order via order_record_fields_v10) ---
    record: Dict[str, Any] = {
        "id": record_id,
        "title": title,
        "description": description,
        "risk_data_type": rdt,
        "publisher": publisher,
    }
    if version:
        record["version"] = version
    if purpose:
        record["purpose"] = purpose
    if details:
        record["details"] = details
    record["contact_point"] = contact_point
    record["creator"] = creator
    record["spatial"] = spatial
    if temporal:
        record["temporal"] = temporal
    record["license"] = license_uri
    if lineage:
        record["lineage"] = lineage
    if referenced_by:
        record["referenced_by"] = referenced_by
    record["resources"] = resources
    record["links"] = [
        {
            "href": "https://docs.riskdatalibrary.org/en/1__0__0/rdls_schema.json",
            "rel": "describedby",
        },
    ]
    if dataset_url:
        record["links"].append({"href": dataset_url, "rel": "source"})

    return order_record_fields_v10(record)
