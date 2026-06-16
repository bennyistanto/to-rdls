"""STAC Item -> RDLS v1.0 record builder.

The climate-risk-stac catalog (https://climate-risk-data.github.io/climate-risk-stac/)
ships each Item with a structured property block that closely mirrors RDLS
v1.0 concepts. This module performs a near-1:1 field-level mapping using the
fixed dictionaries below. No LLM, no extraction cascade.

Mapping decisions (reviewed 2026-06-03):
  - One STAC Item -> one RDLS v1.0 record (Item↔record 1:1).
  - Multi-value `subcategory` strings ("coastal flood,fluvial flood,...")
    map to ONE record with the PRIMARY process; the description gets a
    "Also covers ..." note so secondary processes are not lost.
  - The "nan, nan" Items are extreme-precipitation indices (RX1day/RX5day/
    R99p/R95p). A raw precipitation index is NOT a hazard, so these are
    SKIPPED (return None -> routed to not_rdls), per team guidance 2026-06.
    Do NOT assume precipitation = flood/pluvial_flood.
  - Vague `scenarios` (e.g. "RCPs", "SSPs", "SSP-RCP combinations") cannot
    be matched against RDL's closed climate.scenario codelist; we record
    the raw string in description rather than fabricate a specific code.
  - `spatial resolution` labels like "feature level" / "event level" /
    "city level" / "administrative units (admin1)" are not numeric meters;
    we route them to `spatial_aggregation` (free text) instead of
    `spatial_resolution` (numeric meters).
"""

from __future__ import annotations

import math
import re
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .utils import sanitize_text, sort_rdt_hevl
from .translate import build_entity, order_record_fields_v10


def _abs_url(base_url: str, href: str) -> str:
    """Resolve a STAC link href (possibly relative) against the document's
    `_source_url`. Returns the input href unchanged when no base is known
    or when href is already absolute."""
    if not href:
        return ""
    if href.startswith(("http://", "https://")):
        return href
    if not base_url:
        return href
    try:
        return urllib.parse.urljoin(base_url, href)
    except Exception:
        return href


# ---------------------------------------------------------------------------
# Mapping dictionaries — hazard branch
# ---------------------------------------------------------------------------

# STAC subcategory -> (RDL hazard.type, primary process, secondary-process-list)
_HAZARD_SUBCAT_MAP: Dict[str, Tuple[str, str, List[str]]] = {
    "heat wave":                                                ("extreme_temperature", "extreme_heat", []),
    "cold wave":                                                ("extreme_temperature", "extreme_cold", []),
    "fluvial flood":                                            ("flood", "fluvial_flood", []),
    "coastal flood":                                            ("coastal_flood", "coastal_flood", []),
    "drought":                                                  ("drought", "meteorological_drought", []),
    "wildfire":                                                 ("wildfire", "wildfire", []),
    "tropical cyclone":                                         ("strong_wind", "tropical_cyclone", []),
    "coastal flood,fluvial flood,pluvial flood":                ("flood", "fluvial_flood", ["coastal_flood", "pluvial_flood"]),
    "extratropical cyclone, tropical cyclone":                  ("strong_wind", "extratropical_cyclone", ["tropical_cyclone"]),
    "multi hazard":                                             ("flood", "fluvial_flood", []),  # generic fallback; description carries detail
    # The 12 "nan, nan" precipitation-index items map to pluvial flood.
    "nan, nan":                                                 ("flood", "pluvial_flood", []),
}


# ---------------------------------------------------------------------------
# Mapping dictionaries — exposure / vulnerability branch
# ---------------------------------------------------------------------------

# Each entry: (target_section, category, dimension, quantity_kind, vuln_approach)
# target_section ∈ {"exposure", "vulnerability_func", "socio_economic"}.
_EXVUL_SUBCAT_MAP: Dict[str, Tuple[str, str, str, str, str]] = {
    "urban/built-up footprints":         ("exposure",          "buildings",           "structure",  "area",       ""),
    "population number":                 ("exposure",          "population",          "population", "count",      ""),
    "land use/land cover footprints":    ("exposure",          "natural_environment", "product",    "area",       ""),
    "infrastructure footprints":         ("exposure",          "infrastructure",      "structure",  "count",      ""),
    "building footprints":               ("exposure",          "buildings",           "structure",  "count",      ""),
    "socioeconomic status":              ("socio_economic",    "development_index",   "index",      "count",      ""),
    "demographics":                      ("socio_economic",    "population",          "population", "count",      ""),
    "urban/built-up characteristics":    ("vulnerability_func","buildings",           "structure",  "",           "empirical"),
    "infrastructure characteristics":    ("vulnerability_func","infrastructure",      "structure",  "",           "empirical"),
    "building characteristics":          ("vulnerability_func","buildings",           "structure",  "",           "empirical"),
    "land use/land cover characteristics":("vulnerability_func","natural_environment","product",    "",           "empirical"),
    "vulnerability index":               ("socio_economic",    "development_index",   "index",      "count",      ""),
}


# ---------------------------------------------------------------------------
# data format -> (media_type, format_text)
# ---------------------------------------------------------------------------

_FORMAT_MAP: Dict[str, Tuple[str, str]] = {
    "netcdf":     ("application/x-netcdf",                 "NetCDF"),
    "geotiff":    ("image/tiff;application=geotiff",       "GeoTIFF"),
    "shapefile":  ("application/vnd.shp",                  "Shapefile"),
    "csv":        ("text/csv",                             "CSV"),
    "excel":      ("application/vnd.ms-excel",             "Excel"),
    "txt":        ("text/plain",                           "Text"),
    "ascii":      ("",                                     "ASCII Grid"),
    "grib":       ("application/x-grib2",                  "GRIB"),
    "grib2":      ("application/x-grib2",                  "GRIB2"),
    "geodatabase":("",                                     "File GDB"),
    "pbf":        ("application/x-protobuf",               "PBF"),
    "geoparquet": ("",                                     "GeoParquet"),
    "flatgeobuf": ("",                                     "FlatGeobuf"),
    "unknown":    ("",                                     ""),
}


_SOURCE_TYPE_MAP = {
    "modeled":  "simulated",
    "observed": "observed",
}


_ANALYSIS_TYPE_HAZARD = {"probabilistic", "empirical", "deterministic"}


# vulnerability function approach mapping (13 distinct STAC values)
_VULN_APPROACH_MAP: Dict[str, str] = {
    "areal weighting (unmodeled)":                            "empirical",
    "dasymetric modeling":                                    "analytical",
    "data fusion":                                            "hybrid",
    "random forest modeling":                                 "analytical",
    "equal weighting":                                        "empirical",
    "monte carlo simulations":                                "analytical",
    "cellular automata":                                      "analytical",
    "weighted sum":                                           "empirical",
    "neural network":                                         "analytical",
    "deep learning approach":                                 "analytical",
    "cross-entropy approach":                                 "analytical",
    "ridgeline sampling and regression method":               "analytical",
    "growth difference coastal versus inland":                "empirical",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_NON_NUMERIC_RESOLUTION = {
    "feature level", "event level", "station", "city level",
    "administrative units (admin1)", "administrative units (admin2)",
    "administrative units (admin3)", "hydrological sub-basins",
}


def _parse_spatial_resolution(text: str) -> Tuple[Optional[float], Optional[str]]:
    """Return (resolution_metres, spatial_aggregation_text).

    Numeric inputs go to metres; named scales (admin1, feature level...) go
    to spatial_aggregation (free text)."""
    if not text:
        return None, None
    t = text.strip().lower()
    if t in _NON_NUMERIC_RESOLUTION:
        return None, text
    # "N decimal degrees"
    m = re.match(r"([\d.]+)\s*decimal\s*degrees?", t)
    if m:
        deg = float(m.group(1))
        return round(deg * 111_000), None  # ~111 km per degree at equator
    # "N arc seconds" / "N arc minutes"
    m = re.match(r"([\d.]+)\s*arc\s*seconds?", t)
    if m:
        sec = float(m.group(1))
        return round(sec * 30.87), None  # 1 arc-second ≈ 30.87 m at equator
    m = re.match(r"([\d.]+)\s*arc\s*minutes?", t)
    if m:
        minutes = float(m.group(1))
        return round(minutes * 1851.85), None  # 60 * 30.87 m
    # "N meters" / "N metres" / "N m"
    m = re.match(r"([\d.]+)\s*(?:meters?|metres?|m)\b", t)
    if m:
        return round(float(m.group(1))), None
    # "N kilometers" / "N km"
    m = re.match(r"([\d.]+)\s*(?:kilometers?|kilometres?|km)\b", t)
    if m:
        return round(float(m.group(1)) * 1000), None
    return None, text


def _parse_temporal_cadence(text: Optional[str]) -> Optional[str]:
    """Parse the cadence tail of a STAC `temporal coverage` string.

    Examples:
      "1951-2014 (daily)"        -> "P1D"
      "1850-2014 (monthly)"      -> "P1M"
      "2000-2020 (5-yearly)"     -> "P5Y"
      "1975-2030 (5-yearly)"     -> "P5Y"
      "2010"                     -> None (no parenthetical cadence)
      "1983-2016 (yearly)"       -> "P1Y"
    """
    if not text:
        return None
    m = re.search(r"\(\s*([^)]+?)\s*\)", text)
    if not m:
        return None
    cad = m.group(1).strip().lower()
    # Numeric prefix like "5-yearly" / "10-yearly"
    nm = re.match(r"^(\d+)\s*-\s*(year|month|day|hour)(?:ly)?$", cad)
    if nm:
        n, unit = nm.group(1), nm.group(2)
        return {"year": f"P{n}Y", "month": f"P{n}M", "day": f"P{n}D", "hour": f"PT{n}H"}[unit]
    return {
        "daily":   "P1D",
        "monthly": "P1M",
        "yearly":  "P1Y",
        "annual":  "P1Y",
        "hourly":  "PT1H",
        "weekly":  "P7D",
    }.get(cad)


def _parse_scenarios(text: str) -> Tuple[Optional[str], str]:
    """Try to extract a SPECIFIC climate scenario code (e.g. RCP8.5, SSP5-8.5).

    Returns (specific_code or None, original_text). When the catalog only
    says "RCPs"/"SSPs"/"SSP-RCP combinations" we cannot infer one; downstream
    we'll preserve the original_text in description.
    """
    if not text:
        return None, ""
    t = text.strip()
    raw = t
    # Look for specific codes inside the string
    m = re.search(r"\b(RCP\s?[\d.]+|SSP\s?\d(?:-\s?\d\.?\d?)?)\b", t)
    if m:
        return m.group(1).replace(" ", ""), raw
    return None, raw


def _slug(text: str, max_len: int = 32) -> str:
    s = re.sub(r"[^a-z0-9]+", "", (text or "").lower())
    return s[:max_len] or "item"


# ---------------------------------------------------------------------------
# Generic STAC "stratification dimensions" extractor
# ---------------------------------------------------------------------------
# STAC publishers expose the dimensions along which a dataset varies
# (scenarios, time horizons, return periods, defense levels, model runs, ...)
# in several different shapes:
#
#   * Standard STAC Collection.summaries          -> dict[str, list]
#   * CoCliCo extension summaries_descriptions    -> dict[str, str]   (per-key explainers)
#   * CoCliCo extension summaries_labels          -> dict[str, dict]  (code -> human label)
#   * STAC datacube extension cube:dimensions     -> dict[str, dict]  (values/extent + type)
#   * Standard version                            -> str
#
# The extractor reads each shape if present and returns a normalised dict so
# downstream builders treat all catalogs uniformly. New catalogs with novel
# shapes should be added here, not in per-catalog branches.

# Climate-scenario code map (cross-catalog form -> RDLS codelist code)
_SCENARIO_CODE_MAP: Dict[str, str] = {
    "SSP119": "SSP1-1.9", "SSP1-1.9": "SSP1-1.9", "SSP1.1.9": "SSP1-1.9",
    "SSP126": "SSP1-2.6", "SSP1-2.6": "SSP1-2.6", "SSP1.2.6": "SSP1-2.6",
    "SSP245": "SSP2-4.5", "SSP2-4.5": "SSP2-4.5", "SSP2.4.5": "SSP2-4.5",
    "SSP370": "SSP3-7.0", "SSP3-7.0": "SSP3-7.0", "SSP3.7.0": "SSP3-7.0",
    "SSP434": "SSP4-3.4", "SSP4-3.4": "SSP4-3.4",
    "SSP460": "SSP4-6.0", "SSP4-6.0": "SSP4-6.0",
    "SSP534": "SSP5-3.4", "SSP5-3.4": "SSP5-3.4",
    "SSP585": "SSP5-8.5", "SSP5-8.5": "SSP5-8.5", "SSP5.8.5": "SSP5-8.5",
    "RCP26": "RCP2.6", "RCP2.6": "RCP2.6",
    "RCP45": "RCP4.5", "RCP4.5": "RCP4.5",
    "RCP60": "RCP6.0", "RCP6.0": "RCP6.0",
    "RCP85": "RCP8.5", "RCP8.5": "RCP8.5",
}

# Sentinel values that mean "not a scenario" — drop before counting unique codes
_SCENARIO_SENTINELS = {"none", "no scenario", "no_scenario", "static",
                       "baseline", "historical", "present", ""}


def extract_stac_dimensions(obj: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Generic extractor for STAC stratification dimensions.

    Reads from `summaries`, `summaries_descriptions`, `summaries_labels`,
    `cube:dimensions`, and `version` on a Collection or Item dict. Returns:

        {
            "<dimension_name>": {
                "values":      [v1, v2, ...],
                "labels":      {code: human_label}   (optional),
                "description": "explainer text"      (optional),
            },
            ...
        }

    Returns {} when nothing useful is present.
    """
    out: Dict[str, Dict[str, Any]] = {}
    if not isinstance(obj, dict):
        return out

    summaries = obj.get("summaries") or {}
    descs = obj.get("summaries_descriptions") or {}
    labels = obj.get("summaries_labels") or {}

    if isinstance(summaries, dict):
        for key, vals in summaries.items():
            if vals is None or vals == "" or vals == [] or vals == {}:
                continue
            entry: Dict[str, Any] = {
                "values": vals if isinstance(vals, list) else [vals],
            }
            if isinstance(descs, dict) and descs.get(key):
                entry["description"] = str(descs[key]).strip()
            if isinstance(labels, dict) and isinstance(labels.get(key), dict):
                entry["labels"] = {str(k): str(v) for k, v in labels[key].items()}
            out[key] = entry

    # STAC datacube extension
    cube = obj.get("cube:dimensions") or {}
    if isinstance(cube, dict):
        for key, info in cube.items():
            if key in out or not isinstance(info, dict):
                continue
            vals = info.get("values") or info.get("extent") or []
            if not vals:
                continue
            entry = {"values": vals if isinstance(vals, list) else [vals]}
            if info.get("description"):
                entry["description"] = str(info["description"]).strip()
            if info.get("type"):
                entry["type"] = str(info["type"])
            out[key] = entry

    return out


def format_dimensions_text(
    dims: Dict[str, Dict[str, Any]],
    *,
    include_descriptions: bool = True,
    max_values_per_dim: int = 8,
) -> str:
    """Render the dimensions dict as a one-paragraph human-readable string.

    Per-dimension format: `<name>: code1 (label1), code2 (label2), ... [- explainer]`.
    Returns "" when dims is empty.
    """
    if not dims:
        return ""
    parts: List[str] = []
    for name, info in dims.items():
        vals = info.get("values") or []
        if not isinstance(vals, list):
            vals = [vals]
        if not vals:
            continue
        labels = info.get("labels") or {}
        shown: List[str] = []
        for v in vals[:max_values_per_dim]:
            vstr = str(v)
            if labels.get(vstr) and labels[vstr] != vstr:
                shown.append(f"{vstr} ({labels[vstr]})")
            else:
                shown.append(vstr)
        line = f"{name}: " + ", ".join(shown)
        if len(vals) > max_values_per_dim:
            line += f", ... ({len(vals) - max_values_per_dim} more)"
        if include_descriptions and info.get("description"):
            line += f" - {info['description'].rstrip('.')}"
        parts.append(line)
    return "; ".join(parts)


def extract_numeric_return_periods(dims: Dict[str, Dict[str, Any]]) -> List[float]:
    """Pull numeric return periods (years) from a `rp`/`return period` dimension.

    Drops sentinels like `static`, `0`, `none`, `no return period`. Returns a
    sorted ascending list of floats. Used to populate
    `event_sets[].events[].occurrence.probabilistic.return_period` when the
    Collection genuinely lists discrete RPs.
    """
    SENTINELS = {"", "0", "static", "none", "no return period", "no_return_period", "baseline"}
    for key, info in dims.items():
        if key.strip().lower() not in {"rp", "return_period", "return period", "return periods"}:
            continue
        vals = info.get("values") or []
        out: List[float] = []
        for v in vals:
            vv = str(v).strip()
            if vv.lower() in SENTINELS:
                continue
            try:
                out.append(float(vv))
            except ValueError:
                # Tolerate forms like "100yr" or "RP100"
                m = re.search(r"\d+(?:\.\d+)?", vv)
                if m:
                    try:
                        out.append(float(m.group()))
                    except ValueError:
                        pass
        return sorted(set(out))
    return []


def lift_single_scenario(dims: Dict[str, Dict[str, Any]]) -> Optional[str]:
    """If a `scenario`-like dimension contains exactly one codelist-valid SSP/RCP
    value (after stripping sentinels), return the RDLS climate.scenario code.

    Per RDLS v1.0 template: "use only when resource covers EXACTLY ONE
    scenario." Multi-scenario datasets must NOT set climate.scenario; their
    full scenario list goes into description / analysis_details.
    """
    for key, info in dims.items():
        if key.strip().lower() not in {"scenario", "scenarios", "climate_scenario", "climate scenarios"}:
            continue
        vals = info.get("values") or []
        cleaned: List[str] = []
        for v in vals:
            vv = str(v).strip()
            if vv.lower() in _SCENARIO_SENTINELS:
                continue
            mapped = _SCENARIO_CODE_MAP.get(vv) or _SCENARIO_CODE_MAP.get(vv.replace(" ", ""))
            if not mapped:
                # Try regex extract from prose values like "RCP 8.5"
                m = re.search(r"\b(RCP\s?[\d.]+|SSP\s?\d(?:-\s?\d\.?\d?)?)\b", vv)
                if m:
                    mapped = _SCENARIO_CODE_MAP.get(m.group(1).replace(" ", ""))
            if mapped and mapped not in cleaned:
                cleaned.append(mapped)
        if len(cleaned) == 1:
            return cleaned[0]
    return None


# ---------------------------------------------------------------------------
# Sub-builders
# ---------------------------------------------------------------------------

def _build_publisher_creator_contact(
    collection_doc: Dict[str, Any],
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    """Map STAC Collection providers[] (name + roles[] + url) to RDLS
    top-level publisher / creator / contact_point.

    Strategy: providers carry STAC roles in {licensor, host, producer,
    processor}. We map:
        publisher    <- first provider with role 'licensor' or 'host'
        creator      <- first provider with role 'producer' (else 'processor')
        contact_point<- same as publisher when nothing else is available
    Fall back to the catalog-level placeholder when no providers.
    """
    providers = collection_doc.get("providers") or []
    catalog_fallback = build_entity(
        "Climate Risk STAC",
        url="https://climate-risk-data.github.io/climate-risk-stac/",
    )

    def first_with_role(*roles: str) -> Optional[Dict[str, Any]]:
        for p in providers:
            for r in (p.get("roles") or []):
                if r.lower() in roles:
                    return p
        return None

    def to_entity(p: Optional[Dict[str, Any]], fallback: Dict[str, Any]) -> Dict[str, Any]:
        if not p:
            return fallback
        return build_entity(
            sanitize_text(p.get("name") or "") or "Unknown",
            url=p.get("url") or None,
        )

    licensor = first_with_role("licensor", "host")
    producer = first_with_role("producer", "processor")
    publisher = to_entity(licensor or producer, catalog_fallback)
    creator = to_entity(producer or licensor, publisher)
    contact_point = to_entity(licensor or producer, publisher)
    return publisher, creator, contact_point


_URL_RE = re.compile(r"^https?://", re.IGNORECASE)


def _looks_like_url(s: Optional[str]) -> bool:
    return bool(s) and bool(_URL_RE.match((s or "").strip()))


def _asset_title(
    asset: Dict[str, Any],
    asset_key: str,
    item_title: str,
    collection_title: str,
) -> str:
    """Pick a human-readable title for an asset.

    STAC publishers sometimes put the download URL in the title field;
    when we detect that we fall back to the Item / Collection title."""
    raw = sanitize_text(asset.get("title") or "")
    if raw and not _looks_like_url(raw):
        return raw
    raw_desc = sanitize_text(asset.get("description") or "")
    if raw_desc and not _looks_like_url(raw_desc):
        return raw_desc
    # Compose a sensible fallback using the Item title (preferred) or Collection
    if item_title and not _looks_like_url(item_title):
        return f"{item_title} - data file"
    if collection_title:
        return f"{collection_title} - data file"
    return f"Data file ({asset_key})"


def _build_resources(
    item: Dict[str, Any],
    collection_doc: Dict[str, Any],
    media_type: str,
    format_text: str,
    coordinate_system: str,
    *,
    title: str,
    collection_title: str,
) -> List[Dict[str, Any]]:
    """Build the v1.0 resources[] list.

    Resources emitted, in order:
      1. STAC self-link (access_url + conforms_to STAC API spec) — declares
         the catalogue origin of this metadata record.
      2. Each STAC Item asset as a downloadable resource.
      3. Each cite-as link (Publication / Code) as an access-only reference.
    """
    resources: List[Dict[str, Any]] = []

    # Pass 0: STAC catalog self-link (canonical pointer back to the Item).
    # Prefer Item.self; fall back to the Item's actual fetched URL (stamped
    # on by the crawler) so we always have an ABSOLUTE catalog URL.
    item_base = item.get("_source_url") or ""
    item_self_url = ""
    for link in item.get("links") or []:
        if link.get("rel") == "self":
            item_self_url = _abs_url(item_base, link.get("href") or "")
            break
    if not item_self_url and item_base:
        item_self_url = item_base
    if not item_self_url:
        for link in item.get("links") or []:
            if link.get("rel") == "collection":
                item_self_url = _abs_url(item_base, link.get("href") or "")
                if item_self_url:
                    break
    if item_self_url:
        resources.append({
            "id": "resource_stac_item",
            "title": f"STAC Item: {title}" if title else "STAC Item metadata",
            "description": (
                "Source STAC Item metadata record for this dataset, hosted as part of "
                "the climate-risk-stac catalogue. Conforms to STAC v1.0.0."
            ),
            "media_type": "application/geo+json",
            "conforms_to": "https://api.stacspec.org/v1.0.0/",
            "access_url": item_self_url,
        })

    # Pass 1: assets on the Item
    assets = item.get("assets") or {}
    for key, a in assets.items():
        if not isinstance(a, dict):
            continue
        url = a.get("href")
        if not url:
            continue
        res_title = _asset_title(a, key, title, collection_title)
        res_desc = sanitize_text(a.get("description") or "") or res_title
        if _looks_like_url(res_desc):
            res_desc = res_title
        res: Dict[str, Any] = {
            "id": f"resource_{key}",
            "title": res_title,
            "description": res_desc,
        }
        a_media = a.get("type")
        if a_media:
            res["media_type"] = a_media
        elif media_type:
            res["media_type"] = media_type
        elif format_text:
            res["format"] = format_text
        if coordinate_system:
            res["coordinate_system"] = coordinate_system
        res["download_url"] = url
        resources.append(res)

    # Pass 2: cite-as Publication / Code links from item or collection
    for source_doc in (item, collection_doc):
        for link in source_doc.get("links") or []:
            if link.get("rel") != "cite-as":
                continue
            href = link.get("href")
            if not href:
                continue
            t = sanitize_text(link.get("title") or "") or "Reference"
            res: Dict[str, Any] = {
                "id": f"resource_link_{_slug(t)}",
                "title": t,
                "description": t,
                "format": "Web reference",
                "access_url": href,
            }
            # De-dup by url
            if any(r.get("access_url") == href or r.get("download_url") == href for r in resources):
                continue
            resources.append(res)

    # Fallback: parent Collection self URL if still nothing
    if not resources:
        self_url = ""
        for link in collection_doc.get("links") or []:
            if link.get("rel") == "self":
                self_url = link.get("href") or ""
                break
        if self_url:
            resources.append({
                "id": "resource_collection",
                "title": collection_title or "STAC Collection",
                "description": sanitize_text(collection_doc.get("description") or "")[:300] or "STAC Collection",
                "media_type": "application/json",
                "conforms_to": "https://api.stacspec.org/v1.0.0/",
                "access_url": self_url,
            })
    return resources


def _build_referenced_by(
    item: Dict[str, Any],
    collection_doc: Dict[str, Any],
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    props = item.get("properties") or {}
    doi = (props.get("sci:doi") or "").strip()
    if doi:
        url = doi if doi.startswith("http") else f"https://doi.org/{doi.lstrip('doi:').strip()}"
        out.append({"id": "ref_doi", "doi": doi, "url": url})
    # Publication link from item or collection
    for source in (item, collection_doc):
        for link in source.get("links") or []:
            if link.get("rel") != "cite-as":
                continue
            title = (link.get("title") or "").lower()
            href = link.get("href")
            if not href:
                continue
            if "publication" in title and not any(r.get("url") == href for r in out):
                out.append({
                    "id": f"ref_pub_{_slug(link.get('title') or 'publication', 16)}",
                    "name": sanitize_text(link.get("title") or "Publication") or "Publication",
                    "url": href,
                })
    return out


def _build_lineage(
    item: Dict[str, Any],
    collection_doc: Dict[str, Any],
    risk_data_type: List[str],
) -> Dict[str, Any]:
    """lineage.description ← collection.description
       lineage.sources[]   ← split `underlying data` (comma) + code-link"""
    sources: List[Dict[str, Any]] = []
    props = item.get("properties") or {}

    underlying = (props.get("underlying data") or "").strip()
    if underlying and underlying.lower() not in {"nan", "n/a", "none", ""}:
        for idx, name in enumerate(re.split(r"[,;]+\s*", underlying), 1):
            name = name.strip()
            if not name:
                continue
            entry: Dict[str, Any] = {
                "id": f"source_underlying_{idx}",
                "name": name,
                "type": "dataset",
            }
            if risk_data_type:
                entry["risk_data_type"] = list(risk_data_type)
                entry["used_in"] = risk_data_type[0]
            sources.append(entry)

    # Code-link as model lineage source
    for link in (item.get("links") or []) + (collection_doc.get("links") or []):
        if link.get("rel") != "cite-as":
            continue
        title = (link.get("title") or "").lower()
        href = link.get("href")
        if not href or "code" not in title:
            continue
        if any(s.get("url") == href for s in sources):
            continue
        sources.append({
            "id": f"source_code_{_slug(link.get('title') or 'code', 16)}",
            "name": sanitize_text(link.get("title") or "Code reference") or "Code reference",
            "url": href,
            "type": "model",
        })

    lineage: Dict[str, Any] = {}
    cdesc = sanitize_text(collection_doc.get("description") or "")
    if cdesc:
        lineage["description"] = cdesc
    if sources:
        lineage["sources"] = sources
    return lineage


def _build_temporal(props: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """temporal.start / end from start_datetime / end_datetime (year only)."""
    start = (props.get("start_datetime") or "").strip()
    end = (props.get("end_datetime") or "").strip()
    out: Dict[str, Any] = {}
    if start:
        out["start"] = start[:4]
    if end:
        out["end"] = end[:4]
    if not out:
        return None
    return out


def _build_climate_block(props: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], str]:
    """Return (climate block, raw scenarios text for description)."""
    scenarios_raw = (props.get("scenarios") or "").strip()
    if not scenarios_raw:
        return None, ""
    specific, raw = _parse_scenarios(scenarios_raw)
    block: Dict[str, Any] = {}
    if specific:
        block["scenario"] = specific
    if not block:
        return None, raw  # vague — return only the raw text for description
    return block, raw


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

# Schema link URL (v1.0)
_SCHEMA_LINK = "https://docs.riskdatalibrary.org/en/1__0__0/rdls_schema.json"

# Spatial scale normalisation
_SCALE_MAP = {
    "(near-)global":  "global",
    "near-global":    "global",
    "near global":    "global",
    "global":         "global",
    "regional":       "regional",
    "national":       "national",
    "sub-national":   "sub-national",
    "urban":          "urban",
}


def build_record_stac_v10(
    item: Dict[str, Any],
    collection: Dict[str, Any],
    *,
    org_slug: str = "crstac",
    catalog_name: str = "climate-risk-stac",
) -> Optional[Dict[str, Any]]:
    """Translate a STAC Item + its parent Collection to an RDLS v1.0 record."""
    props = item.get("properties") or {}
    raw_item_title = sanitize_text(props.get("title") or item.get("id") or "")
    if not raw_item_title:
        return None
    collection_title = sanitize_text(collection.get("title") or "")
    # Combine Collection title + Item title when they describe different
    # facets of the dataset. Skip the join when the item title already
    # contains the collection title (case-insensitive) or vice versa.
    if (
        collection_title
        and raw_item_title.lower() not in collection_title.lower()
        and collection_title.lower() not in raw_item_title.lower()
    ):
        title = f"{collection_title} - {raw_item_title}"
    else:
        title = raw_item_title

    raw_rdt = (props.get("risk data type") or "").strip().lower()
    if raw_rdt not in {"hazard", "exposure", "vulnerability", "loss"}:
        return None
    risk_data_type: List[str] = sort_rdt_hevl([raw_rdt])

    description = sanitize_text(props.get("description") or "") or title
    source_suffix = (
        f"[Source: This metadata record was automatically extracted from the "
        f"STAC catalog {catalog_name} (https://climate-risk-data.github.io/climate-risk-stac/)]"
    )

    # --- subcategory normalisation ---
    subcat_raw = (props.get("subcategory") or "").strip()
    subcat_lc = subcat_raw.lower()

    multi_processes: List[str] = []
    if risk_data_type == ["hazard"]:
        # Precipitation-amount indices (RX1day/RX5day/R95p/R99p, etc.) arrive
        # with subcategory "nan, nan". Per team guidance (2026-06): a raw
        # precipitation index is NOT itself a hazard — do NOT assume it is
        # flood/pluvial_flood. Skip it (routes to not_rdls), rather than
        # fabricating a flood classification.
        if subcat_lc == "nan, nan":
            return None
        haz_type, haz_process, secondary = _HAZARD_SUBCAT_MAP.get(
            subcat_lc, ("flood", "fluvial_flood", [])
        )
        multi_processes = secondary
        if multi_processes:
            description = (
                description.rstrip(".") +
                f". Also covers processes: {', '.join(multi_processes)} "
                f"(STAC subcategory: \"{subcat_raw}\")."
            )

    # --- spatial ---
    bbox = item.get("bbox")
    scale_text = (props.get("spatial scale") or "").strip().lower()
    scale = _SCALE_MAP.get(scale_text, "global")
    spatial: Dict[str, Any] = {"scale": scale}
    if scale != "global":
        # Not seen on this catalog yet, but reserved
        pass
    if isinstance(bbox, list) and len(bbox) == 4:
        spatial["bbox"] = bbox

    # --- temporal ---
    temporal = _build_temporal(props)

    # --- climate scenarios (description-only when vague) ---
    climate_block, raw_scenarios_text = _build_climate_block(props)

    # --- spatial resolution / aggregation ---
    res_text = (props.get("spatial resolution") or "").strip()
    res_metres, res_agg_text = _parse_spatial_resolution(res_text)

    # --- coordinate system ---
    coordinate_system = (props.get("proj:code") or "").strip()

    # --- data format ---
    fmt_key = (props.get("data format") or "").strip().lower()
    media_type, format_text = _FORMAT_MAP.get(fmt_key, ("", fmt_key.title() if fmt_key else ""))

    # --- license ---
    license_value = (collection.get("license") or "").strip()
    if license_value and license_value.lower() not in {"proprietary", "various", "other"}:
        # SPDX short code -> SPDX IRI when we can map it; else preserve text
        license_iri = f"https://spdx.org/licenses/{license_value}.html"
    else:
        license_iri = "https://example.org/license/unknown"

    # --- entities ---
    publisher, creator, contact_point = _build_publisher_creator_contact(collection)

    # --- resources ---
    resources = _build_resources(
        item, collection, media_type, format_text, coordinate_system,
        title=title, collection_title=collection_title,
    )
    if not resources:
        return None

    # --- referenced_by + lineage ---
    referenced_by = _build_referenced_by(item, collection)
    lineage = _build_lineage(item, collection, risk_data_type)

    # --- HEVL block ---
    hevl_block_key: Optional[str] = None
    hevl_block_value: Any = None

    if risk_data_type == ["hazard"]:
        # Build a hazard block with one event_set + one hazard primary
        analysis_type_raw = (props.get("analysis type") or "").strip().lower()
        analysis_type = analysis_type_raw if analysis_type_raw in _ANALYSIS_TYPE_HAZARD else "deterministic"
        calc_method = _SOURCE_TYPE_MAP.get(
            (props.get("source type") or "").strip().lower(), "simulated",
        )
        # IMT default per type
        imt_default = {
            "flood":              "wd:m",
            "coastal_flood":      "wd:m",
            "drought":            "spi:dimensionless",
            "extreme_temperature":"AirTemp:C",
            "strong_wind":        "sws_10m:m/s",
            "wildfire":           "burnedarea:ha",
        }.get(_HAZARD_SUBCAT_MAP.get(subcat_lc, ("flood",))[0], "wd:m")
        haz_type, haz_process, _ = _HAZARD_SUBCAT_MAP.get(subcat_lc, ("flood", "fluvial_flood", []))
        hevl_block_key = "hazard"
        hevl_block_value = {
            "event_sets": [
                {
                    "id": "event_set_1",
                    "hazards": [
                        {
                            "id": "hazard_1",
                            "type": haz_type,
                            "process": haz_process,
                            "intensity_measure": imt_default,
                        }
                    ],
                    "analysis_type": analysis_type,
                    "calculation_method": calc_method,
                }
            ]
        }

    elif risk_data_type in (["exposure"], ["vulnerability"]):
        # Pick the right block based on the subcategory map
        target, category, dimension, qty_kind, vuln_approach = _EXVUL_SUBCAT_MAP.get(
            subcat_lc, ("exposure", "buildings", "structure", "count", "empirical"),
        )
        if target == "exposure" and risk_data_type == ["exposure"]:
            hevl_block_key = "exposure"
            measurement: Dict[str, Any] = {"quantity_kind": qty_kind or "count"}
            if qty_kind == "area":
                measurement["unit"] = "square_metre"
            elif qty_kind == "count":
                measurement["unit"] = "count"
            hevl_block_value = [
                {
                    "id": "exposure_1",
                    "category": category,
                    "metrics": [
                        {
                            "id": "metric_1",
                            "dimension": dimension,
                            "measurement": measurement,
                        }
                    ],
                    "asset_type": {
                        "id": category,
                        "description": sanitize_text(subcat_raw),
                    } if subcat_raw else None,
                }
            ]
            # Drop empty asset_type
            if hevl_block_value[0]["asset_type"] is None:
                hevl_block_value[0].pop("asset_type")
        else:
            # vulnerability — either function or socio_economic
            if target == "socio_economic":
                hevl_block_key = "vulnerability"
                # Schema: reference_year must be in [1900, 2100].
                ref_year_raw = (props.get("start_datetime") or "2024")[:4] or "2024"
                try:
                    ref_year = int(ref_year_raw)
                except ValueError:
                    ref_year = 2024
                ref_year = max(1900, min(2100, ref_year))
                hevl_block_value = {
                    "socio_economic": [
                        {
                            "id": "se_1",
                            "indicator_name": sanitize_text(props.get("title") or subcat_raw) or subcat_raw,
                            "indicator_code": _slug(subcat_raw or "indicator", 24),
                            "description": sanitize_text(description) or "Socio-economic indicator.",
                            "reference_year": ref_year,
                        }
                    ]
                }
            else:
                # vulnerability function
                approach_raw = (props.get("analysis type") or "").strip().lower()
                approach = _VULN_APPROACH_MAP.get(approach_raw, vuln_approach or "empirical")
                hazard_imt = "wd:m"  # most function-relevant flood IMT; description carries detail
                hevl_block_key = "vulnerability"
                hevl_block_value = {
                    "functions": {
                        "vulnerability": [
                            {
                                "id": "vf_1",
                                "approach": approach,
                                "relationship": "discrete",
                                "hazard_primary": {
                                    "type": "flood",
                                    "intensity_measure": hazard_imt,
                                },
                                "category": category,
                                "analysis_details": (
                                    f"STAC subcategory: \"{subcat_raw}\"; "
                                    f"approach derived from STAC `analysis type` = "
                                    f"\"{props.get('analysis type','?')}\"."
                                ),
                            }
                        ]
                    }
                }

    # --- ID ---
    # Keep collection and item slugs separate so collisions in one don't
    # leak into the other. Limit collection slug to 30 chars and item slug
    # to 30 chars; total record_id remains under ~80 chars (safe filename).
    rdt_prefix = {
        "hazard": "hzd", "exposure": "exp",
        "vulnerability": "vln", "loss": "lss",
    }[risk_data_type[0]]
    coll_id = collection.get("id") or ""
    item_id = item.get("id") or title
    coll_slug = _slug(coll_id, 50) if coll_id else ""
    item_slug = _slug(item_id, 30)
    record_id = (
        f"rdls_{rdt_prefix}-{org_slug}_{coll_slug}_{item_slug}"
        if coll_slug else
        f"rdls_{rdt_prefix}-{org_slug}_{item_slug}"
    )

    # --- description (append source suffix + climate scenario note) ---
    if raw_scenarios_text and not climate_block:
        # Vague scenarios — preserve original wording
        description = (
            description.rstrip(".") +
            f". Climate scenarios (as published): \"{raw_scenarios_text}\"."
        )
    description = f"{description.rstrip('.')} {source_suffix}"

    # --- assemble ---
    record: Dict[str, Any] = {
        "id": record_id,
        "title": title,
        "description": description,
        "risk_data_type": risk_data_type,
        "publisher": publisher,
    }
    # `usage notes` in the publisher's STAC items captures how / where to
    # obtain or use the data ("free user account needed", "download via API",
    # ...). RDLS v1.0 has a top-level `purpose` optional string that fits.
    usage_notes = sanitize_text(props.get("usage notes") or "")
    if usage_notes:
        record["purpose"] = usage_notes
    # `temporal coverage` carries a parenthetical cadence tail like "(daily)",
    # "(monthly)", "(yearly)", "(5-yearly)". Lift it to top-level
    # temporal_resolution as ISO 8601 duration. The date range itself is
    # already captured via start_datetime / end_datetime.
    temporal_resolution = _parse_temporal_cadence(props.get("temporal coverage"))
    if temporal_resolution:
        record["temporal_resolution"] = temporal_resolution
    record["contact_point"] = contact_point
    record["creator"] = creator
    record["spatial"] = spatial
    if temporal:
        record["temporal"] = temporal
    if res_metres is not None:
        record["spatial_resolution"] = res_metres
    record["license"] = license_iri
    if lineage:
        record["lineage"] = lineage
    if referenced_by:
        record["referenced_by"] = referenced_by

    # Resources: inject spatial_aggregation + climate block on each resource
    for res in resources:
        if res_agg_text:
            res["spatial_aggregation"] = res_agg_text
        if climate_block:
            res["climate"] = dict(climate_block)
    record["resources"] = resources

    if hevl_block_key:
        record[hevl_block_key] = hevl_block_value

    record["links"] = [
        {"href": _SCHEMA_LINK, "rel": "describedby"},
    ]

    return order_record_fields_v10(record)
