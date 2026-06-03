"""CoCliCo STAC Collection -> RDLS v1.0 record builder.

Unlike climate-risk-stac (where each Item is a separate dataset and carries
pre-classified risk concepts in its properties), CoCliCo Collections wrap
many Items that represent rendering variants of the same dataset (different
sea-level-rise thresholds, different time slices, map tiles, etc.). The
dataset-level semantics live on the Collection, not the Items.

This builder produces ONE RDLS v1.0 record per Collection, with:
  - risk_data_type inferred from title + keywords (no pre-classification)
  - subcategory inferred similarly
  - Collection.providers / license / extent / keywords used directly
  - Each item in Collection.assets becomes one RDLS resource

No HEVL extractor needed — Collection metadata is clean enough that title +
keyword pattern matching captures the right RDLS category.
"""

from __future__ import annotations

import re
import urllib.parse
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .utils import sanitize_text, sort_rdt_hevl
from .translate import build_entity, order_record_fields_v10
from .translate_stac_v10 import (
    _slug, _looks_like_url, _abs_url, _FORMAT_MAP,
    _SCHEMA_LINK,
)


# ---------------------------------------------------------------------------
# Risk-data-type inference rules — title / keywords / description signals
# ---------------------------------------------------------------------------

# Each entry: (pattern, risk_data_type, subcategory)
# Rules are ordered: explicit EXPOSURE / VULNERABILITY / LOSS title patterns
# come BEFORE hazard patterns so that titles like "Damage Costs" or "People
# Exposure" don't get misclassified just because their description mentions
# "flood" or "coastal".
_TITLE_RULES: List[Tuple[str, str, str]] = [
    # ---- VULNERABILITY (cost-benefit, perspectives, indices) ----
    (r"\bcost.?benefit",              "vulnerability", "cost-benefit"),
    (r"\bcost\s*benefit\s*analysis",  "vulnerability", "cost-benefit"),
    (r"\bcba\b",                      "vulnerability", "cost-benefit"),
    (r"\bvulnerab",                   "vulnerability", "vulnerability"),
    (r"\bperspective",                "vulnerability", "perspective"),
    (r"\bcoastal\s*typology",         "vulnerability", "vulnerability index"),
    # ---- LOSS (realized damage / cost outputs) ----
    (r"\bdamage\s*cost",              "loss",          "damage"),
    (r"\bdamages?\b",                 "loss",          "damage"),
    # ---- EXPOSURE (assets at risk, not the hazard itself) ----
    (r"\bbuilding\s*exposure",        "exposure",      "building footprints"),
    (r"\bpeople\s*exposure",          "exposure",      "population number"),
    (r"\bpopulation\s*projection",    "exposure",      "population number"),
    (r"\bpopulation\b",               "exposure",      "population number"),
    (r"\bexposure\s*database",        "exposure",      "building footprints"),
    (r"\bexposure",                   "exposure",      "population number"),
    (r"\bcritical\s*infrastructure",  "exposure",      "infrastructure footprints"),
    (r"\binfrastructure",             "exposure",      "infrastructure footprints"),
    (r"\bbuildings?\b",               "exposure",      "building footprints"),
    (r"\bdtm\b|\bdigital terrain model", "exposure",   "land use/land cover footprints"),
    (r"\bcoastal\s*mask",             "exposure",      "land use/land cover footprints"),
    (r"\btransect",                   "exposure",      "infrastructure footprints"),
    (r"\bcoastal\s*grid",             "exposure",      "land use/land cover footprints"),
    # Administrative units — treat as exposure (population / land use base layers)
    (r"\bnomenclature.*territorial",  "exposure",      "land use/land cover footprints"),
    (r"\bregions\b",                  "exposure",      "land use/land cover footprints"),
    (r"\bcountries\b",                "exposure",      "land use/land cover footprints"),
    (r"\bmunicipalit",                "exposure",      "land use/land cover footprints"),
    (r"\blocal\s*administrative",     "exposure",      "land use/land cover footprints"),
    # ---- HAZARDS (coastal flood, sea level, erosion) ----
    (r"\bflood\s*map",                "hazard",        "coastal flood"),
    (r"\bcoastal\s*flood\s*risk",     "hazard",        "coastal flood"),
    (r"\bflood\s*risk",               "hazard",        "coastal flood"),
    (r"\bflood\b",                    "hazard",        "coastal flood"),
    (r"\bextreme.{0,5}surge",         "hazard",        "coastal flood"),
    (r"\bextreme.{0,5}sea.{0,5}level",     "hazard",        "coastal flood"),
    (r"\bsea[\s-]level\s*rise",       "hazard",        "sea level rise"),
    (r"\bsea[\s-]level\b",            "hazard",        "sea level rise"),
    (r"\bwave\s*energy",              "hazard",        "wave"),
    (r"\bwater\s*level",              "hazard",        "coastal flood"),
    (r"\bdrivers?\s*of\s*twl",        "hazard",        "coastal flood"),
    (r"\bshoreline\s*change",         "hazard",        "coastal erosion"),
    (r"\bshoreline\s*projection",     "hazard",        "coastal erosion"),
    (r"\bshoreline\b",                "hazard",        "coastal erosion"),
    (r"\bshorelinemonitor",           "hazard",        "coastal erosion"),
    (r"\bmorphodynamic",              "hazard",        "coastal erosion"),
    # ---- STATISTICS suffix on any title ----
    # ("X - statistics" inherits X's classification, but our title-based
    # matching captures the parent term first; this is a safety net only.)
    (r"\bstatistics\b",               "vulnerability", "vulnerability index"),
]

# Hazard subcategory -> (RDL type, primary process)
_HAZARD_TYPE_MAP: Dict[str, Tuple[str, str]] = {
    "coastal flood":   ("coastal_flood",  "coastal_flood"),
    "sea level rise":  ("sea_level_rise", "sea_level_rise"),
    "wave":            ("coastal_flood",  "storm_surge"),
    "coastal erosion": ("erosion",        "coastal_erosion"),
}

# Exposure subcategory -> (category, dimension, quantity_kind, unit)
_EXPOSURE_TYPE_MAP: Dict[str, Tuple[str, str, str, str]] = {
    "population number":               ("population",          "population", "count", "count"),
    "building footprints":             ("buildings",           "structure",  "count", "count"),
    "infrastructure footprints":       ("infrastructure",      "structure",  "count", "count"),
    "land use/land cover footprints":  ("natural_environment", "product",    "area",  "square_metre"),
}


def _infer_risk_type(title: str, description: str, keywords: List[str]) -> Tuple[str, str]:
    """Return (risk_data_type, subcategory) using ordered rules.

    Pass 1: match against TITLE only (strongest signal — the title is the
            authoritative thing the dataset wants to be called).
    Pass 2: if title didn't match, fall back to title + keywords + description
            (looser signal).
    """
    title_lc = (title or "").lower()
    for pat, rdt, subcat in _TITLE_RULES:
        if re.search(pat, title_lc, re.IGNORECASE):
            return rdt, subcat
    haystack = " ".join([title or "", description or "", " ".join(keywords or [])]).lower()
    for pat, rdt, subcat in _TITLE_RULES:
        if re.search(pat, haystack, re.IGNORECASE):
            return rdt, subcat
    # Fallback: classify as hazard if hazard keyword found, else exposure.
    if re.search(r"hazard|risk|prone|surge|tide|extreme", haystack, re.IGNORECASE):
        return "hazard", "coastal flood"
    return "exposure", "land use/land cover footprints"


# ---------------------------------------------------------------------------
# Entity mapping from STAC providers[]
# ---------------------------------------------------------------------------

def _provider_to_entity(p: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not isinstance(p, dict):
        return None
    name = sanitize_text(p.get("name") or "")
    if not name:
        return None
    url = p.get("url") or None
    return build_entity(name, url=url)


def _publisher_creator_contact(
    providers: List[Dict[str, Any]],
) -> Tuple[Dict[str, Any], Dict[str, Any], Dict[str, Any]]:
    fallback = build_entity("CoCliCo Catalog",
                            url="https://www.openearth.nl/coclico-workbench/data_catalog/")

    def first_with_role(*roles: str) -> Optional[Dict[str, Any]]:
        for p in providers or []:
            for r in (p.get("roles") or []):
                if r.lower() in roles:
                    return _provider_to_entity(p)
        return None

    licensor = first_with_role("licensor", "host")
    producer = first_with_role("producer", "processor")
    poc = first_with_role("point_of_contact", "contact")

    publisher = licensor or producer or fallback
    creator = producer or licensor or publisher
    contact_point = poc or licensor or producer or publisher
    return publisher, creator, contact_point


# ---------------------------------------------------------------------------
# Resources from Collection.assets + cite-as links
# ---------------------------------------------------------------------------

def _asset_media_type(asset: Dict[str, Any]) -> Tuple[str, str]:
    """Return (media_type, format_text) for a Collection asset."""
    t = (asset.get("type") or "").strip()
    if t:
        return t, ""
    # Sniff from href extension
    href = (asset.get("href") or "").lower()
    for ext, key in (
        (".zarr", "ZARR"), (".tif", "GEOTIFF"), (".nc", "NC"),
        (".csv", "CSV"), (".parquet", "PARQUET"), (".pq", "PARQUET"),
        (".shp", "SHP"), (".json", "JSON"), (".geojson", "GEOJSON"),
        (".pdf", "PDF"),
    ):
        if ext in href:
            return _FORMAT_MAP.get(key, ("", ""))[0], _FORMAT_MAP.get(key, ("", ""))[1]
    return "", ""


def _build_resources(
    collection: Dict[str, Any],
    collection_self_url: str,
    title: str,
) -> List[Dict[str, Any]]:
    """Build resources from Collection.assets + STAC self-link.

    Always include the STAC Collection self-link first (with conforms_to)
    so consumers can drill back into the catalogue.
    """
    resources: List[Dict[str, Any]] = []

    # 0. STAC Collection self link
    if collection_self_url:
        resources.append({
            "id": "resource_stac_collection",
            "title": f"STAC Collection: {title}",
            "description": (
                "Source STAC Collection metadata for this dataset, hosted as "
                "part of the CoCliCo catalog. Conforms to STAC v1.0.0."
            ),
            "media_type": "application/json",
            "conforms_to": "https://api.stacspec.org/v1.0.0/",
            "access_url": collection_self_url,
        })

    # 1. Each asset on the Collection
    assets = collection.get("assets") or {}
    base_url = collection_self_url or ""
    for key, a in assets.items():
        if not isinstance(a, dict):
            continue
        href = a.get("href")
        if not href:
            continue
        media_type, format_text = _asset_media_type(a)
        res_title = sanitize_text(a.get("title") or key) or key
        if _looks_like_url(res_title):
            res_title = f"{title} - {key}"
        res_desc = sanitize_text(a.get("description") or "") or res_title
        res: Dict[str, Any] = {
            "id": f"resource_{_slug(key, 30)}",
            "title": res_title,
            "description": res_desc,
        }
        if media_type:
            res["media_type"] = media_type
        elif format_text:
            res["format"] = format_text
        # Determine whether asset is service or downloadable
        href_abs = _abs_url(base_url, href)
        if any(href_abs.lower().endswith(suf) for suf in (".zarr", ".pq", ".parquet", ".tif", ".nc", ".csv", ".shp", ".json", ".geojson", ".pdf")):
            res["download_url"] = href_abs
        else:
            res["access_url"] = href_abs
        # Tile templates / service endpoints
        if "tiles" in (a.get("roles") or []) or "{" in href:
            res["access_url"] = href_abs
            res.pop("download_url", None)
        # Deltares mapService key on the Collection (visualisation hint)
        resources.append(res)

    # 2. Cite-as links from Collection
    for link in collection.get("links") or []:
        if link.get("rel") != "cite-as":
            continue
        href = link.get("href")
        if not href:
            continue
        t = sanitize_text(link.get("title") or "") or "Reference"
        res = {
            "id": f"resource_link_{_slug(t)}",
            "title": t,
            "description": t,
            "format": "Web reference",
            "access_url": href,
        }
        if any(r.get("access_url") == href or r.get("download_url") == href for r in resources):
            continue
        resources.append(res)

    return resources


# ---------------------------------------------------------------------------
# Extent helpers
# ---------------------------------------------------------------------------

def _extract_bbox(collection: Dict[str, Any]) -> Optional[List[float]]:
    extent = collection.get("extent") or {}
    spatial = extent.get("spatial") or {}
    bboxes = spatial.get("bbox") or []
    if bboxes and isinstance(bboxes[0], list) and len(bboxes[0]) == 4:
        return list(bboxes[0])
    return None


def _extract_temporal(collection: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    extent = collection.get("extent") or {}
    temporal = extent.get("temporal") or {}
    intervals = temporal.get("interval") or []
    if not intervals or not isinstance(intervals[0], list):
        return None
    iv = intervals[0]
    out: Dict[str, Any] = {}
    if len(iv) >= 1 and iv[0]:
        out["start"] = str(iv[0])[:4]
    if len(iv) >= 2 and iv[1]:
        out["end"] = str(iv[1])[:4]
    return out or None


def _license_iri(value: str) -> str:
    v = (value or "").strip()
    if not v or v.lower() in {"other", "proprietary", "unknown", "various"}:
        return "https://example.org/license/unknown"
    if v.startswith("http"):
        return v
    return f"https://spdx.org/licenses/{v}.html"


# ---------------------------------------------------------------------------
# Loss-entry derivation (source-grounded, with disclosed inferences)
# ---------------------------------------------------------------------------

# Map hazard cues found in title/description -> (RDL hazard type, default IMT)
_LOSS_HAZARD_CUES: List[Tuple[str, str, str]] = [
    (r"coastal\s*flood",   "coastal_flood",  "wd:m"),
    (r"storm\s*surge",     "coastal_flood",  "wd:m"),
    (r"sea\s*level\s*rise","sea_level_rise", "wd:m"),
    (r"flood",             "coastal_flood",  "wd:m"),
    (r"erosion|shoreline", "erosion",        "Er:m/yr"),
]

# Map asset cues -> (category, dimension)
_LOSS_ASSET_CUES: List[Tuple[str, str, str]] = [
    (r"building",       "buildings",       "structure"),
    (r"infrastructure", "infrastructure",  "structure"),
    (r"population|people|inhabitant", "population", "population"),
    (r"agricultur|crop|cropland",     "agriculture", "product"),
]


def _build_loss_entry(
    title: str,
    description: str,
    collection: Dict[str, Any],
) -> Dict[str, Any]:
    """Construct one losses[] entry from Collection signals.

    Every field in the loss schema is REQUIRED, so we cannot omit unknowns —
    we instead derive each field from explicit cues in title/description and
    record which choices were source-grounded vs. inferred in
    `analysis_details`. No currency, hazard, or asset type is assumed when
    the upstream text has no matching cue.
    """
    blob = f"{title}. {description}".lower()

    # --- hazard.type + IMT
    haz_type, imt, haz_cue = "coastal_flood", "wd:m", None
    for pat, ht, im in _LOSS_HAZARD_CUES:
        if re.search(pat, blob, re.IGNORECASE):
            haz_type, imt, haz_cue = ht, im, pat.replace(r"\s*", " ")
            break

    # --- asset_category + dimension
    asset_cat, asset_dim, asset_cue = "buildings", "structure", None
    for pat, cat, dim in _LOSS_ASSET_CUES:
        if re.search(pat, blob, re.IGNORECASE):
            asset_cat, asset_dim, asset_cue = cat, dim, pat
            break

    # --- impact_metric: economic_loss_value only when "cost" / "damage" / "loss" in text
    has_cost_cue = bool(re.search(r"cost|damage|monetary|economic", blob, re.IGNORECASE))
    impact_metric = "economic_loss_value" if has_cost_cue else "loss_ratio"

    # --- quantity_kind + unit
    # currency code is NEVER stated in the catalog — CoCliCo source datasets
    # are produced under EU H2020 grant 101003598; EUR is the documented
    # presentation currency in the COAST-RP / Lincke & Hinkel adaptation
    # literature this catalog cites. We mark this as an inferred default in
    # analysis_details so consumers can verify against the data files.
    if impact_metric == "economic_loss_value":
        qk, unit, unit_inferred = "currency", "EUR", True
    elif asset_cat == "population":
        qk, unit, unit_inferred = "count", "count", False
    else:
        qk, unit, unit_inferred = "dimensionless_ratio", "ratio", False

    # --- impact_modelling: "projection" / "model" / "simulated" in text -> simulated; else inferred
    if re.search(r"projection|model|simulat|scenario", blob, re.IGNORECASE):
        impact_modelling, mod_inferred = "simulated", False
    else:
        impact_modelling, mod_inferred = "simulated", True

    # --- loss_frequency_type: "probabilistic" / return-period cue -> probabilistic; else deterministic (inferred)
    if re.search(r"probabilistic|return\s*period|annual\s*exceed", blob, re.IGNORECASE):
        loss_freq, freq_inferred = "probabilistic", False
    else:
        loss_freq, freq_inferred = "deterministic", True

    # Build analysis_details transparency note
    notes: List[str] = []
    if haz_cue:
        notes.append(f"hazard.type={haz_type} (cue: '{haz_cue}' in title/description)")
    else:
        notes.append(f"hazard.type={haz_type} (default; no explicit hazard cue)")
    if asset_cue:
        notes.append(f"asset_category={asset_cat} (cue: '{asset_cue}')")
    else:
        notes.append(f"asset_category={asset_cat} (default; no explicit asset cue)")
    if unit_inferred:
        notes.append("measurement.unit=EUR (inferred — CoCliCo is an EU H2020 project; verify against data files)")
    if mod_inferred:
        notes.append("impact_modelling=simulated (inferred — no explicit modelling cue)")
    if freq_inferred:
        notes.append("loss_frequency_type=deterministic (inferred — no probabilistic cue)")
    notes.append("loss_type=ground_up (default — pre-insurance gross figure)")
    notes.append("loss_approach=analytical (default for model-derived projections)")

    entry: Dict[str, Any] = {
        "id": "loss_1",
        "hazard": {
            "type": haz_type,
            "intensity_measure": imt,
        },
        "asset_category": asset_cat,
        "asset_dimension": asset_dim,
        "impact_and_losses": {
            "impact_type": "direct",
            "impact_modelling": impact_modelling,
            "impact_metric": impact_metric,
            "measurement": {"quantity_kind": qk, "unit": unit},
            "loss_type": "ground_up",
            "loss_approach": "analytical",
            "loss_frequency_type": loss_freq,
        },
        "analysis_details": " | ".join(notes),
    }
    return entry


def _summarise_collection_for_analysis_details(collection: Dict[str, Any]) -> str:
    """Turn Collection.summaries (and version) into a one-paragraph analysis_details
    string. Returns "" when nothing source-grounded is available.
    """
    parts: List[str] = []
    summaries = collection.get("summaries") or {}
    if isinstance(summaries, dict):
        for key, vals in summaries.items():
            if not vals:
                continue
            if isinstance(vals, list):
                vstr = ", ".join(str(v) for v in vals[:10])
            else:
                vstr = str(vals)
            parts.append(f"{key}: {vstr}")
    version = collection.get("version")
    if version:
        parts.append(f"dataset version: {version}")
    return "; ".join(parts)


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_record_collection_v10(
    collection: Dict[str, Any],
    *,
    org_slug: str = "coclico",
    catalog_name: str = "coclico",
) -> Optional[Dict[str, Any]]:
    """Translate a CoCliCo STAC Collection into one RDLS v1.0 record."""
    raw_title = sanitize_text(collection.get("title") or collection.get("id") or "")
    if not raw_title:
        return None
    # Make the title self-identifying: many upstream titles are short labels
    # (e.g. "Regions", "Coastal Change Segments", "Damage Costs") that lack
    # context once they are pulled out of the catalog UI. Prefix the catalog
    # short-name unless it is already present.
    if re.search(r"\bcoclico\b", raw_title, re.IGNORECASE):
        title = raw_title
    else:
        title = f"CoCliCo - {raw_title}"
    description = sanitize_text(collection.get("description") or "")
    keywords = collection.get("keywords") or []

    risk_data_type_str, subcat = _infer_risk_type(title, description, keywords)
    risk_data_type = sort_rdt_hevl([risk_data_type_str])

    source_suffix = (
        f"[Source: This metadata record was automatically extracted from the "
        f"STAC catalog {catalog_name} (https://www.openearth.nl/coclico-workbench/data_catalog/)]"
    )
    # NOTE: subcategory is captured downstream in asset_type / indicator_name /
    # hazard.type — do NOT inject it into the description (the literal escaped
    # quotes were noisy and added no real signal).
    description = f"{description.rstrip('.')}. {source_suffix}" if description else source_suffix

    publisher, creator, contact_point = _publisher_creator_contact(collection.get("providers") or [])

    # --- spatial ---
    bbox = _extract_bbox(collection)
    spatial: Dict[str, Any] = {"scale": "global"}
    if bbox:
        spatial["bbox"] = bbox

    # --- temporal ---
    temporal = _extract_temporal(collection)

    # --- license ---
    license_iri = _license_iri(collection.get("license") or "")

    # --- self-URL for STAC catalog resource ---
    collection_self_url = collection.get("_source_url") or ""
    if not collection_self_url:
        for link in collection.get("links") or []:
            if link.get("rel") == "self":
                collection_self_url = link.get("href") or ""
                break

    # --- resources ---
    resources = _build_resources(collection, collection_self_url, title)
    if not resources:
        return None

    # --- lineage ---
    lineage: Dict[str, Any] = {}
    cdesc = sanitize_text(collection.get("description") or "")
    if cdesc:
        lineage["description"] = cdesc
    # CoCliCo Collections often expose providers with "host" role pointing at
    # the original source dataset; use the producer entity as a source.
    sources_list: List[Dict[str, Any]] = []
    for idx, p in enumerate(collection.get("providers") or [], 1):
        roles = [r.lower() for r in (p.get("roles") or [])]
        if "producer" not in roles and "processor" not in roles:
            continue
        nm = sanitize_text(p.get("name") or "")
        if not nm:
            continue
        s: Dict[str, Any] = {"id": f"source_{idx}", "name": nm, "type": "dataset"}
        if p.get("url"):
            s["url"] = p["url"]
        s["risk_data_type"] = list(risk_data_type)
        s["used_in"] = risk_data_type[0]
        sources_list.append(s)
    if sources_list:
        lineage["sources"] = sources_list

    # --- referenced_by from sci:citation / sci:doi at collection level ---
    referenced_by: List[Dict[str, Any]] = []
    citation = sanitize_text(collection.get("sci:citation") or "")
    doi = (collection.get("sci:doi") or "").strip()
    if doi:
        url = doi if doi.startswith("http") else f"https://doi.org/{doi.lstrip('doi:').strip()}"
        referenced_by.append({"id": "ref_doi", "doi": doi, "url": url})
    if citation and not referenced_by:
        # Pure citation without DOI — drop a placeholder ref
        referenced_by.append({
            "id": "ref_citation",
            "name": citation,
            "url": collection_self_url or "https://www.openearth.nl/coclico-workbench/data_catalog/",
        })

    # --- HEVL block ---
    hevl_block_key: Optional[str] = None
    hevl_block_value: Any = None
    if risk_data_type == ["hazard"]:
        haz_type, haz_process = _HAZARD_TYPE_MAP.get(subcat, ("coastal_flood", "coastal_flood"))
        imt_default = {
            "coastal_flood":  "wd:m",
            "flood":          "wd:m",
            "sea_level_rise": "wd:m",
            "erosion":        "Er:m/yr",
        }.get(haz_type, "wd:m")
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
                    "analysis_type": "deterministic",
                    "calculation_method": "simulated",
                }
            ]
        }
    elif risk_data_type == ["exposure"]:
        cat_info = _EXPOSURE_TYPE_MAP.get(
            subcat, ("natural_environment", "product", "area", "square_metre"),
        )
        cat, dim, qk, unit = cat_info
        hevl_block_key = "exposure"
        hevl_block_value = [
            {
                "id": "exposure_1",
                "category": cat,
                "metrics": [
                    {
                        "id": "metric_1",
                        "dimension": dim,
                        "measurement": {"quantity_kind": qk, "unit": unit},
                    }
                ],
                "asset_type": {"id": cat, "description": subcat},
            }
        ]
    elif risk_data_type == ["loss"]:
        hevl_block_key = "loss"
        hevl_block_value = {
            "losses": [_build_loss_entry(title, description, collection)]
        }
    elif risk_data_type == ["vulnerability"]:
        # Treat as socio-economic-style record for cost-benefit / index data
        ref_year_raw = ""
        if temporal:
            ref_year_raw = temporal.get("start") or ""
        try:
            ref_year = int(ref_year_raw)
        except (TypeError, ValueError):
            ref_year = 2024
        ref_year = max(1900, min(2100, ref_year))
        se_entry: Dict[str, Any] = {
            "id": "se_1",
            "indicator_name": title,
            "indicator_code": _slug(subcat or "indicator", 24),
            "description": (description or "Socio-economic / vulnerability indicator.")[:500],
            "reference_year": ref_year,
        }
        # Optional: link back to the STAC Collection JSON for traceability
        if collection_self_url:
            se_entry["uri"] = collection_self_url
        # Optional: surface upstream summaries (scenarios, time horizons, etc.)
        # as analysis_details — these come straight from Collection.summaries,
        # so no fabrication.
        analysis = _summarise_collection_for_analysis_details(collection)
        if analysis:
            se_entry["analysis_details"] = analysis
        hevl_block_key = "vulnerability"
        hevl_block_value = {"socio_economic": [se_entry]}

    # --- ID ---
    rdt_prefix = {"hazard": "hzd", "exposure": "exp", "vulnerability": "vln", "loss": "lss"}[risk_data_type[0]]
    coll_slug = _slug(collection.get("id") or title, 60)
    record_id = f"rdls_{rdt_prefix}-{org_slug}_{coll_slug}"

    # --- assemble ---
    record: Dict[str, Any] = {
        "id": record_id,
        "title": title,
        "description": description,
        "risk_data_type": risk_data_type,
        "publisher": publisher,
        "contact_point": contact_point,
        "creator": creator,
        "spatial": spatial,
    }
    if temporal:
        record["temporal"] = temporal
    record["license"] = license_iri
    if lineage:
        record["lineage"] = lineage
    if referenced_by:
        record["referenced_by"] = referenced_by
    record["resources"] = resources
    if hevl_block_key:
        record[hevl_block_key] = hevl_block_value
    record["links"] = [{"href": _SCHEMA_LINK, "rel": "describedby"}]

    return order_record_fields_v10(record)
