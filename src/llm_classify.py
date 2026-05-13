"""LLM-first HEVL classification + field extraction for RDLS v1.0.

Single-phase pipeline: Claude Haiku classifies AND extracts simultaneously.
No regex pre-screening. Reads raw HDX JSONs directly.

Output: V10Classification dataclass with all fields needed to build a
schema-valid v1.0 record via translate + extract.

Cache: prompt-hash-keyed JSON files in output/hdx/v1.0/cache/.
       New cache - separate from old output/hdx/llm_cache/ (different prompt).

Usage:
    from src.llm_classify import classify_v10, V10Config, LLMCacheV10
"""

from __future__ import annotations

import hashlib
import json
import os
import ssl
import time
import urllib.error
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .sources.ckan_columns import ColumnInfo
from .utils import load_yaml


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class V10Config:
    """Pipeline configuration for the LLM-first v1.0 classifier."""

    # LLM
    model: str = "claude-haiku-4-5-20251001"
    temperature: float = 0.0
    max_tokens: int = 1800
    max_retries: int = 5
    timeout: float = 30.0
    rate_limit_delay: float = 0.3          # seconds between calls

    # Cost guard
    max_cost_usd: float = 120.0
    cost_per_mtok_input: float = 1.00      # Haiku 4.5 pricing
    cost_per_mtok_output: float = 5.00

    # Cache
    cache_dir: str = "output/hdx/v1.0/cache"

    # Prompt limits
    description_max_chars: int = 600
    methodology_max_chars: int = 300
    max_resources_shown: int = 20
    max_columns_shown: int = 50

    @classmethod
    def from_yaml(cls, path: Path) -> "V10Config":
        raw = load_yaml(path)
        c = cls()
        llm = raw.get("llm", {})
        c.model = llm.get("model", c.model)
        c.temperature = llm.get("temperature", c.temperature)
        c.max_tokens = llm.get("max_tokens", c.max_tokens)
        c.max_retries = llm.get("max_retries", c.max_retries)
        c.timeout = llm.get("timeout_seconds", c.timeout)
        c.rate_limit_delay = llm.get("rate_limit_delay", c.rate_limit_delay)
        c.max_cost_usd = llm.get("max_cost_usd", c.max_cost_usd)
        c.cost_per_mtok_input = llm.get("cost_per_mtok_input", c.cost_per_mtok_input)
        c.cost_per_mtok_output = llm.get("cost_per_mtok_output", c.cost_per_mtok_output)
        c.cache_dir = llm.get("cache_dir", c.cache_dir)
        p = raw.get("prompt", {})
        c.description_max_chars = p.get("description_max_chars", c.description_max_chars)
        c.methodology_max_chars = p.get("methodology_max_chars", c.methodology_max_chars)
        c.max_resources_shown = p.get("max_resources_shown", c.max_resources_shown)
        c.max_columns_shown = p.get("max_columns_shown", c.max_columns_shown)
        return c


# ---------------------------------------------------------------------------
# Output dataclass
# ---------------------------------------------------------------------------

@dataclass
class V10Classification:
    """Structured LLM output for one HDX dataset."""

    hdx_id: str
    is_rdls: bool
    not_rdls_reason: Optional[str]
    components: List[str]              # e.g. ["hazard", "exposure"]
    domain: str                        # disaster_risk | humanitarian_ops | climate | ...

    # Per-component details (None / empty list if component absent)
    hazard: Optional[Dict[str, Any]]
    # hazard keys: type, process, analysis_type, imt, return_periods (list),
    #              calculation_method, description
    exposure: List[Dict[str, Any]]
    # each item: category, dimension, quantity_kind, description (opt)
    vulnerability: Optional[Dict[str, Any]]  # {hazard_type, imt, category}
    loss: List[Dict[str, Any]]
    # each item: hazard_type, asset_category, impact_metric, impact_type, imt, description (opt)

    # Spatial hints from LLM
    countries: List[str]               # ISO3 codes
    spatial_scale: Optional[str]       # global|regional|national|sub-national|urban

    confidence: float                  # 0.0-1.0
    reasoning: str

    # Provenance (non-default fields - must come before default fields in dataclass)
    prompt_hash: str
    from_cache: bool
    token_usage: Dict[str, int]        # {input: N, output: N}
    llm_model: str

    # Enhanced extraction: contributing sources and lineage (v2)
    # These have defaults so they must be declared last.
    contributing_sources: List[Dict[str, Any]] = field(default_factory=list)
    # Each: {name, used_in, type, description}  (used_in/type validated on parse)
    lineage_description: Optional[str] = None

    @property
    def confidence_tier(self) -> str:
        """Map confidence to output tier."""
        if self.confidence >= 0.7:
            return "high"
        if self.confidence >= 0.4:
            return "medium"
        return "low"


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """You are an expert in disaster risk data classification for RDLS (Risk Data Library Standard) v1.0.

Classify HDX datasets into RDLS components AND extract structured metadata fields in one step.

## RDLS Components

HAZARD: Data CONTAINING physical hazard measurements or models.
- YES: flood depth/extent grids, shakemaps, wind speed fields, drought index rasters, cyclone track data with physical parameters, storm surge grids, landslide susceptibility maps
- NO: damage reports that reference a hazard, IPC food insecurity scores, admin boundaries

EXPOSURE: Data CONTAINING spatial inventories of assets or populations at risk.
- YES: building footprints/counts per area, gridded population, road network with geometry, facility registries, land use maps with area statistics
- NO: "people affected" columns (that is LOSS), population displacement counts (LOSS)

VULNERABILITY: Data CONTAINING damage/loss functions or susceptibility models.
- YES: fragility curves, depth-damage tables, structural vulnerability indices
- Extremely rare on HDX. Only mark true if functions explicitly present.

LOSS: Data CONTAINING post-event impact records or modeled risk outputs.
- YES: casualty figures, displacement counts, building damage assessments, economic losses, IPC classifications, MSNA needs data, situation reports with impact figures, modeled AAL/PML
- The triggering hazard belongs in loss.hazard_type, NOT as a separate hazard component

## Non-RDLS (is_rdls: false)
- Admin boundaries, gazetteers, reference maps
- General census/demographics with no disaster context
- Humanitarian operations (3W/4W, who-does-what)
- Food prices/market monitoring
- Health facility registers (no disaster context)
- Project documents, reports without data

## Hazard Types (exact codes only)
coastal_flood | convective_storm | drought | earthquake | erosion | extreme_temperature | flood | landslide | pest_infestation | sea_level_rise | strong_wind | tsunami | volcanic | wildfire | dust_sand_storm

## Process Types (exact codes, must match hazard type)
flood: fluvial_flood | pluvial_flood | groundwater_flood | coastal_flood | glacial_lake_outburst
coastal_flood: coastal_flood | storm_surge
earthquake: rupture | ground_motion | liquefaction | subsidence_uplift
strong_wind: tropical_cyclone | extratropical_cyclone | tornado
convective_storm: tornado | lightning | thunderstorm | hail
drought: agricultural_drought | hydrological_drought | meteorological_drought | socioeconomic_drought
extreme_temperature: extreme_heat | extreme_cold
landslide: snow_avalanche | landslide_general | landslide_rockslide | landslide_mudflow | landslide_rockfall
volcanic: ashfall | volcano_ballistics | lahar | lava | pyroclastic_flow | volcano_gas_aerosols
tsunami: tsunami
wildfire: wildfire | wildfire_smoke
erosion: coastal_erosion | soil_erosion
sea_level_rise: sea_level_rise
dust_sand_storm: dust_sand_storm
pest_infestation: pest

## Analysis Types
probabilistic | empirical | deterministic

## Common IMT codes (format: metric:unit)
flood/coastal_flood: wd:m | vv:m/s | AA:km2
earthquake: PGA:g | SA(0.3s):g | MMI:-
strong_wind: sws_10m:m/s | PGWS:m/s
drought: SPI:- | PDSI:- | SMA:-
extreme_temperature: AirTemp:C | WBGT:C
landslide: LSI:-
tsunami: wd:m
wildfire: FWI:-
volcanic: h_vaf:mm
sea_level_rise: slr:cm
erosion: Er:T/ha

## Exposure Categories (exact codes)
agriculture | buildings | infrastructure | population | natural_environment | economic_indicator | development_index

## Loss Impact Metrics (common values)
economic_loss_value | buildings_damaged_count | affected_count | dead_count | injured_count | displaced_count | fatality_count | asset_loss | damage_ratio | loss_annual_average_value

## Spatial Scale (exact codes)
global | regional | national | sub-national | urban

## Hazard Fields
For hazard components, provide these additional fields when information is available:
- calculation_method: How the hazard data was generated - "simulated" (model output), "observed" (actual measurements/post-event), "inferred" (derived/estimated from proxy data)
- return_periods: For probabilistic hazard datasets, list the explicit return period values in years as integers (e.g. [10, 25, 50, 100, 200, 500]). Only include values explicitly stated in the metadata. Use [] if not mentioned.
- description: 1-sentence description of this hazard model (e.g. "Probabilistic flood hazard model with fluvial inundation depths at 6 return periods")

## Exposure Fields
exposure is now a LIST. Include one entry per distinct asset category present in the dataset.
Examples: buildings + population -> 2 entries; only population -> 1 entry.
Each entry:
- category: ONE of the allowed exposure categories
- dimension: structure | content | product | disruption | population | index (NEVER "count", "area", "density")
- quantity_kind: count | area | monetary | length | ...
- description: 1-sentence description of what assets this covers (e.g. "Residential building stock classified by construction type")

## Loss Fields
loss is now a LIST. Include one entry per distinct impact combination present in the dataset.
Examples: direct building damage + displaced population -> 2 entries; multiple hazards each causing deaths -> 1 entry per primary combination.
Each entry:
- hazard_type: ONE hazard type from the allowed list (NEVER "multiple"/"various")
- asset_category: ONE exposure category
- impact_metric: ONE metric from the allowed list
- impact_type: "direct" | "indirect" | "total"
- imt: intensity measure code, or null
- description: 1-sentence description of this impact record (e.g. "Direct building damage from fluvial flooding")

## Contributing Sources
When a dataset aggregates data from multiple organizations (e.g., HDX HAPI, multi-agency compilations, ReliefWeb aggregations), extract each contributing source as a separate entry in contributing_sources.
Only include sources explicitly mentioned in the title, description, methodology, or resource names.
For single-source datasets, contributing_sources may have one entry or be an empty list.

Each contributing_source entry:
- name: Exact organization or dataset name as stated (do not paraphrase)
- used_in: Which RDLS component this source primarily contributes to - ONE of: hazard | exposure | vulnerability | loss
- type: "dataset" if primary observational/administrative/survey data; "model" if analytical model, algorithm, or index methodology
- description: 1-2 sentence scientific description of what this source provides and how it is used in the dataset

## Lineage Description
Provide a 1-3 sentence scientific description of the data pipeline for lineage_description:
- How the source data was collected or generated
- Key processing steps, integration methods, or transformations applied
- The purpose of the overall dataset in a risk data context
Be specific and scientific. Do not simply restate the dataset title. If insufficient information is available, set lineage_description to null.

IMPORTANT rules:
- hazard.return_periods: only list values EXPLICITLY stated in the metadata, never guess. Use [] if not mentioned.
- loss entries: hazard_type must be ONE hazard type. NEVER use "multiple" or "various".
- exposure entries: dimension must be from the closed list. NEVER use "count", "density", "area".
- spatial_scale "global" means WORLDWIDE coverage - do NOT set countries when scale is global.

Respond with ONLY a valid JSON object. No markdown, no text outside JSON."""


# ---------------------------------------------------------------------------
# Prompt builder
# ---------------------------------------------------------------------------

def build_prompt_v10(
    hdx_meta: Dict[str, Any],
    column_infos: List[ColumnInfo],
    config: V10Config,
) -> str:
    """Build the user prompt for LLM v1.0 classification."""
    parts: List[str] = []

    # Metadata
    title = hdx_meta.get("title", "")
    desc = hdx_meta.get("notes", "") or ""
    if len(desc) > config.description_max_chars:
        desc = desc[:config.description_max_chars] + "..."
    tags = hdx_meta.get("tags", [])
    tag_names = [
        (t.get("name", "") if isinstance(t, dict) else str(t)) for t in tags[:15]
    ]
    org = hdx_meta.get("organization", "")
    if isinstance(org, dict):
        org = org.get("title", "") or org.get("name", "")
    methodology = hdx_meta.get("methodology", "") or ""
    if len(methodology) > config.methodology_max_chars:
        methodology = methodology[:config.methodology_max_chars] + "..."
    methodology_other = hdx_meta.get("methodology_other", "") or ""
    if len(methodology_other) > config.methodology_max_chars:
        methodology_other = methodology_other[:config.methodology_max_chars] + "..."
    dataset_source = hdx_meta.get("dataset_source", "")

    parts.append("Classify this HDX dataset for RDLS v1.0.")
    parts.append("")
    parts.append(f"Title: {title}")
    if desc:
        parts.append(f"Description: {desc}")
    if tag_names:
        parts.append(f"Tags: {', '.join(tag_names)}")
    if org:
        parts.append(f"Organization: {org}")
    if dataset_source and dataset_source != org:
        parts.append(f"Data source: {dataset_source}")
    if methodology:
        parts.append(f"Methodology: {methodology}")
    if methodology_other and methodology_other != methodology:
        parts.append(f"Additional methodology: {methodology_other}")

    # Resources
    resources = hdx_meta.get("resources", [])
    parts.append(f"\nResources ({len(resources)} files):")
    for r in resources[:config.max_resources_shown]:
        rname = r.get("name", "") or r.get("description", "")
        rfmt = r.get("format", "")
        line = f"  - {rname}"
        if rfmt:
            line += f" ({rfmt})"
        parts.append(line)
    if len(resources) > config.max_resources_shown:
        parts.append(f"  ... (+{len(resources) - config.max_resources_shown} more)")

    # Column headers
    if column_infos:
        parts.append("\nColumn headers (from data files):")
        for ci in column_infos[:5]:  # limit to 5 resources
            label = ci.resource_name or ci.resource_id
            if ci.sheet_name:
                label += f" [{ci.sheet_name}]"
            cols = [str(c) for c in ci.columns[:config.max_columns_shown]]
            if len(ci.columns) > config.max_columns_shown:
                cols.append(f"...+{len(ci.columns) - config.max_columns_shown}")
            parts.append(f"  {label}: {', '.join(cols)}")

    # Required response schema
    parts.append("")
    parts.append("Respond with ONLY this JSON structure (all fields required):")
    parts.append(
        "{\n"
        '  "is_rdls": true,\n'
        '  "components": ["hazard", "exposure"],\n'
        '  "hazard": {\n'
        '    "type": "flood",\n'
        '    "process": "fluvial_flood",\n'
        '    "analysis_type": "probabilistic",\n'
        '    "imt": "wd:m",\n'
        '    "calculation_method": "simulated",\n'
        '    "return_periods": [10, 25, 50, 100, 200, 500],\n'
        '    "description": "Probabilistic fluvial flood hazard model at 6 return periods"\n'
        '  },\n'
        '  "exposure": [\n'
        '    {\n'
        '      "category": "buildings",\n'
        '      "dimension": "structure",\n'
        '      "quantity_kind": "count",\n'
        '      "description": "General building stock across the study area"\n'
        '    },\n'
        '    {\n'
        '      "category": "population",\n'
        '      "dimension": "population",\n'
        '      "quantity_kind": "count",\n'
        '      "description": "Residential population at risk"\n'
        '    }\n'
        '  ],\n'
        '  "vulnerability": null,\n'
        '  "loss": [\n'
        '    {\n'
        '      "hazard_type": "flood",\n'
        '      "asset_category": "buildings",\n'
        '      "impact_metric": "buildings_damaged_count",\n'
        '      "impact_type": "direct",\n'
        '      "imt": null,\n'
        '      "description": "Direct building damage from fluvial flooding"\n'
        '    }\n'
        '  ],\n'
        '  "spatial_scale": "urban",\n'
        '  "countries": ["GHA"],\n'
        '  "contributing_sources": [\n'
        '    {\n'
        '      "name": "Source organization name",\n'
        '      "used_in": "loss",\n'
        '      "type": "dataset",\n'
        '      "description": "1-2 sentence description of what this source provides"\n'
        '    }\n'
        '  ],\n'
        '  "lineage_description": "1-3 sentence scientific description of the data pipeline",\n'
        '  "confidence": 0.9,\n'
        '  "not_rdls_reason": null,\n'
        '  "domain": "disaster_risk",\n'
        '  "reasoning": "one sentence"\n'
        "}\n"
        "Rules:\n"
        "- components: list only present components\n"
        "- hazard: populate if in components, else null\n"
        "- exposure: LIST of objects (one per asset category), or [] if not in components\n"
        "- loss: LIST of objects (one per impact combination), or [] if not in components\n"
        "- vulnerability: single object or null\n"
        "- hazard.type must be from the allowed hazard types list\n"
        "- hazard.process must match hazard.type from the process types list\n"
        "- hazard.return_periods: only values EXPLICITLY stated in metadata; use [] otherwise\n"
        "- hazard.calculation_method: simulated | observed | inferred\n"
        "- exposure[].category must be from the allowed exposure categories list\n"
        "- loss[].hazard_type: ONE hazard type, NEVER 'multiple' or 'various'\n"
        "- loss[].impact_type: direct | indirect | total\n"
        "- contributing_sources: list organizations/datasets contributing; empty list [] if single-source\n"
        "- lineage_description: scientific pipeline description, or null if insufficient info\n"
        "- countries: ISO 3166-1 alpha-3 codes (3 letters, e.g. KEN, BGD)\n"
        "- confidence: 0.0-1.0 (your certainty in the classification)\n"
        "- domain: disaster_risk | humanitarian_ops | climate | health | reference | other\n"
        "- If is_rdls is false, set not_rdls_reason to a brief explanation"
    )

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# LLM response cache
# ---------------------------------------------------------------------------

class LLMCacheV10:
    """Disk-backed cache for v1.0 LLM responses, keyed by prompt hash."""

    def __init__(self, cache_dir: str):
        self._dir = Path(cache_dir)
        self._dir.mkdir(parents=True, exist_ok=True)

    def _path(self, phash: str) -> Path:
        return self._dir / f"{phash}.json"

    def get(self, phash: str) -> Optional[Dict[str, Any]]:
        p = self._path(phash)
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                return None
        return None

    def put(self, phash: str, data: Dict[str, Any]) -> None:
        p = self._path(phash)
        try:
            p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        except OSError:
            pass  # Cache write failure is non-fatal

    def size(self) -> int:
        return sum(1 for _ in self._dir.glob("*.json"))


def _prompt_hash(prompt: str) -> str:
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:16]


# ---------------------------------------------------------------------------
# LLM API call (urllib, no SDK dependency)
# ---------------------------------------------------------------------------

def _call_api(
    user_prompt: str,
    config: V10Config,
    api_key: str,
) -> tuple[Dict[str, Any], Dict[str, int]]:
    """Call Claude API and return (parsed_json_response, token_usage).

    Uses urllib to avoid httpx hanging issues on Windows.
    Retries with exponential backoff on rate limits.
    """
    url = "https://api.anthropic.com/v1/messages"
    ssl_ctx = ssl.create_default_context()
    last_error: Optional[Exception] = None

    for attempt in range(config.max_retries):
        body = json.dumps({
            "model": config.model,
            "max_tokens": config.max_tokens,
            "temperature": config.temperature,
            "system": _SYSTEM_PROMPT,
            "messages": [{"role": "user", "content": user_prompt}],
        }).encode("utf-8")

        req = urllib.request.Request(url, data=body, headers={
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        })

        try:
            resp = urllib.request.urlopen(req, timeout=int(config.timeout), context=ssl_ctx)
            data = json.loads(resp.read().decode("utf-8"))
            text = ""
            for block in data.get("content", []):
                if block.get("type") == "text":
                    text += block.get("text", "")
            text = text.strip()
            if text.startswith("```"):
                lines = text.split("\n")
                text = "\n".join(lines[1:-1]) if len(lines) > 2 else text
            parsed = json.loads(text)
            usage = {
                "input": data.get("usage", {}).get("input_tokens", 0),
                "output": data.get("usage", {}).get("output_tokens", 0),
            }
            return parsed, usage

        except urllib.error.HTTPError as e:
            body_text = e.read().decode("utf-8", errors="replace")
            last_error = RuntimeError(f"HTTP {e.code}: {body_text}")
            if e.code == 400 and ("spending" in body_text.lower() or "usage limit" in body_text.lower()):
                raise RuntimeError(f"Spending limit reached: {body_text}") from e
            if e.code == 429:
                wait = 60 * (attempt + 1)
                time.sleep(wait)
            elif e.code >= 500:
                time.sleep(10 * (attempt + 1))
            else:
                raise last_error from e

        except (urllib.error.URLError, json.JSONDecodeError, OSError) as e:
            last_error = e
            time.sleep(5 * (attempt + 1))

    raise RuntimeError(f"LLM call failed after {config.max_retries} retries: {last_error}") from last_error


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

_VALID_COMPONENTS = {"hazard", "exposure", "vulnerability", "loss"}
_VALID_SOURCE_TYPES = {"dataset", "model"}
_VALID_USED_IN = {"hazard", "exposure", "vulnerability", "loss"}
_VALID_IMPACT_TYPES = {"direct", "indirect", "total"}
_VALID_CALC_METHODS = {"simulated", "observed", "inferred"}
_VALID_HAZARD_TYPES = {
    "coastal_flood", "convective_storm", "drought", "earthquake", "erosion",
    "extreme_temperature", "flood", "landslide", "pest_infestation",
    "sea_level_rise", "strong_wind", "tsunami", "volcanic", "wildfire",
    "dust_sand_storm",
}
_VALID_EXPOSURE_CATS = {
    "agriculture", "buildings", "infrastructure", "population",
    "natural_environment", "economic_indicator", "development_index",
}
_VALID_ANALYSIS_TYPES = {"probabilistic", "empirical", "deterministic"}
_VALID_SCALES = {"global", "regional", "national", "sub-national", "urban"}


def _parse_response(raw: Dict[str, Any], hdx_id: str) -> V10Classification:
    """Parse and validate LLM JSON response into V10Classification."""

    is_rdls = bool(raw.get("is_rdls", False))
    not_rdls_reason = raw.get("not_rdls_reason") or None

    # Components
    raw_comps = raw.get("components", [])
    if isinstance(raw_comps, list):
        components = [c for c in raw_comps if c in _VALID_COMPONENTS]
    else:
        components = []

    domain = raw.get("domain", "unknown") or "unknown"
    confidence = float(raw.get("confidence", 0.5))
    confidence = max(0.0, min(1.0, confidence))
    reasoning = str(raw.get("reasoning", ""))

    # Spatial
    raw_countries = raw.get("countries", [])
    countries = [c.upper() for c in raw_countries if isinstance(c, str) and len(c) == 3]
    scale_raw = raw.get("spatial_scale", "")
    spatial_scale = scale_raw if scale_raw in _VALID_SCALES else None

    # Hazard details (single optional dict - enhanced with return_periods, calculation_method)
    hazard: Optional[Dict[str, Any]] = None
    raw_haz = raw.get("hazard")
    if raw_haz and isinstance(raw_haz, dict) and "hazard" in components:
        htype = raw_haz.get("type", "")
        if htype in _VALID_HAZARD_TYPES:
            hazard = {
                "type": htype,
                "process": raw_haz.get("process") or None,
                "analysis_type": raw_haz.get("analysis_type", "probabilistic")
                    if raw_haz.get("analysis_type") in _VALID_ANALYSIS_TYPES
                    else "probabilistic",
                "imt": raw_haz.get("imt") or None,
            }

            # Return periods: only accept integers explicitly listed by LLM
            raw_rps = raw_haz.get("return_periods", [])
            if isinstance(raw_rps, list):
                return_periods = []
                for rp in raw_rps:
                    try:
                        rp_int = int(rp)
                        if 1 <= rp_int <= 100_000:
                            return_periods.append(rp_int)
                    except (TypeError, ValueError):
                        pass
                if return_periods:
                    hazard["return_periods"] = return_periods

            # Calculation method
            raw_calc = raw_haz.get("calculation_method", "")
            if raw_calc in _VALID_CALC_METHODS:
                hazard["calculation_method"] = raw_calc

            # Hazard description
            haz_desc = (raw_haz.get("description", "") or "").strip()
            if haz_desc:
                hazard["description"] = haz_desc

    # Exposure details: now a LIST of exposure items.
    # Backward compat: old cached responses return a single dict - wrap in list.
    exposure: List[Dict[str, Any]] = []
    raw_exp = raw.get("exposure")
    if raw_exp is not None and "exposure" in components:
        if isinstance(raw_exp, dict):
            raw_exp = [raw_exp]  # Old cache format: single dict -> wrap in list
        if isinstance(raw_exp, list):
            for exp_item in raw_exp:
                if not isinstance(exp_item, dict):
                    continue
                cat = (exp_item.get("category", "") or "").strip()
                if cat not in _VALID_EXPOSURE_CATS:
                    continue
                parsed_exp: Dict[str, Any] = {
                    "category": cat,
                    "dimension": exp_item.get("dimension") or None,
                    "quantity_kind": exp_item.get("quantity_kind") or None,
                }
                exp_desc = (exp_item.get("description", "") or "").strip()
                if exp_desc:
                    parsed_exp["description"] = exp_desc
                exposure.append(parsed_exp)

    # Vulnerability details (single optional dict - still rare, keep as-is)
    vulnerability: Optional[Dict[str, Any]] = None
    raw_vuln = raw.get("vulnerability")
    if raw_vuln and isinstance(raw_vuln, dict) and "vulnerability" in components:
        _exp_cat = exposure[0].get("category") if exposure else None
        vulnerability = {
            "hazard_type": raw_vuln.get("hazard_type") or (hazard or {}).get("type"),
            "imt": raw_vuln.get("imt") or (hazard or {}).get("imt"),
            "category": raw_vuln.get("category") or _exp_cat,
        }

    # Loss details: now a LIST of loss items.
    # Backward compat: old cached responses return a single dict - wrap in list.
    loss: List[Dict[str, Any]] = []
    raw_loss = raw.get("loss")
    if raw_loss is not None and "loss" in components:
        if isinstance(raw_loss, dict):
            raw_loss = [raw_loss]  # Old cache format: single dict -> wrap in list
        if isinstance(raw_loss, list):
            _fallback_cat = exposure[0].get("category", "buildings") if exposure else "buildings"
            for loss_item in raw_loss:
                if not isinstance(loss_item, dict):
                    continue
                raw_loss_htype = loss_item.get("hazard_type") or (hazard or {}).get("type", "flood")
                # Validate loss hazard_type: LLM sometimes returns "multiple"/"various".
                # Fall back to primary hazard type (or "flood") if invalid.
                loss_htype = raw_loss_htype if raw_loss_htype in _VALID_HAZARD_TYPES else (
                    (hazard or {}).get("type", "flood")
                )
                raw_loss_cat = loss_item.get("asset_category") or _fallback_cat
                loss_cat = raw_loss_cat if raw_loss_cat in _VALID_EXPOSURE_CATS else "buildings"
                raw_impact_type = loss_item.get("impact_type", "direct")
                impact_type = raw_impact_type if raw_impact_type in _VALID_IMPACT_TYPES else "direct"
                parsed_loss: Dict[str, Any] = {
                    "hazard_type": loss_htype,
                    "asset_category": loss_cat,
                    "impact_metric": loss_item.get("impact_metric") or None,
                    "impact_type": impact_type,
                    "imt": loss_item.get("imt") or (hazard or {}).get("imt"),
                }
                loss_desc = (loss_item.get("description", "") or "").strip()
                if loss_desc:
                    parsed_loss["description"] = loss_desc
                loss.append(parsed_loss)

    # Contributing sources (v2): list of {name, used_in, type, description}
    raw_sources = raw.get("contributing_sources", [])
    contributing_sources: List[Dict[str, Any]] = []
    if isinstance(raw_sources, list):
        for s in raw_sources:
            if not isinstance(s, dict):
                continue
            s_name = (s.get("name", "") or "").strip()
            if not s_name:
                continue
            cs: Dict[str, Any] = {"name": s_name}
            s_used_in = s.get("used_in", "")
            if s_used_in in _VALID_USED_IN:
                cs["used_in"] = s_used_in
            s_type = s.get("type", "dataset")
            cs["type"] = s_type if s_type in _VALID_SOURCE_TYPES else "dataset"
            s_desc = (s.get("description", "") or "").strip()
            if s_desc:
                cs["description"] = s_desc
            contributing_sources.append(cs)

    # Lineage description (v2): scientific pipeline description from LLM
    lineage_description: Optional[str] = None
    raw_ld = raw.get("lineage_description")
    if raw_ld and isinstance(raw_ld, str):
        lineage_description = raw_ld.strip() or None

    return V10Classification(
        hdx_id=hdx_id,
        is_rdls=is_rdls,
        not_rdls_reason=not_rdls_reason,
        components=components,
        domain=domain,
        hazard=hazard,
        exposure=exposure,        # List[Dict] - may be empty
        vulnerability=vulnerability,
        loss=loss,                # List[Dict] - may be empty
        countries=countries,
        spatial_scale=spatial_scale,
        confidence=confidence,
        reasoning=reasoning,
        contributing_sources=contributing_sources,
        lineage_description=lineage_description,
        prompt_hash="",      # set by caller
        from_cache=False,    # set by caller
        token_usage={"input": 0, "output": 0},
        llm_model="",        # set by caller
    )


# ---------------------------------------------------------------------------
# Main classification function
# ---------------------------------------------------------------------------

def classify_v10(
    hdx_meta: Dict[str, Any],
    column_infos: List[ColumnInfo],
    cache: LLMCacheV10,
    config: V10Config,
    api_key: str,
) -> V10Classification:
    """Classify one HDX dataset for RDLS v1.0.

    Checks cache first. Falls back to LLM if cache miss.

    Args:
        hdx_meta: Raw HDX dataset JSON.
        column_infos: Column headers from CKAN cache (may be empty list).
        cache: LLMCacheV10 instance.
        config: V10Config instance.
        api_key: Anthropic API key string.

    Returns:
        V10Classification with all fields populated.
    """
    hdx_id = hdx_meta.get("id", "unknown")
    prompt = build_prompt_v10(hdx_meta, column_infos, config)
    phash = _prompt_hash(prompt)

    # Cache hit
    cached = cache.get(phash)
    if cached:
        result = _parse_response(cached["response"], hdx_id)
        result.prompt_hash = phash
        result.from_cache = True
        result.token_usage = cached.get("token_usage", {"input": 0, "output": 0})
        result.llm_model = cached.get("model", config.model)
        return result

    # Cache miss - call LLM
    raw_response, token_usage = _call_api(prompt, config, api_key)

    # Persist to cache
    cache.put(phash, {
        "hdx_id": hdx_id,
        "prompt_hash": phash,
        "model": config.model,
        "response": raw_response,
        "token_usage": token_usage,
    })

    result = _parse_response(raw_response, hdx_id)
    result.prompt_hash = phash
    result.from_cache = False
    result.token_usage = token_usage
    result.llm_model = config.model
    return result


# ---------------------------------------------------------------------------
# Cost tracking helper
# ---------------------------------------------------------------------------

@dataclass
class CostTracker:
    """Tracks LLM API spend across the pipeline run."""

    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_calls: int = 0
    cached_calls: int = 0

    def add(self, usage: Dict[str, int], from_cache: bool) -> None:
        self.total_input_tokens += usage.get("input", 0)
        self.total_output_tokens += usage.get("output", 0)
        self.total_calls += 1
        if from_cache:
            self.cached_calls += 1

    def cost_usd(self, config: V10Config) -> float:
        in_cost = (self.total_input_tokens / 1_000_000) * config.cost_per_mtok_input
        out_cost = (self.total_output_tokens / 1_000_000) * config.cost_per_mtok_output
        return in_cost + out_cost

    def summary(self, config: V10Config) -> str:
        live = self.total_calls - self.cached_calls
        cost = self.cost_usd(config)
        return (
            f"Calls: {self.total_calls} total "
            f"({live} LLM, {self.cached_calls} cached) | "
            f"Tokens: {self.total_input_tokens:,} in / {self.total_output_tokens:,} out | "
            f"Cost: ${cost:.2f}"
        )

    def check_limit(self, config: V10Config) -> bool:
        """Return True if cost limit exceeded."""
        return self.cost_usd(config) >= config.max_cost_usd
