"""v1.0 HEVL block builders.

Takes V10Classification output from llm_classify.py and constructs minimal
but schema-valid RDLS v1.0 HEVL blocks (hazard, exposure, vulnerability, loss).

Design:
- Builds only the blocks present in llm_response.components
- Uses LLM-supplied values (type, process, imt, etc.) directly
- Falls back to per-hazard-type defaults where LLM omits optional fields
- Resulting blocks pass Layer 1 (JSON Schema) validation
- Layer 2/3 (semantic, codelist) may flag open codelist values - expected

HEVL structural approach:
- Hazard: generates events[] array when return_periods are known (probabilistic)
- Exposure: accepts a list of exposure dicts -> multiple exposure items per record
- Loss: accepts a list of loss dicts -> multiple loss entries per record
- All builders maintain backward compat when called with single-item list
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Defaults: used when LLM does not supply a value
# ---------------------------------------------------------------------------

# Default intensity measure per hazard type (metric:unit format)
DEFAULT_IMT: Dict[str, str] = {
    "flood":             "wd:m",
    "coastal_flood":     "wd:m",
    "earthquake":        "PGA:g",
    "strong_wind":       "sws_10m:m/s",
    "convective_storm":  "sws_10m:m/s",
    "drought":           "SPI:-",
    "extreme_temperature": "AirTemp:C",
    "landslide":         "LSI:-",
    "tsunami":           "wd:m",
    "volcanic":          "h_vaf:mm",          # codelist: h_vaf:mm (volcanic ash fall thickness)
    "wildfire":          "FWI:-",
    "sea_level_rise":    "slr:cm",
    "erosion":           "Er:T/ha",           # codelist: Er:T/ha (lost sediment mass t/ha)
    "dust_sand_storm":   "DSI:-",             # codelist: DSI:- (Dust Storm Index)
    "pest_infestation":  "Pest:count/area",   # codelist: Pest:count/area (pest density)
}

# Default process per hazard type
DEFAULT_PROCESS: Dict[str, str] = {
    "flood":             "fluvial_flood",
    "coastal_flood":     "coastal_flood",
    "earthquake":        "ground_motion",
    "strong_wind":       "tropical_cyclone",
    "convective_storm":  "thunderstorm",
    "drought":           "meteorological_drought",
    "extreme_temperature": "extreme_heat",
    "landslide":         "landslide_general",
    "tsunami":           "tsunami",
    "volcanic":          "ashfall",
    "wildfire":          "wildfire",
    "sea_level_rise":    "sea_level_rise",
    "erosion":           "soil_erosion",
    "dust_sand_storm":   "dust_sand_storm",
    "pest_infestation":  "pest",
}

# Valid dimension enum values (closed codelist from schema)
_VALID_DIMENSIONS: set = {"structure", "content", "product", "disruption", "population", "index"}

# Valid calculation method values (closed codelist from schema)
_VALID_CALC_METHODS: set = {"simulated", "observed", "inferred"}

# Valid hazard types (closed codelist from schema)
_VALID_HAZARD_TYPES: set = {
    "coastal_flood", "convective_storm", "drought", "earthquake", "erosion",
    "extreme_temperature", "flood", "landslide", "pest_infestation",
    "sea_level_rise", "strong_wind", "tsunami", "volcanic", "wildfire",
    "dust_sand_storm",
}

# Valid exposure categories (closed codelist from schema)
_VALID_EXPOSURE_CATS: set = {
    "agriculture", "buildings", "infrastructure", "population",
    "natural_environment", "economic_indicator", "development_index",
}

# Default asset_dimension per exposure category
DEFAULT_DIMENSION: Dict[str, str] = {
    "buildings":           "structure",
    "population":          "population",
    "infrastructure":      "structure",
    "agriculture":         "product",
    "natural_environment": "product",
    "economic_indicator":  "index",
    "development_index":   "index",
}

# Default quantity_kind per exposure category
DEFAULT_QUANTITY_KIND: Dict[str, str] = {
    "buildings":           "count",
    "population":          "count",
    "infrastructure":      "count",
    "agriculture":         "area",
    "natural_environment": "area",
    "economic_indicator":  "monetary",
    "development_index":   "count",
}

# Default impact_metric per LLM-supplied metric or category fallback
_LOSS_METRIC_FALLBACK: Dict[str, str] = {
    "buildings":   "buildings_damaged_count",
    "population":  "affected_count",
    "infrastructure": "asset_loss",
    "agriculture": "economic_loss_value",
    "natural_environment": "asset_loss",
    "economic_indicator": "economic_loss_value",
    "development_index": "affected_count",
}

# Valid impact_type values (closed codelist from schema)
_VALID_IMPACT_TYPES: set = {"direct", "indirect", "total"}


# ---------------------------------------------------------------------------
# Hazard block
# ---------------------------------------------------------------------------

def build_hazard_block(
    hazard_type: str,
    process: Optional[str] = None,
    analysis_type: str = "probabilistic",
    imt: Optional[str] = None,
    return_periods: Optional[List[int]] = None,
    calculation_method: Optional[str] = None,
    description: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a minimal valid v1.0 hazard block.

    Creates one event_set with one hazard entry. If return_periods are provided
    AND analysis_type is 'probabilistic', generates an events[] array with one
    event per return period, each with occurrence.probabilistic fields.

    Args:
        hazard_type: RDLS hazard type code (e.g. "flood", "earthquake").
        process: RDLS process code. Defaults to per-hazard default.
        analysis_type: probabilistic | empirical | deterministic.
        imt: Intensity measure type string (e.g. "wd:m", "PGA:g").
        return_periods: List of return period values in years (e.g. [10, 25, 100]).
                        Only used when analysis_type == "probabilistic".
        calculation_method: simulated | observed | inferred.
                            Applied to event_set and each event.
        description: Brief description of the hazard dataset (added to each event).

    Returns:
        Dict with "event_sets" key, schema-valid for v1.0.
    """
    resolved_process = process or DEFAULT_PROCESS.get(hazard_type, hazard_type)
    resolved_imt = imt or DEFAULT_IMT.get(hazard_type, "unknown:unknown")
    resolved_calc = calculation_method if calculation_method in _VALID_CALC_METHODS else "simulated"

    event_set: Dict[str, Any] = {
        "id": "event_set_1",
        "hazards": [
            {
                "id": "hazard_1",
                "type": hazard_type,
                "process": resolved_process,
                "intensity_measure": resolved_imt,
            }
        ],
        "analysis_type": analysis_type,
        "calculation_method": resolved_calc,
    }

    # Generate events[] for probabilistic datasets with explicit return periods.
    # Each event carries its own occurrence.probabilistic block with the return
    # period and corresponding event_rate (1 / return_period).
    # Schema: Event requires id, calculation_method, hazard (type+process+imt), occurrence.
    if return_periods and analysis_type == "probabilistic":
        events: List[Dict[str, Any]] = []
        for rp in return_periods:
            rp_int = int(rp)
            if rp_int <= 0:
                continue
            event: Dict[str, Any] = {
                "id": f"event_{rp_int}yr",
                "calculation_method": resolved_calc,
                "hazard": {
                    "type": hazard_type,
                    "process": resolved_process,
                    "intensity_measure": resolved_imt,
                },
                "occurrence": {
                    "probabilistic": {
                        "return_period": rp_int,
                        "event_rate": round(1.0 / rp_int, 8),
                    }
                },
            }
            if description:
                event["description"] = description
            events.append(event)
        if events:
            event_set["events"] = events

    return {"event_sets": [event_set]}


# ---------------------------------------------------------------------------
# Exposure block
# ---------------------------------------------------------------------------

def build_exposure_block(
    exposure_list: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Build minimal valid v1.0 exposure block (list of exposure items).

    Accepts a list of exposure dicts, each optionally containing:
        category (required), dimension, quantity_kind, description

    Multiple exposure categories are supported: one Exposure_item per entry.
    If a description is provided, it is added as asset_type.description.

    Invalid or duplicate categories are skipped. Falls back to a default
    buildings item if the input list produces no valid items.

    Args:
        exposure_list: List of exposure dicts from LLM classification.

    Returns:
        List of schema-valid Exposure_item dicts.
    """
    result: List[Dict[str, Any]] = []
    seen_categories: set = set()

    for idx, exp in enumerate(exposure_list):
        if not isinstance(exp, dict):
            continue
        category = (exp.get("category", "") or "").strip()
        if category not in _VALID_EXPOSURE_CATS:
            continue
        if category in seen_categories:
            continue
        seen_categories.add(category)

        dimension = exp.get("dimension")
        quantity_kind = exp.get("quantity_kind")
        description = (exp.get("description", "") or "").strip()

        # Validate dimension against closed enum; LLM often returns "count", "area", etc.
        resolved_dim = (
            dimension if dimension in _VALID_DIMENSIONS
            else DEFAULT_DIMENSION.get(category, "structure")
        )
        resolved_qk = quantity_kind or DEFAULT_QUANTITY_KIND.get(category, "count")

        item: Dict[str, Any] = {
            "id": f"exposure_{len(result) + 1}",
            "category": category,
        }

        # asset_type follows the Classification schema: requires "id", optional "description".
        # Use category as the classification id; add LLM description if available.
        if description:
            item["asset_type"] = {
                "id": category,
                "description": description,
            }

        item["metrics"] = [
            {
                "id": "metric_1",
                "dimension": resolved_dim,
                "measurement": {
                    "quantity_kind": resolved_qk,
                },
            }
        ]

        result.append(item)

    # Fallback: produce a minimal valid item if nothing passed validation
    if not result:
        result = [
            {
                "id": "exposure_1",
                "category": "buildings",
                "metrics": [
                    {
                        "id": "metric_1",
                        "dimension": "structure",
                        "measurement": {
                            "quantity_kind": "count",
                        },
                    }
                ],
            }
        ]

    return result


# ---------------------------------------------------------------------------
# Vulnerability block
# ---------------------------------------------------------------------------

def build_vulnerability_block(
    hazard_type: str,
    imt: Optional[str] = None,
    category: Optional[str] = None,
) -> Dict[str, Any]:
    """Build a minimal valid v1.0 vulnerability block.

    Vulnerability data is extremely rare on HDX.
    Creates one generic vulnerability function.
    """
    resolved_imt = imt or DEFAULT_IMT.get(hazard_type, "unknown:unknown")

    func: Dict[str, Any] = {
        "id": "vulnerability_function_1",
        "approach": "empirical",
        "relationship": "math_parametric",
        "hazard_primary": {
            "type": hazard_type,
            "intensity_measure": resolved_imt,
        },
    }
    if category:
        func["category"] = category

    return {"functions": {"vulnerability": [func]}}


# ---------------------------------------------------------------------------
# Loss block
# ---------------------------------------------------------------------------

def build_loss_block(
    loss_list: List[Dict[str, Any]],
    default_analysis_type: str = "empirical",
    hazard_info: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build a minimal valid v1.0 loss block from a list of loss entries.

    Accepts a list of loss dicts, each optionally containing:
        hazard_type, asset_category, impact_metric, impact_type, imt,
        analysis_type, description

    Multiple loss entries are supported (e.g. direct casualties + economic losses).
    Falls back to defaults from hazard_info when per-entry values are absent.

    Args:
        loss_list: List of loss dicts from LLM classification.
        default_analysis_type: Used when individual loss entry has no analysis_type.
        hazard_info: LLM hazard dict - used as fallback for hazard_type and imt.

    Returns:
        Dict with "losses" key containing a list of schema-valid Loss entries.
    """
    _fallback_htype = (hazard_info or {}).get("type", "flood")
    _fallback_imt = (hazard_info or {}).get("imt")

    losses: List[Dict[str, Any]] = []

    for loss_item in loss_list:
        if not isinstance(loss_item, dict):
            continue

        # Hazard type: item-level overrides hazard_info fallback
        raw_htype = loss_item.get("hazard_type") or _fallback_htype
        haz_type = raw_htype if raw_htype in _VALID_HAZARD_TYPES else _fallback_htype
        if haz_type not in _VALID_HAZARD_TYPES:
            haz_type = "flood"

        # Asset category
        raw_cat = loss_item.get("asset_category", "")
        asset_category = raw_cat if raw_cat in _VALID_EXPOSURE_CATS else "buildings"

        # Impact metric
        impact_metric = (
            loss_item.get("impact_metric")
            or _LOSS_METRIC_FALLBACK.get(asset_category, "affected_count")
        )

        # Impact type (direct / indirect / total)
        raw_impact_type = loss_item.get("impact_type", "direct")
        impact_type = raw_impact_type if raw_impact_type in _VALID_IMPACT_TYPES else "direct"

        # IMT: item-level > hazard_info fallback > per-hazard default
        imt = loss_item.get("imt") or _fallback_imt or DEFAULT_IMT.get(haz_type, "unknown:unknown")

        # Analysis type and related fields
        analysis_type = loss_item.get("analysis_type") or default_analysis_type
        if analysis_type not in {"probabilistic", "empirical", "deterministic"}:
            analysis_type = default_analysis_type
        # impact_modelling (codelist_data_calculation_type): observed/simulated/inferred.
        # - empirical datasets -> "observed" (data collected in the field)
        # - probabilistic models -> "simulated" (model output at return periods)
        # - deterministic models -> "inferred" (single scenario, analytically derived)
        impact_modelling = (
            "observed" if analysis_type == "empirical"
            else "simulated" if analysis_type == "probabilistic"
            else "inferred"
        )
        # loss_approach (codelist_function_approach): analytical/empirical/hybrid/judgement.
        # - empirical datasets -> "empirical" (based on observed data)
        # - model-based (probabilistic/deterministic) -> "analytical" (model-derived)
        loss_approach = "empirical" if analysis_type == "empirical" else "analytical"
        loss_freq = analysis_type

        # Description (optional, schema allows it on Losses)
        description = (loss_item.get("description", "") or "").strip()

        asset_dim = DEFAULT_DIMENSION.get(asset_category, "structure")

        loss_entry: Dict[str, Any] = {
            "id": f"loss_{len(losses) + 1}",
            "hazard": {
                "type": haz_type,
                "intensity_measure": imt,
            },
            "asset_category": asset_category,
            "asset_dimension": asset_dim,
            "impact_and_losses": {
                "impact_type": impact_type,
                "impact_modelling": impact_modelling,
                "impact_metric": impact_metric,
                "measurement": {
                    "quantity_kind": DEFAULT_QUANTITY_KIND.get(asset_category, "count"),
                },
                "loss_type": "ground_up",
                "loss_approach": loss_approach,
                "loss_frequency_type": loss_freq,
            },
        }

        if description:
            loss_entry["description"] = description

        losses.append(loss_entry)

    # Fallback: if no valid entries were produced, create a default loss item
    if not losses:
        fallback_imt = _fallback_imt or DEFAULT_IMT.get(_fallback_htype, "unknown:unknown")
        losses = [
            {
                "id": "loss_1",
                "hazard": {
                    "type": _fallback_htype,
                    "intensity_measure": fallback_imt,
                },
                "asset_category": "buildings",
                "asset_dimension": "structure",
                "impact_and_losses": {
                    "impact_type": "direct",
                    "impact_modelling": "observed",
                    "impact_metric": "buildings_damaged_count",
                    "measurement": {"quantity_kind": "count"},
                    "loss_type": "ground_up",
                    "loss_approach": "empirical",
                    "loss_frequency_type": default_analysis_type,
                },
            }
        ]

    return {"losses": losses}


# ---------------------------------------------------------------------------
# HEVL integration
# ---------------------------------------------------------------------------

def integrate_hevl_v10(
    base_record: Dict[str, Any],
    components: List[str],
    hazard_info: Optional[Dict[str, Any]] = None,
    exposure_info: Optional[List[Dict[str, Any]]] = None,
    vulnerability_info: Optional[Dict[str, Any]] = None,
    loss_info: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Merge HEVL blocks into the base record.

    Args:
        base_record: Output of build_base_record_v10() (no HEVL fields).
        components: Which components are present e.g. ["hazard", "exposure"].
        hazard_info: LLM hazard dict (type, process, analysis_type, imt,
                     return_periods, calculation_method, description) or None.
        exposure_info: List of LLM exposure dicts (category, dimension,
                       quantity_kind, description) or None.
        vulnerability_info: LLM vulnerability dict or None.
        loss_info: List of LLM loss dicts (hazard_type, asset_category,
                   impact_metric, impact_type, description) or None.

    Returns:
        Complete record with HEVL blocks inserted before links.
    """
    record = dict(base_record)

    # Insert HEVL blocks before links
    links = record.pop("links", [])

    if "hazard" in components and hazard_info:
        record["hazard"] = build_hazard_block(
            hazard_type=hazard_info.get("type", "flood"),
            process=hazard_info.get("process"),
            analysis_type=hazard_info.get("analysis_type", "probabilistic"),
            imt=hazard_info.get("imt"),
            return_periods=hazard_info.get("return_periods"),
            calculation_method=hazard_info.get("calculation_method"),
            description=hazard_info.get("description"),
        )

    if "exposure" in components and exposure_info:
        record["exposure"] = build_exposure_block(exposure_list=exposure_info)

    if "vulnerability" in components and vulnerability_info:
        haz_type = (
            vulnerability_info.get("hazard_type")
            or (hazard_info or {}).get("type", "flood")
        )
        record["vulnerability"] = build_vulnerability_block(
            hazard_type=haz_type,
            imt=vulnerability_info.get("imt") or (hazard_info or {}).get("imt"),
            category=vulnerability_info.get("category"),
        )

    if "loss" in components and loss_info:
        record["loss"] = build_loss_block(
            loss_list=loss_info,
            default_analysis_type=(hazard_info or {}).get("analysis_type", "empirical"),
            hazard_info=hazard_info,
        )

    record["links"] = links
    return record
