"""
HEVL integration: merge hazard, exposure, vulnerability, and loss blocks
into base RDLS records.

Handles risk_data_type reconciliation, component validation,
and record ID generation. Source-independent.
"""

import copy
from typing import Any, Dict, List, Optional, Set, Tuple

from .naming import build_rdls_id, encode_component_types
from .utils import load_yaml


# ---------------------------------------------------------------------------
# Component combination validation
# ---------------------------------------------------------------------------

def validate_component_combination(
    components: Set[str],
    require_he_for_vl: bool = False,
) -> Tuple[bool, str]:
    """Validate that component combinations are logically valid.

    Rule: if require_he_for_vl is True, vulnerability or loss
    require hazard or exposure to also be present.

    Default is False (standalone V, L, V+L are valid).

    Returns (is_valid, reason).
    """
    if not require_he_for_vl:
        return True, ""

    needs_he = {"vulnerability", "loss"}
    has_he = {"hazard", "exposure"}

    if components & needs_he and not (components & has_he):
        return False, "vulnerability/loss require hazard or exposure"

    return True, ""


# ---------------------------------------------------------------------------
# Risk data type reconciliation
# ---------------------------------------------------------------------------

def determine_risk_data_types(
    base_types: List[str],
    hevl_flags: Dict[str, bool],
) -> List[str]:
    """Reconcile base record risk_data_types with HEVL extraction flags.

    Args:
        base_types: risk_data_type from NB 06 translation.
        hevl_flags: {"hazard": True/False, "exposure": True/False, ...}

    Returns:
        Updated risk_data_type list.
    """
    types = set(base_types)

    # Add types found by HEVL extraction
    for component, found in hevl_flags.items():
        if found and component in {"hazard", "exposure", "vulnerability", "loss"}:
            types.add(component)

    return sorted(types)


# ---------------------------------------------------------------------------
# Filename prefix (DEPRECATED — kept for backward compatibility)
# ---------------------------------------------------------------------------

def determine_filename_prefix(
    components: List[str],
    prefix_map: Optional[Dict[str, str]] = None,
    prefix_priority: Optional[List[str]] = None,
) -> str:
    """Select filename prefix by highest-priority component present.

    DEPRECATED: Use naming.encode_component_types() or naming.build_rdls_id()
    instead. This function is retained only for backward compatibility with
    code that hasn't been updated yet.

    Priority order (default): loss > vulnerability > exposure > hazard.
    """
    if prefix_map is None:
        prefix_map = {
            "hazard": "rdls_hzd",
            "exposure": "rdls_exp",
            "vulnerability": "rdls_vln",
            "loss": "rdls_lss",
        }
    if prefix_priority is None:
        prefix_priority = ["loss", "vulnerability", "exposure", "hazard"]

    comp_set = set(components)
    for comp in prefix_priority:
        if comp in comp_set and comp in prefix_map:
            return prefix_map[comp]

    return "rdls_hzd"  # fallback


# ---------------------------------------------------------------------------
# Integrated ID builder
# ---------------------------------------------------------------------------

def build_integrated_id(
    components: List[str],
    iso3_codes: List[str],
    org_name: str,
    org_slug: str,
    naming_config: Dict[str, Any],
    title: str = "",
) -> str:
    """Build the integrated record ID using the new naming convention.

    Wrapper around naming.build_rdls_id() for use in the integration step.

    Args:
        components: Final risk_data_type list after HEVL merge.
        iso3_codes: ISO3 country codes from spatial block.
        org_name: Organization display name.
        org_slug: Organization slug.
        naming_config: Loaded naming config dict.
        title: Dataset title for slug generation.

    Returns:
        RDLS record ID string.
    """
    return build_rdls_id(
        components=components,
        iso3_codes=iso3_codes,
        org_name=org_name,
        org_slug=org_slug,
        config=naming_config,
        title=title,
    )


# ---------------------------------------------------------------------------
# HEVL block helpers (extract types/categories from merged blocks)
# ---------------------------------------------------------------------------

def extract_hazard_types_from_block(
    hazard_block: Optional[Dict[str, Any]],
) -> List[str]:
    """Extract hazard_type values from an RDLS hazard block.

    Handles both event_set[].hazards[] and direct hazard[] structures.
    """
    if not hazard_block:
        return []
    types = set()

    # event_set structure (NB 09 output)
    for evt in hazard_block.get("event_set", []):
        for hz in evt.get("hazards", []):
            ht = hz.get("hazard_type")
            if ht:
                types.add(ht)

    # Direct hazard_type at block level
    ht = hazard_block.get("hazard_type")
    if ht:
        types.add(ht)

    return sorted(types)


def extract_exposure_categories_from_block(
    exposure_block: Optional[Any],
) -> List[str]:
    """Extract exposure_category values from an RDLS exposure block.

    Handles both list and single-dict exposure structures.
    """
    if not exposure_block:
        return []
    cats = set()

    if isinstance(exposure_block, list):
        for item in exposure_block:
            cat = item.get("category")
            if cat:
                cats.add(cat)
    elif isinstance(exposure_block, dict):
        cat = exposure_block.get("category")
        if cat:
            cats.add(cat)

    return sorted(cats)


def extract_iso3_from_spatial(spatial: Dict[str, Any]) -> List[str]:
    """Extract ISO3 codes from RDLS spatial block."""
    countries = spatial.get("countries", [])
    return [c for c in countries if isinstance(c, str) and len(c) == 3]


def extract_org_from_attributions(attributions: List[Dict[str, Any]]) -> str:
    """Extract publisher org name from attributions."""
    for attr in (attributions or []):
        if attr.get("role") == "publisher":
            return attr.get("entity", {}).get("name", "")
    return ""


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------

def build_hdx_provenance_note(dataset_url: str) -> str:
    """Build standard HDX provenance note.

    Format: [Source: ...HDX); Original dataset: URL]

    Args:
        dataset_url: Full HDX dataset URL.

    Returns:
        Formatted provenance string.
    """
    return (
        "[Source: This metadata record was automatically extracted from the "
        f"Humanitarian Data Exchange (HDX); Original dataset: {dataset_url}]"
    )


def append_provenance(
    record: Dict[str, Any],
    provenance_note: str,
) -> None:
    """Append a provenance note to the record's description (in-place).

    Ensures proper punctuation before appending.

    Args:
        record: RDLS record dict (modified in place).
        provenance_note: Text to append, e.g. "[Source: ...]".
    """
    desc = (record.get("description") or "").rstrip()
    if desc:
        if desc[-1] not in '.!?:;)\'"':
            desc += "."
        record["description"] = f"{desc} {provenance_note}"
    else:
        record["description"] = provenance_note


# ---------------------------------------------------------------------------
# Merge HEVL into record
# ---------------------------------------------------------------------------

def merge_hevl_into_record(
    base_record: Dict[str, Any],
    hevl_blocks: Dict[str, Any],
) -> Dict[str, Any]:
    """Merge HEVL component blocks into a base RDLS record.

    Args:
        base_record: Base RDLS record from translate.build_rdls_record().
        hevl_blocks: Dict with optional keys 'hazard', 'exposure',
                     'vulnerability', 'loss' — each containing the
                     corresponding RDLS JSON block.

    Returns:
        New record dict with HEVL blocks merged in.
    """
    record = copy.deepcopy(base_record)

    for component in ["hazard", "exposure", "vulnerability", "loss"]:
        block = hevl_blocks.get(component)
        if block:
            record[component] = block

    # Update risk_data_type to reflect actual components present
    hevl_flags = {
        comp: comp in hevl_blocks and hevl_blocks[comp] is not None
        for comp in ["hazard", "exposure", "vulnerability", "loss"]
    }
    record["risk_data_type"] = determine_risk_data_types(
        record.get("risk_data_type", []),
        hevl_flags,
    )

    return record


# ---------------------------------------------------------------------------
# Full integration pipeline step
# ---------------------------------------------------------------------------

def integrate_record(
    base_record: Dict[str, Any],
    hazard_block: Optional[Dict[str, Any]] = None,
    exposure_block: Optional[Any] = None,
    vulnerability_block: Optional[Dict[str, Any]] = None,
    loss_block: Optional[Dict[str, Any]] = None,
    require_he_for_vl: bool = False,
    naming_config: Optional[Dict[str, Any]] = None,
    provenance_note: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """Integrate base record with HEVL blocks.

    Validates component combinations. If naming_config is provided,
    rebuilds the record ID using the new naming convention. If
    provenance_note is provided, appends it to the description.

    Args:
        base_record: Base RDLS record.
        hazard_block: Optional hazard JSON block.
        exposure_block: Optional exposure JSON block.
        vulnerability_block: Optional vulnerability JSON block.
        loss_block: Optional loss JSON block.
        require_he_for_vl: Whether V/L require H or E (default False).
        naming_config: Optional naming config for ID generation.
        provenance_note: Optional provenance text to append to description.

    Returns:
        Merged record dict, or None if validation fails.
    """
    hevl_blocks = {}
    if hazard_block:
        hevl_blocks["hazard"] = hazard_block
    if exposure_block:
        hevl_blocks["exposure"] = exposure_block
    if vulnerability_block:
        hevl_blocks["vulnerability"] = vulnerability_block
    if loss_block:
        hevl_blocks["loss"] = loss_block

    # Validate
    components = set(hevl_blocks.keys()) | set(base_record.get("risk_data_type", []))
    is_valid, reason = validate_component_combination(components, require_he_for_vl)
    if not is_valid:
        return None

    record = merge_hevl_into_record(base_record, hevl_blocks)

    # Rebuild ID if naming config provided
    if naming_config:
        iso3_list = extract_iso3_from_spatial(record.get("spatial", {}))
        org_name = extract_org_from_attributions(record.get("attributions", []))
        title = record.get("title", "")

        record["id"] = build_integrated_id(
            components=record.get("risk_data_type", []),
            iso3_codes=iso3_list,
            org_name=org_name,
            org_slug="",  # slug not available in integrated record
            naming_config=naming_config,
            title=title,
        )

    # Append provenance note if provided
    if provenance_note:
        append_provenance(record, provenance_note)

    return record
