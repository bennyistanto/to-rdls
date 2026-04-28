"""
RDLS v1.0 Metadata Validator
=============================
Three-layer validation for RDLS JSON metadata files:
  Layer 1: JSON Schema validation (structure, types, required fields)
            Uses Draft7Validator + FormatChecker to match rdl-standard schema draft.
  Layer 2: Codelist validation (values against CSV codelists)
            Checks closed codelists as errors; open codelists as warnings.
            Covers: risk_data_type, spatial.scale, spatial.countries, attributions.role,
            lineage.sources.type, resources.*.climate.scenario, resources.*.spatial.*,
            hazard event_set fields, exposure category+dimension+asset_type.scheme,
            vulnerability function fields (all 4 types: approach, relationship,
            hazard_analysis_type, category, impact.type, impact.modelling, impact.metric,
            taxonomy, damage_scale_name), loss asset_category, asset_dimension,
            impact_and_losses fields.
  Layer 3: Semantic / cross-field validation
            Rule 1: hazard.type -> valid process values
            Rule 2: hazard.type -> valid intensity_measure (open, warning only)
            Rule 3: measurement.quantity_kind -> valid unit
            Rule 4: spatial.scale -> countries requirement (national/sub-national/urban: >= 1)
            Rule 5: event_set.analysis_type -> event.occurrence key matches
            Rule 6: publisher/creator/contact_point/attribution entity must have name + email/url
            Rule 7: risk_data_type -> corresponding section must be present
            Rule 8: resource.climate.scenario -> resource.baseline_period should be present

Usage:
    python scripts/validate_v1.0.py <metadata.json> [--schema <schema.json>] [--codelists <dir>]

Defaults:
    --schema    : rdls_schema.json from sibling rdl-standard repo (falls back to schema/rdls_schema_v1.0.json)
    --codelists : ../rdl-standard/schema/codelists (local rdl-standard clone)
"""

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Codelist loader
# ---------------------------------------------------------------------------

class CodelistRegistry:
    """Loads and caches CSV codelists from closed/ and open/ subdirectories."""

    def __init__(self, codelists_dir: Path):
        self.dir = codelists_dir
        self._cache: dict[str, tuple[set[str], bool]] = {}  # name -> (codes, is_open)
        self._master_imt: dict[str, set[str]] | None = None  # hazard_type -> codes

    def load(self, name: str) -> tuple[set[str], bool]:
        """Return (set_of_valid_codes, is_open_codelist) for a codelist name."""
        if name in self._cache:
            return self._cache[name]

        # Try closed/ first, then open/
        closed_path = self.dir / "closed" / name
        open_path = self.dir / "open" / name

        if closed_path.exists():
            codes = self._read_csv_codes(closed_path)
            self._cache[name] = (codes, False)
        elif open_path.exists():
            codes = self._read_csv_codes(open_path)
            self._cache[name] = (codes, True)
        else:
            # Codelist file not found — skip validation
            self._cache[name] = (set(), True)
        return self._cache[name]

    def load_master_imt(self) -> dict[str, set[str]]:
        """Load master IMT.csv and return dict of hazard_type -> set of codes.

        IMT.csv has columns: Code, Title, Description, Metric, Unit, Hazard
        The Hazard column may contain comma-separated hazard types or 'universal'.
        """
        if self._master_imt is not None:
            return self._master_imt

        self._master_imt = {}
        imt_path = self.dir / "open" / "IMT.csv"
        if not imt_path.exists():
            return self._master_imt

        with open(imt_path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get("Code", "").strip()
                hazards_raw = row.get("Hazard", "").strip()
                if not code or not hazards_raw:
                    continue
                hazard_types = [h.strip() for h in hazards_raw.split(",")]
                for ht in hazard_types:
                    if ht not in self._master_imt:
                        self._master_imt[ht] = set()
                    self._master_imt[ht].add(code)

        return self._master_imt

    def get_imt_codes_for_type(self, hazard_type: str) -> set[str]:
        """Get combined IMT codes for a hazard type from both per-type file and master IMT.csv."""
        # Per-type codelist
        codelist_name = TYPE_TO_IMT_CODELIST.get(hazard_type)
        per_type_codes = set()
        if codelist_name:
            per_type_codes, _ = self.load(codelist_name)

        # Master IMT.csv
        master = self.load_master_imt()
        master_codes = master.get(hazard_type, set())
        # Also include 'universal' entries
        universal_codes = master.get("universal", set())

        return per_type_codes | master_codes | universal_codes

    def _read_csv_codes(self, path: Path) -> set[str]:
        """Read the 'Code' column from a CSV codelist file."""
        codes = set()
        with open(path, encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get("Code", "").strip()
                if code:
                    codes.add(code)
        return codes


# ---------------------------------------------------------------------------
# Validation result
# ---------------------------------------------------------------------------

class ValidationResult:
    """Collects errors and warnings across all validation layers."""

    def __init__(self):
        self.errors: list[dict] = []     # Must fix
        self.warnings: list[dict] = []   # Should fix (semantic)

    def error(self, layer: str, path: str, message: str, **extra):
        self.errors.append({"layer": layer, "path": path, "message": message, **extra})

    def warning(self, layer: str, path: str, message: str, **extra):
        self.warnings.append({"layer": layer, "path": path, "message": message, **extra})

    @property
    def is_valid(self) -> bool:
        return len(self.errors) == 0

    def summary(self) -> str:
        lines = []
        if self.errors:
            lines.append(f"\n{'='*70}")
            lines.append(f"ERRORS: {len(self.errors)}")
            lines.append(f"{'='*70}")
            for e in self.errors:
                lines.append(f"  [{e['layer']}] {e['path']}")
                lines.append(f"    {e['message']}")
                if "allowed" in e:
                    allowed_str = ", ".join(sorted(e["allowed"])[:20])
                    if len(e["allowed"]) > 20:
                        allowed_str += f" ... ({len(e['allowed'])} total)"
                    lines.append(f"    Allowed: {allowed_str}")
        if self.warnings:
            lines.append(f"\n{'='*70}")
            lines.append(f"WARNINGS: {len(self.warnings)}")
            lines.append(f"{'='*70}")
            for w in self.warnings:
                lines.append(f"  [{w['layer']}] {w['path']}")
                lines.append(f"    {w['message']}")
        if not self.errors and not self.warnings:
            lines.append("\nAll checks passed.")
        lines.append(f"\nTotal: {len(self.errors)} errors, {len(self.warnings)} warnings")
        return "\n".join(lines)


# ---------------------------------------------------------------------------
# Layer 1: JSON Schema validation
# ---------------------------------------------------------------------------

def validate_layer1_schema(data: dict, schema: dict, result: ValidationResult):
    """Standard JSON Schema validation using jsonschema library.

    Uses Draft7Validator to match the rdl-standard schema (JSON Schema Draft 7).
    FormatChecker is required for 'date' and 'iri' format validation.
    """
    try:
        from jsonschema import Draft7Validator, FormatChecker
    except ImportError:
        result.warning("schema", "(root)", "jsonschema library not installed — skipping Layer 1. Install with: pip install jsonschema")
        return

    validator = Draft7Validator(schema, format_checker=FormatChecker())
    for err in validator.iter_errors(data):
        path = " -> ".join(str(p) for p in err.absolute_path) if err.absolute_path else "(root)"
        result.error("schema", path, err.message[:500])


# ---------------------------------------------------------------------------
# Layer 2: Codelist validation
# ---------------------------------------------------------------------------

# Mapping of JSON paths to their codelist files.
# Format: (json_pointer_pattern, codelist_file, description)
# Patterns use * for array indices and ** for recursive match.

CODELIST_CHECKS = [
    # Dataset level
    ("risk_data_type.*", "risk_data_type.csv", "risk data type"),
    ("spatial.scale", "spatial_scale.csv", "spatial scale"),
    ("spatial.countries.*", "country.csv", "country code"),

    # Attribution
    ("attributions.*.role", "roles.csv", "attribution role"),

    # Source
    ("lineage.sources.*.type", "source_type.csv", "source type"),

    # Climate scenario (on resources)
    ("resources.*.climate.scenario", "climate_scenario.csv", "climate scenario"),

    # Hazard event_set level
    ("hazard.event_sets.*.analysis_type", "analysis_type.csv", "analysis type"),
    ("hazard.event_sets.*.frequency_distribution", "frequency_distribution.csv", "frequency distribution"),
    ("hazard.event_sets.*.seasonality", "seasonality.csv", "seasonality"),
    ("hazard.event_sets.*.calculation_method", "data_calculation_type.csv", "calculation method"),

    # Event level
    ("hazard.event_sets.*.events.*.calculation_method", "data_calculation_type.csv", "calculation method"),

    # Exposure
    ("exposure.*.category", "exposure_category.csv", "exposure category"),
    ("exposure.*.metrics.*.dimension", "metric_dimension.csv", "metric dimension"),
    ("exposure.*.asset_type.scheme", "classification_scheme.csv", "asset type classification scheme"),

    # Resource spatial (same rules as dataset.spatial)
    ("resources.*.spatial.scale", "spatial_scale.csv", "resource spatial scale"),
    ("resources.*.spatial.countries.*", "country.csv", "resource country code"),

    # Vulnerability functions -- all four types share $ref: Function (same base schema)
    # impact.type / impact.modelling / impact.metric are NESTED under impact object (v1.0)
    # Closed codelists: approach, relationship, hazard_analysis_type, category, impact.type, impact.modelling
    # Open codelists:   impact.metric, taxonomy, damage_scale_name
    ("vulnerability.functions.vulnerability.*.approach", "function_approach.csv", "function approach"),
    ("vulnerability.functions.vulnerability.*.relationship", "relationship_type.csv", "relationship type"),
    ("vulnerability.functions.vulnerability.*.hazard_analysis_type", "analysis_type.csv", "analysis type"),
    ("vulnerability.functions.vulnerability.*.category", "exposure_category.csv", "exposure category"),
    ("vulnerability.functions.vulnerability.*.impact.type", "impact_type.csv", "impact type"),
    ("vulnerability.functions.vulnerability.*.impact.modelling", "data_calculation_type.csv", "impact modelling"),
    ("vulnerability.functions.vulnerability.*.impact.metric", "impact_metric.csv", "impact metric"),
    ("vulnerability.functions.vulnerability.*.taxonomy", "classification_scheme.csv", "taxonomy"),
    ("vulnerability.functions.fragility.*.approach", "function_approach.csv", "function approach"),
    ("vulnerability.functions.fragility.*.relationship", "relationship_type.csv", "relationship type"),
    ("vulnerability.functions.fragility.*.hazard_analysis_type", "analysis_type.csv", "analysis type"),
    ("vulnerability.functions.fragility.*.category", "exposure_category.csv", "exposure category"),
    ("vulnerability.functions.fragility.*.impact.type", "impact_type.csv", "impact type"),
    ("vulnerability.functions.fragility.*.impact.modelling", "data_calculation_type.csv", "impact modelling"),
    ("vulnerability.functions.fragility.*.impact.metric", "impact_metric.csv", "impact metric"),
    ("vulnerability.functions.fragility.*.taxonomy", "classification_scheme.csv", "taxonomy"),
    ("vulnerability.functions.fragility.*.damage_scale_name", "damage_scale_name.csv", "damage scale name"),
    # damage_to_loss and engineering_demand share same Function base -- all Function fields apply
    ("vulnerability.functions.damage_to_loss.*.approach", "function_approach.csv", "function approach"),
    ("vulnerability.functions.damage_to_loss.*.relationship", "relationship_type.csv", "relationship type"),
    ("vulnerability.functions.damage_to_loss.*.hazard_analysis_type", "analysis_type.csv", "analysis type"),
    ("vulnerability.functions.damage_to_loss.*.category", "exposure_category.csv", "exposure category"),
    ("vulnerability.functions.damage_to_loss.*.impact.type", "impact_type.csv", "impact type"),
    ("vulnerability.functions.damage_to_loss.*.impact.modelling", "data_calculation_type.csv", "impact modelling"),
    ("vulnerability.functions.damage_to_loss.*.impact.metric", "impact_metric.csv", "impact metric"),
    ("vulnerability.functions.damage_to_loss.*.taxonomy", "classification_scheme.csv", "taxonomy"),
    ("vulnerability.functions.damage_to_loss.*.damage_scale_name", "damage_scale_name.csv", "damage scale name"),
    ("vulnerability.functions.engineering_demand.*.approach", "function_approach.csv", "function approach"),
    ("vulnerability.functions.engineering_demand.*.relationship", "relationship_type.csv", "relationship type"),
    ("vulnerability.functions.engineering_demand.*.hazard_analysis_type", "analysis_type.csv", "analysis type"),
    ("vulnerability.functions.engineering_demand.*.category", "exposure_category.csv", "exposure category"),
    ("vulnerability.functions.engineering_demand.*.impact.type", "impact_type.csv", "impact type"),
    ("vulnerability.functions.engineering_demand.*.impact.modelling", "data_calculation_type.csv", "impact modelling"),
    ("vulnerability.functions.engineering_demand.*.impact.metric", "impact_metric.csv", "impact metric"),
    ("vulnerability.functions.engineering_demand.*.taxonomy", "classification_scheme.csv", "taxonomy"),
    ("vulnerability.functions.engineering_demand.*.damage_scale_name", "damage_scale_name.csv", "damage scale name"),

    # Loss
    ("loss.losses.*.asset_category", "exposure_category.csv", "asset category"),
    ("loss.losses.*.asset_dimension", "metric_dimension.csv", "asset dimension"),
    ("loss.losses.*.impact_and_losses.impact_type", "impact_type.csv", "impact type"),
    ("loss.losses.*.impact_and_losses.impact_modelling", "data_calculation_type.csv", "impact modelling"),
    ("loss.losses.*.impact_and_losses.impact_metric", "impact_metric.csv", "impact metric"),
    ("loss.losses.*.impact_and_losses.loss_type", "loss_type.csv", "loss type"),
    ("loss.losses.*.impact_and_losses.loss_approach", "function_approach.csv", "loss approach"),
    ("loss.losses.*.impact_and_losses.loss_frequency_type", "analysis_type.csv", "loss frequency type"),
]


def _resolve_pattern(data: Any, parts: list[str], current_path: str = "") -> list[tuple[str, Any]]:
    """Resolve a dot-separated pattern with * wildcards against data.
    Returns list of (json_path, value) pairs."""
    if not parts:
        return [(current_path, data)]

    head, *tail = parts

    if head == "*":
        if isinstance(data, list):
            results = []
            for i, item in enumerate(data):
                results.extend(_resolve_pattern(item, tail, f"{current_path}[{i}]"))
            return results
        return []

    if isinstance(data, dict) and head in data:
        next_path = f"{current_path}.{head}" if current_path else head
        return _resolve_pattern(data[head], tail, next_path)

    return []


def validate_layer2_codelists(data: dict, registry: CodelistRegistry, result: ValidationResult):
    """Validate field values against their codelist CSV files."""
    for pattern, codelist_name, description in CODELIST_CHECKS:
        codes, is_open = registry.load(codelist_name)
        if not codes:
            continue  # Codelist file not found — skip

        parts = pattern.split(".")
        matches = _resolve_pattern(data, parts)

        for path, value in matches:
            if not isinstance(value, str) or not value:
                continue
            if value not in codes:
                if is_open:
                    result.warning(
                        "codelist", path,
                        f"Value '{value}' not in open codelist {codelist_name} for {description}. "
                        f"Custom values allowed but verify it's intentional.",
                        allowed=codes,
                    )
                else:
                    result.error(
                        "codelist", path,
                        f"Value '{value}' not in closed codelist {codelist_name} for {description}.",
                        allowed=codes,
                    )


# ---------------------------------------------------------------------------
# Layer 3: Semantic / cross-field validation
# ---------------------------------------------------------------------------

# RULE 1: hazard.type -> hazard.process
TYPE_TO_PROCESS = {
    "coastal_flood": {"coastal_flood", "storm_surge"},
    "convective_storm": {"tornado", "lightning", "thunderstorm", "hail"},
    "drought": {"agricultural_drought", "hydrological_drought", "meteorological_drought", "socioeconomic_drought"},
    # process_type.csv has "rupture" (singular). Schema conditional also uses "rupture".
    "earthquake": {"rupture", "ground_motion", "liquefaction", "subsidence_uplift"},
    "erosion": {"coastal_erosion", "soil_erosion"},
    "extreme_temperature": {"extreme_cold", "extreme_heat"},
    "flood": {"fluvial_flood", "pluvial_flood", "groundwater_flood", "coastal_flood", "glacial_lake_outburst"},
    "landslide": {"snow_avalanche", "landslide_general", "landslide_rockslide", "landslide_mudflow", "landslide_rockfall"},
    "pest_infestation": {"pest"},
    "sea_level_rise": {"sea_level_rise"},
    "strong_wind": {"extratropical_cyclone", "tropical_cyclone", "tornado"},
    "tsunami": {"tsunami"},
    "volcanic": {"ashfall", "volcano_ballistics", "lahar", "lava", "pyroclastic_flow", "volcano_gas_aerosols"},
    "wildfire": {"wildfire", "wildfire_smoke"},
    "dust_sand_storm": {"dust_sand_storm"},
}

# RULE 2: hazard.type -> intensity_measure codelist file
TYPE_TO_IMT_CODELIST = {
    "coastal_flood": "imt_coastal_flood.csv",
    "convective_storm": "imt_convective_storm.csv",
    "drought": "imt_drought.csv",
    "dust_sand_storm": "imt_dust_sand_storm.csv",
    "earthquake": "imt_earthquake.csv",
    "erosion": "imt_erosion.csv",
    "extreme_temperature": "imt_extreme_temperature.csv",
    "flood": "imt_flood.csv",
    "landslide": "imt_landslide.csv",
    "pest_infestation": "imt_pest_infestation.csv",
    "strong_wind": "imt_strong_wind.csv",
    "tsunami": "imt_tsunami.csv",
    "volcanic": "imt_volcanic.csv",
    "wildfire": "imt_wildfire.csv",
}

# RULE 3: quantity_kind -> unit codelist file
QUANTITY_TO_UNIT_CODELIST = {
    "area": ("unit_area.csv", True),
    "count": ("unit_count.csv", True),
    "currency": ("unit_currency.csv", False),  # Closed!
    "dimensionless_ratio": ("unit_dimensionless_ratio.csv", True),
    "energy": ("unit_energy.csv", True),
    "length": ("unit_length.csv", True),
    "mass": ("unit_mass.csv", True),
    "mass_per_area": ("unit_mass_per_area.csv", True),
    "time": ("unit_time.csv", True),
    "volume": ("unit_volume.csv", True),
}

# RULE 5: analysis_type -> expected occurrence key
ANALYSIS_TO_OCCURRENCE = {
    "probabilistic": "probabilistic",
    "empirical": "empirical",
    "deterministic": "deterministic",
}


def _collect_hazard_objects(data: dict) -> list[tuple[str, dict]]:
    """Find all hazard objects (with type/process/intensity_measure) in the data."""
    hazards = []

    # Event set hazards
    for i, es in enumerate(data.get("hazard", {}).get("event_sets", [])):
        for j, h in enumerate(es.get("hazards", [])):
            hazards.append((f"hazard.event_sets[{i}].hazards[{j}]", h))
            if "trigger" in h and isinstance(h["trigger"], dict):
                hazards.append((f"hazard.event_sets[{i}].hazards[{j}].trigger", h["trigger"]))

        for k, evt in enumerate(es.get("events", [])):
            h = evt.get("hazard", {})
            if h:
                hazards.append((f"hazard.event_sets[{i}].events[{k}].hazard", h))
                if "trigger" in h and isinstance(h["trigger"], dict):
                    hazards.append((f"hazard.event_sets[{i}].events[{k}].hazard.trigger", h["trigger"]))

    # Vulnerability function hazards
    for func_type in ["vulnerability", "fragility", "damage_to_loss", "engineering_demand"]:
        funcs = data.get("vulnerability", {}).get("functions", {}).get(func_type, [])
        for i, fn in enumerate(funcs):
            for hfield in ["hazard_primary", "hazard_secondary"]:
                h = fn.get(hfield, {})
                if h:
                    hazards.append((f"vulnerability.functions.{func_type}[{i}].{hfield}", h))

    # Loss hazards
    for i, loss in enumerate(data.get("loss", {}).get("losses", [])):
        h = loss.get("hazard", {})
        if h:
            hazards.append((f"loss.losses[{i}].hazard", h))

    return hazards


def _collect_measurement_objects(data: dict) -> list[tuple[str, dict]]:
    """Find all measurement objects (with quantity_kind/unit) in the data."""
    measurements = []

    # Exposure metrics
    for i, exp in enumerate(data.get("exposure", [])):
        for j, metric in enumerate(exp.get("metrics", [])):
            m = metric.get("measurement", {})
            if m:
                measurements.append((f"exposure[{i}].metrics[{j}].measurement", m))

    # Vulnerability functions -- measurement is nested under impact.measurement (v1.0)
    for func_type in ["vulnerability", "fragility", "damage_to_loss", "engineering_demand"]:
        funcs = data.get("vulnerability", {}).get("functions", {}).get(func_type, [])
        for i, fn in enumerate(funcs):
            m = fn.get("impact", {}).get("measurement", {})
            if m:
                measurements.append((f"vulnerability.functions.{func_type}[{i}].impact.measurement", m))

    # Loss
    for i, loss in enumerate(data.get("loss", {}).get("losses", [])):
        m = loss.get("impact_and_losses", {}).get("measurement", {})
        if m:
            measurements.append((f"loss.losses[{i}].impact_and_losses.measurement", m))

    return measurements


def validate_layer3_semantic(data: dict, registry: CodelistRegistry, result: ValidationResult):
    """Cross-field semantic validation (activation rules, triplets)."""

    # --- RULE 1: type -> process ---
    for path, h in _collect_hazard_objects(data):
        htype = h.get("type")
        process = h.get("process")
        if htype and process:
            allowed = TYPE_TO_PROCESS.get(htype)
            if allowed and process not in allowed:
                result.error(
                    "semantic", f"{path}.process",
                    f"Process '{process}' is invalid for hazard type '{htype}'.",
                    allowed=allowed,
                )

    # --- RULE 2: type -> intensity_measure ---
    # Checks both per-type imt_*.csv AND master IMT.csv (with Hazard column filter)
    for path, h in _collect_hazard_objects(data):
        htype = h.get("type")
        imt = h.get("intensity_measure")
        if htype and imt:
            combined_codes = registry.get_imt_codes_for_type(htype)
            if combined_codes and imt not in combined_codes:
                # IMT codelists are always open, so this is a warning
                codelist_name = TYPE_TO_IMT_CODELIST.get(htype, "IMT.csv")
                result.warning(
                    "semantic", f"{path}.intensity_measure",
                    f"Intensity measure '{imt}' not in {codelist_name} or IMT.csv for type '{htype}'. "
                    f"Open codelist — custom values allowed but verify.",
                    allowed=combined_codes,
                )

    # --- RULE 3: quantity_kind -> unit ---
    for path, m in _collect_measurement_objects(data):
        qk = m.get("quantity_kind")
        unit = m.get("unit")
        if qk and unit:
            mapping = QUANTITY_TO_UNIT_CODELIST.get(qk)
            if mapping:
                codelist_name, is_open = mapping
                codes, _ = registry.load(codelist_name)
                if codes and unit not in codes:
                    if is_open:
                        result.warning(
                            "semantic", f"{path}.unit",
                            f"Unit '{unit}' not in {codelist_name} for quantity_kind '{qk}'. "
                            f"Open codelist — custom values allowed but verify.",
                            allowed=codes,
                        )
                    else:
                        result.error(
                            "semantic", f"{path}.unit",
                            f"Unit '{unit}' not in closed codelist {codelist_name} for quantity_kind '{qk}'.",
                            allowed=codes,
                        )

    # --- RULE 4: scale -> countries ---
    # Pass the spatial sub-object directly (not the parent dict)
    if "spatial" in data:
        _check_scale_countries(data["spatial"], "spatial", result)
    for i, res in enumerate(data.get("resources", [])):
        if "spatial" in res:
            _check_scale_countries(res["spatial"], f"resources[{i}].spatial", result)

    # --- RULE 5: analysis_type -> occurrence ---
    for i, es in enumerate(data.get("hazard", {}).get("event_sets", [])):
        analysis_type = es.get("analysis_type")
        expected_key = ANALYSIS_TO_OCCURRENCE.get(analysis_type)
        if not expected_key:
            continue
        for j, evt in enumerate(es.get("events", [])):
            occ = evt.get("occurrence", {})
            if not occ:
                continue
            present_keys = [k for k in ["probabilistic", "empirical", "deterministic"] if k in occ]
            if expected_key not in present_keys:
                result.warning(
                    "semantic", f"hazard.event_sets[{i}].events[{j}].occurrence",
                    f"Event_set analysis_type is '{analysis_type}' but occurrence uses "
                    f"{present_keys or 'none'}. Expected '{expected_key}'.",
                )

    # --- RULE 6: Entity needs name + (email or url) ---
    _check_entity(data, "publisher", result, required=True)
    _check_entity(data, "contact_point", result, required=True)
    _check_entity(data, "creator", result, required=True)
    for i, attr in enumerate(data.get("attributions", [])):
        _check_entity(attr, f"attributions[{i}].entity", result, field="entity")

    # --- RULE 7: risk_data_type -> section activation ---
    rdt = set(data.get("risk_data_type", []))
    section_map = {
        "hazard": "hazard",
        "exposure": "exposure",
        "vulnerability": "vulnerability",
        "loss": "loss",
    }
    for rdt_val, section_key in section_map.items():
        has_section = section_key in data and data[section_key]
        if rdt_val in rdt and not has_section:
            result.warning(
                "semantic", section_key,
                f"risk_data_type includes '{rdt_val}' but '{section_key}' section is missing or empty.",
            )
        if rdt_val not in rdt and has_section:
            result.warning(
                "semantic", section_key,
                f"'{section_key}' section is present but '{rdt_val}' is not in risk_data_type.",
            )

    # --- RULE 8: climate.scenario -> baseline_period ---
    for i, res in enumerate(data.get("resources", [])):
        climate = res.get("climate", {})
        scenario = climate.get("scenario")
        baseline = res.get("baseline_period")
        if scenario and not baseline:
            result.warning(
                "semantic", f"resources[{i}]",
                f"Resource has climate.scenario='{scenario}' but no baseline_period. "
                f"Projected data should reference a baseline period.",
            )


def _check_scale_countries(spatial: dict, path_prefix: str, result: ValidationResult):
    """RULE 4: Validate scale -> countries requirement.

    Receives the spatial sub-object directly (not the parent dict).
    path_prefix is used in error messages (e.g. 'spatial' or 'resources[0].spatial').
    """
    scale = spatial.get("scale")
    countries = spatial.get("countries", [])

    if not scale:
        return

    if scale == "global":
        pass  # countries not required
    elif scale == "regional":
        if len(countries) < 2:
            result.error(
                "semantic", f"{path_prefix}.countries",
                f"Scale is 'regional' but countries has {len(countries)} items (minimum 2 required).",
            )
    else:  # national, sub-national, urban
        if len(countries) < 1:
            result.error(
                "semantic", f"{path_prefix}.countries",
                f"Scale is '{scale}' but countries is empty (at least 1 required).",
            )


def _check_entity(obj: dict, path: str, result: ValidationResult, required: bool = False, field: str = None):
    """RULE 6: Entity needs name + (email or url)."""
    entity = obj.get(field, obj) if field else obj.get(path.split(".")[-1], {})
    if not entity:
        if required:
            result.error("semantic", path, f"Required entity '{path}' is missing.")
        return
    if not isinstance(entity, dict):
        return
    name = entity.get("name")
    email = entity.get("email")
    url = entity.get("url")
    if not name:
        result.error("semantic", f"{path}.name", "Entity name is required.")
    if not email and not url:
        result.error("semantic", path, "Entity must have at least one of 'email' or 'url'.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def validate(data: dict, schema: dict, registry: CodelistRegistry) -> ValidationResult:
    """Run all three validation layers."""
    result = ValidationResult()

    print("Layer 1: JSON Schema validation...")
    validate_layer1_schema(data, schema, result)
    l1_errors = len(result.errors)
    print(f"  {l1_errors} errors found")

    print("Layer 2: Codelist validation...")
    validate_layer2_codelists(data, registry, result)
    l2_errors = len(result.errors) - l1_errors
    print(f"  {l2_errors} errors, {len(result.warnings)} warnings found")

    print("Layer 3: Semantic / cross-field validation...")
    l2_total = len(result.errors)
    l2_warnings = len(result.warnings)
    validate_layer3_semantic(data, registry, result)
    l3_errors = len(result.errors) - l2_total
    l3_warnings = len(result.warnings) - l2_warnings
    print(f"  {l3_errors} errors, {l3_warnings} warnings found")

    return result


def main():
    parser = argparse.ArgumentParser(description="RDLS v1.0 Metadata Validator")
    parser.add_argument("metadata", help="Path to RDLS JSON metadata file")
    parser.add_argument("--schema", default=None, help="Path to RDLS v1.0 JSON schema file")
    parser.add_argument("--codelists", default=None, help="Path to codelists directory (with closed/ and open/ subdirs)")
    args = parser.parse_args()

    script_dir = Path(__file__).parent
    repo_root = script_dir.parent

    # Resolve schema path
    # Priority: --schema arg → rdl-standard sibling repo → local snapshot
    schema_dir = repo_root / "schema"
    if args.schema:
        schema_path = Path(args.schema)
    else:
        rdl_standard_schema = repo_root.parent / "rdl-standard" / "schema" / "rdls_schema.json"
        if rdl_standard_schema.exists():
            schema_path = rdl_standard_schema
        else:
            schema_path = schema_dir / "rdls_schema_v1.0.json"
    if not schema_path.exists():
        print(f"ERROR: Schema file not found: {schema_path}")
        sys.exit(1)

    # Resolve codelists path
    if args.codelists:
        codelists_dir = Path(args.codelists)
    else:
        # Try local rdl-standard clone first (sibling of repo root)
        rdl_standard = repo_root.parent / "rdl-standard" / "schema" / "codelists"
        if rdl_standard.exists():
            codelists_dir = rdl_standard
        else:
            # Fallback to schema/codelists in this repo
            codelists_dir = schema_dir / "codelists"

    if not codelists_dir.exists():
        print(f"WARNING: Codelists directory not found: {codelists_dir}")
        print("  Layer 2 and parts of Layer 3 will be skipped.")
        print(f"  Clone rdl-standard repo or specify --codelists path")

    # Load files
    metadata_path = Path(args.metadata)
    if not metadata_path.exists():
        print(f"ERROR: Metadata file not found: {metadata_path}")
        sys.exit(1)

    print(f"Validating: {metadata_path.name}")
    print(f"Schema:     {schema_path.name}")
    print(f"Codelists:  {codelists_dir}")
    print()

    with open(schema_path, encoding="utf-8") as f:
        schema = json.load(f)

    with open(metadata_path, encoding="utf-8") as f:
        raw = json.load(f)

    # Handle both wrapped {"datasets": [...]} and unwrapped formats
    if "datasets" in raw and isinstance(raw["datasets"], list):
        datasets = raw["datasets"]
        print(f"Found {len(datasets)} dataset(s) in wrapper\n")
    else:
        datasets = [raw]
        print("Single dataset (unwrapped)\n")

    registry = CodelistRegistry(codelists_dir)

    all_valid = True
    for i, dataset in enumerate(datasets):
        if len(datasets) > 1:
            print(f"\n{'#'*70}")
            print(f"Dataset {i+1}: {dataset.get('id', '(no id)')}")
            print(f"{'#'*70}")

        result = validate(dataset, schema, registry)
        print(result.summary())

        if not result.is_valid:
            all_valid = False

    sys.exit(0 if all_valid else 1)


if __name__ == "__main__":
    main()
