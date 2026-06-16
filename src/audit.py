"""
RDLS v1.0 three-layer post-generation audit validator.

Checks schema-valid records against all layers of correctness:
  Layer 1: JSON Schema validation (structure, types, required fields)
            Uses Draft7Validator + FormatChecker to match rdl-standard schema draft.
  Layer 2: Codelist validation (values against CSV codelists)
            Closed codelists: error if value absent.
            Open codelists: silent if value matches _VALID_CODE_PATTERN (well-formed custom code);
            warning only for malformed values (spaces, leading digit, unexpected chars).
            Covers: risk_data_type, spatial.scale, spatial.countries, attributions.role,
            lineage.sources.type, resources.*.climate.scenario, resources.*.spatial.*,
            hazard event_set fields, exposure category+dimension+asset_type.scheme,
            vulnerability function fields (all 4 types: approach, relationship,
            hazard_analysis_type, category, impact.type, impact.modelling, impact.metric,
            taxonomy, damage_scale_name), loss asset_category, asset_dimension,
            impact_and_losses fields.
  Layer 3: Semantic / cross-field validation
            Rule 1: hazard.type -> valid process values
            Rule 2: hazard.type -> valid intensity_measure (open; warn only if not identifier:unit format)
            Rule 3: measurement.quantity_kind -> valid unit
            Rule 4: spatial.scale -> countries requirement (national/sub-national/urban: >= 1)
            Rule 5: event_set.analysis_type -> event.occurrence key matches
            Rule 6: publisher/creator/contact_point/attribution entity must have name + email/url
            Rule 7: risk_data_type -> corresponding section must be present
            Rule 8: resource.climate.scenario -> resource.baseline_period should be present

Used for post-generation auditing (scripts/validate_records.py).
For pipeline-time QA during record generation, see src/validate.py.

Public API:
    from src.audit import validate, CodelistRegistry, ValidationResult
"""

import csv
import re
from pathlib import Path
from typing import Any

# ---------------------------------------------------------------------------
# Format patterns for open codelist validation
# ---------------------------------------------------------------------------

# A well-formed codelist code: starts with a letter, contains only alphanumeric,
# underscores, colons, hyphens, dots, parentheses, slashes. No spaces. Not purely numeric.
# Matches: "FAPAR:-", "rfh:mm", "wd:m", "PGA:g", "my_custom_value", "RRI"
# Rejects: "buildings damaged" (space), "123" (purely numeric), "" (empty)
_VALID_CODE_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_:()\-./]*$")

# A well-formed IMT value must follow identifier:unit format (colon separator required).
# Matches: "FAPAR:-", "rfh:mm", "wd:m", "PGA:g", "MMI:-", "Sd(T):cm"
# Rejects: "fluvial_flood" (no colon), "buildings damaged" (space), "bad value"
_VALID_IMT_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9()._\-]*:[A-Za-z0-9/_().\-]+$")

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

    Uses Draft202012Validator to match the RDLS schema, which declares
    "$schema": ".../draft/2020-12/schema" and uses 2020-12-only keywords such
    as `prefixItems` (on `links`). A Draft7 validator silently ignores those
    keywords, under-reporting errors - so we pin 2020-12 and only fall back to
    Draft7 if the library is too old. FormatChecker enables 'date'/'iri' checks.

    NOTE: pass the BAKED schema (schema/rdls_schema_v1.0.json via
    scripts/refresh_published_schema.py). The rdl-standard SOURCE schema has
    empty hazard conditionals and will under-report hazard type/process errors.
    """
    try:
        from jsonschema import Draft202012Validator as _Validator, FormatChecker
    except ImportError:
        try:
            from jsonschema import Draft7Validator as _Validator, FormatChecker
            result.warning("schema", "(root)", "jsonschema too old for Draft 2020-12; using Draft7 (prefixItems etc. not checked). Upgrade jsonschema.")
        except ImportError:
            result.warning("schema", "(root)", "jsonschema library not installed — skipping Layer 1. Install with: pip install jsonschema")
            return

    validator = _Validator(schema, format_checker=FormatChecker())
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
    ("vulnerability.functions.vulnerability.*.impact.loss_statistic", "loss_statistic.csv", "loss statistic"),
    ("vulnerability.functions.vulnerability.*.taxonomy", "classification_scheme.csv", "taxonomy"),
    ("vulnerability.functions.fragility.*.approach", "function_approach.csv", "function approach"),
    ("vulnerability.functions.fragility.*.relationship", "relationship_type.csv", "relationship type"),
    ("vulnerability.functions.fragility.*.hazard_analysis_type", "analysis_type.csv", "analysis type"),
    ("vulnerability.functions.fragility.*.category", "exposure_category.csv", "exposure category"),
    ("vulnerability.functions.fragility.*.impact.type", "impact_type.csv", "impact type"),
    ("vulnerability.functions.fragility.*.impact.modelling", "data_calculation_type.csv", "impact modelling"),
    ("vulnerability.functions.fragility.*.impact.metric", "impact_metric.csv", "impact metric"),
    ("vulnerability.functions.fragility.*.impact.loss_statistic", "loss_statistic.csv", "loss statistic"),
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
    ("vulnerability.functions.damage_to_loss.*.impact.loss_statistic", "loss_statistic.csv", "loss statistic"),
    ("vulnerability.functions.damage_to_loss.*.taxonomy", "classification_scheme.csv", "taxonomy"),
    ("vulnerability.functions.damage_to_loss.*.damage_scale_name", "damage_scale_name.csv", "damage scale name"),
    ("vulnerability.functions.engineering_demand.*.approach", "function_approach.csv", "function approach"),
    ("vulnerability.functions.engineering_demand.*.relationship", "relationship_type.csv", "relationship type"),
    ("vulnerability.functions.engineering_demand.*.hazard_analysis_type", "analysis_type.csv", "analysis type"),
    ("vulnerability.functions.engineering_demand.*.category", "exposure_category.csv", "exposure category"),
    ("vulnerability.functions.engineering_demand.*.impact.type", "impact_type.csv", "impact type"),
    ("vulnerability.functions.engineering_demand.*.impact.modelling", "data_calculation_type.csv", "impact modelling"),
    ("vulnerability.functions.engineering_demand.*.impact.metric", "impact_metric.csv", "impact metric"),
    ("vulnerability.functions.engineering_demand.*.impact.loss_statistic", "loss_statistic.csv", "loss statistic"),
    ("vulnerability.functions.engineering_demand.*.taxonomy", "classification_scheme.csv", "taxonomy"),
    ("vulnerability.functions.engineering_demand.*.damage_scale_name", "damage_scale_name.csv", "damage scale name"),

    # Loss -- impact details are NESTED under impact_and_losses.impact (v1.0);
    # loss_type/loss_approach/loss_frequency_type stay on impact_and_losses.
    ("loss.losses.*.asset_category", "exposure_category.csv", "asset category"),
    ("loss.losses.*.asset_dimension", "metric_dimension.csv", "asset dimension"),
    ("loss.losses.*.impact_and_losses.impact.type", "impact_type.csv", "impact type"),
    ("loss.losses.*.impact_and_losses.impact.modelling", "data_calculation_type.csv", "impact modelling"),
    ("loss.losses.*.impact_and_losses.impact.metric", "impact_metric.csv", "impact metric"),
    ("loss.losses.*.impact_and_losses.impact.loss_statistic", "loss_statistic.csv", "loss statistic"),
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
                    # For open codelists: only warn if the value looks malformed.
                    # Well-formed custom codes (e.g. "FAPAR:-", "rfh:mm", "MY_METRIC")
                    # are silently accepted — they are intentional extensions.
                    if not _VALID_CODE_PATTERN.match(value):
                        result.warning(
                            "codelist", path,
                            f"Value '{value}' not in open codelist {codelist_name} for {description} "
                            f"and has an unexpected format (spaces, leading digit, or special chars). "
                            f"Verify it's intentional.",
                            allowed=codes,
                        )
                else:
                    result.error(
                        "codelist", path,
                        f"Value '{value}' not in closed codelist {codelist_name} for {description}.",
                        allowed=codes,
                    )

    # GED4ALL asset_type.id check: the JSON Schema does NOT link asset_type.id to
    # taxonomy_ged4all (it is a free Classification.id + scheme from classification_scheme),
    # so a schema-walk misses it. When scheme=='GED4ALL', the id should be a
    # taxonomy_ged4all code. Warn (not error): some assets have no single code
    # (multimodal_transport_network, points_of_interest), and 'bui' is pending.
    ged_codes, _ = registry.load("taxonomy_ged4all.csv")
    if ged_codes:
        ged_ok = set(ged_codes) | {"bui"}
        for i, exp in enumerate(data.get("exposure", []) if isinstance(data.get("exposure"), list) else []):
            at = exp.get("asset_type") if isinstance(exp, dict) else None
            if isinstance(at, dict) and at.get("scheme") == "GED4ALL":
                aid = at.get("id")
                if aid and aid not in ged_ok:
                    result.warning(
                        "codelist", f"exposure[{i}].asset_type.id",
                        f"asset_type.id '{aid}' is not a taxonomy_ged4all code but scheme='GED4ALL'. "
                        f"Map to the GED4ALL code for the exposure_category (or drop the scheme if "
                        f"no code fits, e.g. multimodal networks / mixed points-of-interest).",
                        allowed=sorted(ged_ok),
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
# Obsolete / incorrect quantity_kind values that DO have a correct v1.0 form.
# quantity_kind.csv is an OPEN codelist and the Metadata Editor accepts novel
# relevant custom values (e.g. `power` for installed generation capacity), so a
# value merely being outside the codelist is NOT an error. These specific values
# ARE errors: they are stale v0.3 terms (or a unit mistaken for a quantity_kind)
# that have a canonical v1.0 codelist code. A record carrying them shows a
# blank/unrecognised measurement because the editor's dropdown cannot match them.
QUANTITY_KIND_OBSOLETE = {
    "monetary": "currency",                              # v0.3 -> v1.0 rename
    "weight": "mass",                                    # v0.3 -> v1.0 rename
    "percent": "dimensionless_ratio (with unit 'percent')",  # percent is a unit, not a quantity_kind
}

# Obsolete impact_metric values (old codelist vocabulary) -> current v1.0
# impact_metric codelist code. impact_metric is an OPEN codelist, but these are
# stale terms from a previous codelist version (not intentional extensions), so
# they error like QUANTITY_KIND_OBSOLETE. casualty_count and damage_ratio are
# deliberately ABSENT - they have no unambiguous v1.0 target (pending decision).
IMPACT_METRIC_OBSOLETE = {
    "exposure_to_hazard": "exposure",
    "displaced_count": "displaced",
    "asset_loss": "loss",
    "economic_loss_value": "loss",
    "economic_loss": "loss",
    "loss_annual_average_value": "loss",
    "loss_probable_maximum_value": "loss",
    "fatality_count": "death",
    "injured_count": "ppl_injured",
    "affected_population": "ppl_affected",
    "downtime_loss": "downtime",
    "disruption_days": "disruption",
    "damage_ratio": "damage",  # ratio nature carried by measurement.quantity_kind=dimensionless_ratio
}
# casualty_count is deliberately ABSENT: kept as a valid custom value (no single
# v1.0 code means killed+injured); the open-codelist branch passes it silently.

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

    # Loss -- v1.0 nests measurement under impact_and_losses.impact.measurement;
    # legacy v0.3 records put it flat at impact_and_losses.measurement.
    for i, loss in enumerate(data.get("loss", {}).get("losses", [])):
        ial = loss.get("impact_and_losses", {})
        if not isinstance(ial, dict):
            continue
        nested = ial.get("impact", {}).get("measurement", {}) if isinstance(ial.get("impact"), dict) else {}
        if nested:
            measurements.append((f"loss.losses[{i}].impact_and_losses.impact.measurement", nested))
        flat = ial.get("measurement", {})
        if flat:
            measurements.append((f"loss.losses[{i}].impact_and_losses.measurement", flat))

    return measurements


def _collect_impact_metrics(data: dict) -> list[tuple[str, str]]:
    """Find all impact_metric values (loss + vulnerability function), covering
    both the v1.0 nested path (impact.metric) and the v0.3 flat path."""
    out = []
    for i, loss in enumerate(data.get("loss", {}).get("losses", [])):
        if not isinstance(loss, dict):
            continue
        ial = loss.get("impact_and_losses", {})
        if not isinstance(ial, dict):
            continue
        if isinstance(ial.get("impact"), dict) and ial["impact"].get("metric"):
            out.append((f"loss.losses[{i}].impact_and_losses.impact.metric", ial["impact"]["metric"]))
        if ial.get("impact_metric"):  # legacy flat
            out.append((f"loss.losses[{i}].impact_and_losses.impact_metric", ial["impact_metric"]))
    for func_type in ["vulnerability", "fragility", "damage_to_loss", "engineering_demand"]:
        funcs = data.get("vulnerability", {}).get("functions", {}).get(func_type, [])
        for i, fn in enumerate(funcs):
            if isinstance(fn, dict) and isinstance(fn.get("impact"), dict) and fn["impact"].get("metric"):
                out.append((f"vulnerability.functions.{func_type}[{i}].impact.metric", fn["impact"]["metric"]))
    return out


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
    # Checks both per-type imt_*.csv AND master IMT.csv (with Hazard column filter).
    # IMT codelists are always open. A value not in the codelist is only warned about
    # if it also fails the IMT format check (identifier:unit, e.g. "FAPAR:-", "rfh:mm").
    # Well-formed custom IMTs are silently accepted as intentional extensions.
    for path, h in _collect_hazard_objects(data):
        htype = h.get("type")
        imt = h.get("intensity_measure")
        if htype and imt:
            combined_codes = registry.get_imt_codes_for_type(htype)
            if combined_codes and imt not in combined_codes:
                if not _VALID_IMT_PATTERN.match(imt):
                    # Malformed IMT (no colon, spaces, etc.) — likely a classification error
                    codelist_name = TYPE_TO_IMT_CODELIST.get(htype, "IMT.csv")
                    result.warning(
                        "semantic", f"{path}.intensity_measure",
                        f"Intensity measure '{imt}' not in {codelist_name} or IMT.csv for type '{htype}' "
                        f"and does not follow identifier:unit format. "
                        f"Open codelist — verify it's intentional.",
                        allowed=combined_codes,
                    )

    # --- RULE 3: quantity_kind -> unit ---
    # quantity_kind.csv is an OPEN codelist. The Metadata Editor drives a
    # dropdown from it but ALSO accepts custom values, so a novel relevant value
    # (e.g. `power` for installed generation capacity, megawatt) is a legitimate
    # open-codelist extension - NOT an error. We only hard-error on the specific
    # obsolete v0.3 terms / unit-as-quantity_kind mistakes that have a canonical
    # v1.0 code (QUANTITY_KIND_OBSOLETE): those do not round-trip in the editor's
    # dropdown and must be the codelist code. Well-formed novel values are
    # accepted; only a malformed value (not snake_case) is warned about.
    _qk_codes, _ = registry.load("quantity_kind.csv")
    for path, m in _collect_measurement_objects(data):
        qk = m.get("quantity_kind")
        unit = m.get("unit")
        if qk and qk in QUANTITY_KIND_OBSOLETE:
            result.error(
                "semantic", f"{path}.quantity_kind",
                f"quantity_kind '{qk}' is an obsolete v0.3 value; use "
                f"'{QUANTITY_KIND_OBSOLETE[qk]}'. (quantity_kind is an open codelist - "
                f"novel relevant values like 'power' are allowed, but stale renames are not "
                f"recognised by the Metadata Editor's dropdown.)",
                allowed=_qk_codes,
            )
        elif qk and _qk_codes and qk not in _qk_codes:
            # Open codelist: a well-formed novel value is an intentional, MDE-accepted
            # custom extension. Only flag a value that is not even well-formed.
            if not _VALID_CODE_PATTERN.match(qk):
                result.warning(
                    "semantic", f"{path}.quantity_kind",
                    f"quantity_kind '{qk}' is not in the codelist and is not well-formed "
                    f"snake_case; verify it's an intentional custom value.",
                    allowed=_qk_codes,
                )
        if qk and unit:
            mapping = QUANTITY_TO_UNIT_CODELIST.get(qk)
            if mapping:
                codelist_name, is_open = mapping
                codes, _ = registry.load(codelist_name)
                if codes and unit not in codes:
                    if is_open:
                        # Only warn if the unit value looks malformed
                        if not _VALID_CODE_PATTERN.match(unit):
                            result.warning(
                                "semantic", f"{path}.unit",
                                f"Unit '{unit}' not in {codelist_name} for quantity_kind '{qk}' "
                                f"and has an unexpected format. "
                                f"Open codelist — verify it's intentional.",
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

    # --- RULE 9: loss impact must be NESTED (v1.0), not flat (v0.3) ---
    # v1.0 nests impact under impact_and_losses.impact.{type,modelling,metric,
    # measurement} ($defs/Impact). The flat keys impact_type/impact_modelling/
    # impact_metric on impact_and_losses are the v0.3 structure; the schema
    # tolerates them as extras (impact is optional) so Layer 1 misses them.
    _FLAT_IMPACT_KEYS = ("impact_type", "impact_modelling", "impact_metric")
    for i, loss in enumerate(data.get("loss", {}).get("losses", [])):
        if not isinstance(loss, dict):
            continue
        ial = loss.get("impact_and_losses")
        if isinstance(ial, dict):
            flat = [k for k in _FLAT_IMPACT_KEYS if k in ial]
            if flat and "impact" not in ial:
                result.error(
                    "semantic", f"loss.losses[{i}].impact_and_losses",
                    f"Loss uses the v0.3 FLAT impact structure ({', '.join(flat)}); RDLS v1.0 "
                    f"nests these under an 'impact' object: "
                    f"impact_and_losses.impact.{{type, modelling, metric, measurement}}.",
                )

    # --- RULE 10: impact_metric -> current codelist (obsolete-value check) ---
    # impact_metric is an OPEN codelist, but the old codelist vocabulary
    # (economic_loss_value, displaced_count, ...) was fully replaced. Stale
    # values error like QUANTITY_KIND_OBSOLETE; novel well-formed values pass.
    _im_codes, _ = registry.load("impact_metric.csv")
    for path, metric in _collect_impact_metrics(data):
        if not metric:
            continue
        if metric in IMPACT_METRIC_OBSOLETE:
            result.error(
                "semantic", path,
                f"impact_metric '{metric}' is from a superseded codelist; use "
                f"'{IMPACT_METRIC_OBSOLETE[metric]}'. (impact_metric is open - novel relevant "
                f"values are allowed, but stale codelist terms are not recognised.)",
                allowed=_im_codes,
            )
        elif _im_codes and metric not in _im_codes:
            if not _VALID_CODE_PATTERN.match(metric):
                result.warning(
                    "semantic", path,
                    f"impact_metric '{metric}' is not in the codelist and is not well-formed; "
                    f"verify it's an intentional custom value.",
                    allowed=_im_codes,
                )

    # --- RULE 11: event_set hazards[] processes vs events[] process coverage ---
    # The schema requires a single `process` per Event.hazard, so a set can declare
    # multiple processes at hazards[] level while each event carries one. That is
    # CORRECT when the source ships ONE combined map per scenario (e.g. a single
    # "fluvial/pluvial" depth raster). It is WRONG when the source ships SEPARATE
    # per-process maps - then events should mirror each declared process.
    # We cannot tell combined vs separate from the data alone, so this is a WARNING
    # asking the author to verify, not a hard error.
    for i, es in enumerate(data.get("hazard", {}).get("event_sets", [])):
        if not isinstance(es, dict):
            continue
        set_procs = {h.get("process") for h in es.get("hazards", []) or []
                     if isinstance(h, dict) and h.get("process")}
        ev_procs = {ev.get("hazard", {}).get("process") for ev in es.get("events", []) or []
                    if isinstance(ev.get("hazard"), dict) and ev["hazard"].get("process")}
        if len(set_procs) > 1 and ev_procs and ev_procs < set_procs:
            missing = sorted(set_procs - ev_procs)
            result.warning(
                "semantic", f"hazard.event_sets[{i}]",
                f"event_set declares processes {sorted(set_procs)} but its events only use "
                f"{sorted(ev_procs)} (not covered: {missing}). VERIFY: if the source ships "
                f"SEPARATE per-process hazard maps, add events for the missing process(es); if "
                f"the maps are COMBINED (one raster per scenario), this is correct - processes "
                f"are declared at set level only.",
            )
        extra = ev_procs - set_procs
        if set_procs and extra:
            result.warning(
                "semantic", f"hazard.event_sets[{i}]",
                f"events use process(es) {sorted(extra)} not declared in the event_set hazards[] "
                f"{sorted(set_procs)}; add them to hazards[] so the set-level scope is complete.",
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

def validate_layer4_consistency(data: dict, result: ValidationResult):
    """Layer 4: cross-field CONSISTENCY checks the JSON Schema cannot catch.

    The schema only checks shape (type, codelist membership, required). It
    cannot detect a value that is well-formed but factually WRONG relative to
    what it describes - e.g. media_type="text/csv" on a ".zip" download. This
    layer re-derives ground truth from the data itself and flags contradictions.

    Rule C1 - resource media_type vs URL (conservative, never guesses):
      Derive the media_type the URL DEFINITIVELY implies (file extension or
      explicit OGC format param), then flag only PROVABLE contradictions:
        * URL is a .zip container, but the declared type is a single-file
          content type a zip cannot be (csv, tiff, png, json, geojson, pdf,
          netcdf, xml, parquet). Multi-file geospatial formats (shapefile,
          file-geodatabase, geopackage, zarr) are NOT flagged on a zip - they
          are legitimately distributed zipped.
        * URL is a specific non-zip data file (.tif/.csv/.json/...) but the
          declared type is incompatible with that exact type.
      No assertion is made when the URL gives no definitive signal (landing
      pages, extensionless endpoints) or resolves to text/html (viewer/app
      pages are not reliable evidence of a resource's data format).
    """
    from src.utils import (media_type_from_url, media_types_compatible,
                           format_label_to_media_type, normalize_media_type,
                           ZIP_INCOMPATIBLE_SINGLE_FILE_TYPES)

    for i, r in enumerate(data.get("resources", []) or []):
        if not isinstance(r, dict):
            continue
        # The artifact you actually fetch determines the media_type:
        # download_url first, else access_url.
        expected = None
        src_key = None
        for key in ("download_url", "access_url"):
            expected = media_type_from_url(r.get(key))
            if expected:
                src_key = key
                break
        if not expected or expected == "text/html":
            continue  # not definitive for a data format -> do not assert

        declared_mt = r.get("media_type")
        declared_fmt = r.get("format")
        # Resolve a free-text format to a media_type for comparison (may be None).
        effective = declared_mt or format_label_to_media_type(declared_fmt)
        if effective is None:
            # No media_type, and format (if any) is free text we cannot map.
            # Only the zip-vs-single-file rule could apply, and we can't tell -
            # so do not assert (schema anyOf already requires media_type|format).
            continue
        path = f"resources -> {i}"
        rid = r.get("id", f"[{i}]")
        declared_label = declared_mt or f"format '{declared_fmt}'"

        if expected == "application/zip":
            # A zip only contradicts a single-file content type.
            if normalize_media_type(effective) in ZIP_INCOMPATIBLE_SINGLE_FILE_TYPES:
                result.error(
                    "consistency", f"{path}",
                    f"resource '{rid}': declared {declared_label} but the {src_key} "
                    f"is a .zip archive - a single-file type cannot describe a zip. "
                    f"Use media_type 'application/zip' (note the contents in the description).")
        else:
            # Specific non-zip data file: declared must match that exact type.
            if not media_types_compatible(effective, expected):
                result.error(
                    "consistency", f"{path}",
                    f"resource '{rid}': declared {declared_label} contradicts the "
                    f"{src_key} which is definitively '{expected}'.")


def _nested_get(obj, dotted):
    """Walk a dotted path (e.g. 'impact.measurement.quantity_kind'); None if absent."""
    cur = obj
    for part in dotted.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur


def _is_empty(v):
    return v is None or (isinstance(v, str) and v.strip() == "")


# Required-field rules enforced by the RDLS Metadata Editor (RDL_MDE.html) that
# go BEYOND the JSON Schema. Mirrors GFDRR/CCDR-tools _static/RDL_MDE.html
# (validateDatasetRequirements). The JSON Schema treats these as optional, so
# Layer 1 cannot catch them - but the editor / DDH ingestion rejects records
# that omit them. Keep this list in sync with the MDE if the team updates it.
# NOTE: v1.0 nests impact under impact_and_losses.impact.{type,modelling,metric,
# measurement} ($defs/Impact). The flat v0.3 paths (impact_type, impact_metric,
# ...) are WRONG; a separate Layer 3 rule errors on the legacy flat structure.
_MDE_LOSS_REQUIRED = [
    ("hazard.type", "hazard type"),
    ("asset_category", "asset category"),
    ("asset_dimension", "asset dimension"),
    ("impact_and_losses.impact.type", "impact type"),
    ("impact_and_losses.impact.modelling", "impact modelling"),
    ("impact_and_losses.impact.metric", "impact metric"),
    ("impact_and_losses.impact.measurement.quantity_kind", "quantity kind"),
    ("impact_and_losses.loss_type", "loss type"),
    ("impact_and_losses.loss_approach", "loss approach"),
    ("impact_and_losses.loss_frequency_type", "loss frequency type"),
]
_MDE_VULN_FUNC_REQUIRED = [
    ("hazard_primary", "primary hazard type"),
    ("hazard_primary.intensity_measure", "hazard intensity measurement"),
    ("category", "exposure category"),
    ("impact.type", "impact type"),
    ("impact.modelling", "impact modelling"),
    ("impact.metric", "impact metric"),
    ("impact.measurement.quantity_kind", "quantity kind"),
]
_MDE_SOCIOECON_REQUIRED = [
    ("indicator_name", "indicator name"),
    ("indicator_code", "indicator code"),
    ("description", "description"),
    ("reference_year", "reference year"),
]


def validate_layer5_mde_rules(data: dict, result: ValidationResult):
    """Layer 5: business rules enforced by the RDLS Metadata Editor / DDH
    ingestion that the JSON Schema does NOT (it marks these fields optional).

    Faithfully mirrors RDL_MDE.html's validateDatasetRequirements so a record
    that passes here will pass the editor. Covers exposure metrics, loss
    impact_and_losses sub-fields, vulnerability functions' impact.* and
    hazard_primary, and socio-economic indicators. Reported as ERRORS.
    """
    rdt = [t for t in (data.get("risk_data_type") or []) if t]

    # --- Exposure ---
    if any("exposure" in t.lower() for t in rdt) and isinstance(data.get("exposure"), list):
        for i, exp in enumerate(data["exposure"]):
            if not isinstance(exp, dict):
                continue
            if _is_empty(exp.get("category")):
                result.error("mde", f"exposure -> {i}", f"exposure {i+1} is missing required category")
            metrics = exp.get("metrics") or []
            if not metrics:
                result.error("mde", f"exposure -> {i}", f"exposure {i+1} must have at least one metric")
            for j, m in enumerate(metrics):
                if not isinstance(m, dict):
                    continue
                if _is_empty(m.get("dimension")):
                    result.error("mde", f"exposure -> {i} -> metrics -> {j}", f"exposure {i+1}, metric {j+1} is missing required dimension")
                if not isinstance(m.get("measurement"), dict) or not m.get("measurement"):
                    result.error("mde", f"exposure -> {i} -> metrics -> {j}", f"exposure {i+1}, metric {j+1} is missing required measurement")

    # --- Loss ---
    if any("loss" in t.lower() for t in rdt):
        losses = _nested_get(data, "loss.losses") or []
        for i, loss in enumerate(losses):
            if not isinstance(loss, dict):
                continue
            for field, name in _MDE_LOSS_REQUIRED:
                if _is_empty(_nested_get(loss, field)):
                    result.error("mde", f"loss -> losses -> {i}", f"loss {i+1} is missing required {name}")

    # --- Vulnerability ---
    if any("vulnerability" in t.lower() for t in rdt):
        vuln = data.get("vulnerability") or {}
        funcs = vuln.get("functions") or {}
        socio = vuln.get("socio_economic") or []
        has_funcs = any(isinstance(funcs.get(k), list) and funcs.get(k) for k in funcs)
        has_socio = bool(socio)
        if not has_funcs and not has_socio:
            result.error("mde", "vulnerability",
                "vulnerability must specify at least one approach: functions (vulnerability/fragility/"
                "damage_to_loss/engineering_demand) or socio_economic indicators")
        # function-based: every function needs the MDE-required fields
        for func_type, arr in funcs.items():
            if isinstance(arr, list):
                for i, func in enumerate(arr):
                    if not isinstance(func, dict):
                        continue
                    for field, name in _MDE_VULN_FUNC_REQUIRED:
                        if _is_empty(_nested_get(func, field)):
                            result.error("mde", f"vulnerability -> functions -> {func_type} -> {i}",
                                f"{func_type} function {i+1} is missing required {name}")
        # socio-economic indicators
        for i, ind in enumerate(socio):
            if not isinstance(ind, dict):
                continue
            for field, name in _MDE_SOCIOECON_REQUIRED:
                if _is_empty(_nested_get(ind, field)):
                    result.error("mde", f"vulnerability -> socio_economic -> {i}",
                        f"socio-economic indicator {i+1} is missing required {name}")

    # --- Hazard `id` recommendation (WARNING) ---
    # `id` is REQUIRED only on event_set.hazards[] (RDLS schema; caught by Layer 1).
    # On loss.hazard / event.hazard / vuln hazard_primary|secondary it is OPTIONAL:
    # the schema doesn't require it there, and rdl-jkan make_hazard now reads it via
    # hazard.get("id") (PR #159) so a missing id no longer breaks ingestion. We keep a
    # WARNING (not error) because a stable local id is good practice for references.
    # event_set.hazards[] id is left to Layer 1 (schema-required) to avoid double-flagging.
    for path, h in _collect_hazard_objects(data):
        if (isinstance(h, dict) and h.get("type") and not h.get("id")
                and ".hazards[" not in path):  # event_set.hazards[] handled by schema/Layer 1
            result.warning("mde", f"{path}.id",
                "hazard object has no 'id'. Optional here (schema requires it only on "
                "event_set.hazards[]; rdl-jkan reads it via .get since PR #159), but a stable "
                "local id is recommended for references.")


def validate(data: dict, schema: dict, registry: CodelistRegistry) -> ValidationResult:
    """Run all validation layers."""
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

    print("Layer 4: Resource media_type / URL consistency...")
    l3_total = len(result.errors)
    validate_layer4_consistency(data, result)
    l4_errors = len(result.errors) - l3_total
    print(f"  {l4_errors} errors found")

    print("Layer 5: Metadata Editor (MDE) business rules...")
    l4_total = len(result.errors)
    validate_layer5_mde_rules(data, result)
    l5_errors = len(result.errors) - l4_total
    print(f"  {l5_errors} errors found")

    return result
