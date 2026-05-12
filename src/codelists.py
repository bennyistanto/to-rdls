"""
RDLS v1.0 codelist utilities - single source of truth.

Loads all codelist CSV files from rdl-standard/schema/codelists/ (closed + open)
and provides normalisation helpers for unit, source_type, and other open fields.

Usage from any script:
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))  # add project root
    from src.codelists import load_codelists_v10, normalise_unit, normalise_source_type

Codelist directory convention:
    rdl-standard/ is a sibling repo to to-rdls/ at the same parent level.
    Default path: <project_root>/../rdl-standard/schema/codelists/
"""

import csv
import re
from pathlib import Path
from typing import Dict, FrozenSet, Optional, Union

# Path to rdl-standard codelists (sibling repo)
_PROJECT_ROOT = Path(__file__).parent.parent
_RDL_CODELISTS_DIR = _PROJECT_ROOT.parent / "rdl-standard" / "schema" / "codelists"

# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_codelists_v10(codelists_dir: Union[str, Path, None] = None) -> Dict[str, FrozenSet[str]]:
    """Load all v1.0 codelist CSV files from closed/ and open/ subdirectories.

    Returns a dict mapping codelist file stem -> frozenset of Code values.
    Returns an empty dict if the directory is not found (graceful fallback).

    Args:
        codelists_dir: Path to the codelists/ directory. Defaults to the
                       sibling rdl-standard repo at the standard location.
    """
    if codelists_dir is None:
        codelists_dir = _RDL_CODELISTS_DIR
    codelists_dir = Path(codelists_dir)
    result: Dict[str, FrozenSet[str]] = {}
    if not codelists_dir.is_dir():
        return result
    for subdir in ("closed", "open"):
        sub = codelists_dir / subdir
        if not sub.is_dir():
            continue
        for csv_path in sorted(sub.glob("*.csv")):
            codes: set = set()
            try:
                with open(csv_path, encoding="utf-8", newline="") as fh:
                    for row in csv.DictReader(fh):
                        code = row.get("Code", "").strip()
                        if code:
                            codes.add(code)
            except (OSError, csv.Error):
                pass
            result[csv_path.stem] = frozenset(codes)
    return result


# ---------------------------------------------------------------------------
# Module-level codelist data (loaded once at import time)
# ---------------------------------------------------------------------------

_CODELISTS: Dict[str, FrozenSet[str]] = load_codelists_v10()

# Unit codes: union of all unit_* codelists except unit_currency (separate closed codelist)
_loaded_units: FrozenSet[str] = frozenset().union(
    *[v for k, v in _CODELISTS.items() if k.startswith("unit_") and k != "unit_currency"]
) if _CODELISTS else frozenset()

# Hardcoded fallback (used if rdl-standard repo not found)
_VALID_UNIT_CODES_BUILTIN: FrozenSet[str] = frozenset({
    "acre", "hectare", "square_kilometre", "square_metre", "square_mile",
    "centimetre", "foot", "hectometre", "kilometre", "metre", "mile", "yard",
    "count", "percent", "kilowatt_hour",
    "kilogram", "metric_ton", "ton",
    "kilogram_per_hectare", "kilogram_per_square_metre",
    "day", "hour", "minute", "month", "second", "week", "year",
    "cubic_metre", "litre", "cubic_foot", "us_gallon", "bushel", "barrel",
})

VALID_UNIT_CODES:          FrozenSet[str] = _loaded_units or _VALID_UNIT_CODES_BUILTIN
VALID_CURRENCY_CODES:      FrozenSet[str] = _CODELISTS.get("unit_currency", frozenset())
VALID_QUANTITY_KINDS:      FrozenSet[str] = _CODELISTS.get("quantity_kind", frozenset())
VALID_SOURCE_TYPES:        FrozenSet[str] = _CODELISTS.get("source_type", frozenset())
VALID_HAZARD_TYPES:        FrozenSet[str] = _CODELISTS.get("hazard_type", frozenset())
VALID_EXPOSURE_CATEGORIES: FrozenSet[str] = _CODELISTS.get("exposure_category", frozenset())
VALID_ANALYSIS_TYPES:      FrozenSet[str] = _CODELISTS.get("analysis_type", frozenset())
VALID_IMPACT_TYPES:        FrozenSet[str] = _CODELISTS.get("impact_type", frozenset())
VALID_RISK_DATA_TYPES:     FrozenSet[str] = _CODELISTS.get("risk_data_type", frozenset())
VALID_MEDIA_TYPES:         FrozenSet[str] = _CODELISTS.get("media_type", frozenset())
VALID_PROCESS_TYPES:       FrozenSet[str] = _CODELISTS.get("process_type", frozenset())
VALID_LOSS_TYPES:          FrozenSet[str] = _CODELISTS.get("loss_type", frozenset())
VALID_METRIC_DIMENSIONS:   FrozenSet[str] = _CODELISTS.get("metric_dimension", frozenset())
VALID_CLIMATE_SCENARIOS:   FrozenSet[str] = _CODELISTS.get("climate_scenario", frozenset())
VALID_SPATIAL_SCALES:      FrozenSet[str] = _CODELISTS.get("spatial_scale", frozenset())

# ---------------------------------------------------------------------------
# Unit normalisation
#
# Maps informal abbreviations / synonyms to exact codelist codes.
# Lookup is case-insensitive. Exact codelist codes pass through unchanged.
# Currency codes (unit_currency.csv) pass through unchanged.
# Unknown values pass through unchanged.
# ---------------------------------------------------------------------------

UNIT_NORMALISE: Dict[str, str] = {
    # area
    "m2":                       "square_metre",
    "m²":                  "square_metre",
    "sq m":                     "square_metre",
    "sqm":                      "square_metre",
    "sq_m":                     "square_metre",
    "square meter":             "square_metre",
    "square meters":            "square_metre",
    "square metres":            "square_metre",
    "ha":                       "hectare",
    "km2":                      "square_kilometre",
    "km²":                 "square_kilometre",
    "sq km":                    "square_kilometre",
    "sq_km":                    "square_kilometre",
    "mi2":                      "square_mile",
    "sq mi":                    "square_mile",
    "ac":                       "acre",
    # length
    "m":                        "metre",
    "meter":                    "metre",
    "meters":                   "metre",
    "metres":                   "metre",
    "km":                       "kilometre",
    "kilometer":                "kilometre",
    "kilometers":               "kilometre",
    "kilometres":               "kilometre",
    "cm":                       "centimetre",
    "centimeter":               "centimetre",
    "centimeters":              "centimetre",
    "centimetres":              "centimetre",
    "ft":                       "foot",
    "feet":                     "foot",
    "yd":                       "yard",
    "yards":                    "yard",
    "mi":                       "mile",
    "miles":                    "mile",
    "hm":                       "hectometre",
    # dimensionless ratio
    "%":                        "percent",
    "pct":                      "percent",
    # energy
    "kwh":                      "kilowatt_hour",
    "kw-h":                     "kilowatt_hour",
    # mass
    "kg":                       "kilogram",
    "t":                        "metric_ton",
    "tonne":                    "metric_ton",
    "tonnes":                   "metric_ton",
    "metric ton":               "metric_ton",
    "metric tons":              "metric_ton",
    # mass per area
    "kg/ha":                    "kilogram_per_hectare",
    "kg/m2":                    "kilogram_per_square_metre",
    "kg/m²":               "kilogram_per_square_metre",
    # volume
    "m3":                       "cubic_metre",
    "m³":                  "cubic_metre",
    "cubic meter":              "cubic_metre",
    "cubic meters":             "cubic_metre",
    "cubic metres":             "cubic_metre",
    "l":                        "litre",
    "liter":                    "litre",
    "liters":                   "litre",
    "litres":                   "litre",
    "ft3":                      "cubic_foot",
    "cubic feet":               "cubic_foot",
    "gal":                      "us_gallon",
    "gallon":                   "us_gallon",
    "gallons":                  "us_gallon",
    # time
    "s":                        "second",
    "sec":                      "second",
    "seconds":                  "second",
    "min":                      "minute",
    "minutes":                  "minute",
    "h":                        "hour",
    "hr":                       "hour",
    "hours":                    "hour",
    "d":                        "day",
    "days":                     "day",
    "w":                        "week",
    "wk":                       "week",
    "weeks":                    "week",
    "mo":                       "month",
    "months":                   "month",
    "yr":                       "year",
    "y":                        "year",
    "years":                    "year",
}


def normalise_unit(unit: str) -> str:
    """Normalise a measurement.unit value to the exact codelist code.

    Lookup order:
      1. Exact match in VALID_UNIT_CODES (unit_*.csv) -> return as-is
      2. Known informal abbreviation (UNIT_NORMALISE) -> mapped code
      3. Valid currency code (unit_currency.csv or 3-letter uppercase fallback)
      4. Unknown -> return as-is

    Always call this before writing any measurement.unit value.
    """
    if not unit or not unit.strip():
        return unit
    if unit in VALID_UNIT_CODES:
        return unit
    mapped = UNIT_NORMALISE.get(unit) or UNIT_NORMALISE.get(unit.lower())
    if mapped:
        return mapped
    if VALID_CURRENCY_CODES:
        if unit in VALID_CURRENCY_CODES:
            return unit
    elif re.fullmatch(r"[A-Z]{3}", unit):
        return unit
    return unit


def normalise_source_type(stype: str) -> str:
    """Validate a lineage.sources[].type value against source_type.csv.

    Valid values from closed codelist: 'dataset', 'model'.
    Unknown values are returned unchanged.
    """
    if not stype or not stype.strip():
        return stype
    if not VALID_SOURCE_TYPES or stype in VALID_SOURCE_TYPES:
        return stype
    return stype


def is_valid_unit(unit: str) -> bool:
    """Return True if unit is a valid codelist code or known currency."""
    if not unit:
        return False
    if unit in VALID_UNIT_CODES:
        return True
    if unit in UNIT_NORMALISE:
        return False  # abbreviation, not the canonical code
    if VALID_CURRENCY_CODES and unit in VALID_CURRENCY_CODES:
        return True
    if not VALID_CURRENCY_CODES and re.fullmatch(r"[A-Z]{3}", unit):
        return True
    return False


def get_unit_for_quantity_kind(quantity_kind: str) -> Optional[str]:
    """Suggest the typical unit codelist file for a given quantity_kind.

    Returns the codelist name (e.g. 'unit_area') or None if not mappable.
    Useful for guiding what unit is expected for a given quantity_kind.
    """
    _QK_TO_UNIT_CODELIST = {
        "area":             "unit_area",
        "length":           "unit_length",
        "count":            "unit_count",
        "dimensionless_ratio": "unit_dimensionless_ratio",
        "energy":           "unit_energy",
        "mass":             "unit_mass",
        "mass_per_area":    "unit_mass_per_area",
        "time":             "unit_time",
        "volume":           "unit_volume",
        "currency":         "unit_currency",
    }
    return _QK_TO_UNIT_CODELIST.get(quantity_kind)
