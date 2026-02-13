"""
Spatial utilities for RDLS metadata transformation.

Handles country name → ISO3 resolution, region expansion,
and RDLS spatial block inference. Source-independent.
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from .utils import load_yaml


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_spatial_config(yaml_path: Union[str, Path]) -> Dict[str, Any]:
    """Load spatial configuration (regions, country fixes, non-country groups).

    Returns dict with keys:
        region_to_countries: dict[str, list[str]]  (lowercased keys)
        country_name_fixes: dict[str, str]          (normalized keys)
        non_country_groups: set[str]
    """
    cfg = load_yaml(yaml_path)

    # Normalize region keys to lowercase
    regions = {k.lower().strip(): v for k, v in cfg.get("region_to_countries", {}).items()}

    # Normalize country fix keys
    fixes = {_norm_country_key(k): v for k, v in cfg.get("country_name_fixes", {}).items()}

    non_country = set(cfg.get("non_country_groups", []))

    return {
        "region_to_countries": regions,
        "country_name_fixes": fixes,
        "non_country_groups": non_country,
    }


# ---------------------------------------------------------------------------
# Country name normalization
# ---------------------------------------------------------------------------

def _norm_country_key(s: str) -> str:
    """Normalize country name for lookup."""
    s = (s or "").strip().lower()
    s = re.sub(r"[\(\)\[\]\{\}\.\,\;\:]", " ", s)
    s = s.replace("&", " and ")
    s = re.sub(r"[^a-z0-9\s\-']", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _try_pycountry(name: str) -> Optional[str]:
    """Try to resolve country name using pycountry."""
    try:
        import pycountry
        c = pycountry.countries.lookup(name)
        return getattr(c, "alpha_3", None)
    except Exception:
        return None


def country_name_to_iso3(
    name: str,
    fixes: Optional[Dict[str, str]] = None,
    iso3_table: Optional[Dict[str, str]] = None,
) -> Optional[str]:
    """Convert country name to ISO 3166-1 alpha-3 code.

    Resolution order:
        1. Already a 3-letter alpha code → return uppercased
        2. Check country_name_fixes dict
        3. Check iso3_table (from CSV)
        4. Try pycountry library

    Args:
        name: Country name or ISO3 code.
        fixes: Normalized country name fixes dict (from spatial config).
        iso3_table: Optional lookup table (normalized name → ISO3).

    Returns:
        ISO3 code or None.
    """
    n = (name or "").strip()
    if not n:
        return None

    # Already ISO3?
    if len(n) == 3 and n.isalpha():
        return n.upper()

    key = _norm_country_key(n)

    # Check fixes
    if fixes and key in fixes:
        return fixes[key]

    # Check table
    if iso3_table and key in iso3_table:
        return iso3_table[key]

    # Try pycountry
    return _try_pycountry(n)


def load_country_iso3_table(csv_path: Union[str, Path]) -> Dict[str, str]:
    """Load country name → ISO3 mapping from CSV file.

    Expects columns like 'name'/'country' and 'iso3'/'alpha_3'/'code'.
    Returns dict with normalized keys.
    """
    csv_path = Path(csv_path)
    if not csv_path.exists():
        return {}
    try:
        import pandas as pd
        df = pd.read_csv(csv_path)
        cols = {c.lower(): c for c in df.columns}
        name_col = cols.get("name") or cols.get("country") or cols.get("country_name")
        iso3_col = cols.get("iso3") or cols.get("alpha_3") or cols.get("code")
        if not name_col or not iso3_col:
            return {}
        return {
            _norm_country_key(str(r.get(name_col, ""))): str(r.get(iso3_col, "")).strip().upper()
            for _, r in df.iterrows()
            if str(r.get(name_col, "")).strip() and len(str(r.get(iso3_col, "")).strip()) == 3
        }
    except Exception:
        return {}


# ---------------------------------------------------------------------------
# Spatial block inference
# ---------------------------------------------------------------------------

def infer_spatial(
    groups: List[str],
    region_map: Dict[str, List[str]],
    country_fixes: Dict[str, str],
    non_country_groups: Optional[set] = None,
    iso3_table: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
    """Infer RDLS spatial block from country/region group names.

    Logic:
      - "World"/"global" → scale: "global", no countries
      - Regional name (Africa, Europe, etc.) → scale: "regional", countries: [members]
      - Single country → scale: "national", countries: [iso3]
      - Multiple countries → scale: "regional", countries: [iso3s]
      - Completely unresolvable → scale: "global"

    Args:
        groups: List of group names (e.g. from HDX groups field).
        region_map: Lowercased region name → list of ISO3 codes.
        country_fixes: Normalized country name → ISO3 fixes.
        non_country_groups: Set of group names to skip (e.g. "world").
        iso3_table: Optional CSV-based lookup table.

    Returns:
        Dict with 'scale' and optionally 'countries'.
    """
    if not groups:
        return {"scale": "global"}

    non_country = non_country_groups or set()
    norm_groups = [g.strip().lower() for g in groups]

    # Filter out non-country groups
    if any(g in non_country for g in norm_groups):
        country_groups = [g for g, ng in zip(groups, norm_groups) if ng not in non_country]
        if not country_groups:
            return {"scale": "global"}
        groups = country_groups
        norm_groups = [g.strip().lower() for g in groups]

    # Resolve regions and countries
    all_iso3s = []
    remaining_groups = []
    is_regional = False

    for g, ng in zip(groups, norm_groups):
        if ng in region_map:
            all_iso3s.extend(region_map[ng])
            is_regional = True
        else:
            remaining_groups.append(g)

    # Resolve remaining as country names
    for g in remaining_groups:
        iso3 = country_name_to_iso3(g, fixes=country_fixes, iso3_table=iso3_table)
        if iso3:
            all_iso3s.append(iso3)

    # Deduplicate and sort
    iso3s = sorted(set(all_iso3s))

    if is_regional and iso3s:
        return {"scale": "regional", "countries": iso3s}
    if len(iso3s) == 1:
        return {"scale": "national", "countries": iso3s}
    if len(iso3s) > 1:
        return {"scale": "regional", "countries": iso3s}

    return {"scale": "global"}


def infer_scale(countries: List[str]) -> str:
    """Infer spatial scale from country count."""
    if not countries:
        return "global"
    if len(countries) == 1:
        return "national"
    return "regional"
