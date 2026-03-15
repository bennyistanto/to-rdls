"""
RDLS record ID and filename generation.

Builds structured IDs in the format:
    rdls_{types}-{iso3}{org}_{titleslug}

Where:
    types      = Component codes (single: hzd/exp/vln/lss;
                 multi: single-letter concat in HEVL order, e.g. he, hev, hevl)
    iso3       = Lowercase ISO3 codes concatenated without separator (3-char fixed width);
                 omitted for regional/global datasets (>max_countries)
    org        = Org abbreviation appended after ISO3 (from YAML lookup or auto-truncate)
    titleslug  = Slugified dataset title (country + org + stop words removed, max 20 chars)

Source-independent. Loads naming convention from configs/naming.yaml.
"""

import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

from .utils import load_yaml, slugify_token


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_naming_config(yaml_path: Union[str, Path]) -> Dict[str, Any]:
    """Load naming convention config from YAML.

    Args:
        yaml_path: Path to configs/naming.yaml.

    Returns:
        Parsed naming config dict.
    """
    return load_yaml(yaml_path)


# ---------------------------------------------------------------------------
# Component type encoding
# ---------------------------------------------------------------------------

def encode_component_types(components: List[str], config: Dict[str, Any]) -> str:
    """Encode risk_data_type list into the type segment of the ID.

    Rules:
        - Single component → 3-letter code (hzd, exp, vln, lss)
        - Multiple components → single-letter concat in HEVL order (he, hev, hevl)

    Args:
        components: List of RDLS component names
                    (hazard, exposure, vulnerability, loss).
        config: Naming config dict (from load_naming_config).

    Returns:
        Type code string (e.g. "hzd", "he", "hevl").
    """
    codes = config.get("component_codes", {})
    single_map = codes.get("single", {
        "hazard": "hzd", "exposure": "exp",
        "vulnerability": "vln", "loss": "lss",
    })
    letter_map = codes.get("letter", {
        "hazard": "h", "exposure": "e",
        "vulnerability": "v", "loss": "l",
    })
    order = codes.get("order", ["hazard", "exposure", "vulnerability", "loss"])

    # Normalize component names
    comp_set = set(components)
    present = [c for c in order if c in comp_set]

    if not present:
        return "unk"

    if len(present) == 1:
        return single_map.get(present[0], present[0][:3])

    # Multi-component: concatenate single letters in HEVL order
    return "".join(letter_map.get(c, c[0]) for c in present)


# ---------------------------------------------------------------------------
# Country encoding
# ---------------------------------------------------------------------------

def encode_countries(
    iso3_codes: List[str],
    config: Optional[Dict[str, Any]] = None,
) -> str:
    """Encode country ISO3 codes into the countries segment.

    Concatenates 3-char lowercase ISO3 codes without separator, sorted
    alphabetically for determinism. When the number of unique codes
    exceeds ``max_countries`` (default 5), returns "" (empty) so that
    only the org segment remains in the ID.

    Args:
        iso3_codes: List of ISO3 country codes (e.g. ["URY", "ITA"]).
        config: Optional naming config dict.  Reads countries.max_countries.

    Returns:
        Concatenated country string (e.g. "itaury"), or "" if no codes
        or if the dataset is regional/global (>max_countries).
    """
    if not iso3_codes:
        return ""

    max_countries = (config or {}).get("countries", {}).get("max_countries", 5)

    # Deduplicate, sort, lowercase, take first 3 chars
    seen = set()
    clean = []
    for c in sorted(iso3_codes):
        low = c.strip().lower()[:3]
        if low and len(low) == 3 and low.isalpha() and low not in seen:
            seen.add(low)
            clean.append(low)

    if len(clean) > max_countries:
        return ""

    return "".join(clean)


# ---------------------------------------------------------------------------
# Shortname resolution
# ---------------------------------------------------------------------------

def resolve_shortname(
    org_name: str,
    org_slug: str,
    config: Dict[str, Any],
) -> str:
    """Resolve organization name to a short name for the ID.

    Resolution order:
        1. Exact match on org_slug in org_abbreviations
        2. Slugified org_name match in org_abbreviations
        3. Fallback: auto-truncate slugified name to max_length

    Args:
        org_name: Organization display name (e.g. "World Food Programme").
        org_slug: Organization slug/machine name (e.g. "world-food-programme").
        config: Naming config dict.

    Returns:
        Short name string (lowercase, alphanumeric + underscores, max 15 chars).
    """
    abbreviations = config.get("org_abbreviations", {})
    max_len = config.get("shortname", {}).get("max_length", 15)

    # Try slug lookup first (convert hyphens to underscores for matching)
    slug_key = (org_slug or "").strip().lower().replace("-", "_")
    if slug_key and slug_key in abbreviations:
        return str(abbreviations[slug_key])

    # Try slugified display name
    name_slug = slugify_token(org_name or "", max_len=80)
    if name_slug and name_slug in abbreviations:
        return str(abbreviations[name_slug])

    # Fallback: auto-truncate the best available identifier
    best = org_slug or org_name or "unknown"
    return slugify_token(best, max_len=max_len)


# ---------------------------------------------------------------------------
# Item encoding
# ---------------------------------------------------------------------------

def encode_items(
    hazard_types: List[str],
    exposure_categories: List[str],
    config: Dict[str, Any],
) -> str:
    """Encode hazard types and exposure categories into 2-char item codes.

    All codes are exactly 2 characters, concatenated without separator.
    Hazard codes come first (in config order), then exposure codes.

    Args:
        hazard_types: List of RDLS hazard_type values (e.g. ["flood", "earthquake"]).
        exposure_categories: List of RDLS exposure_category values (e.g. ["buildings"]).
        config: Naming config dict.

    Returns:
        Item codes string (e.g. "fleq", "flbd", "fleqbd").
        Empty string if no items.
    """
    hz_codes = config.get("hazard_item_codes", {})
    exp_codes = config.get("exposure_item_codes", {})

    parts = []

    # Hazard types first (in config key order for determinism)
    hz_set = set(hazard_types or [])
    for ht in hz_codes:
        if ht in hz_set:
            parts.append(hz_codes[ht])

    # Exposure categories second (in config key order)
    exp_set = set(exposure_categories or [])
    for ec in exp_codes:
        if ec in exp_set:
            parts.append(exp_codes[ec])

    return "".join(parts)


# ---------------------------------------------------------------------------
# Title slug generation
# ---------------------------------------------------------------------------

def _iso3_to_names(iso3_codes: List[str], config: Dict[str, Any]) -> List[str]:
    """Resolve ISO3 codes to lowercase country name strings for title stripping.

    Uses the iso3_to_name mapping from naming config. Codes not found in the
    mapping are silently skipped (the country name stays in the title slug).

    Args:
        iso3_codes: ISO3 codes (e.g. ["URY", "KEN"]).
        config: Naming config dict.

    Returns:
        List of lowercase country names (e.g. ["uruguay", "kenya"]).
    """
    mapping = config.get("iso3_to_name", {})
    names = []
    for code in iso3_codes:
        name = mapping.get(code.upper().strip(), "")
        if name:
            names.append(name.lower())
    return names


def is_valid_iso3(code: str, config: Dict[str, Any]) -> bool:
    """Check if a 3-letter string is a valid ISO3 country code.

    Validates against the iso3_to_name keys in the naming config. If no
    mapping is loaded, falls back to accepting any 3-letter alphabetic string.

    Args:
        code: 3-character string to check.
        config: Naming config dict.

    Returns:
        True if valid ISO3.
    """
    mapping = config.get("iso3_to_name", {})
    if mapping:
        return code.upper().strip() in mapping
    # Fallback: accept any 3-letter alpha string
    return len(code) == 3 and code.isalpha()


def slugify_title(
    title: str,
    iso3_codes: List[str],
    org_name: str,
    org_slug: str,
    config: Dict[str, Any],
) -> str:
    """Generate compact title slug for RDLS record ID.

    Steps:
        1. NFKD normalize → ASCII → lowercase
        2. Strip country names matching the record's ISO3 codes
        3. Strip org name and its abbreviation (avoid redundancy)
        4. Tokenize into alphanumeric words
        5. Remove stop words (from config)
        6. Concatenate without separators
        7. Truncate to max_length (default 25)

    Args:
        title: Dataset title string.
        iso3_codes: ISO3 codes used in this record's ID.
        org_name: Organization display name.
        org_slug: Organization slug/machine name.
        config: Naming config dict.

    Returns:
        Compact slug string (e.g. "floodhazardmapcoasta"), or "unk" if empty.
    """
    title_cfg = config.get("title_slug", {})
    max_len = title_cfg.get("max_length", 20)
    stop_words = set(title_cfg.get("stop_words", []))

    # 1. Normalize to ASCII lowercase
    t = unicodedata.normalize("NFKD", title or "")
    t = t.encode("ascii", "ignore").decode("ascii").lower()

    # 2. Strip country names matching ISO3 codes
    for name in _iso3_to_names(iso3_codes, config):
        # Word-boundary removal for multi-word names too
        t = re.sub(r'\b' + re.escape(name) + r'\b', ' ', t)

    # 2b. Strip country name aliases (variants, adjectives, abbreviations)
    aliases_cfg = config.get("country_name_aliases", {})
    for code in (iso3_codes or []):
        for alias in aliases_cfg.get(code.upper(), []):
            a = unicodedata.normalize("NFKD", alias)
            a = a.encode("ascii", "ignore").decode("ascii").lower()
            if a and len(a) >= 4:
                t = re.sub(r'\b' + re.escape(a) + r'\b', ' ', t)

    # 3. Strip org name and abbreviation
    org_lower = (org_name or "").strip().lower()
    org_ascii = ""
    if org_lower:
        # Normalize org name to ASCII for matching
        org_ascii = unicodedata.normalize("NFKD", org_lower)
        org_ascii = org_ascii.encode("ascii", "ignore").decode("ascii")
        if org_ascii:
            t = t.replace(org_ascii, " ")

    # Also strip the resolved abbreviation
    org_short = resolve_shortname(org_name, org_slug, config).lower()
    if org_short and org_short != "unknown":
        t = re.sub(r'\b' + re.escape(org_short) + r'\b', ' ', t)

    # 3c. Strip individual words from org name (catches partial matches
    #     like "heidelberg" from "HeiGIT Heidelberg Institute...")
    if org_ascii:
        org_words = re.findall(r'[a-z]{4,}', org_ascii)
        for w in org_words:
            if w not in stop_words:
                t = re.sub(r'\b' + re.escape(w) + r'\b', ' ', t)

    # 4. Tokenize: extract alphanumeric words
    words = re.findall(r'[a-z0-9]+', t)

    # 5. Remove stop words
    words = [w for w in words if w not in stop_words]

    # 6. Concatenate
    slug = "".join(words)

    # 7. Truncate
    slug = slug[:max_len]

    return slug or "unk"


# ---------------------------------------------------------------------------
# Full ID builder
# ---------------------------------------------------------------------------

def build_rdls_id(
    components: List[str],
    iso3_codes: List[str],
    org_name: str,
    org_slug: str,
    config: Dict[str, Any],
    title: str = "",
) -> str:
    """Build the full RDLS record ID.

    Format: rdls_{types}-{iso3}_{org}_{titleslug}

    The iso3 and org are separated by underscore.
    ISO3 codes are fixed 3-char width; org follows after underscore.
    For regional/global datasets (>max_countries), iso3 is omitted
    and the format becomes: rdls_{types}-{org}_{titleslug}

    Args:
        components: RDLS risk_data_type list (e.g. ["hazard", "exposure"]).
        iso3_codes: ISO3 country codes (e.g. ["URY"]).
        org_name: Organization display name.
        org_slug: Organization slug/machine name.
        config: Naming config dict.
        title: Dataset title for slug generation.

    Returns:
        RDLS record ID string (e.g. "rdls_hzd-ury_ucra_floodhazardmap").
    """
    types_seg = encode_component_types(components, config)
    countries_seg = encode_countries(iso3_codes, config)
    shortname_seg = resolve_shortname(org_name, org_slug, config)
    title_seg = slugify_title(title, iso3_codes, org_name, org_slug, config)

    # Segment: {iso3}_{org} (underscore separator) or just {org} if no countries
    geo_org = f"{countries_seg}_{shortname_seg}" if countries_seg else shortname_seg

    # Build: rdls_{types}-{geo_org}_{titleslug}
    return f"rdls_{types_seg}-{geo_org}_{title_seg}"


# ---------------------------------------------------------------------------
# Collision avoidance
# ---------------------------------------------------------------------------

def build_rdls_id_with_collision(
    base_id: str,
    existing_ids: Set[str],
    dataset_uuid: str = "",
    config: Optional[Dict[str, Any]] = None,
) -> str:
    """Apply collision avoidance to an RDLS ID.

    If base_id already exists in existing_ids, appends __{uuid8}.

    Args:
        base_id: The candidate ID.
        existing_ids: Set of already-used IDs.
        dataset_uuid: Source dataset UUID for collision suffix.
        config: Naming config dict (optional).

    Returns:
        Unique ID string.
    """
    if base_id not in existing_ids:
        return base_id

    suffix_fmt = (config or {}).get("collision", {}).get(
        "suffix_format", "__{uuid8}"
    )
    uuid8 = (dataset_uuid or "00000000")[:8]
    suffix = suffix_fmt.replace("{uuid8}", uuid8)
    return f"{base_id}{suffix}"


# ---------------------------------------------------------------------------
# ID parsing (for debugging / validation)
# ---------------------------------------------------------------------------

_ID_PATTERN = re.compile(
    r'^rdls_'
    r'([a-z]{1,4})'          # types segment (1-4 chars: h, he, hev, hevl, hzd, etc.)
    r'-'                       # dash separator
    r'(.+?)'                   # geo_org segment (iso3_org, may contain _)
    r'_'                       # underscore separator before title
    r'([a-z0-9]+)'            # title_slug (alphanumeric, no underscores)
    r'(?:__([a-f0-9]{8}))?'   # optional collision suffix (__hex8)
    r'$'
)


def parse_rdls_id(
    rdls_id: str,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """Parse an RDLS ID back into its component segments.

    Format: rdls_{types}-{iso3}_{org}_{titleslug}

    The geo_org segment uses underscore between iso3 and org.
    For regional/global datasets, geo_org is just the org (no ISO3 prefix).

    Args:
        rdls_id: Full RDLS ID string.
        config: Naming config dict (optional; needed for ISO3 validation).

    Returns:
        Dict with keys: types, geo_org, iso3, iso3_list, org, title_slug,
        collision.
        If parsing fails, returns {"raw": rdls_id}.
    """
    m = _ID_PATTERN.match(rdls_id or "")
    if not m:
        return {"raw": rdls_id or ""}

    geo_org_str = m.group(2)
    title_slug = m.group(3) or ""
    cfg = config or {}

    # Parse geo_org: split on first underscore to get iso3_part vs org_part
    iso3_list = []
    org_part = geo_org_str

    if "_" in geo_org_str:
        iso3_candidate, org_candidate = geo_org_str.split("_", 1)
        # Validate: iso3_candidate should be 3-char chunks of valid ISO3
        pos = 0
        valid = True
        while pos + 3 <= len(iso3_candidate):
            chunk = iso3_candidate[pos:pos + 3]
            if chunk.isalpha() and is_valid_iso3(chunk, cfg):
                iso3_list.append(chunk.upper())
                pos += 3
            else:
                valid = False
                break
        if valid and pos == len(iso3_candidate) and iso3_list:
            org_part = org_candidate
        else:
            # No valid ISO3 prefix - entire string is org
            iso3_list = []
            org_part = geo_org_str

    return {
        "types": m.group(1),
        "geo_org": geo_org_str,
        "iso3": "".join(c.lower() for c in iso3_list),
        "iso3_list": iso3_list,
        "org": org_part,
        "title_slug": title_slug,
        "collision": m.group(4) or "",
    }
