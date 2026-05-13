"""
RDLS v0.3 Semantic Validation
==============================
Complements jsonschema validation by checking semantic rules that
jsonschema cannot enforce (open codelist values, single-value fields,
IANA link relations, cross-field consistency).

Catches issues jsonschema misses:
  - Multi-value strings where single value is intended (e.g., "PGA:g; SA:g")
  - Invalid open codelist values (any string passes type check)
  - Non-standard IANA link relation types
  - Cross-field logical inconsistencies

Public API:
    from src.validate_v03 import validate_semantic, SemanticIssue
"""

import re
from typing import List, Optional


# ---------------------------------------------------------------------------
# Known valid values for open codelists and conventions
# ---------------------------------------------------------------------------

# IANA Link Relation Types (common subset)
# Full list: https://www.iana.org/assignments/link-relations/link-relations.xhtml
IANA_LINK_RELATIONS = {
    "about", "alternate", "appendix", "archives", "author", "bookmark",
    "canonical", "chapter", "cite-as", "collection", "contents",
    "convertedfrom", "copyright", "create-form", "current", "describedby",
    "describes", "disclosure", "duplicate", "edit", "edit-form",
    "edit-media", "enclosure", "external", "first", "glossary", "help",
    "hosts", "hub", "icon", "index", "intervalafter", "intervalbefore",
    "item", "last", "latest-version", "license", "linkset", "lrdd",
    "memento", "monitor", "monitor-group", "next", "next-archive",
    "nofollow", "noreferrer", "original", "payment", "predecessor-version",
    "prefetch", "prev", "prev-archive", "preview", "previous", "privacy-policy",
    "profile", "related", "replies", "restconf", "ruleinput", "search",
    "section", "self", "service", "service-desc", "service-doc",
    "service-meta", "source", "start", "status", "stylesheet", "subsection",
    "successor-version", "sunset", "tag", "terms-of-service", "timegate",
    "timemap", "type", "up", "version-history", "via", "working-copy",
    "working-copy-of",
}

# Intensity measure format: "MEASURE:UNIT" (single value)
IM_PATTERN = re.compile(r"^[A-Za-z0-9_()]+:[A-Za-z0-9²/\-]+$")

# Known quantity_kind values (QUDT + schema suggestions)
KNOWN_QUANTITY_KINDS = {
    "area", "count", "monetary", "length", "time",
    "fraction", "ratio", "dimensionless_ratio",
    "probability", "percentage", "rate",
}

# Access modality closed codelist
VALID_ACCESS_MODALITY = {
    "file_download", "download_page", "API", "OGC_API", "GEE_collection",
    "WMS", "WFS", "WCS", "STAC", "REST", "dashboard",
}


# ---------------------------------------------------------------------------
# Result class
# ---------------------------------------------------------------------------

class SemanticIssue:
    """Represents a semantic validation finding."""

    def __init__(self, severity: str, path: str, message: str, suggestion: Optional[str] = None):
        self.severity = severity  # "error", "warning", "info"
        self.path = path
        self.message = message
        self.suggestion = suggestion

    def __str__(self):
        icon = {"error": "ERROR", "warning": "WARN ", "info": "INFO "}[self.severity]
        s = f"[{icon}] {self.path}: {self.message}"
        if self.suggestion:
            s += f"\n        -> Suggestion: {self.suggestion}"
        return s


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def validate_semantic(record: dict, schema: Optional[dict] = None) -> List[SemanticIssue]:
    """Run all semantic checks on an RDLS v0.3 record. Returns list of SemanticIssue."""
    issues: List[SemanticIssue] = []

    # Support both wrapped {datasets: [...]} and unwrapped format
    if "datasets" in record:
        datasets = record["datasets"]
    else:
        datasets = [record]

    for ds_idx, ds in enumerate(datasets):
        prefix = f"datasets[{ds_idx}]" if len(datasets) > 1 else ""
        _check_dataset(ds, prefix, issues, schema)

    return issues


# ---------------------------------------------------------------------------
# Internal checkers
# ---------------------------------------------------------------------------

def _check_dataset(ds: dict, prefix: str, issues: List[SemanticIssue], schema):
    _check_single_value_strings(ds, prefix, issues)
    _check_links(ds, prefix, issues)
    _check_open_codelists(ds, prefix, issues)
    _check_cross_field(ds, prefix, issues)
    _check_resources(ds, prefix, issues)
    _check_attributions(ds, prefix, issues)


def _check_single_value_strings(ds: dict, prefix: str, issues: List[SemanticIssue]):
    """Check that single-value string fields don't contain multiple values."""
    separators = [";", "|", " / ", " , "]

    vuln = ds.get("vulnerability", {})
    funcs = vuln.get("functions", {})

    for func_type in ["vulnerability", "fragility", "damage_to_loss", "engineering_demand"]:
        for j, fn in enumerate(funcs.get(func_type, [])):
            path = f"{prefix}.vulnerability.functions.{func_type}[{j}]".strip(".")

            im = fn.get("intensity_measure", "")
            if im:
                for sep in separators:
                    if sep in im:
                        issues.append(SemanticIssue(
                            "error", f"{path}.intensity_measure",
                            f"Contains separator '{sep.strip()}' suggesting multiple values: '{im}'. "
                            f"Schema expects a single intensity measure string (e.g., 'PGA:g').",
                            "Use one primary IM and document others in analysis_details."
                        ))
                        break
                if not IM_PATTERN.match(im):
                    issues.append(SemanticIssue(
                        "warning", f"{path}.intensity_measure",
                        f"Value '{im}' doesn't match expected 'MEASURE:UNIT' format.",
                        "Check intensity_measure_definitions in schema for valid codes."
                    ))

            for field in ["approach", "relationship", "category", "taxonomy",
                          "impact_type", "impact_modelling", "impact_metric"]:
                val = fn.get(field, "")
                if val:
                    for sep in separators:
                        if sep in val:
                            issues.append(SemanticIssue(
                                "error", f"{path}.{field}",
                                f"Contains separator '{sep.strip()}' suggesting multiple values: '{val}'. "
                                f"Schema expects a single value from the codelist.",
                            ))
                            break


def _check_links(ds: dict, prefix: str, issues: List[SemanticIssue]):
    """Check link relation types against IANA registry."""
    links = ds.get("links", [])

    if not links:
        issues.append(SemanticIssue(
            "warning", f"{prefix}.links".strip("."),
            "No links array found. First link should be 'describedby' pointing to RDLS schema.",
        ))
        return

    if links[0].get("rel") != "describedby":
        issues.append(SemanticIssue(
            "error", f"{prefix}.links[0].rel".strip("."),
            f"First link rel must be 'describedby', got '{links[0].get('rel')}'.",
        ))

    valid_schema_hrefs = {
        "https://docs.riskdatalibrary.org/en/0__3__0/rdls_schema.json",
        "https://docs.riskdatalibrary.org/en/1__0__0/rdls_schema.json",
    }
    href = links[0].get("href", "")
    if href not in valid_schema_hrefs:
        issues.append(SemanticIssue(
            "warning", f"{prefix}.links[0].href".strip("."),
            f"First link href '{href}' is not a recognised RDLS schema URL. "
            f"Expected '...0__3__0/rdls_schema.json' (v0.3) or '...1__0__0/rdls_schema.json' (v1.0).",
        ))

    for i, link in enumerate(links):
        rel = link.get("rel", "")
        if rel and rel not in IANA_LINK_RELATIONS:
            issues.append(SemanticIssue(
                "warning", f"{prefix}.links[{i}].rel".strip("."),
                f"'{rel}' is not a standard IANA link relation type.",
                "See https://www.iana.org/assignments/link-relations/ for valid values."
            ))


def _check_open_codelists(ds: dict, prefix: str, issues: List[SemanticIssue]):
    """Check open codelist values for common mistakes."""
    vuln = ds.get("vulnerability", {})
    funcs = vuln.get("functions", {})

    for func_type in ["vulnerability", "fragility", "damage_to_loss", "engineering_demand"]:
        for j, fn in enumerate(funcs.get(func_type, [])):
            path = f"{prefix}.vulnerability.functions.{func_type}[{j}]".strip(".")

            qk = fn.get("quantity_kind", "")
            if qk and qk not in KNOWN_QUANTITY_KINDS:
                issues.append(SemanticIssue(
                    "info", f"{path}.quantity_kind",
                    f"'{qk}' is not in common quantity_kind values.",
                    f"Schema suggestions: area, count, monetary, length, time."
                ))

            im = fn.get("intensity_measure", "")
            if im and ":" not in im:
                issues.append(SemanticIssue(
                    "warning", f"{path}.intensity_measure",
                    f"'{im}' missing unit separator ':'. Expected format: 'MEASURE:UNIT'.",
                ))


def _check_cross_field(ds: dict, prefix: str, issues: List[SemanticIssue]):
    """Check cross-field logical consistency."""
    vuln = ds.get("vulnerability", {})
    funcs = vuln.get("functions", {})

    for func_type in ["vulnerability", "fragility", "damage_to_loss", "engineering_demand"]:
        for j, fn in enumerate(funcs.get(func_type, [])):
            path = f"{prefix}.vulnerability.functions.{func_type}[{j}]".strip(".")

            if func_type == "fragility" and fn.get("impact_metric") not in ("probability", "damage_index"):
                issues.append(SemanticIssue(
                    "info", f"{path}.impact_metric",
                    f"Fragility function using '{fn.get('impact_metric')}' - "
                    f"fragility functions typically use 'probability' (exceedance probability).",
                ))

            if (func_type == "damage_to_loss" and
                fn.get("relationship") == "discrete" and
                fn.get("approach") not in ("empirical", "judgement")):
                issues.append(SemanticIssue(
                    "info", f"{path}",
                    f"Discrete damage-to-loss function with '{fn.get('approach')}' approach - "
                    f"discrete relationships are typically empirical or judgement-based.",
                ))

            dsn = fn.get("damage_states_names")
            if dsn is not None and isinstance(dsn, str):
                issues.append(SemanticIssue(
                    "error", f"{path}.damage_states_names",
                    f"Should be an array of strings, not a single string.",
                    'Use ["DS1", "DS2", "DS3", "DS4"] instead of "DS1, DS2, DS3, DS4".'
                ))

    bbox = ds.get("spatial", {}).get("bbox", [])
    if bbox and len(bbox) == 4:
        west, south, east, north = bbox
        if west > east:
            issues.append(SemanticIssue(
                "warning", f"{prefix}.spatial.bbox".strip("."),
                f"West ({west}) > East ({east}). bbox format is [west, south, east, north].",
            ))
        if south > north:
            issues.append(SemanticIssue(
                "warning", f"{prefix}.spatial.bbox".strip("."),
                f"South ({south}) > North ({north}). bbox format is [west, south, east, north].",
            ))


def _check_resources(ds: dict, prefix: str, issues: List[SemanticIssue]):
    """Check resource field conventions."""
    for i, r in enumerate(ds.get("resources", [])):
        path = f"{prefix}.resources[{i}]".strip(".")

        if "format" in r and "data_format" not in r:
            issues.append(SemanticIssue(
                "error", f"{path}",
                "Uses 'format' instead of 'data_format'.",
                "Rename 'format' to 'data_format'."
            ))

        if "url" in r and "download_url" not in r and "access_url" not in r:
            issues.append(SemanticIssue(
                "error", f"{path}",
                "Uses 'url' instead of 'download_url' or 'access_url'.",
                "Use 'download_url' for direct downloads, 'access_url' for access pages."
            ))

        am = r.get("access_modality", "")
        if am and am not in VALID_ACCESS_MODALITY:
            issues.append(SemanticIssue(
                "error", f"{path}.access_modality",
                f"'{am}' is not in the closed access_modality codelist.",
                f"Valid values: {', '.join(sorted(VALID_ACCESS_MODALITY))}"
            ))


def _check_attributions(ds: dict, prefix: str, issues: List[SemanticIssue]):
    """Check attribution completeness."""
    for i, attr in enumerate(ds.get("attributions", [])):
        path = f"{prefix}.attributions[{i}]".strip(".")
        entity = attr.get("entity", {})
        if entity and "email" not in entity and "url" not in entity:
            issues.append(SemanticIssue(
                "error", f"{path}.entity",
                f"Entity '{entity.get('name', '?')}' must have either 'email' or 'url'.",
            ))
