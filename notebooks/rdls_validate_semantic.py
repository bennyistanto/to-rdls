"""
RDLS Semantic Validator
=======================
Complements jsonschema validation by checking semantic rules that
jsonschema cannot enforce (open codelist values, single-value fields,
IANA link relations, cross-field consistency).

Lesson learned: jsonschema only validates data TYPES (string, array, etc.)
and closed enums. It does NOT catch:
  - Multi-value strings where single value is intended (e.g., "PGA:g; SA:g")
  - Invalid open codelist values (any string passes type check)
  - Non-standard IANA link relation types
  - Cross-field logical inconsistencies

Usage:
  python notebooks/validate_rdls_semantic.py <rdls_json_file> [schema_file]

If schema_file is not provided, defaults to schema/rdls_schema_v0.3.json
"""

import json
import sys
import os
import re
from pathlib import Path


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
# From schema's intensity_measure_definitions keys
IM_PATTERN = re.compile(r"^[A-Za-z0-9_()]+:[A-Za-z0-9²/]+$")

# Known quantity_kind values (QUDT + schema suggestions)
KNOWN_QUANTITY_KINDS = {
    "area", "count", "monetary", "length", "time",  # schema suggestions
    "fraction", "ratio", "dimensionless_ratio",       # common for vulnerability
    "probability", "percentage", "rate",               # other common kinds
}

# Access modality closed codelist
VALID_ACCESS_MODALITY = {
    "file_download", "download_page", "API", "OGC_API", "GEE_collection",
    "WMS", "WFS", "WCS", "STAC", "REST", "dashboard",
}


class SemanticIssue:
    """Represents a semantic validation finding."""

    def __init__(self, severity, path, message, suggestion=None):
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


def validate_semantic(record, schema=None):
    """Run all semantic checks on an RDLS record. Returns list of SemanticIssue."""
    issues = []

    # Support both wrapped {datasets: [...]} and unwrapped format
    if "datasets" in record:
        datasets = record["datasets"]
    else:
        datasets = [record]

    for ds_idx, ds in enumerate(datasets):
        prefix = f"datasets[{ds_idx}]" if len(datasets) > 1 else ""
        _check_dataset(ds, prefix, issues, schema)

    return issues


def _check_dataset(ds, prefix, issues, schema):
    """Check a single dataset record."""

    # --- 1. Single-value string fields with separators ---
    _check_single_value_strings(ds, prefix, issues)

    # --- 2. Link relation types ---
    _check_links(ds, prefix, issues)

    # --- 3. Open codelist values ---
    _check_open_codelists(ds, prefix, issues)

    # --- 4. Cross-field consistency ---
    _check_cross_field(ds, prefix, issues)

    # --- 5. Resource field conventions ---
    _check_resources(ds, prefix, issues)

    # --- 6. Attribution completeness ---
    _check_attributions(ds, prefix, issues)


def _check_single_value_strings(ds, prefix, issues):
    """Check that single-value string fields don't contain multiple values."""

    separators = [";", "|", " / ", " , "]

    # Collect all intensity_measure values from all function types
    vuln = ds.get("vulnerability", {})
    funcs = vuln.get("functions", {})

    for func_type in ["vulnerability", "fragility", "damage_to_loss", "engineering_demand"]:
        for j, fn in enumerate(funcs.get(func_type, [])):
            path = f"{prefix}.vulnerability.functions.{func_type}[{j}]".strip(".")
            fn_id = fn.get("id", "?")

            # Check intensity_measure
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

                # Check format matches MEASURE:UNIT pattern
                if not IM_PATTERN.match(im):
                    issues.append(SemanticIssue(
                        "warning", f"{path}.intensity_measure",
                        f"Value '{im}' doesn't match expected 'MEASURE:UNIT' format.",
                        "Check intensity_measure_definitions in schema for valid codes."
                    ))

            # Check other string fields that should be single values
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


def _check_links(ds, prefix, issues):
    """Check link relation types against IANA registry."""

    links = ds.get("links", [])

    if not links:
        issues.append(SemanticIssue(
            "warning", f"{prefix}.links".strip("."),
            "No links array found. First link should be 'describedby' pointing to RDLS schema.",
        ))
        return

    # First link must be describedby
    if links[0].get("rel") != "describedby":
        issues.append(SemanticIssue(
            "error", f"{prefix}.links[0].rel".strip("."),
            f"First link rel must be 'describedby', got '{links[0].get('rel')}'.",
        ))

    expected_schema_href = "https://docs.riskdatalibrary.org/en/0__3__0/rdls_schema.json"
    if links[0].get("href") != expected_schema_href:
        issues.append(SemanticIssue(
            "warning", f"{prefix}.links[0].href".strip("."),
            f"First link href should be '{expected_schema_href}'.",
        ))

    # Check all link rel values against IANA
    for i, link in enumerate(links):
        rel = link.get("rel", "")
        if rel and rel not in IANA_LINK_RELATIONS:
            issues.append(SemanticIssue(
                "warning", f"{prefix}.links[{i}].rel".strip("."),
                f"'{rel}' is not a standard IANA link relation type.",
                "See https://www.iana.org/assignments/link-relations/ for valid values. "
                "Common ones: 'related', 'source', 'cite-as', 'alternate', 'describedby'."
            ))


def _check_open_codelists(ds, prefix, issues):
    """Check open codelist values for common mistakes."""

    vuln = ds.get("vulnerability", {})
    funcs = vuln.get("functions", {})

    for func_type in ["vulnerability", "fragility", "damage_to_loss", "engineering_demand"]:
        for j, fn in enumerate(funcs.get(func_type, [])):
            path = f"{prefix}.vulnerability.functions.{func_type}[{j}]".strip(".")

            # quantity_kind
            qk = fn.get("quantity_kind", "")
            if qk and qk not in KNOWN_QUANTITY_KINDS:
                issues.append(SemanticIssue(
                    "info", f"{path}.quantity_kind",
                    f"'{qk}' is not in common quantity_kind values.",
                    f"Schema suggestions: area, count, monetary, length, time. "
                    f"For dimensionless ratios: 'fraction' or 'ratio'. "
                    f"See QUDT Quantity Kind Vocabulary for other values."
                ))

            # Check intensity_measure against schema's known definitions
            im = fn.get("intensity_measure", "")
            if im and ":" not in im:
                issues.append(SemanticIssue(
                    "warning", f"{path}.intensity_measure",
                    f"'{im}' missing unit separator ':'. Expected format: 'MEASURE:UNIT'.",
                ))


def _check_cross_field(ds, prefix, issues):
    """Check cross-field logical consistency."""

    vuln = ds.get("vulnerability", {})
    funcs = vuln.get("functions", {})

    for func_type in ["vulnerability", "fragility", "damage_to_loss", "engineering_demand"]:
        for j, fn in enumerate(funcs.get(func_type, [])):
            path = f"{prefix}.vulnerability.functions.{func_type}[{j}]".strip(".")

            # Fragility functions should typically use 'probability' as impact_metric
            if func_type == "fragility" and fn.get("impact_metric") not in ("probability", "damage_index"):
                issues.append(SemanticIssue(
                    "info", f"{path}.impact_metric",
                    f"Fragility function using '{fn.get('impact_metric')}' — "
                    f"fragility functions typically use 'probability' (exceedance probability).",
                ))

            # Damage-to-loss with 'discrete' relationship should typically be 'empirical' or 'judgement'
            if (func_type == "damage_to_loss" and
                fn.get("relationship") == "discrete" and
                fn.get("approach") not in ("empirical", "judgement")):
                issues.append(SemanticIssue(
                    "info", f"{path}",
                    f"Discrete damage-to-loss function with '{fn.get('approach')}' approach — "
                    f"discrete relationships are typically empirical or judgement-based.",
                ))

            # damage_states_names should be array, not string
            dsn = fn.get("damage_states_names")
            if dsn is not None and isinstance(dsn, str):
                issues.append(SemanticIssue(
                    "error", f"{path}.damage_states_names",
                    f"Should be an array of strings, not a single string.",
                    'Use ["DS1", "DS2", "DS3", "DS4"] instead of "DS1, DS2, DS3, DS4".'
                ))

    # Check spatial bbox format [west, south, east, north]
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


def _check_resources(ds, prefix, issues):
    """Check resource field conventions."""

    for i, r in enumerate(ds.get("resources", [])):
        path = f"{prefix}.resources[{i}]".strip(".")

        # Check for legacy field names
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

        # Check access_modality
        am = r.get("access_modality", "")
        if am and am not in VALID_ACCESS_MODALITY:
            issues.append(SemanticIssue(
                "error", f"{path}.access_modality",
                f"'{am}' is not in the closed access_modality codelist.",
                f"Valid values: {', '.join(sorted(VALID_ACCESS_MODALITY))}"
            ))


def _check_attributions(ds, prefix, issues):
    """Check attribution completeness."""

    attributions = ds.get("attributions", [])
    roles = {a.get("role") for a in attributions}

    # Entity must have name + (email or url)
    for i, attr in enumerate(attributions):
        path = f"{prefix}.attributions[{i}]".strip(".")
        entity = attr.get("entity", {})
        if entity and "email" not in entity and "url" not in entity:
            issues.append(SemanticIssue(
                "error", f"{path}.entity",
                f"Entity '{entity.get('name', '?')}' must have either 'email' or 'url'.",
            ))


def main():
    if len(sys.argv) < 2:
        print(f"Usage: python {sys.argv[0]} <rdls_json_file> [schema_file]")
        sys.exit(1)

    json_file = sys.argv[1]
    schema_file = sys.argv[2] if len(sys.argv) > 2 else None

    # Load record
    with open(json_file, encoding="utf-8") as f:
        record = json.load(f)

    # Load schema if provided
    schema = None
    if schema_file:
        with open(schema_file, encoding="utf-8") as f:
            schema = json.load(f)

    print(f"Semantic validation: {os.path.basename(json_file)}")
    print("=" * 60)

    issues = validate_semantic(record, schema)

    if not issues:
        print("\nSemantic validation: PASSED (0 issues)")
    else:
        errors = [i for i in issues if i.severity == "error"]
        warnings = [i for i in issues if i.severity == "warning"]
        infos = [i for i in issues if i.severity == "info"]

        print(f"\nFound {len(issues)} issue(s): "
              f"{len(errors)} error(s), {len(warnings)} warning(s), {len(infos)} info(s)\n")

        for issue in issues:
            print(issue)
            print()

        if errors:
            print("FAILED — errors must be fixed before submission.")
            sys.exit(1)
        else:
            print("PASSED with warnings/info — review recommended.")

    sys.exit(0)


if __name__ == "__main__":
    main()
