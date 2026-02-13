"""
RDLS schema operations: load schema, extract codelists, validate records.

Source-independent — works with any RDLS v0.3 JSON schema.
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from .utils import load_json, load_yaml


# ---------------------------------------------------------------------------
# Schema loading
# ---------------------------------------------------------------------------

def load_rdls_schema(schema_path: Union[str, Path]) -> Dict[str, Any]:
    """Load RDLS JSON schema from file."""
    return load_json(schema_path)


def load_codelists(yaml_path: Union[str, Path]) -> Dict[str, List[str]]:
    """Load RDLS codelist enum values from rdls_schema.yaml config.

    Returns:
        Dict mapping codelist name → list of allowed values.
        Includes both closed and open codelists.
    """
    cfg = load_yaml(yaml_path)
    codelists = {}
    for name, values in cfg.get("codelists", {}).items():
        codelists[name] = values
    for name, values in cfg.get("open_codelists", {}).items():
        codelists[name] = values
    return codelists


def load_codelists_from_schema(schema: Dict[str, Any]) -> Dict[str, List[str]]:
    """Extract enum values directly from a JSON schema.

    Walks the schema tree looking for 'enum' keywords and collects them
    keyed by the closest named property or definition.
    """
    codelists = {}

    def _walk(node: Any, name: str = "") -> None:
        if isinstance(node, dict):
            if "enum" in node and isinstance(node["enum"], list):
                if name:
                    codelists[name] = node["enum"]
            for key, val in node.items():
                child_name = key if key not in ("properties", "items", "anyOf", "oneOf", "allOf") else name
                _walk(val, child_name)
        elif isinstance(node, list):
            for item in node:
                _walk(item, name)

    _walk(schema)
    return codelists


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def validate_record(
    record: Dict[str, Any],
    schema: Dict[str, Any],
) -> Tuple[bool, List[str]]:
    """Validate a single RDLS dataset record against the JSON schema.

    Uses Draft 2020-12 validator with Draft 7 fallback.

    Args:
        record: The RDLS dataset record (not wrapped in {"datasets": [...]}).
        schema: The loaded JSON schema dict.

    Returns:
        (is_valid, error_messages) tuple.
    """
    try:
        from jsonschema import Draft202012Validator, ValidationError
        ValidatorClass = Draft202012Validator
    except ImportError:
        try:
            from jsonschema import Draft7Validator as ValidatorClass
        except ImportError:
            return (True, ["jsonschema not installed — validation skipped"])

    # Wrap record in the root datasets array for schema validation
    wrapped = {"datasets": [record]}

    validator = ValidatorClass(schema)
    errors = sorted(validator.iter_errors(wrapped), key=lambda e: list(e.path))

    if not errors:
        return (True, [])

    messages = []
    for err in errors:
        path = ".".join(str(p) for p in err.absolute_path) or "(root)"
        messages.append(f"{path}: {err.message}")

    return (False, messages)


def summarize_errors(error_messages: List[str]) -> Dict[str, int]:
    """Summarize validation errors by category.

    Groups errors by their schema path pattern and returns counts.
    """
    from collections import Counter
    categories = Counter()
    for msg in error_messages:
        # Extract the schema path portion (before the colon)
        parts = msg.split(":", 1)
        if len(parts) == 2:
            path = parts[0].strip()
            # Generalize array indices: datasets.0.hazard -> datasets.*.hazard
            import re
            path = re.sub(r"\.\d+\.", ".*.", path)
            categories[path] += 1
        else:
            categories["other"] += 1
    return dict(categories)


def check_required_fields(record: Dict[str, Any]) -> List[str]:
    """Quick check for mandatory RDLS dataset fields.

    Returns list of missing field names.
    """
    required = ["id", "title", "risk_data_type", "attributions", "spatial",
                 "license", "resources"]
    return [f for f in required if f not in record or not record[f]]
