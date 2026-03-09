"""
RDLS schema operations: load schema, extract codelists, validate records.

Includes SchemaContext — a class that bundles all schema-derived lookup
structures (enum values, field aliases, required fields, allowed properties)
built once from the JSON schema.

Source-independent — works with any RDLS v0.3 JSON schema.
"""

import json
from difflib import get_close_matches
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

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


# ---------------------------------------------------------------------------
# SchemaContext — bundles all schema-derived lookup structures
# ---------------------------------------------------------------------------

class SchemaContext:
    """Bundle of schema-derived lookup structures for validation and auto-fix.

    Built once from a loaded JSON schema. Provides fast lookup of enum values,
    field aliases, required fields, and allowed properties per schema $def.

    Attributes:
        schema: The raw JSON schema dict.
        enum_lookup: field_name -> set of valid enum values.
        field_aliases: property_name -> $defs enum name
                       (e.g., "dimension" -> "metric_dimension").
        required_lookup: $def_name -> set of required field names.
        allowed_props: $def_name -> set of allowed property names.
        property_to_def: property path -> $defs name mapping.
    """

    def __init__(self, schema: Dict[str, Any]) -> None:
        self.schema = schema
        self.enum_lookup: Dict[str, Set[str]] = {}
        self.field_aliases: Dict[str, str] = {}
        self.required_lookup: Dict[str, Set[str]] = {}
        self.allowed_props: Dict[str, Set[str]] = {}
        self.property_to_def: Dict[str, str] = {}

        self._build_enum_lookup()
        self._build_field_aliases()
        self._build_required_lookup()
        self._build_allowed_props()
        self._build_property_to_def()

    # -- Builders -----------------------------------------------------------

    def _build_enum_lookup(self) -> None:
        """Recursively extract all enum constraints from the schema."""

        def _walk(node: Any, name: str = "") -> None:
            if isinstance(node, dict):
                if "enum" in node and isinstance(node["enum"], list):
                    if name:
                        existing = self.enum_lookup.get(name, set())
                        self.enum_lookup[name] = existing | set(node["enum"])
                for key, val in node.items():
                    child_name = (
                        key
                        if key not in (
                            "properties", "items", "anyOf", "oneOf", "allOf",
                            "$defs", "required", "if", "then", "else",
                        )
                        else name
                    )
                    _walk(val, child_name)
            elif isinstance(node, list):
                for item in node:
                    _walk(item, name)

        _walk(self.schema)

    def _build_field_aliases(self) -> None:
        """Build property_name -> ENUM_LOOKUP key map from $ref pointers.

        E.g., the JSON property "dimension" $refs "$defs/metric_dimension"
        which contains the enum. Validation errors report "dimension" but
        enum_lookup is keyed by "metric_dimension". This map bridges them.
        """
        defs = self.schema.get("$defs", {})
        for def_name, defn in defs.items():
            props = defn.get("properties", {})
            for prop_name, prop_spec in props.items():
                if not isinstance(prop_spec, dict):
                    continue
                ref = prop_spec.get("$ref", "")
                if ref.startswith("#/$defs/"):
                    ref_name = ref.split("/")[-1]
                    if ref_name in self.enum_lookup and prop_name != ref_name:
                        self.field_aliases[prop_name] = ref_name

    def _build_required_lookup(self) -> None:
        """Map each $def name to its set of required field names."""
        for def_name, defn in self.schema.get("$defs", {}).items():
            req = defn.get("required", [])
            if req:
                self.required_lookup[def_name] = set(req)

    def _build_allowed_props(self) -> None:
        """Map each $def name to its set of allowed property names."""
        # Root-level allowed properties
        self.allowed_props["root"] = set(
            self.schema.get("properties", {}).keys()
        )
        # $defs allowed properties
        for def_name, defn in self.schema.get("$defs", {}).items():
            if isinstance(defn, dict) and "properties" in defn:
                self.allowed_props[def_name] = set(defn["properties"].keys())

    def _build_property_to_def(self) -> None:
        """Build mapping from property paths to $defs names.

        Covers root properties, array items, nested sub-properties,
        and $defs internal references.
        """
        # Root-level properties
        for prop_name, prop_spec in self.schema.get("properties", {}).items():
            if not isinstance(prop_spec, dict):
                continue
            # Direct $ref
            ref = prop_spec.get("$ref", "")
            if ref.startswith("#/$defs/"):
                self.property_to_def[prop_name] = ref.split("/")[-1]
            # Array items $ref
            items = prop_spec.get("items", {})
            if isinstance(items, dict) and "$ref" in items:
                iref = items["$ref"]
                if iref.startswith("#/$defs/"):
                    self.property_to_def[f"{prop_name}[]"] = iref.split("/")[-1]
            # Nested properties with items refs
            if "properties" in prop_spec:
                for sub_name, sub_spec in prop_spec["properties"].items():
                    if isinstance(sub_spec, dict):
                        sub_items = sub_spec.get("items", {})
                        if isinstance(sub_items, dict) and "$ref" in sub_items:
                            sub_ref = sub_items["$ref"]
                            if sub_ref.startswith("#/$defs/"):
                                self.property_to_def[
                                    f"{prop_name}.{sub_name}[]"
                                ] = sub_ref.split("/")[-1]

        # $defs internal references
        for def_name, defn in self.schema.get("$defs", {}).items():
            if not isinstance(defn, dict) or "properties" not in defn:
                continue
            for prop_name, prop_spec in defn["properties"].items():
                if not isinstance(prop_spec, dict):
                    continue
                ref = prop_spec.get("$ref", "")
                if ref.startswith("#/$defs/"):
                    self.property_to_def[
                        f"{def_name}.{prop_name}"
                    ] = ref.split("/")[-1]
                items = prop_spec.get("items", {})
                if isinstance(items, dict) and "$ref" in items:
                    iref = items["$ref"]
                    if iref.startswith("#/$defs/"):
                        self.property_to_def[
                            f"{def_name}.{prop_name}[]"
                        ] = iref.split("/")[-1]

    # -- Query methods ------------------------------------------------------

    def fuzzy_codelist_fix(
        self, bad_value: str, field_name: str
    ) -> Optional[str]:
        """Find the closest valid codelist value for a given field.

        Strategy (field-scoped only — never searches across unrelated enums):
          1. Resolve field_name via field_aliases
          2. Case-insensitive exact match
          3. Substring match (exactly ONE match only)
          4. Contained match (exactly ONE valid value in bad_value)
          5. difflib fuzzy match with cutoff=0.7

        Returns the best match or None if no good match found.
        """
        bad_lower = bad_value.lower().strip()
        if not bad_lower:
            return None

        resolved_name = self.field_aliases.get(field_name, field_name)
        valid_values = self.enum_lookup.get(resolved_name, set())

        if not valid_values:
            return None

        # 1. Exact case-insensitive
        for v in valid_values:
            if v.lower() == bad_lower:
                return v

        # 2. Substring: bad_value in exactly ONE valid value
        sub = [v for v in valid_values if bad_lower in v.lower()]
        if len(sub) == 1:
            return sub[0]

        # 3. Contained: exactly ONE valid value (len>=3) in bad_value
        cont = [v for v in valid_values if len(v) >= 3 and v.lower() in bad_lower]
        if len(cont) == 1:
            return cont[0]

        # 4. Fuzzy match with cutoff=0.7
        matches = get_close_matches(
            bad_lower, [v.lower() for v in valid_values], n=1, cutoff=0.7
        )
        if matches:
            for v in valid_values:
                if v.lower() == matches[0]:
                    return v

        return None

    def is_field_required(self, parts: List[str]) -> bool:
        """Check whether a field is required based on its schema position.

        Uses a heuristic: checks the field name against root required fields
        and all $defs required field sets.
        """
        if not parts:
            return False
        field_name = parts[-1]

        # Root required fields
        root_required = set(self.schema.get("required", []))
        if len(parts) == 1 and field_name in root_required:
            return True

        # Check against $defs required fields
        for req_fields in self.required_lookup.values():
            if field_name in req_fields:
                return True

        return False
