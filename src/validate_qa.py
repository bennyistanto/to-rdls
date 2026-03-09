"""
Validation and QA for RDLS records.

Handles JSON Schema validation, business-rule checking, schema-driven
auto-fix (5-pass engine), composite confidence scoring, tiered distribution,
and report generation. Source-independent.
"""

import copy
import re
import shutil
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from .schema import SchemaContext, validate_record
from .utils import (
    load_json,
    load_yaml,
    navigate_path,
    remove_at_path,
    set_at_path,
    write_json,
)


# ---------------------------------------------------------------------------
# Detailed validation (rich error dicts)
# ---------------------------------------------------------------------------

def validate_against_schema(
    record: Dict[str, Any],
    schema: Dict[str, Any],
) -> List[Dict[str, Any]]:
    """Run JSON Schema validation on a single dataset record.

    Uses Draft 2020-12 validator (Draft 7 fallback).

    Args:
        record: The RDLS dataset dict (NOT wrapped in {"datasets": [...]}).
        schema: The loaded JSON schema dict.

    Returns:
        List of error dicts with keys: path, message, category, validator,
        schema_path, value, expected_type.
    """
    try:
        from jsonschema import Draft202012Validator
    except ImportError:
        try:
            from jsonschema import Draft7Validator as Draft202012Validator
        except ImportError:
            return []

    validator = Draft202012Validator(schema)
    raw_errors = sorted(
        validator.iter_errors(record), key=lambda e: list(e.path)
    )

    results = []
    for err in raw_errors:
        path = ".".join(str(p) for p in err.absolute_path) or "(root)"
        schema_path = ".".join(str(p) for p in err.absolute_schema_path) or "(root)"

        category = categorize_error(err, path)
        expected_type = (
            err.validator_value if err.validator == "type" else None
        )

        results.append({
            "path": path,
            "message": err.message,
            "category": category,
            "validator": err.validator,
            "schema_path": schema_path,
            "value": err.instance,
            "expected_type": expected_type,
        })

    return results


def categorize_error(err: Any, path: str) -> str:
    """Assign a human-readable category to a jsonschema validation error."""
    val = err.validator
    instance = err.instance

    if instance == "" and val in ("enum", "type", "minLength"):
        return "empty_string"
    if instance == "" and val == "minProperties":
        return "empty_string"
    if isinstance(instance, dict) and len(instance) == 0:
        return "empty_object"
    if isinstance(instance, list) and len(instance) == 0:
        return "empty_array"
    if val == "required":
        return "missing_required"
    if val == "enum":
        return "invalid_codelist"
    if val == "type":
        return "wrong_type"
    if val == "minProperties":
        return "empty_object"
    if val == "minItems":
        return "empty_array"
    if val == "minLength":
        return "empty_string"
    return "other"


# ---------------------------------------------------------------------------
# Business rules (beyond JSON Schema)
# ---------------------------------------------------------------------------

_DEFAULT_REQUIRED_ROLES = {"publisher", "creator", "contact_point"}
_DEFAULT_SCHEMA_LINK_PATTERN = "rdls_schema"


def check_business_rules(
    record: Dict[str, Any],
    required_roles: Optional[Set[str]] = None,
    schema_link_pattern: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Check RDLS business rules beyond JSON Schema.

    Args:
        record: RDLS dataset record dict.
        required_roles: Set of attribution roles that must be present.
            Defaults to {"publisher", "creator", "contact_point"}.
        schema_link_pattern: Substring to look for in links[].href to
            validate schema link presence. Defaults to "rdls_schema".

    Returns:
        List of issue dicts with keys: path, message, category.
    """
    if required_roles is None:
        required_roles = _DEFAULT_REQUIRED_ROLES
    if schema_link_pattern is None:
        schema_link_pattern = _DEFAULT_SCHEMA_LINK_PATTERN

    issues: List[Dict[str, Any]] = []

    # 1. Required attribution roles
    found_roles: Set[str] = set()
    for attr in record.get("attributions", []):
        role = attr.get("role", "")
        if role:
            found_roles.add(role)
    missing_roles = required_roles - found_roles
    if missing_roles:
        issues.append({
            "path": "attributions",
            "message": f"Missing required attribution roles: {', '.join(sorted(missing_roles))}",
            "category": "business_rule",
        })

    # 2. Resource must have download_url OR access_url
    for i, res in enumerate(record.get("resources", [])):
        has_download = bool(res.get("download_url", "").strip())
        has_access = bool(res.get("access_url", "").strip())
        if not has_download and not has_access:
            issues.append({
                "path": f"resources.{i}",
                "message": "Resource must have at least download_url or access_url",
                "category": "business_rule",
            })

    # 3. Entity must have email OR url
    for i, attr in enumerate(record.get("attributions", [])):
        entity = attr.get("entity", {})
        has_email = bool(entity.get("email", "").strip())
        has_url = bool(entity.get("url", "").strip())
        if not has_email and not has_url:
            issues.append({
                "path": f"attributions.{i}.entity",
                "message": "Entity must have at least email or url",
                "category": "business_rule",
            })

    # 4. Schema link in links array
    links = record.get("links", [])
    has_schema_link = any(
        lnk.get("rel") == "describedby"
        and schema_link_pattern in lnk.get("href", "")
        for lnk in links
    )
    if not has_schema_link:
        issues.append({
            "path": "links",
            "message": "Missing schema link (rel='describedby') in links array",
            "category": "business_rule",
        })

    # 5. risk_data_type consistency with blocks
    declared_types = set(record.get("risk_data_type", []))
    actual: Set[str] = set()
    if record.get("hazard"):
        actual.add("hazard")
    if record.get("exposure"):
        actual.add("exposure")
    if record.get("vulnerability"):
        actual.add("vulnerability")
    if record.get("loss"):
        actual.add("loss")

    declared_no_block = declared_types - actual
    if declared_no_block:
        issues.append({
            "path": "risk_data_type",
            "message": f"Declared types without corresponding block: {', '.join(sorted(declared_no_block))}",
            "category": "business_rule_warning",
        })
    block_no_declared = actual - declared_types
    if block_no_declared:
        issues.append({
            "path": "risk_data_type",
            "message": f"Blocks present but not declared in risk_data_type: {', '.join(sorted(block_no_declared))}",
            "category": "business_rule_warning",
        })

    # 6. Spatial: countries list should not be empty if scale is national/sub-national
    spatial = record.get("spatial", {})
    scale = spatial.get("scale", "")
    countries = spatial.get("countries", [])
    if scale in ("national", "sub-national") and not countries:
        issues.append({
            "path": "spatial.countries",
            "message": f"spatial.scale is '{scale}' but countries list is empty",
            "category": "business_rule",
        })

    # 7. Metric: quantity_kind=monetary requires currency
    for i, exp_item in enumerate(record.get("exposure", [])):
        if isinstance(exp_item, dict):
            for j, metric in enumerate(exp_item.get("metrics", [])):
                if isinstance(metric, dict):
                    qk = metric.get("quantity_kind", "")
                    cur = metric.get("currency", "")
                    if qk == "monetary" and not cur:
                        issues.append({
                            "path": f"exposure.{i}.metrics.{j}.currency",
                            "message": "currency is required when quantity_kind is monetary",
                            "category": "business_rule",
                        })

    # 8. Loss impact_and_losses: quantity_kind=monetary requires currency
    loss_block = record.get("loss", {})
    if isinstance(loss_block, dict):
        for li, loss in enumerate(loss_block.get("losses", [])):
            if isinstance(loss, dict):
                ial = loss.get("impact_and_losses", {})
                if isinstance(ial, dict):
                    qk = ial.get("quantity_kind", "")
                    cur = ial.get("currency", "")
                    if qk == "monetary" and not cur:
                        issues.append({
                            "path": f"loss.losses.{li}.impact_and_losses.currency",
                            "message": "currency is required when quantity_kind is monetary",
                            "category": "business_rule",
                        })

    return issues


# ---------------------------------------------------------------------------
# Confidence scoring
# ---------------------------------------------------------------------------

def compute_composite_confidence(
    record: Dict[str, Any],
    weights: Optional[Dict[str, float]] = None,
) -> float:
    """Compute weighted composite confidence from HEVL component confidences.

    Args:
        record: RDLS record with optional hazard/exposure/vulnerability/loss blocks.
        weights: Component confidence weights. Defaults to
                 hazard=0.3, exposure=0.3, vulnerability=0.2, loss=0.2.

    Returns:
        Composite confidence score (0.0-1.0).
    """
    if weights is None:
        weights = {
            "hazard": 0.3,
            "exposure": 0.3,
            "vulnerability": 0.2,
            "loss": 0.2,
        }

    scores = []
    for component in ["hazard", "exposure", "vulnerability", "loss"]:
        block = record.get(component)
        if block and isinstance(block, dict):
            conf = block.get("overall_confidence", 0.5)
            if isinstance(conf, (int, float)):
                scores.append((conf, weights.get(component, 0.25)))

    if not scores:
        return 0.0

    weighted_sum = sum(c * w for c, w in scores)
    total_weight = sum(w for _, w in scores)
    return weighted_sum / total_weight if total_weight > 0 else 0.0


# ---------------------------------------------------------------------------
# Distribution tier
# ---------------------------------------------------------------------------

def compute_distribution_tier(
    is_valid: bool,
    confidence: float,
    threshold_high: float = 0.8,
    threshold_medium: float = 0.5,
) -> str:
    """Determine distribution tier from validity and confidence.

    Returns one of: 'high', 'medium', 'low', 'invalid/high', 'invalid/medium', 'invalid/low'.
    """
    if confidence >= threshold_high:
        level = "high"
    elif confidence >= threshold_medium:
        level = "medium"
    else:
        level = "low"

    if is_valid:
        return level
    else:
        return f"invalid/{level}"


# ---------------------------------------------------------------------------
# Batch validation & distribution
# ---------------------------------------------------------------------------

def validate_and_score(
    record: Dict[str, Any],
    schema: Dict[str, Any],
    confidence_weights: Optional[Dict[str, float]] = None,
) -> Dict[str, Any]:
    """Validate a record and compute confidence.

    Returns dict with:
        id, is_valid, errors, confidence, tier
    """
    ds_id = record.get("id", "unknown")
    is_valid, errors = validate_record(record, schema)
    confidence = compute_composite_confidence(record, confidence_weights)
    tier = compute_distribution_tier(is_valid, confidence)

    return {
        "id": ds_id,
        "is_valid": is_valid,
        "errors": errors,
        "error_count": len(errors),
        "confidence": round(confidence, 4),
        "tier": tier,
    }


def distribute_records(
    records: List[Tuple[Dict[str, Any], Dict[str, Any]]],
    dist_dir: Union[str, Path],
    threshold_high: float = 0.8,
    threshold_medium: float = 0.5,
) -> Dict[str, int]:
    """Distribute validated records into tiered folders.

    Args:
        records: List of (record, validation_result) tuples.
        dist_dir: Base distribution directory.
        threshold_high: High confidence threshold.
        threshold_medium: Medium confidence threshold.

    Returns:
        Dict of tier -> record count.
    """
    dist_dir = Path(dist_dir)
    tier_counts: Counter = Counter()

    for record, result in records:
        tier = result["tier"]
        tier_dir = dist_dir / tier
        tier_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{record.get('id', 'unknown')}.json"
        write_json(tier_dir / filename, record)
        tier_counts[tier] += 1

    return dict(tier_counts)


# ---------------------------------------------------------------------------
# Report generation
# ---------------------------------------------------------------------------

def create_validation_report(
    results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Generate a summary report from validation results.

    Returns dict with counts, percentages, and error breakdown.
    """
    total = len(results)
    if total == 0:
        return {"total": 0}

    valid_count = sum(1 for r in results if r["is_valid"])
    invalid_count = total - valid_count

    tier_counts = Counter(r["tier"] for r in results)

    all_errors: List[str] = []
    for r in results:
        all_errors.extend(r.get("errors", []))
    error_categories: Counter = Counter()
    for err in all_errors:
        parts = err.split(":", 1)
        if len(parts) == 2:
            path = re.sub(r"\.\d+\.", ".*.", parts[0].strip())
            error_categories[path] += 1

    confidences = [r["confidence"] for r in results]
    avg_conf = sum(confidences) / len(confidences) if confidences else 0

    return {
        "total": total,
        "valid": valid_count,
        "invalid": invalid_count,
        "valid_pct": round(100 * valid_count / total, 1),
        "tier_distribution": dict(tier_counts),
        "avg_confidence": round(avg_conf, 4),
        "top_errors": dict(error_categories.most_common(20)),
    }


# ===========================================================================
# AUTO-FIX ENGINE (Schema-Driven)
# ===========================================================================
#
# 5 passes:
#   Pass 0 - Structural repair (fix wrong JSON types)
#   Pass 1 - Error-driven fixes (empty removal, type coercion, codelist correction)
#   Pass 2 - Deep clean remaining empties in non-required fields
#   Pass 3 - Structural inference (rebuild missing/empty required fields from context)
#   Pass 4 - Additional properties cleanup (remove fields not in schema)
# ===========================================================================

class AutoFixer:
    """Schema-driven auto-fix engine for RDLS records.

    Rules:
      - Never CREATE new fields that don't already exist in the record
      - Optional empty fields -> REMOVE them
      - Mandatory empty fields -> try to INFER from context within the record
      - All auto-filled mandatory fields are FLAGGED for user review
      - Remove fields NOT defined in schema (with gap field preservation)
      - Fix STRUCTURAL type mismatches (wrong JSON type for a field)

    Args:
        ctx: SchemaContext with all schema-derived lookups.
        defaults: Dict from rdls_defaults.yaml (for hazard_process_defaults, etc.).
        schema_gap_fields: Dict mapping $def names to lists of gap field names
            to preserve during Pass 4 cleanup. Loaded from
            rdls_defaults.yaml ``schema_gap_fields`` section.
    """

    def __init__(
        self,
        ctx: SchemaContext,
        defaults: Dict[str, Any],
        schema_gap_fields: Optional[Dict[str, List[str]]] = None,
    ) -> None:
        self.ctx = ctx
        self.hazard_process_defaults: Dict[str, str] = defaults.get(
            "hazard_process_defaults", {}
        )
        # Convert lists to sets for fast lookup
        self._schema_gap_fields: Dict[str, Set[str]] = {}
        if schema_gap_fields:
            for def_name, fields in schema_gap_fields.items():
                self._schema_gap_fields[def_name] = set(fields)

    # -- Public entry point -------------------------------------------------

    def fix_record(
        self,
        record: Dict[str, Any],
        errors: List[Dict[str, Any]],
        schema: Dict[str, Any],
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        """Apply 5-pass auto-fix to a record.

        Returns:
            (fixed_record, fix_log) where fix_log entries have keys:
            path, action, old, new, severity.
            Severity levels: "auto", "review", "manual".
        """
        fixed = copy.deepcopy(record)
        fix_log: List[Dict[str, Any]] = []

        # Pass 0: Structural repair
        pass0_log = self._structural_repair(fixed)
        fix_log.extend(pass0_log)

        # Re-validate after structural repair
        if pass0_log:
            errors = validate_against_schema(fixed, schema) + check_business_rules(fixed)

        # Track required fields with empty values (for Pass 3)
        required_empties: Set[str] = set()

        # Pass 1: Error-driven fixes
        for err in errors:
            path = err["path"]
            category = err["category"]
            value = err.get("value")
            parts = path.split(".")
            field_name = parts[-1] if parts else ""

            is_required = self.ctx.is_field_required(parts)

            # --- Empty string ---
            if category == "empty_string":
                if is_required:
                    required_empties.add(path)
                else:
                    if remove_at_path(fixed, parts):
                        fix_log.append({
                            "path": path, "action": "removed_empty_string",
                            "old": value, "new": "(field removed)",
                            "severity": "auto",
                        })

            # --- Empty object {} ---
            elif category == "empty_object":
                if is_required:
                    required_empties.add(path)
                else:
                    if remove_at_path(fixed, parts):
                        fix_log.append({
                            "path": path, "action": "removed_empty_object",
                            "old": value, "new": "(field removed)",
                            "severity": "auto",
                        })

            # --- Empty array [] ---
            elif category == "empty_array":
                if is_required:
                    required_empties.add(path)
                else:
                    if remove_at_path(fixed, parts):
                        fix_log.append({
                            "path": path, "action": "removed_empty_array",
                            "old": value, "new": "(field removed)",
                            "severity": "auto",
                        })

            # --- Wrong type (bidirectional: string<->number) ---
            elif category == "wrong_type":
                expected = err.get("expected_type")

                # number/integer -> string coercion
                if isinstance(value, (int, float)) and expected == "string":
                    new_val = (
                        str(int(value))
                        if isinstance(value, int) or value == int(value)
                        else str(value)
                    )
                    if set_at_path(fixed, parts, new_val):
                        fix_log.append({
                            "path": path, "action": "coerced_number_to_string",
                            "old": value, "new": new_val,
                            "severity": "auto",
                        })
                # string -> number coercion
                elif isinstance(value, str) and value.strip():
                    try:
                        new_val = float(value) if "." in value else int(value)
                        if set_at_path(fixed, parts, new_val):
                            fix_log.append({
                                "path": path, "action": "coerced_type",
                                "old": value, "new": new_val,
                                "severity": "auto",
                            })
                    except (ValueError, TypeError):
                        pass
                elif isinstance(value, str) and value.strip() == "":
                    if is_required:
                        required_empties.add(path)
                    else:
                        if remove_at_path(fixed, parts):
                            fix_log.append({
                                "path": path, "action": "removed_empty_for_number",
                                "old": value, "new": "(field removed)",
                                "severity": "auto",
                            })

            # --- Invalid codelist value -> fuzzy match ---
            elif category == "invalid_codelist":
                if isinstance(value, str) and value:
                    new_val = self.ctx.fuzzy_codelist_fix(value, field_name)
                    if new_val and set_at_path(fixed, parts, new_val):
                        fix_log.append({
                            "path": path, "action": "codelist_correction",
                            "old": value, "new": new_val,
                            "severity": "auto",
                        })

        # Pass 2: Deep clean remaining empties in non-required fields
        pass2_log = self._deep_clean_empties(fixed, schema)
        fix_log.extend(pass2_log)

        # Pass 3: Structural inference
        pass3_log = self._infer_missing_required(fixed, schema, required_empties)
        fix_log.extend(pass3_log)

        # Pass 4: Additional properties cleanup
        pass4_log = self._clean_non_schema_fields(fixed)
        fix_log.extend(pass4_log)

        return fixed, fix_log

    # -- Pass 0: Structural Repair ------------------------------------------

    def _structural_repair(self, record: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Fix structural type mismatches in the record."""
        log: List[Dict[str, Any]] = []

        # 1. exposure: object {categories:[...]} -> array [...]
        exposure = record.get("exposure")
        if isinstance(exposure, dict):
            categories = exposure.get("categories", [])
            if isinstance(categories, list) and len(categories) > 0:
                record["exposure"] = categories
                log.append({
                    "path": "exposure",
                    "action": "restructured_exposure (object->array)",
                    "old": f"{{categories: [{len(categories)} items]}}",
                    "new": f"[{len(categories)} Exposure_items]",
                    "severity": "review",
                })
            else:
                for key in exposure:
                    val = exposure[key]
                    if isinstance(val, list) and len(val) > 0 and isinstance(val[0], dict):
                        record["exposure"] = val
                        log.append({
                            "path": "exposure",
                            "action": f"restructured_exposure (object.{key}->array)",
                            "old": f"{{{key}: [{len(val)} items]}}",
                            "new": f"[{len(val)} Exposure_items]",
                            "severity": "review",
                        })
                        break

        # 2-5. hazard structure repairs
        hazard_block = record.get("hazard")
        if isinstance(hazard_block, dict):
            for es_idx, event_set in enumerate(hazard_block.get("event_sets", [])):
                if not isinstance(event_set, dict):
                    continue
                es_path = f"hazard.event_sets.{es_idx}"

                for h_idx, hz in enumerate(event_set.get("hazards", [])):
                    if isinstance(hz, dict):
                        self._repair_hazard_obj(hz, f"{es_path}.hazards.{h_idx}", log)

                for ev_idx, event in enumerate(event_set.get("events", [])):
                    if not isinstance(event, dict):
                        continue
                    ev_hazard = event.get("hazard")
                    if isinstance(ev_hazard, dict):
                        self._repair_hazard_obj(
                            ev_hazard,
                            f"{es_path}.events.{ev_idx}.hazard",
                            log,
                        )

        return log

    def _repair_hazard_obj(
        self, hz: Dict[str, Any], path: str, log: List[Dict[str, Any]]
    ) -> None:
        """Repair structural issues in a Hazard object."""
        # hazard_processes (plural) -> hazard_process (singular)
        if "hazard_processes" in hz:
            old_val = hz.pop("hazard_processes")
            new_val = ""
            if isinstance(old_val, list) and len(old_val) > 0:
                new_val = old_val[0] if isinstance(old_val[0], str) else str(old_val[0])
            elif isinstance(old_val, str):
                new_val = old_val

            existing = hz.get("hazard_process", "")
            if not existing or existing == "" or isinstance(existing, (dict, list)):
                hz["hazard_process"] = new_val
                log.append({
                    "path": f"{path}.hazard_process",
                    "action": "renamed_hazard_processes_to_singular",
                    "old": f"hazard_processes={old_val}",
                    "new": new_val,
                    "severity": "review",
                })

        # hazard_process: array -> string (unwrap)
        hp = hz.get("hazard_process")
        if isinstance(hp, list):
            new_val = hp[0] if len(hp) > 0 and isinstance(hp[0], str) else ""
            hz["hazard_process"] = new_val
            log.append({
                "path": f"{path}.hazard_process",
                "action": "unwrapped_array_to_string",
                "old": str(hp), "new": new_val,
                "severity": "review",
            })

        # hazard_process: {} (empty object) -> "" (empty string)
        hp = hz.get("hazard_process")
        if isinstance(hp, dict):
            hz["hazard_process"] = ""
            log.append({
                "path": f"{path}.hazard_process",
                "action": "replaced_empty_object_with_string",
                "old": "{}", "new": "(empty, will infer in Pass 3)",
                "severity": "auto",
            })

        # Remove dot-path keys (e.g. "occurrence.deterministic.thresholds")
        dot_keys = [k for k in hz.keys() if "." in k]
        for dk in dot_keys:
            old_val = hz.pop(dk)
            log.append({
                "path": f"{path}.{dk}",
                "action": "removed_dot_path_key",
                "old": str(old_val)[:60],
                "new": "(field removed)",
                "severity": "auto",
            })

    # -- Pass 2: Deep clean empties -----------------------------------------

    def _deep_clean_empties(
        self,
        obj: Any,
        schema: Dict[str, Any],
        path: str = "",
    ) -> List[Dict[str, Any]]:
        """Recursively remove empty strings/dicts/arrays from non-required fields."""
        log: List[Dict[str, Any]] = []
        if not isinstance(obj, dict):
            return log

        # Gather all known required field names (heuristic)
        all_required: Set[str] = set(schema.get("required", []))
        for req_fields in self.ctx.required_lookup.values():
            all_required |= req_fields

        keys_to_remove: List[str] = []
        for key, val in list(obj.items()):
            field_path = f"{path}.{key}" if path else key
            if isinstance(val, dict):
                sub_schema = (
                    schema.get("properties", {}).get(key, {})
                    if isinstance(schema, dict)
                    else {}
                )
                log.extend(self._deep_clean_empties(val, sub_schema, field_path))
                if not val and key not in all_required:
                    keys_to_remove.append(key)
                    log.append({
                        "path": field_path,
                        "action": "deep_clean_empty_object",
                        "old": {},
                        "new": "(field removed)",
                        "severity": "auto",
                    })
            elif isinstance(val, list):
                if not val and key not in all_required:
                    keys_to_remove.append(key)
                    log.append({
                        "path": field_path,
                        "action": "deep_clean_empty_array",
                        "old": [],
                        "new": "(field removed)",
                        "severity": "auto",
                    })
                else:
                    for i, item in enumerate(val):
                        if isinstance(item, dict):
                            items_schema = (
                                schema.get("properties", {})
                                .get(key, {})
                                .get("items", {})
                                if isinstance(schema, dict)
                                else {}
                            )
                            log.extend(
                                self._deep_clean_empties(
                                    item, items_schema, f"{field_path}.{i}"
                                )
                            )
            elif val == "" and key not in all_required:
                keys_to_remove.append(key)
                log.append({
                    "path": field_path,
                    "action": "deep_clean_empty_string",
                    "old": "",
                    "new": "(field removed)",
                    "severity": "auto",
                })
        for key in keys_to_remove:
            del obj[key]
        return log

    # -- Pass 3: Structural inference ---------------------------------------

    def _infer_missing_required(
        self,
        record: Dict[str, Any],
        schema: Dict[str, Any],
        required_empties: Set[str],
    ) -> List[Dict[str, Any]]:
        """Fill missing/empty required fields by inferring from context."""
        log: List[Dict[str, Any]] = []

        # --- Hazard event_sets ---
        hazard_block = record.get("hazard", {})
        if isinstance(hazard_block, dict):
            for es_idx, event_set in enumerate(hazard_block.get("event_sets", [])):
                if not isinstance(event_set, dict):
                    continue
                es_path = f"hazard.event_sets.{es_idx}"
                es_calc = event_set.get("calculation_method")
                es_analysis = event_set.get("analysis_type", "")
                es_hazards = event_set.get("hazards", [])
                primary_hazard = es_hazards[0] if es_hazards else {}

                # Fix hazard_process in event_set.hazards[]
                for h_idx, hz in enumerate(es_hazards):
                    if not isinstance(hz, dict):
                        continue
                    hp = hz.get("hazard_process", "")
                    if not hp:
                        inferred = self._infer_hazard_process_from_events(event_set)
                        if inferred:
                            hz["hazard_process"] = inferred
                            log.append({
                                "path": f"{es_path}.hazards.{h_idx}.hazard_process",
                                "action": "inferred_from_events",
                                "old": hp or "(missing)", "new": inferred,
                                "severity": "review",
                            })
                        else:
                            hz_type = hz.get("type", "")
                            default_proc = self.hazard_process_defaults.get(hz_type)
                            if default_proc:
                                hz["hazard_process"] = default_proc
                                log.append({
                                    "path": f"{es_path}.hazards.{h_idx}.hazard_process",
                                    "action": f"default_from_hazard_type ({hz_type})",
                                    "old": hp or "(missing)", "new": default_proc,
                                    "severity": "review",
                                })

                # Fix individual events
                for ev_idx, event in enumerate(event_set.get("events", [])):
                    if not isinstance(event, dict):
                        continue
                    ev_path = f"{es_path}.events.{ev_idx}"

                    # calculation_method -> inherit from event_set
                    cm = event.get("calculation_method", "")
                    if not cm and es_calc:
                        event["calculation_method"] = es_calc
                        log.append({
                            "path": f"{ev_path}.calculation_method",
                            "action": "inherited_from_event_set",
                            "old": cm or "(missing)", "new": es_calc,
                            "severity": "review",
                        })

                    # hazard block -> copy from event_set.hazards[0]
                    ev_hazard = event.get("hazard", {})
                    if not ev_hazard and primary_hazard.get("id"):
                        event["hazard"] = {
                            k: v
                            for k, v in primary_hazard.items()
                            if k in ("id", "type", "hazard_process", "intensity_measure")
                            and v
                        }
                        log.append({
                            "path": f"{ev_path}.hazard",
                            "action": "copied_from_event_set_hazards",
                            "old": "(missing)",
                            "new": f"{{id: {primary_hazard.get('id')}}}",
                            "severity": "review",
                        })
                    elif isinstance(ev_hazard, dict):
                        ev_hp = ev_hazard.get("hazard_process", "")
                        if not ev_hp:
                            if primary_hazard.get("hazard_process"):
                                ev_hazard["hazard_process"] = primary_hazard[
                                    "hazard_process"
                                ]
                                log.append({
                                    "path": f"{ev_path}.hazard.hazard_process",
                                    "action": "copied_from_event_set_hazard",
                                    "old": ev_hp or "(missing)",
                                    "new": primary_hazard["hazard_process"],
                                    "severity": "review",
                                })
                            else:
                                ev_type = ev_hazard.get("type", "")
                                default_proc = self.hazard_process_defaults.get(ev_type)
                                if default_proc:
                                    ev_hazard["hazard_process"] = default_proc
                                    log.append({
                                        "path": f"{ev_path}.hazard.hazard_process",
                                        "action": f"default_from_hazard_type ({ev_type})",
                                        "old": ev_hp or "(missing)",
                                        "new": default_proc,
                                        "severity": "review",
                                    })

                    # occurrence -> add placeholder
                    occ = event.get("occurrence")
                    if not occ or (isinstance(occ, dict) and len(occ) == 0):
                        occurrence, desc = self._build_occurrence_placeholder(
                            es_analysis
                        )
                        event["occurrence"] = occurrence
                        log.append({
                            "path": f"{ev_path}.occurrence",
                            "action": "added_placeholder",
                            "old": str(occ) if occ else "(missing)",
                            "new": desc,
                            "severity": "review",
                        })

        # --- Vulnerability socio_economic ---
        vuln_block = record.get("vulnerability", {})
        if isinstance(vuln_block, dict):
            for se_idx, se in enumerate(vuln_block.get("socio_economic", [])):
                if isinstance(se, dict):
                    ry = se.get("reference_year")
                    if ry is None or ry == "" or ry == 0:
                        se["reference_year"] = 1900
                        log.append({
                            "path": f"vulnerability.socio_economic.{se_idx}.reference_year",
                            "action": "added_schema_minimum",
                            "old": str(ry) if ry is not None else "(missing)",
                            "new": 1900,
                            "severity": "review",
                        })

        return log

    @staticmethod
    def _infer_hazard_process_from_events(
        event_set: Dict[str, Any],
    ) -> Optional[str]:
        """Look at events in an event_set to find a hazard_process value."""
        for ev in event_set.get("events", []):
            if isinstance(ev, dict):
                ev_hz = ev.get("hazard", {})
                if isinstance(ev_hz, dict) and ev_hz.get("hazard_process"):
                    hp = ev_hz["hazard_process"]
                    if isinstance(hp, str) and hp:
                        return hp
        return None

    @staticmethod
    def _build_occurrence_placeholder(
        analysis_type: str,
    ) -> Tuple[Dict[str, Any], str]:
        """Build a schema-valid occurrence placeholder based on analysis_type."""
        if analysis_type == "deterministic":
            return (
                {"deterministic": {"index_criteria": "Scenario-based deterministic analysis"}},
                "deterministic placeholder",
            )
        elif analysis_type == "probabilistic":
            return (
                {"probabilistic": {"return_period": 0}},
                "probabilistic placeholder (return_period=0)",
            )
        elif analysis_type == "empirical":
            return (
                {"empirical": {"return_period": 0}},
                "empirical placeholder (return_period=0)",
            )
        else:
            return (
                {"deterministic": {"index_criteria": "Scenario-based analysis"}},
                f"deterministic fallback (analysis_type={analysis_type!r})",
            )

    # -- Pass 4: Additional properties cleanup ------------------------------

    def _clean_non_schema_fields(
        self, record: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Remove fields from the record that are NOT defined in the RDLS schema."""
        log: List[Dict[str, Any]] = []
        schema = self.ctx.schema

        root_allowed = self.ctx.allowed_props.get("root", set())
        self._clean_obj(record, root_allowed, "root", "", log)

        spatial = record.get("spatial")
        if isinstance(spatial, dict):
            allowed = self.ctx.allowed_props.get("Location", set())
            if allowed:
                self._clean_obj(spatial, allowed, "Location", "spatial", log)

        for i, attr in enumerate(record.get("attributions", [])):
            if isinstance(attr, dict):
                allowed = self.ctx.allowed_props.get("Attribution", set())
                if allowed:
                    self._clean_obj(attr, allowed, "Attribution", f"attributions.{i}", log)
                entity = attr.get("entity")
                if isinstance(entity, dict):
                    e_allowed = self.ctx.allowed_props.get("Entity", set())
                    if e_allowed:
                        self._clean_obj(entity, e_allowed, "Entity", f"attributions.{i}.entity", log)

        for i, src in enumerate(record.get("sources", [])):
            if isinstance(src, dict):
                allowed = self.ctx.allowed_props.get("Source", set())
                if allowed:
                    self._clean_obj(src, allowed, "Source", f"sources.{i}", log)

        for i, ref in enumerate(record.get("referenced_by", [])):
            if isinstance(ref, dict):
                allowed = self.ctx.allowed_props.get("Related_resource", set())
                if allowed:
                    self._clean_obj(ref, allowed, "Related_resource", f"referenced_by.{i}", log)

        for i, res in enumerate(record.get("resources", [])):
            if isinstance(res, dict):
                allowed = self.ctx.allowed_props.get("Resource", set())
                if allowed:
                    self._clean_obj(res, allowed, "Resource", f"resources.{i}", log)
                temporal = res.get("temporal")
                if isinstance(temporal, dict):
                    t_allowed = self.ctx.allowed_props.get("Period", set())
                    if t_allowed:
                        self._clean_obj(temporal, t_allowed, "Period", f"resources.{i}.temporal", log)

        for i, link in enumerate(record.get("links", [])):
            if isinstance(link, dict):
                allowed = self.ctx.allowed_props.get("Link", set())
                if allowed:
                    self._clean_obj(link, allowed, "Link", f"links.{i}", log)

        for i, exp_item in enumerate(record.get("exposure", [])):
            if isinstance(exp_item, dict):
                allowed = self.ctx.allowed_props.get("Exposure_item", set())
                if allowed:
                    self._clean_obj(
                        exp_item, allowed, "Exposure_item", f"exposure.{i}", log,
                        schema_gap_fields=self._schema_gap_fields.get("Exposure_item"),
                    )
                for j, metric in enumerate(exp_item.get("metrics", [])):
                    if isinstance(metric, dict):
                        m_allowed = self.ctx.allowed_props.get("Metric", set())
                        if m_allowed:
                            self._clean_obj(
                                metric, m_allowed, "Metric", f"exposure.{i}.metrics.{j}", log,
                                schema_gap_fields=self._schema_gap_fields.get("Metric"),
                            )

        hazard_block = record.get("hazard")
        if isinstance(hazard_block, dict):
            h_allowed = set(
                schema.get("properties", {})
                .get("hazard", {})
                .get("properties", {})
                .keys()
            )
            if h_allowed:
                self._clean_obj(hazard_block, h_allowed, "hazard_root", "hazard", log)
            for es_i, event_set in enumerate(hazard_block.get("event_sets", [])):
                if isinstance(event_set, dict):
                    es_allowed = self.ctx.allowed_props.get("Event_set", set())
                    if es_allowed:
                        self._clean_obj(
                            event_set, es_allowed, "Event_set",
                            f"hazard.event_sets.{es_i}", log,
                        )
                    for h_i, hz in enumerate(event_set.get("hazards", [])):
                        if isinstance(hz, dict):
                            hz_allowed = self.ctx.allowed_props.get("Hazard", set())
                            if hz_allowed:
                                self._clean_obj(
                                    hz, hz_allowed, "Hazard",
                                    f"hazard.event_sets.{es_i}.hazards.{h_i}", log,
                                )
                    for ev_i, event in enumerate(event_set.get("events", [])):
                        if isinstance(event, dict):
                            ev_allowed = self.ctx.allowed_props.get("Event", set())
                            if ev_allowed:
                                self._clean_obj(
                                    event, ev_allowed, "Event",
                                    f"hazard.event_sets.{es_i}.events.{ev_i}", log,
                                )
                            ev_hz = event.get("hazard")
                            if isinstance(ev_hz, dict):
                                hz_allowed = self.ctx.allowed_props.get("Hazard", set())
                                if hz_allowed:
                                    self._clean_obj(
                                        ev_hz, hz_allowed, "Hazard",
                                        f"hazard.event_sets.{es_i}.events.{ev_i}.hazard",
                                        log,
                                    )

        vuln_block = record.get("vulnerability")
        if isinstance(vuln_block, dict):
            functions = vuln_block.get("functions")
            if isinstance(functions, dict):
                _func_def_map = {
                    "vulnerability": "VulnerabilityFunction",
                    "fragility": "FragilityFunction",
                    "damage_to_loss": "DamageToLossFunction",
                    "engineering_demand": "EngineeringDemandFunction",
                }
                for func_type, def_name in _func_def_map.items():
                    func_list = functions.get(func_type, [])
                    if isinstance(func_list, list):
                        for fi, func in enumerate(func_list):
                            if isinstance(func, dict):
                                f_allowed = self.ctx.allowed_props.get(def_name, set())
                                if f_allowed:
                                    self._clean_obj(
                                        func, f_allowed, def_name,
                                        f"vulnerability.functions.{func_type}.{fi}",
                                        log,
                                        schema_gap_fields=self._schema_gap_fields.get(def_name),
                                    )
            for se_i, se in enumerate(vuln_block.get("socio_economic", [])):
                if isinstance(se, dict):
                    se_allowed = self.ctx.allowed_props.get("SocioEconomicIndex", set())
                    if se_allowed:
                        self._clean_obj(
                            se, se_allowed, "SocioEconomicIndex",
                            f"vulnerability.socio_economic.{se_i}", log,
                            schema_gap_fields=self._schema_gap_fields.get("SocioEconomicIndex"),
                        )

        loss_block = record.get("loss")
        if isinstance(loss_block, dict):
            for li, loss in enumerate(loss_block.get("losses", [])):
                if isinstance(loss, dict):
                    l_allowed = self.ctx.allowed_props.get("Losses", set())
                    if l_allowed:
                        self._clean_obj(
                            loss, l_allowed, "Losses", f"loss.losses.{li}", log,
                        )

        return log

    @staticmethod
    def _clean_obj(
        obj: Dict[str, Any],
        allowed: Set[str],
        context: str,
        path: str,
        log: List[Dict[str, Any]],
        schema_gap_fields: Optional[Set[str]] = None,
    ) -> None:
        """Remove keys from obj that are not in the allowed set."""
        if not allowed:
            return
        preserve = schema_gap_fields or set()
        keys_to_remove = [
            key for key in obj.keys()
            if key not in allowed and key not in preserve
        ]
        for key in keys_to_remove:
            old_val = obj.pop(key)
            field_path = f"{path}.{key}" if path else key
            old_preview = (
                str(old_val)[:60]
                if not isinstance(old_val, str)
                else old_val[:60]
            )
            log.append({
                "path": field_path,
                "action": f"removed_non_schema_field ({context})",
                "old": old_preview,
                "new": "(field removed)",
                "severity": "auto",
            })
