"""
Validation and QA for RDLS records.

Handles JSON Schema validation, composite confidence scoring,
tiered distribution, and report generation. Source-independent.
"""

import copy
import shutil
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

from .schema import validate_record
from .utils import load_json, load_yaml, write_json


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
        Composite confidence score (0.0–1.0).
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
        Dict of tier → record count.
    """
    dist_dir = Path(dist_dir)
    tier_counts = Counter()

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

    # Tier distribution
    tier_counts = Counter(r["tier"] for r in results)

    # Error categories
    all_errors = []
    for r in results:
        all_errors.extend(r.get("errors", []))
    error_categories = Counter()
    for err in all_errors:
        # Extract path pattern
        parts = err.split(":", 1)
        if len(parts) == 2:
            import re
            path = re.sub(r"\.\d+\.", ".*.", parts[0].strip())
            error_categories[path] += 1

    # Confidence stats
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
