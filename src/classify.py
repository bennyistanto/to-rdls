"""
Dataset classification for RDLS components.

Scores datasets against tag weights, keyword patterns, and org hints
to determine which RDLS components (hazard, exposure, vulnerability, loss)
are relevant. Source-independent.
"""

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Union

from .utils import as_list, load_yaml, normalize_text


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class Classification:
    """Result of classifying a dataset for RDLS components."""
    scores: Dict[str, int]
    components: List[str]
    rdls_candidate: bool
    confidence: str  # "high", "medium", "low"
    top_signals: List[str]


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def load_classification_config(yaml_path: Union[str, "Path"]) -> Dict[str, Any]:
    """Load classification config (tag weights, keywords, org hints, scoring params).

    Returns dict with keys: scoring, tag_weights, keyword_patterns, org_hints.
    """
    cfg = load_yaml(yaml_path)
    return {
        "scoring": cfg.get("scoring", {}),
        "components": cfg.get("components", ["hazard", "exposure", "vulnerability_proxy", "loss_impact"]),
        "tag_weights": cfg.get("tag_weights", {}),
        "keyword_patterns": cfg.get("keyword_patterns", {}),
        "org_hints": cfg.get("org_hints", {}),
    }


# Mapping from exclusion group name keywords to RDLS component
_EXCLUSION_GROUP_TO_COMPONENT = {
    "flood": "hazard",
    "population": "exposure",
    "infrastructure": "exposure",
}


def load_exclusion_patterns(
    signal_dict_path: Union[str, "Path"],
) -> Dict[str, List[str]]:
    """Load exclusion patterns from signal_dictionary.yaml.

    Maps each pattern group to an RDLS component based on group name:
        flood* → hazard, population* → exposure, infrastructure* → exposure.

    Args:
        signal_dict_path: Path to signal_dictionary.yaml.

    Returns:
        Dict mapping component names to lists of regex pattern strings.
    """
    cfg = load_yaml(signal_dict_path)
    raw = cfg.get("exclusion_patterns") or {}
    by_component: Dict[str, List[str]] = {}

    for group_name, patterns in raw.items():
        if not isinstance(patterns, list):
            continue
        comp = "hazard"  # default
        for keyword, target in _EXCLUSION_GROUP_TO_COMPONENT.items():
            if keyword in group_name:
                comp = target
                break
        by_component.setdefault(comp, []).extend(patterns)

    return by_component


def _compile_exclusions(
    exclusion_patterns: Dict[str, List[str]],
) -> Dict[str, List["re.Pattern"]]:
    """Compile exclusion regex patterns by component."""
    compiled: Dict[str, List["re.Pattern"]] = {}
    for component, patterns in exclusion_patterns.items():
        compiled[component] = [re.compile(p, re.IGNORECASE) for p in patterns]
    return compiled


def _compile_keywords(keyword_patterns: Dict[str, List[str]]) -> Dict[str, List["re.Pattern"]]:
    """Compile regex patterns from config."""
    compiled = {}
    for component, patterns in keyword_patterns.items():
        compiled[component] = [re.compile(p, re.IGNORECASE) for p in patterns]
    return compiled


# ---------------------------------------------------------------------------
# Classification logic
# ---------------------------------------------------------------------------

def classify_dataset(
    meta: Dict[str, Any],
    config: Dict[str, Any],
    compiled_keywords: Optional[Dict[str, List["re.Pattern"]]] = None,
    compiled_exclusions: Optional[Dict[str, List["re.Pattern"]]] = None,
) -> Classification:
    """Score a dataset and determine RDLS component assignments.

    Args:
        meta: Dataset metadata dict with keys like 'tags', 'title', 'notes',
              'organization' (or source-specific equivalents).
        config: Classification config from load_classification_config().
        compiled_keywords: Pre-compiled keyword patterns (optional optimization).
        compiled_exclusions: Pre-compiled exclusion patterns from
            _compile_exclusions(load_exclusion_patterns(...)). If None, exclusion
            step is skipped.

    Returns:
        Classification result.
    """
    scoring = config.get("scoring", {})
    keyword_hit_weight = scoring.get("keyword_hit_weight", 2)
    exclusion_penalty = scoring.get("exclusion_penalty", 3)
    candidate_min_score = scoring.get("candidate_min_score", 5)
    conf_high = scoring.get("confidence_thresholds", {}).get("high", 7)
    conf_med = scoring.get("confidence_thresholds", {}).get("medium", 4)

    tag_weights = config.get("tag_weights", {})
    org_hints = config.get("org_hints", {})
    components = config.get("components", ["hazard", "exposure", "vulnerability_proxy", "loss_impact"])

    if compiled_keywords is None:
        compiled_keywords = _compile_keywords(config.get("keyword_patterns", {}))

    # Extract text fields
    tags = as_list(meta.get("tags", []))
    tag_names = [t.get("name", "").lower() if isinstance(t, dict) else str(t).lower() for t in tags]
    title = normalize_text(meta.get("title", ""))
    notes = normalize_text(meta.get("notes", ""))
    org_title = ""
    org = meta.get("organization")
    if isinstance(org, dict):
        org_title = org.get("title", "") or org.get("name", "")
    elif isinstance(org, str):
        org_title = org

    text = f"{title} {notes}"
    top_signals: List[str] = []
    scores: Dict[str, int] = {c: 0 for c in components}

    # 1. Tag scoring
    for component in components:
        weights = tag_weights.get(component, {})
        for tag_name in tag_names:
            if tag_name in weights:
                scores[component] += weights[tag_name]
                top_signals.append(f"tag:{tag_name}={weights[tag_name]}")

    # 2. Keyword scoring
    for component in components:
        patterns = compiled_keywords.get(component, [])
        for pat in patterns:
            if pat.search(text):
                scores[component] += keyword_hit_weight
                top_signals.append(f"kw:{pat.pattern}")

    # 3. Org hints
    for org_name, boosts in org_hints.items():
        if org_name.lower() in org_title.lower():
            for component, boost in boosts.items():
                if component in scores:
                    scores[component] += boost
                    top_signals.append(f"org:{org_name}={boost}")

    # 4. Exclusion patterns (false-positive reduction)
    if text and compiled_exclusions:
        for comp, excl_pats in compiled_exclusions.items():
            if comp not in scores:
                continue
            for pat in excl_pats:
                if pat.search(text):
                    scores[comp] = max(0, scores[comp] - exclusion_penalty)
                    top_signals.append(f"excl:{pat.pattern}(-{exclusion_penalty})→{comp}")

    # Determine components and confidence
    active_components = [c for c in components if scores[c] >= conf_med]
    max_score = max(scores.values()) if scores else 0
    rdls_candidate = max_score >= candidate_min_score

    if max_score >= conf_high:
        confidence = "high"
    elif max_score >= conf_med:
        confidence = "medium"
    else:
        confidence = "low"

    return Classification(
        scores=scores,
        components=active_components,
        rdls_candidate=rdls_candidate,
        confidence=confidence,
        top_signals=top_signals[:20],
    )


# ---------------------------------------------------------------------------
# Overrides & dependency enforcement
# ---------------------------------------------------------------------------

def apply_overrides(
    classification: Classification,
    overrides: Dict[str, Any],
    dataset_id: str,
) -> Classification:
    """Apply manual classification overrides.

    Args:
        classification: Original classification.
        overrides: Overrides dict keyed by dataset_id.
        dataset_id: ID of the dataset.

    Returns:
        Modified classification (or original if no override).
    """
    override = overrides.get(dataset_id)
    if not override:
        return classification

    decision = override.get("decision", "keep")
    if decision == "exclude":
        return Classification(
            scores=classification.scores,
            components=[],
            rdls_candidate=False,
            confidence="excluded",
            top_signals=classification.top_signals + ["override:excluded"],
        )

    new_components = override.get("components")
    if new_components:
        return Classification(
            scores=classification.scores,
            components=new_components,
            rdls_candidate=True,
            confidence=classification.confidence,
            top_signals=classification.top_signals + ["override:components"],
        )

    return classification


def enforce_component_deps(
    components: List[str],
    rules: Optional[Dict[str, Any]] = None,
) -> List[str]:
    """Enforce component dependency rules (V/L require H or E).

    If vulnerability or loss appears without hazard or exposure,
    auto-add exposure.

    Args:
        components: List of component names.
        rules: Dependency rules from rdls_defaults.yaml. If None, uses defaults.

    Returns:
        Updated component list.
    """
    comps = set(components)

    # Map classifier names to RDLS names
    name_map = {
        "vulnerability_proxy": "vulnerability",
        "loss_impact": "loss",
    }
    mapped = {name_map.get(c, c) for c in comps}

    needs_he = {"vulnerability", "loss"}
    has_he = {"hazard", "exposure"}

    if mapped & needs_he and not (mapped & has_he):
        mapped.add("exposure")

    return sorted(mapped)
