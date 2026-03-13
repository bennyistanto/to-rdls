"""LLM-assisted HEVL classification pipeline for RDLS records.

4-phase pipeline:
  Phase 1: Signal triage (existing regex, fast, free)
  Phase 2: Column enrichment (CKAN API, cached)
  Phase 3: LLM classification (Claude Haiku, ~$7)
  Phase 4: Validation + merge

Usage:
    python -m src.llm_review \\
        --dist-dir path/to/rdls/dist \\
        --metadata-dir path/to/dataset_metadata \\
        --output-dir output/llm \\
        [--config configs/llm_review.yaml] \\
        [--dry-run] [--max-records N]
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import pickle
import random
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple

from .ckan_columns import ColumnCache, ColumnInfo, load_columns_for_uuid
from .hdx_review import (
    HEVLAssessment,
    ReviewableRecord,
    _scan_dist_tiers,
    assess_hevl,
    build_hdx_index,
    load_rdls_record,
    revise_record,
)
from .naming import (
    build_rdls_id_with_collision,
    encode_component_types,
    load_naming_config,
    parse_rdls_id,
)
from .utils import load_json, load_yaml, write_json


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class LLMClassification:
    """Structured LLM response for one record."""

    rdls_id: str
    is_rdls_relevant: bool
    components: Dict[str, bool]            # {"hazard": T/F, ...}
    component_reasoning: Dict[str, str]    # per-component explanation
    overall_reasoning: str
    confidence: float                      # 0.0 - 1.0
    domain_category: str                   # disaster_risk, humanitarian_ops, ...
    llm_model: str
    prompt_hash: str
    token_usage: Dict[str, int]            # {"input": N, "output": N}


@dataclass
class TriageBucket:
    """Phase 1 triage results."""

    confident: List[str] = field(default_factory=list)       # rdls_ids
    borderline: List[str] = field(default_factory=list)
    no_signal: List[str] = field(default_factory=list)
    validation_sample: List[str] = field(default_factory=list)  # subset of confident


@dataclass
class ReviewConfig:
    """Pipeline configuration loaded from YAML."""

    # Phase 1
    confident_score_min: int = 5
    max_components_for_confident: int = 2
    validation_sample_pct: float = 0.05
    # Phase 2
    ckan_base_url: str = "https://data.humdata.org/api/3/action"
    ckan_delay: float = 0.5
    ckan_timeout: float = 15.0
    column_cache_dir: str = "output/column_cache"
    max_resources_per_dataset: int = 10
    # Phase 3
    llm_model: str = "claude-haiku-4-5-20251001"
    llm_temperature: float = 0.0
    llm_max_tokens: int = 400
    llm_max_concurrent: int = 2
    llm_max_retries: int = 5
    llm_timeout: float = 30.0
    llm_cache_dir: str = "output/llm_review/cache"
    max_cost_usd: float = 15.0
    cost_per_mtok_input: float = 1.00
    cost_per_mtok_output: float = 5.00
    # Phase 4
    llm_overrides_signals: bool = True
    disagreement_confidence_min: float = 0.7
    # Prompt
    description_max_chars: int = 500
    methodology_max_chars: int = 300
    max_resources_shown: int = 20
    max_columns_shown: int = 50

    @classmethod
    def from_yaml(cls, path: Path) -> "ReviewConfig":
        """Load config from YAML file."""
        raw = load_yaml(path)
        c = cls()
        # Phase 1
        t = raw.get("triage", {})
        c.confident_score_min = t.get("confident_score_min", c.confident_score_min)
        c.max_components_for_confident = t.get("max_components_for_confident", c.max_components_for_confident)
        c.validation_sample_pct = t.get("validation_sample_pct", c.validation_sample_pct)
        # Phase 2
        k = raw.get("ckan", {})
        c.ckan_base_url = k.get("base_url", c.ckan_base_url)
        c.ckan_delay = k.get("delay_seconds", c.ckan_delay)
        c.ckan_timeout = k.get("timeout_seconds", c.ckan_timeout)
        c.column_cache_dir = k.get("cache_dir", c.column_cache_dir)
        c.max_resources_per_dataset = k.get("max_resources_per_dataset", c.max_resources_per_dataset)
        # Phase 3
        llm = raw.get("llm", {})
        c.llm_model = llm.get("model", c.llm_model)
        c.llm_temperature = llm.get("temperature", c.llm_temperature)
        c.llm_max_tokens = llm.get("max_tokens", c.llm_max_tokens)
        c.llm_max_concurrent = llm.get("max_concurrent", c.llm_max_concurrent)
        c.llm_max_retries = llm.get("max_retries", c.llm_max_retries)
        c.llm_timeout = llm.get("timeout_seconds", c.llm_timeout)
        c.llm_cache_dir = llm.get("cache_dir", c.llm_cache_dir)
        c.max_cost_usd = llm.get("max_cost_usd", c.max_cost_usd)
        c.cost_per_mtok_input = llm.get("cost_per_mtok_input", c.cost_per_mtok_input)
        c.cost_per_mtok_output = llm.get("cost_per_mtok_output", c.cost_per_mtok_output)
        # Phase 4
        m = raw.get("merge", {})
        c.llm_overrides_signals = m.get("llm_overrides_signals", c.llm_overrides_signals)
        c.disagreement_confidence_min = m.get("disagreement_confidence_min", c.disagreement_confidence_min)
        # Prompt
        p = raw.get("prompt", {})
        c.description_max_chars = p.get("description_max_chars", c.description_max_chars)
        c.methodology_max_chars = p.get("methodology_max_chars", c.methodology_max_chars)
        c.max_resources_shown = p.get("max_resources_shown", c.max_resources_shown)
        c.max_columns_shown = p.get("max_columns_shown", c.max_columns_shown)
        return c


def load_review_config(
    yaml_path: Optional[Path] = None,
) -> ReviewConfig:
    """Load LLM review config from YAML, with defaults fallback."""
    if yaml_path is None:
        yaml_path = Path(__file__).resolve().parent.parent / "configs" / "llm_review.yaml"
    if yaml_path.exists():
        return ReviewConfig.from_yaml(yaml_path)
    return ReviewConfig()


# ---------------------------------------------------------------------------
# System prompt
# ---------------------------------------------------------------------------

_SYSTEM_PROMPT = """You are an expert in disaster risk data classification. You classify datasets into RDLS (Risk Data Library Standard) components.

RDLS defines four data components:
- HAZARD: Data describing natural hazard events or scenarios (flood maps, earthquake ground motion, cyclone wind fields, drought indices). Must represent a physical phenomenon, not just mention a hazard name.
- EXPOSURE: Data describing assets, population, or infrastructure AT RISK (building footprints, population grids, road networks, land use maps). Must enumerate things that could be damaged.
- VULNERABILITY: Data describing damage/loss relationships (fragility curves, damage functions, depth-damage curves). Rare. Must define how damage scales with hazard intensity.
- LOSS: Data describing disaster impacts, damage assessments, economic losses, casualties, displacement, or modeled risk results (AAL, PML, loss exceedance curves). Also includes humanitarian key figures and situation reports.

Key distinctions:
- "Key Figures" / humanitarian indicators = primarily LOSS (not all four)
- Admin boundaries / reference layers = NOT RDLS relevant
- Climate projections (RCP/SSP) without specific hazard events = NOT hazard
- Population census without hazard context = NOT exposure (just demographics)
- Needs assessments (MSNA, DTM, displacement tracking) = primarily LOSS
- Humanitarian response / who-does-what / 3W/4W = NOT RDLS relevant
- Food prices / market monitoring = NOT RDLS relevant (contextual economic data, not asset inventories)
- Health indicators with mortality/morbidity data = LOSS (if disaster-related) or NOT RDLS (if general health)
- A dataset can legitimately have multiple components (e.g., damage assessment = EXPOSURE + LOSS)

Respond with ONLY a JSON object. No markdown, no explanation outside JSON."""


# ---------------------------------------------------------------------------
# Phase 1: Triage
# ---------------------------------------------------------------------------

def triage_records(
    assessments: Dict[str, HEVLAssessment],
    config: ReviewConfig,
) -> TriageBucket:
    """Classify records into confident / borderline / no-signal buckets."""
    bucket = TriageBucket()

    for rdls_id, asmt in assessments.items():
        scores = asmt.component_scores
        max_score = max(scores.values()) if scores else 0
        active_components = sum(1 for s in scores.values() if s > 0)

        if max_score == 0:
            bucket.no_signal.append(rdls_id)
        elif (
            max_score >= config.confident_score_min
            and active_components <= config.max_components_for_confident
        ):
            bucket.confident.append(rdls_id)
        else:
            bucket.borderline.append(rdls_id)

    # Sample validation set from confident
    n_sample = max(1, int(len(bucket.confident) * config.validation_sample_pct))
    if bucket.confident:
        bucket.validation_sample = random.sample(
            bucket.confident, min(n_sample, len(bucket.confident)),
        )

    return bucket


# ---------------------------------------------------------------------------
# Phase 3: Prompt building
# ---------------------------------------------------------------------------

def _prompt_hash(prompt: str) -> str:
    """Deterministic hash of prompt text for caching."""
    return hashlib.sha256(prompt.encode("utf-8")).hexdigest()[:16]


def build_classification_prompt(
    reviewable: ReviewableRecord,
    hdx_meta: Dict[str, Any],
    column_infos: List[ColumnInfo],
    assessment: Optional[HEVLAssessment],
    config: ReviewConfig,
) -> str:
    """Build the user prompt for LLM classification."""
    parts: List[str] = []

    # Metadata
    title = hdx_meta.get("title", "") or reviewable.record.get("title", "")
    desc = hdx_meta.get("notes", "") or reviewable.record.get("description", "")
    if len(desc) > config.description_max_chars:
        desc = desc[: config.description_max_chars] + "..."
    tags = hdx_meta.get("tags", [])
    tag_names = [t.get("name", "") if isinstance(t, dict) else str(t) for t in tags]
    org = ""
    org_obj = hdx_meta.get("organization")
    if isinstance(org_obj, dict):
        org = org_obj.get("title", "") or org_obj.get("name", "")
    elif isinstance(org_obj, str):
        org = org_obj
    methodology = (hdx_meta.get("methodology", "") or "")
    if len(methodology) > config.methodology_max_chars:
        methodology = methodology[: config.methodology_max_chars] + "..."

    parts.append("Classify this dataset's ACTUAL DATA CONTENT into RDLS components.")
    parts.append("")
    parts.append("## RDLS Component Definitions (classify by DATA CONTENT, not topic)")
    parts.append(
        "- **hazard**: The dataset CONTAINS hazard measurements or models "
        "(e.g., ShakeMap, PAGER, flood depth grids, SPI/PDSI drought indices, "
        "wind speed maps, event catalogs with physical parameters, hazard scenarios). "
        "A dataset that merely REFERENCES a hazard event (e.g., 'Earthquake Damage Assessment') "
        "does NOT contain hazard data — the earthquake is the CONTEXT, not the content."
    )
    parts.append(
        "- **exposure**: The dataset CONTAINS spatial inventories of assets or populations at risk "
        "(e.g., building footprints, population grids, infrastructure databases, land use maps, "
        "facility registries with locations). Columns like 'Total Affected' or 'people in need' "
        "are LOSS metrics, not exposure inventories."
    )
    parts.append(
        "- **vulnerability**: The dataset CONTAINS damage/fragility functions or susceptibility models "
        "(e.g., depth-damage curves, structural fragility parameters, vulnerability indices "
        "that relate hazard intensity to damage). Columns like 'Building Damaged (Severe/Moderate/Minor)' "
        "are post-event LOSS observations, not vulnerability functions."
    )
    parts.append(
        "- **loss**: The dataset CONTAINS post-event impact data, damage assessments, or humanitarian "
        "key figures (e.g., casualties, displacement counts, building damage status, affected populations, "
        "economic losses, operational status after a disaster, food insecurity figures, livestock deaths). "
        "The hazard that caused the loss belongs in loss.hazard_type, NOT as a separate hazard component."
    )
    parts.append("")
    parts.append(
        "CRITICAL: Ask 'what does the DATA actually contain?' not 'what TOPIC is the dataset about?' "
        "A spreadsheet with columns like 'Damaged (Y/N)', 'Status (Open/Closed)', 'Total Affected' "
        "is LOSS data, even if the title says 'Earthquake' or 'Flood'. "
        "Only mark hazard=true if the dataset contains actual hazard measurements/models."
    )
    parts.append("")
    parts.append("## Metadata")
    parts.append(f"- Title: {title}")
    parts.append(f"- Description: {desc}")
    if tag_names:
        parts.append(f"- Tags: {', '.join(tag_names[:15])}")
    if org:
        parts.append(f"- Organization: {org}")
    if methodology:
        parts.append(f"- Methodology: {methodology}")

    # Resources
    resources = hdx_meta.get("resources", [])
    parts.append("")
    parts.append(f"## Resources ({len(resources)} files)")
    for i, res in enumerate(resources[: config.max_resources_shown]):
        rname = res.get("name", "") or res.get("title", "")
        rfmt = res.get("format", "")
        parts.append(f"- {rname} ({rfmt})")

    # Column headers
    parts.append("")
    if column_infos:
        all_cols: List[str] = []
        for ci in column_infos:
            label = ci.resource_name or ci.resource_id
            if ci.sheet_name:
                label += f" [{ci.sheet_name}]"
            cols_str = ", ".join(str(c) for c in ci.columns[: config.max_columns_shown])
            if len(ci.columns) > config.max_columns_shown:
                cols_str += f" ... (+{len(ci.columns) - config.max_columns_shown} more)"
            all_cols.append(f"- {label}: {cols_str}")
        parts.append("## Column Headers (from data files)")
        parts.extend(all_cols)
    else:
        parts.append("## Column Headers")
        parts.append("Not available (no tabular data in cache)")

    # Signal hints
    if assessment:
        parts.append("")
        parts.append("## Signal Hints (automated pre-screening)")
        parts.append(f"Current classification: {', '.join(assessment.current_rdt)}")
        scores = assessment.component_scores
        parts.append(
            f"Signal scores: H={scores.get('H', 0)} "
            f"E={scores.get('E', 0)} "
            f"V={scores.get('V', 0)} "
            f"L={scores.get('L', 0)}"
        )

    # Response format
    parts.append("")
    parts.append("## Instructions")
    parts.append(
        'Respond with ONLY a JSON object:\n'
        '{\n'
        '  "is_rdls_relevant": true/false,\n'
        '  "components": {\n'
        '    "hazard": true/false,\n'
        '    "exposure": true/false,\n'
        '    "vulnerability": true/false,\n'
        '    "loss": true/false\n'
        '  },\n'
        '  "reasoning": {\n'
        '    "hazard": "one sentence why yes/no",\n'
        '    "exposure": "one sentence why yes/no",\n'
        '    "vulnerability": "one sentence why yes/no",\n'
        '    "loss": "one sentence why yes/no"\n'
        '  },\n'
        '  "overall": "one sentence summary",\n'
        '  "confidence": 0.0-1.0,\n'
        '  "domain": "disaster_risk|humanitarian_ops|climate|health|reference|other"\n'
        '}'
    )

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Phase 3: LLM calling
# ---------------------------------------------------------------------------

class LLMResponseCache:
    """Disk-backed cache for LLM responses, keyed by prompt hash."""

    def __init__(self, cache_dir: Path):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, phash: str) -> Path:
        return self.cache_dir / f"{phash}.json"

    def get(self, phash: str) -> Optional[Dict[str, Any]]:
        p = self._path(phash)
        if p.exists():
            try:
                return json.loads(p.read_text(encoding="utf-8"))
            except (json.JSONDecodeError, OSError):
                return None
        return None

    def put(self, phash: str, data: Dict[str, Any]) -> None:
        p = self._path(phash)
        p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def _is_rate_limit(err: Exception) -> bool:
    """Check if error is a 429 rate-limit error."""
    err_str = str(err)
    return "429" in err_str or "rate_limit" in err_str


def _is_connection_error(err: Exception) -> bool:
    """Check if error is a transient connection error."""
    err_str = str(err).lower()
    return any(k in err_str for k in ("connection", "timeout", "reset", "broken pipe"))


def call_llm(
    user_prompt: str,
    config: ReviewConfig,
    client: Any,
) -> Tuple[Dict[str, Any], Dict[str, int]]:
    """Call Claude API and return (parsed_response, token_usage).

    Retries with rate-limit-aware backoff:
      - 429 rate limit: wait 30-60s (the API needs time to reset)
      - Connection error: wait 5-15s with jitter
      - JSON parse error: wait 2-4s, append JSON instruction
    """
    last_error = None
    for attempt in range(config.llm_max_retries):
        try:
            message = client.messages.create(
                model=config.llm_model,
                max_tokens=config.llm_max_tokens,
                temperature=config.llm_temperature,
                system=_SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_prompt}],
            )

            # Extract text content
            text = ""
            for block in message.content:
                if hasattr(block, "text"):
                    text += block.text

            # Parse JSON
            text = text.strip()
            # Strip markdown code fences if present
            if text.startswith("```"):
                lines = text.split("\n")
                text = "\n".join(lines[1:-1]) if len(lines) > 2 else text

            parsed = json.loads(text)
            usage = {
                "input": message.usage.input_tokens,
                "output": message.usage.output_tokens,
            }
            return parsed, usage

        except json.JSONDecodeError as e:
            last_error = e
            if attempt < config.llm_max_retries - 1:
                user_prompt += "\n\nIMPORTANT: Respond with valid JSON only. No markdown."
                time.sleep(2 + random.random() * 2)
        except Exception as e:
            last_error = e
            if attempt < config.llm_max_retries - 1:
                if _is_rate_limit(e):
                    # Rate limit: long wait with jitter to desync threads
                    wait = 30 + random.random() * 30
                    time.sleep(wait)
                elif _is_connection_error(e):
                    # Connection: moderate wait with jitter
                    wait = 5 + random.random() * 10
                    time.sleep(wait)
                else:
                    # Unknown error: short exponential backoff
                    time.sleep(2 ** (attempt + 1))

    raise RuntimeError(f"LLM call failed after {config.llm_max_retries} attempts: {last_error}")


def parse_llm_response(
    raw: Dict[str, Any],
    rdls_id: str,
    phash: str,
    model: str,
    usage: Dict[str, int],
) -> LLMClassification:
    """Parse raw LLM JSON response into LLMClassification."""
    components = raw.get("components", {})
    reasoning = raw.get("reasoning", {})

    return LLMClassification(
        rdls_id=rdls_id,
        is_rdls_relevant=bool(raw.get("is_rdls_relevant", True)),
        components={
            "hazard": bool(components.get("hazard", False)),
            "exposure": bool(components.get("exposure", False)),
            "vulnerability": bool(components.get("vulnerability", False)),
            "loss": bool(components.get("loss", False)),
        },
        component_reasoning={
            "hazard": str(reasoning.get("hazard", "")),
            "exposure": str(reasoning.get("exposure", "")),
            "vulnerability": str(reasoning.get("vulnerability", "")),
            "loss": str(reasoning.get("loss", "")),
        },
        overall_reasoning=str(raw.get("overall", "")),
        confidence=float(raw.get("confidence", 0.5)),
        domain_category=str(raw.get("domain", "other")),
        llm_model=model,
        prompt_hash=phash,
        token_usage=usage,
    )


def classify_single(
    rdls_id: str,
    reviewable: ReviewableRecord,
    hdx_meta: Dict[str, Any],
    column_infos: List[ColumnInfo],
    assessment: Optional[HEVLAssessment],
    config: ReviewConfig,
    client: Any,
    response_cache: LLMResponseCache,
) -> Optional[LLMClassification]:
    """Classify a single record via LLM. Uses cache if available."""
    prompt = build_classification_prompt(
        reviewable, hdx_meta, column_infos, assessment, config,
    )
    phash = _prompt_hash(prompt)

    # Check cache
    cached = response_cache.get(phash)
    if cached:
        return parse_llm_response(
            cached.get("response", cached),
            rdls_id, phash, config.llm_model,
            cached.get("usage", {"input": 0, "output": 0}),
        )

    # Call LLM
    try:
        raw_response, usage = call_llm(prompt, config, client)
        # Cache the response
        response_cache.put(phash, {"response": raw_response, "usage": usage})
        return parse_llm_response(raw_response, rdls_id, phash, config.llm_model, usage)
    except Exception as e:
        print(f"  LLM error for {rdls_id}: {e}")
        return None


# ---------------------------------------------------------------------------
# Phase 4: Merge
# ---------------------------------------------------------------------------

def merge_classification_into_assessment(
    assessment: HEVLAssessment,
    llm_result: LLMClassification,
    config: ReviewConfig,
) -> HEVLAssessment:
    """Merge LLM classification into an HEVLAssessment.

    When LLM disagrees with signals:
      - If llm_overrides_signals=True and LLM confidence >= threshold: use LLM
      - Otherwise: keep signal result but flag disagreement
    """
    import copy
    merged = copy.deepcopy(assessment)

    if not config.llm_overrides_signals:
        return merged

    if llm_result.confidence < config.disagreement_confidence_min:
        return merged

    # Build new assessed_rdt from LLM
    new_rdt = []
    for comp in ("hazard", "exposure", "vulnerability", "loss"):
        if llm_result.components.get(comp, False):
            new_rdt.append(comp)

    # If LLM says not RDLS relevant, keep original but flag
    if not llm_result.is_rdls_relevant:
        merged.changes = [f"LLM: not RDLS relevant (domain={llm_result.domain_category})"]
        merged.confidence = "low"
        return merged

    # Determine changes vs current
    current_set = set(merged.current_rdt)
    new_set = set(new_rdt)
    changes = []
    for comp in sorted(new_set - current_set):
        reason = llm_result.component_reasoning.get(comp, "")
        changes.append(f"ADD {comp} (LLM: {reason})")
    for comp in sorted(current_set - new_set):
        reason = llm_result.component_reasoning.get(comp, "")
        changes.append(f"REMOVE {comp} (LLM: {reason})")

    if changes:
        merged.assessed_rdt = sorted(new_rdt)
        merged.assessed_components = new_set
        merged.changes = changes
        merged.has_discrepancy = True
        if llm_result.confidence >= 0.8:
            merged.confidence = "high"
        elif llm_result.confidence >= 0.5:
            merged.confidence = "medium"
        else:
            merged.confidence = "low"

    return merged


# ---------------------------------------------------------------------------
# ID rebuild after reclassification
# ---------------------------------------------------------------------------

def _rebuild_id_for_new_rdt(
    old_id: str,
    new_rdt: List[str],
    naming_config: Dict[str, Any],
) -> str:
    """Swap the type segment in an RDLS ID to match updated risk_data_type.

    Parses the old ID, recomputes the type prefix from new_rdt, and
    reconstructs the ID keeping geo_org, title_slug, and collision intact.
    """
    parsed = parse_rdls_id(old_id, naming_config)
    if "raw" in parsed:
        # Parse failed — return as-is
        return old_id

    if not new_rdt:
        # No components — keep old type prefix to avoid 'unk'
        return old_id

    new_types = encode_component_types(new_rdt, naming_config)
    collision = f"__{parsed['collision']}" if parsed.get("collision") else ""
    return f"rdls_{new_types}-{parsed['geo_org']}_{parsed['title_slug']}{collision}"


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_llm_review(
    dist_dir: Path,
    metadata_dir: Path,
    output_dir: Path,
    config: ReviewConfig,
    dry_run: bool = False,
    max_records: Optional[int] = None,
    api_key: Optional[str] = None,
    verbose: bool = True,
) -> Dict[str, Any]:
    """Full 4-phase pipeline orchestrator."""

    # --- Setup ---
    dist_dir = Path(dist_dir)
    metadata_dir = Path(metadata_dir)
    output_dir = Path(output_dir)
    revised_dir = output_dir / "revised"
    reports_dir = output_dir / "reports"
    for d in (revised_dir, reports_dir):
        d.mkdir(parents=True, exist_ok=True)

    column_cache = ColumnCache(Path(config.column_cache_dir))
    response_cache = LLMResponseCache(Path(config.llm_cache_dir))

    # --- Phase 1: Signal triage (with cache) ---
    # Cache key: dist_dir path + max_records to avoid stale data
    cache_key_src = f"{dist_dir}|{max_records or 'all'}"
    cache_hash = hashlib.sha256(cache_key_src.encode()).hexdigest()[:12]
    phase1_cache_path = output_dir / f".phase1_cache_{cache_hash}.pkl"

    records: Dict[str, ReviewableRecord] = {}
    assessments: Dict[str, HEVLAssessment] = {}
    hdx_metas: Dict[str, Dict] = {}

    t0 = time.time()

    if phase1_cache_path.exists():
        # Load from cache
        if verbose:
            print("[Phase 1] Loading from cache...")
        try:
            with open(phase1_cache_path, "rb") as f:
                cached = pickle.load(f)
            records = cached["records"]
            assessments = cached["assessments"]
            hdx_metas = cached["hdx_metas"]
            if verbose:
                print(f"  {len(records)} records loaded from cache")
        except Exception as e:
            if verbose:
                print(f"  Cache load failed ({e}), rebuilding...")
            records, assessments, hdx_metas = {}, {}, {}

    if not records:
        # Build from scratch
        if verbose:
            print("[llm_review] Building HDX metadata index...")
        hdx_index = build_hdx_index(metadata_dir)
        if verbose:
            print(f"  {len(hdx_index)} HDX metadata files indexed")

        if verbose:
            print("[llm_review] Scanning dist tiers...")
        file_list = _scan_dist_tiers(dist_dir)
        if max_records:
            file_list = file_list[:max_records]
        if verbose:
            print(f"  {len(file_list)} records to process")

        if verbose:
            print("\n[Phase 1] Signal triage...")

        for filepath, tier in file_list:
            reviewable = load_rdls_record(filepath, tier)
            if reviewable is None:
                continue

            records[reviewable.rdls_id] = reviewable

            # Load HDX metadata
            if reviewable.hdx_uuid and reviewable.hdx_uuid in hdx_index:
                try:
                    hdx_meta = load_json(hdx_index[reviewable.hdx_uuid])
                    hdx_metas[reviewable.rdls_id] = hdx_meta
                    assessment = assess_hevl(reviewable, hdx_meta)
                    assessments[reviewable.rdls_id] = assessment
                except Exception:
                    pass

        # Save cache for next run
        try:
            with open(phase1_cache_path, "wb") as f:
                pickle.dump({
                    "records": records,
                    "assessments": assessments,
                    "hdx_metas": hdx_metas,
                }, f, protocol=pickle.HIGHEST_PROTOCOL)
            if verbose:
                print(f"  Phase 1 cache saved ({phase1_cache_path.name})")
        except Exception as e:
            if verbose:
                print(f"  Warning: Could not save cache: {e}")

    bucket = triage_records(assessments, config)
    t1 = time.time()

    if verbose:
        print(f"  Confident:    {len(bucket.confident)} (skip LLM)")
        print(f"  Borderline:   {len(bucket.borderline)} (send to LLM)")
        print(f"  No-signal:    {len(bucket.no_signal)} (send to LLM)")
        print(f"  Validation:   {len(bucket.validation_sample)} (5% cross-check)")
        print(f"  Time:         {t1 - t0:.1f}s")
        print(f"  Records:      {len(records)}")

    # Records to send to LLM
    llm_ids = set(bucket.borderline + bucket.no_signal + bucket.validation_sample)

    # --- Cost estimate ---
    est_input_tokens = len(llm_ids) * 1100  # ~1100 tokens/prompt
    est_output_tokens = len(llm_ids) * 150   # ~150 tokens/response
    est_cost = (
        est_input_tokens / 1_000_000 * config.cost_per_mtok_input
        + est_output_tokens / 1_000_000 * config.cost_per_mtok_output
    )

    if verbose:
        print(f"\n  Estimated LLM cost: ${est_cost:.2f}")
        print(f"  Records for LLM:   {len(llm_ids)}")
        print(f"  Cost guardrail:    ${config.max_cost_usd:.2f}")

    if dry_run:
        # Write triage summary and exit
        _write_triage_summary(reports_dir, bucket, assessments)
        if verbose:
            print(f"\n[DRY RUN] Triage summary written to {reports_dir}")
        return {
            "total": len(records),
            "confident": len(bucket.confident),
            "borderline": len(bucket.borderline),
            "no_signal": len(bucket.no_signal),
            "validation_sample": len(bucket.validation_sample),
            "estimated_cost_usd": est_cost,
            "dry_run": True,
        }

    if est_cost > config.max_cost_usd:
        print(f"\n  WARNING: Estimated cost ${est_cost:.2f} exceeds guardrail ${config.max_cost_usd:.2f}")
        print(f"  Increase max_cost_usd in config or reduce records")
        return {"error": "cost_guardrail_exceeded", "estimated_cost_usd": est_cost}

    # --- Phase 2: Column enrichment (from cache only) ---
    if verbose:
        print(f"\n[Phase 2] Loading column headers from cache...")

    col_data: Dict[str, List[ColumnInfo]] = {}
    col_hit = 0
    col_miss = 0
    for rdls_id in llm_ids:
        if rdls_id not in hdx_metas:
            continue
        hdx_meta = hdx_metas[rdls_id]
        infos = load_columns_for_uuid(
            records[rdls_id].hdx_uuid or "", hdx_meta, column_cache,
        )
        if infos:
            col_data[rdls_id] = infos
            col_hit += 1
        else:
            col_miss += 1

    if verbose:
        print(f"  With columns:    {col_hit}")
        print(f"  Without columns: {col_miss}")

    # --- Phase 3: LLM classification ---
    if verbose:
        print(f"\n[Phase 3] LLM classification ({len(llm_ids)} records)...")

    # Initialize Anthropic client
    try:
        import anthropic
    except ImportError:
        print("ERROR: 'anthropic' package not installed.")
        print("  Install: pip install anthropic")
        return {"error": "anthropic_not_installed"}

    resolved_api_key = api_key or os.environ.get("ANTHROPIC_API_KEY", "")
    if not resolved_api_key:
        # Try .env file in project root
        env_path = Path(".env")
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("ANTHROPIC_API_KEY="):
                    resolved_api_key = line.split("=", 1)[1].strip().strip("'\"")
                    break
    if not resolved_api_key:
        print("ERROR: ANTHROPIC_API_KEY not set.")
        print("  Options:")
        print("    1. set ANTHROPIC_API_KEY=sk-ant-...")
        print("    2. --api-key sk-ant-...")
        print("    3. Create .env file with ANTHROPIC_API_KEY=sk-ant-...")
        return {"error": "no_api_key"}

    client = anthropic.Anthropic(api_key=resolved_api_key)

    llm_results: Dict[str, LLMClassification] = {}
    total_input_tokens = 0
    total_output_tokens = 0
    llm_errors = 0
    llm_cached = 0

    t2 = time.time()
    llm_list = sorted(llm_ids)

    # Process with ThreadPoolExecutor for concurrency
    def _classify_one(rdls_id: str) -> Tuple[str, Optional[LLMClassification]]:
        reviewable = records.get(rdls_id)
        hdx_meta = hdx_metas.get(rdls_id, {})
        columns = col_data.get(rdls_id, [])
        assessment = assessments.get(rdls_id)
        if not reviewable:
            return rdls_id, None
        result = classify_single(
            rdls_id, reviewable, hdx_meta, columns,
            assessment, config, client, response_cache,
        )
        return rdls_id, result

    # Feed work in batches to avoid overwhelming the API.
    # The executor has max_concurrent workers; we submit in chunks
    # and collect results to maintain steady throughput.
    batch_size = max(config.llm_max_concurrent * 5, 20)
    completed = 0
    error_ids: List[str] = []

    for batch_start in range(0, len(llm_list), batch_size):
        batch = llm_list[batch_start : batch_start + batch_size]

        with ThreadPoolExecutor(max_workers=config.llm_max_concurrent) as executor:
            futures = {executor.submit(_classify_one, rid): rid for rid in batch}

            for future in as_completed(futures):
                completed += 1
                rdls_id = futures[future]
                try:
                    _, result = future.result()
                except Exception as e:
                    result = None
                    if verbose:
                        print(f"  LLM error for {rdls_id}: {e}")

                if result:
                    llm_results[rdls_id] = result
                    total_input_tokens += result.token_usage.get("input", 0)
                    total_output_tokens += result.token_usage.get("output", 0)
                    if result.token_usage.get("input", 0) == 0:
                        llm_cached += 1
                else:
                    llm_errors += 1
                    error_ids.append(rdls_id)

                # Progress
                if verbose and completed % 50 == 0:
                    elapsed = time.time() - t2
                    rate = completed / elapsed if elapsed > 0 else 0
                    remaining = len(llm_list) - completed
                    eta = remaining / rate if rate > 0 else 0
                    cost_so_far = (
                        total_input_tokens / 1_000_000 * config.cost_per_mtok_input
                        + total_output_tokens / 1_000_000 * config.cost_per_mtok_output
                    )
                    print(
                        f"  [{completed}/{len(llm_list)}] "
                        f"cached={llm_cached} errors={llm_errors} "
                        f"cost=${cost_so_far:.2f} "
                        f"({rate:.1f} rec/s, ETA {eta:.0f}s)"
                    )

        # Brief pause between batches to respect rate limits (50K tokens/min)
        if batch_start + batch_size < len(llm_list):
            time.sleep(1.5)

    # Log failed IDs for retry
    if error_ids:
        failed_path = reports_dir / "failed_ids.txt"
        failed_path.write_text("\n".join(sorted(error_ids)), encoding="utf-8")
        if verbose:
            print(f"\n  Failed IDs ({len(error_ids)}): saved to {failed_path}")

    t3 = time.time()
    actual_cost = (
        total_input_tokens / 1_000_000 * config.cost_per_mtok_input
        + total_output_tokens / 1_000_000 * config.cost_per_mtok_output
    )

    if verbose:
        print(f"\n  LLM complete: {len(llm_results)} classified, {llm_errors} errors")
        print(f"  Cached:       {llm_cached}")
        print(f"  Tokens:       {total_input_tokens:,} in / {total_output_tokens:,} out")
        print(f"  Cost:         ${actual_cost:.2f}")
        print(f"  Time:         {t3 - t2:.1f}s")

    # --- Phase 4: Merge + write ---
    if verbose:
        print(f"\n[Phase 4] Merging results and writing output...")

    # Initialize extractors for revise_record
    from .hdx_review import _init_extractors
    extractors = _init_extractors()

    # Load naming config for ID rebuild after reclassification
    naming_yaml = Path(__file__).resolve().parent.parent / "configs" / "naming.yaml"
    naming_config = load_naming_config(naming_yaml) if naming_yaml.exists() else {}

    total = len(records)
    changed = 0
    unchanged = 0
    renamed = 0
    disagreements: List[Dict[str, Any]] = []
    all_rows: List[Dict[str, Any]] = []
    used_ids: Set[str] = set()

    # Clean revised_dir before writing to avoid stale files from prior runs
    if not dry_run and revised_dir.exists():
        import shutil
        for child in revised_dir.iterdir():
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)

    for rdls_id, reviewable in records.items():
        assessment = assessments.get(rdls_id)
        if not assessment:
            # No assessment — rebuild ID from current rdt, then copy
            record = reviewable.record
            new_id = _rebuild_id_for_new_rdt(
                rdls_id, record.get("risk_data_type", []), naming_config,
            )
            new_id = build_rdls_id_with_collision(new_id, used_ids, reviewable.hdx_uuid)
            used_ids.add(new_id)
            if new_id != rdls_id:
                record = {**record, "id": new_id}
                renamed += 1
            if not dry_run:
                tier_out = revised_dir / reviewable.dist_tier
                tier_out.mkdir(parents=True, exist_ok=True)
                write_json(tier_out / f"{new_id}.json", {"datasets": [record]})
            unchanged += 1
            continue

        # Determine final assessment
        final_assessment = assessment
        source = "signal"
        llm_cls = llm_results.get(rdls_id)

        if llm_cls and rdls_id in llm_ids:
            merged = merge_classification_into_assessment(assessment, llm_cls, config)
            final_assessment = merged
            source = "llm"

            # Track disagreements (validation sample)
            if rdls_id in bucket.validation_sample:
                signal_rdt = set(assessment.assessed_rdt)
                llm_rdt = {c for c, v in llm_cls.components.items() if v}
                if signal_rdt != llm_rdt:
                    disagreements.append({
                        "rdls_id": rdls_id,
                        "signal_rdt": "|".join(sorted(signal_rdt)),
                        "llm_rdt": "|".join(sorted(llm_rdt)),
                        "llm_confidence": llm_cls.confidence,
                        "llm_domain": llm_cls.domain_category,
                        "llm_overall": llm_cls.overall_reasoning,
                    })

        if final_assessment.has_discrepancy:
            changed += 1
            hdx_meta = hdx_metas.get(rdls_id, {})
            revised = revise_record(reviewable, final_assessment, hdx_meta, extractors)

            # Rebuild ID based on updated risk_data_type
            new_rdt = revised.get("risk_data_type", [])
            new_id = _rebuild_id_for_new_rdt(rdls_id, new_rdt, naming_config)
            new_id = build_rdls_id_with_collision(new_id, used_ids, reviewable.hdx_uuid)
            used_ids.add(new_id)
            if new_id != rdls_id:
                revised["id"] = new_id
                renamed += 1

            if not dry_run:
                tier_out = revised_dir / reviewable.dist_tier
                tier_out.mkdir(parents=True, exist_ok=True)
                write_json(tier_out / f"{new_id}.json", {"datasets": [revised]})
        else:
            unchanged += 1
            record = reviewable.record

            # Rebuild ID even for unchanged records (fix pre-existing mismatches)
            new_id = _rebuild_id_for_new_rdt(
                rdls_id, record.get("risk_data_type", []), naming_config,
            )
            new_id = build_rdls_id_with_collision(new_id, used_ids, reviewable.hdx_uuid)
            used_ids.add(new_id)
            if new_id != rdls_id:
                record = {**record, "id": new_id}
                renamed += 1

            if not dry_run:
                tier_out = revised_dir / reviewable.dist_tier
                tier_out.mkdir(parents=True, exist_ok=True)
                write_json(tier_out / f"{new_id}.json", {"datasets": [record]})

        # Build report row
        row = {
            "rdls_id": rdls_id,
            "new_id": new_id,
            "original_rdt": "|".join(assessment.current_rdt),
            "final_rdt": "|".join(final_assessment.assessed_rdt),
            "source": source,
            "has_change": final_assessment.has_discrepancy,
            "changes": "; ".join(final_assessment.changes) if final_assessment.changes else "",
            "confidence": final_assessment.confidence,
            "llm_confidence": llm_cls.confidence if llm_cls else "",
            "llm_domain": llm_cls.domain_category if llm_cls else "",
        }
        all_rows.append(row)

    if verbose:
        print(f"  Renamed:      {renamed} IDs updated to match risk_data_type")

    # --- Write reports ---
    _write_triage_summary(reports_dir, bucket, assessments)
    _write_review_report(reports_dir, all_rows)
    _write_disagreements(reports_dir, disagreements)
    _write_llm_audit(reports_dir, llm_results)
    _write_summary_md(reports_dir, {
        "total": total,
        "changed": changed,
        "unchanged": unchanged,
        "confident": len(bucket.confident),
        "borderline": len(bucket.borderline),
        "no_signal": len(bucket.no_signal),
        "validation_sample": len(bucket.validation_sample),
        "llm_classified": len(llm_results),
        "llm_errors": llm_errors,
        "llm_cached": llm_cached,
        "col_with": col_hit,
        "col_without": col_miss,
        "disagreements": len(disagreements),
        "actual_cost_usd": actual_cost,
        "total_input_tokens": total_input_tokens,
        "total_output_tokens": total_output_tokens,
        "elapsed_seconds": time.time() - t0,
    })

    if verbose:
        print(f"\n{'='*60}")
        print(f"  LLM REVIEW COMPLETE")
        print(f"{'='*60}")
        print(f"  Total:          {total}")
        print(f"  Changed:        {changed} ({changed/total*100:.1f}%)" if total else "")
        print(f"  Unchanged:      {unchanged}")
        print(f"  Disagreements:  {len(disagreements)} (validation sample)")
        print(f"  LLM cost:       ${actual_cost:.2f}")
        print(f"  Time:           {time.time() - t0:.1f}s")
        print(f"  Reports:        {reports_dir}")
        print(f"  Revised:        {revised_dir}")
        print(f"{'='*60}")

    return {
        "total": total,
        "changed": changed,
        "unchanged": unchanged,
        "renamed": renamed,
        "disagreements": len(disagreements),
        "actual_cost_usd": actual_cost,
    }


# ---------------------------------------------------------------------------
# Report writers
# ---------------------------------------------------------------------------

def _write_triage_summary(reports_dir: Path, bucket: TriageBucket, assessments: Dict[str, HEVLAssessment]):
    rows = []
    for rdls_id, bucket_name in [
        *[(rid, "confident") for rid in bucket.confident],
        *[(rid, "borderline") for rid in bucket.borderline],
        *[(rid, "no_signal") for rid in bucket.no_signal],
    ]:
        asmt = assessments.get(rdls_id)
        scores = asmt.component_scores if asmt else {}
        rows.append({
            "rdls_id": rdls_id,
            "bucket": bucket_name,
            "validation_sample": rdls_id in bucket.validation_sample,
            "score_H": scores.get("H", 0),
            "score_E": scores.get("E", 0),
            "score_V": scores.get("V", 0),
            "score_L": scores.get("L", 0),
            "max_score": max(scores.values()) if scores else 0,
        })

    if rows:
        path = reports_dir / "triage_summary.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)


def _write_review_report(reports_dir: Path, rows: List[Dict]):
    if rows:
        path = reports_dir / "review_report.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rows[0].keys())
            writer.writeheader()
            writer.writerows(rows)


def _write_disagreements(reports_dir: Path, disagreements: List[Dict]):
    if disagreements:
        path = reports_dir / "disagreements.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=disagreements[0].keys())
            writer.writeheader()
            writer.writerows(disagreements)


def _write_llm_audit(reports_dir: Path, results: Dict[str, LLMClassification]):
    path = reports_dir / "llm_classifications.jsonl"
    with open(path, "w", encoding="utf-8") as f:
        for rdls_id, cls in sorted(results.items()):
            entry = {
                "rdls_id": rdls_id,
                "is_rdls_relevant": cls.is_rdls_relevant,
                "components": cls.components,
                "reasoning": cls.component_reasoning,
                "overall": cls.overall_reasoning,
                "confidence": cls.confidence,
                "domain": cls.domain_category,
                "model": cls.llm_model,
                "prompt_hash": cls.prompt_hash,
                "tokens": cls.token_usage,
            }
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _write_summary_md(reports_dir: Path, stats: Dict[str, Any]):
    total = stats.get("total", 0)
    changed = stats.get("changed", 0)
    pct = f"{changed/total*100:.1f}%" if total else "0%"

    lines = [
        "# LLM-Assisted HEVL Review",
        "",
        "## Summary",
        "",
        "| Metric | Value |",
        "|--------|-------|",
        f"| **Total records** | {total} |",
        f"| **Changed** | {changed} ({pct}) |",
        f"| **Unchanged** | {stats.get('unchanged', 0)} |",
        "",
        "## Triage (Phase 1)",
        "",
        "| Bucket | Count |",
        "|--------|------:|",
        f"| Confident (skip LLM) | {stats.get('confident', 0)} |",
        f"| Borderline (LLM) | {stats.get('borderline', 0)} |",
        f"| No-signal (LLM) | {stats.get('no_signal', 0)} |",
        f"| Validation sample | {stats.get('validation_sample', 0)} |",
        "",
        "## Column Enrichment (Phase 2)",
        "",
        f"- With column headers: {stats.get('col_with', 0)}",
        f"- Without column headers: {stats.get('col_without', 0)}",
        "",
        "## LLM Classification (Phase 3)",
        "",
        f"- Classified: {stats.get('llm_classified', 0)}",
        f"- Cached: {stats.get('llm_cached', 0)}",
        f"- Errors: {stats.get('llm_errors', 0)}",
        f"- Input tokens: {stats.get('total_input_tokens', 0):,}",
        f"- Output tokens: {stats.get('total_output_tokens', 0):,}",
        f"- **Cost: ${stats.get('actual_cost_usd', 0):.2f}**",
        "",
        "## Validation (Phase 4)",
        "",
        f"- Disagreements (LLM vs regex): {stats.get('disagreements', 0)}",
        "",
        f"## Timing",
        "",
        f"- Total: {stats.get('elapsed_seconds', 0):.1f}s",
        "",
    ]

    path = reports_dir / "review_summary.md"
    path.write_text("\n".join(lines), encoding="utf-8")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="LLM-assisted HEVL classification pipeline",
    )
    parser.add_argument(
        "--dist-dir", required=True,
        help="Path to RDLS dist directory",
    )
    parser.add_argument(
        "--metadata-dir", required=True,
        help="Path to HDX dataset_metadata directory",
    )
    parser.add_argument(
        "--output-dir", default="output/llm",
        help="Output directory (default: output/llm)",
    )
    parser.add_argument(
        "--config", default="configs/llm_review.yaml",
        help="Config YAML path (default: configs/llm_review.yaml)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Phase 1 triage only — no LLM calls, no writes",
    )
    parser.add_argument(
        "--max-records", type=int, default=None,
        help="Max records to process (for testing)",
    )
    parser.add_argument(
        "--api-key", default=None,
        help="Anthropic API key (or set ANTHROPIC_API_KEY env var, or .env file)",
    )

    args = parser.parse_args()

    config_path = Path(args.config)
    if config_path.exists():
        config = ReviewConfig.from_yaml(config_path)
    else:
        print(f"Warning: config not found at {config_path}, using defaults")
        config = ReviewConfig()

    run_llm_review(
        dist_dir=Path(args.dist_dir),
        metadata_dir=Path(args.metadata_dir),
        output_dir=Path(args.output_dir),
        config=config,
        dry_run=args.dry_run,
        max_records=args.max_records,
        api_key=args.api_key,
    )


if __name__ == "__main__":
    main()
