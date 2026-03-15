# hdx_review.py -- Second-pass HEVL review of RDLS JSON records
# ----------------------------------------------------------------
# Re-analyzes RDLS JSON files from the HDX metadata crawler using
# improved signal matching (column_detection patterns, resource-name
# signals) and cross-references with original HDX metadata.
#
# Flags and fixes HEVL misclassifications, writes revised JSONs.
#
# Usage:
#   python -m src.hdx_review \
#     --dist-dir "path/to/rdls/dist" \
#     --metadata-dir "path/to/dataset_metadata" \
#     --output-dir output/hdx
#
# Benny Istanto, GOST/DEC Data Group/The World Bank

from __future__ import annotations

import argparse
import copy
import csv
import json
import os
import re
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import unquote, urlparse

# Ensure src/ is importable when run as module
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from .utils import load_json, write_json, normalize_text
from .review import (
    _match_signals,
    _match_column_signals,
    load_review_config,
    HAZARD_SIGNALS,
    EXPOSURE_SIGNALS,
    VULNERABILITY_SIGNALS,
    LOSS_SIGNALS,
    COLUMN_DETECTION,
)
from .integrate import merge_hevl_into_record, determine_risk_data_types


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_CONFIGS_DIR = Path(__file__).resolve().parent.parent / "configs"

_COMP_TO_RDT = {"H": "hazard", "E": "exposure", "V": "vulnerability", "L": "loss"}
_RDT_TO_COMP = {v: k for k, v in _COMP_TO_RDT.items()}

# Tier processing order (highest value first)
_TIER_ORDER = ["high", os.path.join("invalid", "high"), os.path.join("invalid", "medium")]


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ReviewableRecord:
    """A loaded RDLS record with cross-reference metadata."""
    filepath: Path
    record: Dict[str, Any]       # datasets[0]
    rdls_id: str
    hdx_uuid: str                # from links[].href where rel='source'
    current_rdt: List[str]       # current risk_data_type
    current_blocks: Dict[str, bool]  # {hazard: T/F, exposure: T/F, ...}
    dist_tier: str               # "high", "invalid/high", etc.


@dataclass
class HEVLAssessment:
    """Result of re-assessing HEVL classification for one record."""
    rdls_id: str
    hdx_uuid: str

    # Current state
    current_rdt: List[str]
    current_blocks: Dict[str, bool]

    # Re-assessed state
    assessed_rdt: List[str]
    assessed_components: Set[str]  # H, E, V, L

    # Signal evidence
    metadata_signals: Dict[str, List[str]]     # H/E/V/L -> matched patterns
    resource_signals: Dict[str, List[str]]      # H/E/V/L -> resource name matches
    column_proxy_signals: List[Dict[str, Any]]  # from _match_column_signals

    # Scoring
    component_scores: Dict[str, int]  # H/E/V/L -> total weighted score

    # Verdict
    has_discrepancy: bool
    changes: List[str]                 # Human-readable change descriptions
    confidence: str                    # "high", "medium", "low"


@dataclass
class ReviewReport:
    """Summary of batch review results."""
    total_records: int
    records_changed: int
    records_unchanged: int
    records_skipped: int       # no HDX metadata found
    records_errored: int
    changes_by_type: Dict[str, int]   # "add_L", "remove_V", etc.
    assessment_details: List[Dict[str, Any]]  # per-record summary rows


# ---------------------------------------------------------------------------
# Step 1: HDX metadata index
# ---------------------------------------------------------------------------

def build_hdx_index(metadata_dir: Path) -> Dict[str, Path]:
    """Build UUID -> filepath index from dataset_metadata/ directory.

    Files are named {uuid}__{slug}.json.
    Returns dict mapping UUID to full file path.
    """
    index: Dict[str, Path] = {}
    for f in metadata_dir.iterdir():
        if f.suffix == ".json" and "__" in f.name:
            uuid = f.name.split("__")[0]
            index[uuid] = f
    return index


# ---------------------------------------------------------------------------
# Step 2: Record loading
# ---------------------------------------------------------------------------

def _extract_hdx_uuid(record: Dict[str, Any]) -> str:
    """Extract HDX dataset UUID from links[].href where rel='source'."""
    for link in record.get("links", []):
        if link.get("rel") == "source":
            href = link.get("href", "")
            # Pattern: https://data.humdata.org/dataset/{uuid}
            if "/dataset/" in href:
                parts = href.rstrip("/").split("/dataset/")
                if len(parts) >= 2:
                    return parts[-1].split("/")[0]
    # Fallback: try to extract from id
    return ""


def _detect_current_blocks(record: Dict[str, Any]) -> Dict[str, bool]:
    """Check which HEVL blocks exist and have content."""
    blocks = {}
    for comp in ("hazard", "exposure", "vulnerability", "loss"):
        block = record.get(comp)
        if block is None:
            blocks[comp] = False
        elif isinstance(block, dict):
            # hazard has event_sets, vulnerability has functions/socio_economic
            blocks[comp] = bool(block)
        elif isinstance(block, list):
            # exposure is a list
            blocks[comp] = len(block) > 0
        else:
            blocks[comp] = False
    return blocks


def load_rdls_record(filepath: Path, dist_tier: str) -> Optional[ReviewableRecord]:
    """Load RDLS JSON and extract review metadata."""
    try:
        data = load_json(filepath)
    except Exception:
        return None

    datasets = data.get("datasets", [])
    if not datasets:
        return None

    record = datasets[0]
    rdls_id = record.get("id", filepath.stem)
    hdx_uuid = _extract_hdx_uuid(record)
    current_rdt = record.get("risk_data_type", [])
    current_blocks = _detect_current_blocks(record)

    return ReviewableRecord(
        filepath=filepath,
        record=record,
        rdls_id=rdls_id,
        hdx_uuid=hdx_uuid,
        current_rdt=current_rdt,
        current_blocks=current_blocks,
        dist_tier=dist_tier,
    )


# ---------------------------------------------------------------------------
# Step 3: HEVL re-assessment
# ---------------------------------------------------------------------------

def _extract_resource_texts(
    hdx_meta: Dict[str, Any],
) -> Tuple[str, List[str], List[str]]:
    """Extract resource names and filenames from HDX metadata.

    Returns:
        Tuple of:
        - combined_resource_text: all resource text for signal matching (Layer B)
        - resource_short_names: short names/filenames for column-proxy matching (Layer C)
        - resource_full_texts: full texts including descriptions for Layer B only
    """
    resource_short_names: List[str] = []  # For column-proxy (Layer C)
    resource_full_texts: List[str] = []   # For resource signal matching (Layer B)

    for res in hdx_meta.get("resources", []):
        # Resource name/title - used for both Layer B and C
        rname = res.get("name", "") or res.get("title", "")
        if rname:
            rname_lower = rname.lower()
            resource_short_names.append(rname_lower)
            resource_full_texts.append(rname_lower)

        # Resource description - ONLY for Layer B (signal matching), NOT for
        # column-proxy. Descriptions are long text that would cause false
        # positives if matched against column-name patterns.
        rdesc = res.get("description", "")
        if rdesc:
            resource_full_texts.append(rdesc.lower())

        # Extract filename from download URL - used for both Layer B and C
        url = res.get("download_url", "") or res.get("url", "")
        if url:
            parsed = urlparse(url)
            url_path = unquote(parsed.path)
            filename = url_path.split("/")[-1] if "/" in url_path else ""
            if filename and "." in filename:
                # Remove extension, replace separators with spaces
                name_part = filename.rsplit(".", 1)[0]
                name_part = re.sub(r"[_\-.]", " ", name_part).lower()
                resource_short_names.append(name_part)
                resource_full_texts.append(name_part)

    combined = " ".join(resource_full_texts)
    return combined, resource_short_names, resource_full_texts


def _build_metadata_text(rdls_record: Dict[str, Any], hdx_meta: Dict[str, Any]) -> str:
    """Build combined metadata text for signal matching."""
    parts = []
    # RDLS record fields
    parts.append(rdls_record.get("title", ""))
    parts.append(rdls_record.get("description", ""))

    # HDX metadata fields
    parts.append(hdx_meta.get("title", ""))
    parts.append(hdx_meta.get("notes", ""))
    parts.append(hdx_meta.get("methodology", "") or "")
    parts.append(hdx_meta.get("methodology_other", "") or "")

    # Tags
    for tag in hdx_meta.get("tags", []):
        if isinstance(tag, dict):
            parts.append(tag.get("name", ""))
        elif isinstance(tag, str):
            parts.append(tag)

    return " ".join(p for p in parts if p)


def _score_component(
    comp: str,
    metadata_matches: List[str],
    resource_matches: List[str],
    column_proxy_signals: List[Dict[str, Any]],
) -> int:
    """Compute weighted score for a single HEVL component.

    metadata_score   = count * 1
    resource_score   = count * 2  (resource names are more specific)
    column_proxy     = sum of weights (high=3, medium=2, low=1)
    """
    metadata_score = len(metadata_matches)
    resource_score = len(resource_matches) * 2
    proxy_score = sum(
        s["weight"] for s in column_proxy_signals
        if s["component"] == comp
    )
    return metadata_score + resource_score + proxy_score


def assess_hevl(
    reviewable: ReviewableRecord,
    hdx_meta: Dict[str, Any],
) -> HEVLAssessment:
    """Re-assess HEVL classification using enhanced signal analysis."""

    # --- Layer A: Metadata text signals ---
    meta_text = _build_metadata_text(reviewable.record, hdx_meta)
    metadata_signals: Dict[str, List[str]] = {"H": [], "E": [], "V": [], "L": []}

    for sig_name, sig_info in HAZARD_SIGNALS.items():
        matches = _match_signals(meta_text, sig_info["patterns"])
        if matches:
            metadata_signals["H"].extend(matches)

    for sig_name, sig_info in EXPOSURE_SIGNALS.items():
        matches = _match_signals(meta_text, sig_info["patterns"])
        if matches:
            metadata_signals["E"].extend(matches)

    v_matches = _match_signals(meta_text, VULNERABILITY_SIGNALS["patterns"])
    if v_matches:
        metadata_signals["V"].extend(v_matches)

    l_matches = _match_signals(meta_text, LOSS_SIGNALS["patterns"])
    if l_matches:
        metadata_signals["L"].extend(l_matches)

    # --- Layer B: Resource name signals ---
    resource_text, resource_short_names, _ = _extract_resource_texts(hdx_meta)
    resource_signals: Dict[str, List[str]] = {"H": [], "E": [], "V": [], "L": []}

    for sig_name, sig_info in HAZARD_SIGNALS.items():
        matches = _match_signals(resource_text, sig_info["patterns"])
        if matches:
            resource_signals["H"].extend(matches)

    for sig_name, sig_info in EXPOSURE_SIGNALS.items():
        matches = _match_signals(resource_text, sig_info["patterns"])
        if matches:
            resource_signals["E"].extend(matches)

    v_matches = _match_signals(resource_text, VULNERABILITY_SIGNALS["patterns"])
    if v_matches:
        resource_signals["V"].extend(v_matches)

    l_matches = _match_signals(resource_text, LOSS_SIGNALS["patterns"])
    if l_matches:
        resource_signals["L"].extend(l_matches)

    # --- Layer C: Column-proxy detection on resource SHORT names/filenames ---
    # Only use short names (resource titles + URL filenames), NOT descriptions.
    # Long descriptions cause false positives in column-name pattern matching.
    col_proxy = _match_column_signals(resource_short_names, COLUMN_DETECTION)

    # --- Compute per-component scores ---
    component_scores: Dict[str, int] = {}
    for comp in ("H", "E", "V", "L"):
        component_scores[comp] = _score_component(
            comp, metadata_signals[comp], resource_signals[comp], col_proxy,
        )

    # --- Determine assessed HEVL ---
    assessed_components: Set[str] = set()
    current_comps = {_RDT_TO_COMP.get(r, "") for r in reviewable.current_rdt} - {""}

    for comp in ("H", "E", "V", "L"):
        score = component_scores[comp]
        if score >= 3:
            assessed_components.add(comp)
        elif comp in current_comps:
            # If currently assigned and has ANY signal, keep it
            if score > 0:
                assessed_components.add(comp)
            # If score == 0 and no existing block, drop it
            elif not reviewable.current_blocks.get(_COMP_TO_RDT[comp], False):
                pass  # will be flagged as removal
            else:
                # Has block content but no signals -- keep but flag
                assessed_components.add(comp)

    assessed_rdt = sorted([_COMP_TO_RDT[c] for c in assessed_components])

    # --- Detect discrepancies ---
    changes: List[str] = []
    for comp in ("H", "E", "V", "L"):
        rdt = _COMP_TO_RDT[comp]
        was_present = rdt in reviewable.current_rdt
        now_present = comp in assessed_components

        if now_present and not was_present:
            # Evidence sources for the change description
            sources = []
            if resource_signals[comp]:
                sources.append(f"resource signals: {resource_signals[comp][:3]}")
            proxy_for_comp = [s for s in col_proxy if s["component"] == comp]
            if proxy_for_comp:
                sources.append(f"column proxy: {[s['label'] for s in proxy_for_comp[:2]]}")
            if metadata_signals[comp]:
                sources.append(f"metadata: {metadata_signals[comp][:2]}")
            evidence_str = "; ".join(sources) if sources else "aggregate score"
            changes.append(f"ADD {rdt} (score={component_scores[comp]}, {evidence_str})")

        elif was_present and not now_present:
            changes.append(
                f"REMOVE {rdt} (score={component_scores[comp]}, "
                f"no signals, no block content)"
            )

    has_discrepancy = len(changes) > 0

    # --- Confidence ---
    if has_discrepancy:
        # How many signal sources agree on the changes?
        sources_agreeing = 0
        for comp in assessed_components - current_comps:
            if metadata_signals.get(comp):
                sources_agreeing += 1
            if resource_signals.get(comp):
                sources_agreeing += 1
            proxy_match = any(s["component"] == comp for s in col_proxy)
            if proxy_match:
                sources_agreeing += 1
        confidence = "high" if sources_agreeing >= 2 else "medium"
    else:
        confidence = "high"  # no change = confident it's correct

    return HEVLAssessment(
        rdls_id=reviewable.rdls_id,
        hdx_uuid=reviewable.hdx_uuid,
        current_rdt=reviewable.current_rdt,
        current_blocks=reviewable.current_blocks,
        assessed_rdt=assessed_rdt,
        assessed_components=assessed_components,
        metadata_signals=metadata_signals,
        resource_signals=resource_signals,
        column_proxy_signals=col_proxy,
        component_scores=component_scores,
        has_discrepancy=has_discrepancy,
        changes=changes,
        confidence=confidence,
    )


# ---------------------------------------------------------------------------
# Step 4: Record revision
# ---------------------------------------------------------------------------

def _init_extractors() -> Dict[str, Any]:
    """Initialize HEVL extractors (load configs once)."""
    from .extract_hazard import HazardExtractor
    from .extract_exposure import ExposureExtractor
    from .extract_vulnloss import VulnerabilityExtractor, LossExtractor

    defaults_path = _CONFIGS_DIR / "rdls_defaults.yaml"
    from .utils import load_yaml
    defaults = load_yaml(defaults_path) if defaults_path.exists() else {}

    signal_dict_path = _CONFIGS_DIR / "signal_dictionary.yaml"
    signal_dict = load_yaml(signal_dict_path) if signal_dict_path.exists() else {}

    return {
        "hazard": HazardExtractor(signal_dict, defaults),
        "exposure": ExposureExtractor(signal_dict, defaults),
        "vulnerability": VulnerabilityExtractor(signal_dict, defaults),
        "loss": LossExtractor(signal_dict, defaults),
    }


def _build_extractor_input(hdx_meta: Dict[str, Any]) -> Dict[str, Any]:
    """Convert raw HDX metadata to the format expected by extractors.

    Extractors expect: title, name, notes, methodology, tags, resources.
    """
    return {
        "title": hdx_meta.get("title", ""),
        "name": hdx_meta.get("name", ""),
        "notes": hdx_meta.get("notes", ""),
        "methodology": hdx_meta.get("methodology", "") or "",
        "tags": hdx_meta.get("tags", []),
        "resources": hdx_meta.get("resources", []),
        "organization": hdx_meta.get("organization", ""),
        "dataset_source": hdx_meta.get("dataset_source", ""),
    }


def revise_record(
    reviewable: ReviewableRecord,
    assessment: HEVLAssessment,
    hdx_meta: Dict[str, Any],
    extractors: Dict[str, Any],
) -> Dict[str, Any]:
    """Apply HEVL revisions to an RDLS record.

    Returns a new record dict (deep copy) with changes applied.
    """
    from .extract_hazard import build_hazard_block
    from .extract_exposure import build_exposure_block
    from .extract_vulnloss import build_vulnerability_block, build_loss_block

    record = copy.deepcopy(reviewable.record)

    if not assessment.has_discrepancy:
        return record

    ext_input = _build_extractor_input(hdx_meta)
    current_comps = {_RDT_TO_COMP.get(r, "") for r in reviewable.current_rdt} - {""}

    for change in assessment.changes:
        if change.startswith("ADD "):
            comp_name = change.split(" ")[1]  # "hazard", "exposure", etc.
            comp_key = _RDT_TO_COMP.get(comp_name, "")

            # Run the appropriate extractor
            if comp_name == "hazard" and "hazard" in extractors:
                extraction = extractors["hazard"].extract(ext_input)
                block = build_hazard_block(extraction)
                if block:
                    record["hazard"] = block

            elif comp_name == "exposure" and "exposure" in extractors:
                extraction = extractors["exposure"].extract(ext_input)
                block = build_exposure_block(extraction)
                if block:
                    record["exposure"] = block

            elif comp_name == "vulnerability" and "vulnerability" in extractors:
                hazard_types = []
                if "hazard" in record and isinstance(record["hazard"], dict):
                    for es in record["hazard"].get("event_sets", []):
                        for hz in es.get("hazards", []):
                            ht = hz.get("type")
                            if ht:
                                hazard_types.append(ht)
                extraction = extractors["vulnerability"].extract(
                    ext_input, hazard_types=hazard_types or None,
                )
                block = build_vulnerability_block(extraction)
                if block:
                    record["vulnerability"] = block

            elif comp_name == "loss" and "loss" in extractors:
                hazard_types = []
                if "hazard" in record and isinstance(record["hazard"], dict):
                    for es in record["hazard"].get("event_sets", []):
                        for hz in es.get("hazards", []):
                            ht = hz.get("type")
                            if ht:
                                hazard_types.append(ht)
                extraction = extractors["loss"].extract(
                    ext_input, hazard_types=hazard_types or None,
                )
                block = build_loss_block(extraction)
                if block:
                    record["loss"] = block

        elif change.startswith("REMOVE "):
            comp_name = change.split(" ")[1]
            if comp_name in record:
                del record[comp_name]

    # Update risk_data_type - only include components that have actual blocks.
    # Start from original rdt (preserves what the 1st-iteration pipeline set),
    # then add only those components whose blocks were successfully created.
    final_rdt = set(reviewable.current_rdt)
    for comp in ("hazard", "exposure", "vulnerability", "loss"):
        if comp in record and bool(record[comp]) and comp not in final_rdt:
            final_rdt.add(comp)
    # Handle removals: drop component only if block is actually gone
    for change in assessment.changes:
        if change.startswith("REMOVE "):
            comp_name = change.split(" ")[1]
            if comp_name not in record or not record.get(comp_name):
                final_rdt.discard(comp_name)
    record["risk_data_type"] = sorted(final_rdt)

    return record


# ---------------------------------------------------------------------------
# Step 5-6: Batch processing
# ---------------------------------------------------------------------------

def _scan_dist_tiers(dist_dir: Path, tiers: Optional[List[str]] = None) -> List[Tuple[Path, str]]:
    """Scan dist directory for RDLS JSON files, grouped by tier.

    Returns list of (filepath, tier_name) tuples.
    """
    if tiers is None:
        tiers = _TIER_ORDER

    results: List[Tuple[Path, str]] = []
    for tier in tiers:
        tier_dir = dist_dir / tier
        if not tier_dir.is_dir():
            continue
        for f in sorted(tier_dir.iterdir()):
            if f.suffix == ".json":
                results.append((f, tier))
    return results


def _write_checkpoint(checkpoint_path: Path, processed: int, changed: int, total: int):
    """Write progress checkpoint for resume capability."""
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)
    write_json(checkpoint_path, {
        "processed": processed,
        "changed": changed,
        "total": total,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    })


def run_batch_review(
    dist_dir: Path,
    metadata_dir: Path,
    output_dir: Path,
    tiers: Optional[List[str]] = None,
    dry_run: bool = False,
    max_records: Optional[int] = None,
    verbose: bool = True,
) -> ReviewReport:
    """Run batch HEVL review across all (or selected) records."""

    # --- Initialize ---
    if verbose:
        print("[hdx_review] Building HDX metadata index...")
    hdx_index = build_hdx_index(metadata_dir)
    if verbose:
        print(f"  {len(hdx_index)} HDX metadata files indexed")

    if verbose:
        print("[hdx_review] Initializing HEVL extractors...")
    extractors = _init_extractors()

    # --- Scan dist tiers ---
    if verbose:
        print("[hdx_review] Scanning dist tiers...")
    file_list = _scan_dist_tiers(dist_dir, tiers)
    if max_records:
        file_list = file_list[:max_records]
    if verbose:
        print(f"  {len(file_list)} records to process")

    # --- Output directories ---
    revised_dir = output_dir / "revised"
    reports_dir = output_dir / "reports"
    checkpoint_dir = output_dir / "checkpoints"
    for d in (revised_dir, reports_dir, checkpoint_dir):
        d.mkdir(parents=True, exist_ok=True)

    # --- Process records ---
    total = len(file_list)
    changed_count = 0
    unchanged_count = 0
    skipped_count = 0
    errored_count = 0
    changes_by_type: Dict[str, int] = {}
    assessment_rows: List[Dict[str, Any]] = []

    t0 = time.time()
    for idx, (filepath, tier) in enumerate(file_list):
        # Progress
        if verbose and idx > 0 and idx % 100 == 0:
            elapsed = time.time() - t0
            rate = idx / elapsed if elapsed > 0 else 0
            eta = (total - idx) / rate if rate > 0 else 0
            print(
                f"  [{idx}/{total}] "
                f"changed={changed_count} skipped={skipped_count} "
                f"({rate:.0f} rec/s, ETA {eta:.0f}s)"
            )

        # Load record
        reviewable = load_rdls_record(filepath, tier)
        if reviewable is None:
            errored_count += 1
            continue

        # Find HDX metadata
        if not reviewable.hdx_uuid or reviewable.hdx_uuid not in hdx_index:
            # No cross-reference available -- copy as-is
            skipped_count += 1
            if not dry_run:
                tier_out = revised_dir / tier
                tier_out.mkdir(parents=True, exist_ok=True)
                write_json(tier_out / filepath.name, {"datasets": [reviewable.record]})
            continue

        # Load HDX metadata
        try:
            hdx_meta = load_json(hdx_index[reviewable.hdx_uuid])
        except Exception:
            skipped_count += 1
            if not dry_run:
                tier_out = revised_dir / tier
                tier_out.mkdir(parents=True, exist_ok=True)
                write_json(tier_out / filepath.name, {"datasets": [reviewable.record]})
            continue

        # Assess HEVL
        assessment = assess_hevl(reviewable, hdx_meta)

        # Build report row
        row = {
            "rdls_id": assessment.rdls_id,
            "hdx_uuid": assessment.hdx_uuid,
            "original_rdt": "|".join(assessment.current_rdt),
            "revised_rdt": "|".join(assessment.assessed_rdt),
            "has_discrepancy": assessment.has_discrepancy,
            "changes": "; ".join(assessment.changes) if assessment.changes else "",
            "confidence": assessment.confidence,
            "score_H": assessment.component_scores.get("H", 0),
            "score_E": assessment.component_scores.get("E", 0),
            "score_V": assessment.component_scores.get("V", 0),
            "score_L": assessment.component_scores.get("L", 0),
            "original_tier": tier,
            "original_file": filepath.name,
        }
        assessment_rows.append(row)

        if assessment.has_discrepancy:
            changed_count += 1
            for change in assessment.changes:
                # Track change types: "ADD loss", "REMOVE vulnerability", etc.
                ctype = change.split("(")[0].strip()
                changes_by_type[ctype] = changes_by_type.get(ctype, 0) + 1

            if not dry_run:
                # Revise the record
                revised = revise_record(reviewable, assessment, hdx_meta, extractors)
                tier_out = revised_dir / tier
                tier_out.mkdir(parents=True, exist_ok=True)
                write_json(tier_out / filepath.name, {"datasets": [revised]})
        else:
            unchanged_count += 1
            if not dry_run:
                # Copy unchanged record as-is
                tier_out = revised_dir / tier
                tier_out.mkdir(parents=True, exist_ok=True)
                write_json(tier_out / filepath.name, {"datasets": [reviewable.record]})

        # Checkpoint
        if idx > 0 and idx % 500 == 0:
            _write_checkpoint(
                checkpoint_dir / "_progress.json",
                idx, changed_count, total,
            )

    elapsed_total = time.time() - t0

    # --- Final checkpoint ---
    _write_checkpoint(
        checkpoint_dir / "_progress.json",
        total, changed_count, total,
    )

    # --- Write reports ---
    if verbose:
        print(f"\n[hdx_review] Writing reports...")

    # CSV report
    csv_path = reports_dir / "review_report.csv"
    if assessment_rows:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=assessment_rows[0].keys())
            writer.writeheader()
            writer.writerows(assessment_rows)

    # Flagged for review (discrepancies only)
    flagged = [r for r in assessment_rows if r["has_discrepancy"]]
    flagged_path = reports_dir / "flagged_for_review.csv"
    if flagged:
        with open(flagged_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=flagged[0].keys())
            writer.writeheader()
            writer.writerows(flagged)

    # Markdown summary
    md_lines = [
        "# HDX RDLS Second-Pass HEVL Review",
        "",
        "## Summary",
        "",
        f"| Metric | Value |",
        f"|--------|-------|",
        f"| **Total records** | {total} |",
        f"| **Changed** | {changed_count} ({100*changed_count/max(total,1):.1f}%) |",
        f"| **Unchanged** | {unchanged_count} |",
        f"| **Skipped** (no HDX metadata) | {skipped_count} |",
        f"| **Errors** | {errored_count} |",
        f"| **Processing time** | {elapsed_total:.1f}s |",
        f"| **Mode** | {'dry-run' if dry_run else 'full'} |",
        "",
        "## Changes by Type",
        "",
        "| Change | Count |",
        "|--------|------:|",
    ]
    for ctype, count in sorted(changes_by_type.items(), key=lambda x: -x[1]):
        md_lines.append(f"| {ctype} | {count} |")
    md_lines.append("")

    # Top changed records
    if flagged:
        md_lines.append("## Sample Changed Records (first 20)")
        md_lines.append("")
        md_lines.append("| ID | Original | Revised | Changes |")
        md_lines.append("|----|----------|---------|---------|")
        for row in flagged[:20]:
            md_lines.append(
                f"| {row['rdls_id'][:40]} | {row['original_rdt']} | "
                f"{row['revised_rdt']} | {row['changes'][:60]} |"
            )
        md_lines.append("")

    md_path = reports_dir / "review_summary.md"
    with open(md_path, "w", encoding="utf-8") as f:
        f.write("\n".join(md_lines) + "\n")

    # --- Print summary ---
    if verbose:
        print(f"\n{'='*60}")
        print(f"  REVIEW COMPLETE")
        print(f"{'='*60}")
        print(f"  Total:     {total}")
        print(f"  Changed:   {changed_count} ({100*changed_count/max(total,1):.1f}%)")
        print(f"  Unchanged: {unchanged_count}")
        print(f"  Skipped:   {skipped_count}")
        print(f"  Errors:    {errored_count}")
        print(f"  Time:      {elapsed_total:.1f}s")
        if changes_by_type:
            print(f"\n  Change breakdown:")
            for ctype, count in sorted(changes_by_type.items(), key=lambda x: -x[1]):
                print(f"    {ctype}: {count}")
        print(f"\n  Reports: {reports_dir}")
        if not dry_run:
            print(f"  Revised: {revised_dir}")

    return ReviewReport(
        total_records=total,
        records_changed=changed_count,
        records_unchanged=unchanged_count,
        records_skipped=skipped_count,
        records_errored=errored_count,
        changes_by_type=changes_by_type,
        assessment_details=assessment_rows,
    )


# ---------------------------------------------------------------------------
# Step 7: CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Second-pass HEVL review of RDLS JSON records from HDX pipeline"
    )
    parser.add_argument(
        "--dist-dir", required=True,
        help="Path to rdls/dist/ folder containing first-iteration RDLS JSONs",
    )
    parser.add_argument(
        "--metadata-dir", required=True,
        help="Path to dataset_metadata/ folder containing raw HDX JSONs",
    )
    parser.add_argument(
        "--output-dir", default="output/hdx",
        help="Output directory for revised records and reports (default: output/hdx)",
    )
    parser.add_argument(
        "--tiers", nargs="+", default=None,
        help="Tiers to process (default: all). Options: high, invalid/high, invalid/medium",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Analyze and report only, don't write revised records",
    )
    parser.add_argument(
        "--max-records", type=int, default=None,
        help="Maximum number of records to process (for testing)",
    )
    parser.add_argument(
        "--quiet", action="store_true",
        help="Suppress progress output",
    )

    args = parser.parse_args()

    dist_dir = Path(args.dist_dir).resolve()
    metadata_dir = Path(args.metadata_dir).resolve()
    output_dir = Path(args.output_dir).resolve()

    if not dist_dir.is_dir():
        print(f"Error: dist directory not found: {dist_dir}", file=sys.stderr)
        sys.exit(1)
    if not metadata_dir.is_dir():
        print(f"Error: metadata directory not found: {metadata_dir}", file=sys.stderr)
        sys.exit(1)

    run_batch_review(
        dist_dir=dist_dir,
        metadata_dir=metadata_dir,
        output_dir=output_dir,
        tiers=args.tiers,
        dry_run=args.dry_run,
        max_records=args.max_records,
        verbose=not args.quiet,
    )


if __name__ == "__main__":
    main()
