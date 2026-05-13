"""
rdls_hdx_pipeline.py - LLM-First HDX -> RDLS v1.0 Pipeline
============================================================

Direct single-phase pipeline: raw HDX JSON -> LLM classify+extract -> RDLS v1.0 record.
No regex pre-screening. One LLM call per dataset does both classification
and structured field extraction simultaneously.

Full pipeline context:
    This script is independent of the v0.3 pipeline (rdls_hdx_llm_review.py).
    It reads raw HDX JSONs directly from the crawler dump, not from the
    regex-review output. Output is RDLS v1.0 JSON (not v0.3).

Prerequisites:
    1. Column cache populated: output/hdx/column_cache/ (~88K files)
       (populate with: python -m src.sources.ckan_columns --metadata-dir ... --stats-only
        to verify, or run the ckan_columns crawler if cache is missing)
    2. Anthropic API key: ANTHROPIC_API_KEY environment variable
    3. Python environment: to-rdls conda env

Usage:
    cd C:\\\\Users\\\\benny\\\\OneDrive\\\\Documents\\\\Github\\\\to-rdls
    set ANTHROPIC_API_KEY=sk-ant-...
    C:/Users/benny/miniforge3/envs/to-rdls/python.exe notebooks/rdls_hdx_pipeline.py

    Optional args:
    --max-datasets 100    process only first N datasets (testing)
    --dry-run             classify only, do not write output records
    --config path/to.yaml use alternate config file

Inputs:
    - hdx-metadata-crawler/hdx_dataset_metadata_dump/dataset_metadata/  (26,246 files)
    - output/hdx/column_cache/    (CKAN column headers, pre-populated)
    - rdl-standard/schema/rdls_schema.json    (v1.0 JSON schema, authoritative)
    - configs/llm_review.yaml    (pipeline settings)

Outputs:
    - output/hdx/v1.0/dist/high/      confidence >= 0.7, schema-valid
    - output/hdx/v1.0/dist/medium/    confidence 0.4-0.69, schema-valid
    - output/hdx/v1.0/dist/invalid/   schema validation failed
    - output/hdx/v1.0/not_rdls/       is_rdls: false from LLM
    - output/hdx/v1.0/reports/        progress JSONL, summary CSV, cost log

Resumption:
    Progress is tracked in output/hdx/v1.0/reports/progress.jsonl.
    Re-running the script automatically skips already-processed datasets.
    To restart from scratch: delete progress.jsonl (keeps dist/ and not_rdls/).

Cost estimate:  ~$60-90 for 26,246 datasets (Claude Haiku 4.5)
Time estimate:  ~50-80 min (0.3s between live calls; cache hits are instant)
"""

# %% [markdown]
# # Setup

# %%
import argparse
import csv
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Set

# --- Project root ---
PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR))

# --- Input paths ---
HDX_CRAWLER_DIR = PROJECT_DIR.parent / "hdx-metadata-crawler"
METADATA_DIR = HDX_CRAWLER_DIR / "hdx_dataset_metadata_dump" / "dataset_metadata"
SCHEMA_PATH = PROJECT_DIR.parent / "rdl-standard" / "schema" / "rdls_schema.json"
CONFIG_PATH = PROJECT_DIR / "configs" / "llm_review.yaml"

# --- Load .env if present (avoids needing to set ANTHROPIC_API_KEY in the shell) ---
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_DIR / ".env", override=True)
except ImportError:
    pass  # python-dotenv not installed; rely on shell environment

# --- Parse CLI args (supports running both as a script and in notebook cells) ---
_parser = argparse.ArgumentParser(add_help=False)
_parser.add_argument("--max-datasets", type=int, default=None)
_parser.add_argument("--dry-run", action="store_true")
_parser.add_argument("--config", default=str(CONFIG_PATH))
_args, _unknown = _parser.parse_known_args()

MAX_DATASETS: Optional[int] = _args.max_datasets
DRY_RUN: bool = _args.dry_run
CONFIG_PATH = Path(_args.config)

# --- Verify prerequisites ---
for label, path in [
    ("METADATA_DIR", METADATA_DIR),
    ("SCHEMA_PATH",  SCHEMA_PATH),
    ("CONFIG_PATH",  CONFIG_PATH),
]:
    if not path.exists():
        print(f"ERROR: {label} not found: {path}")
        sys.exit(1)

print(f"Project:     {PROJECT_DIR}")
print(f"Metadata:    {METADATA_DIR}")
print(f"Schema:      {SCHEMA_PATH}")
print(f"Config:      {CONFIG_PATH}")
if MAX_DATASETS:
    print(f"Max datasets: {MAX_DATASETS} (testing mode)")
if DRY_RUN:
    print("DRY RUN: output files will NOT be written")


# %% [markdown]
# # Load Configuration and Schema

# %%
from src.utils import load_json, load_yaml
from src.schema import validate_record
from src.naming import load_naming_config
from src.spatial import load_spatial_config
from src.sources.ckan_columns import ColumnCache, load_columns_for_uuid
from src.llm_classify import (
    V10Config,
    LLMCacheV10,
    CostTracker,
    classify_v10,
)
from src.translate import (
    build_base_record_v10,
    wrap_datasets_v10,
    order_record_fields_v10,
)
from src.extract import integrate_hevl_v10

# Load pipeline config
raw_cfg = load_yaml(CONFIG_PATH)
cfg = V10Config.from_yaml(CONFIG_PATH)

# Output directories (from config)
_out_base = PROJECT_DIR / raw_cfg.get("output", {}).get("base_dir", "output/hdx/v1.0")
DIST_HIGH_DIR    = _out_base / "dist" / "high"
DIST_MEDIUM_DIR  = _out_base / "dist" / "medium"
DIST_INVALID_DIR = _out_base / "dist" / "invalid"
NOT_RDLS_DIR     = _out_base / "not_rdls"
REPORTS_DIR      = _out_base / "reports"

CONFIDENCE_HIGH   = float(raw_cfg.get("output", {}).get("confidence_high",   0.7))
CONFIDENCE_MEDIUM = float(raw_cfg.get("output", {}).get("confidence_medium", 0.4))

PROGRESS_FILE = PROJECT_DIR / raw_cfg.get("progress", {}).get(
    "file", "output/hdx/v1.0/reports/progress.jsonl"
)
SAVE_EVERY = int(raw_cfg.get("progress", {}).get("save_every", 100))

# Column cache dir (read-only during pipeline - pre-populated)
COL_CACHE_DIR = PROJECT_DIR / raw_cfg.get("ckan", {}).get(
    "cache_dir", "output/hdx/column_cache"
)

if not DRY_RUN:
    for d in [DIST_HIGH_DIR, DIST_MEDIUM_DIR, DIST_INVALID_DIR, NOT_RDLS_DIR, REPORTS_DIR]:
        d.mkdir(parents=True, exist_ok=True)
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

# Load JSON Schema
schema = load_json(SCHEMA_PATH)

# Verify schema version sanity
_schema_id = schema.get("$id", "")
print(f"Schema loaded: {_schema_id or '(no $id)'}  ({SCHEMA_PATH.name})")

# Load naming and spatial configs (shared across all records)
naming_config = load_naming_config(PROJECT_DIR / "configs" / "naming.yaml")
spatial_config = load_spatial_config(PROJECT_DIR / "configs" / "spatial.yaml")

print(f"\nOutput dirs:")
print(f"  High:    {DIST_HIGH_DIR}")
print(f"  Medium:  {DIST_MEDIUM_DIR}")
print(f"  Invalid: {DIST_INVALID_DIR}")
print(f"  Not RDLS:{NOT_RDLS_DIR}")
print(f"  Reports: {REPORTS_DIR}")
print(f"\nConfidence thresholds: high>={CONFIDENCE_HIGH}, medium>={CONFIDENCE_MEDIUM}")


# %% [markdown]
# # Load Progress (Resumption)

# %%
def load_done_ids(progress_file: Path) -> Set[str]:
    """Load set of already-processed HDX dataset IDs from progress JSONL."""
    done: Set[str] = set()
    if not progress_file.exists():
        return done
    with open(progress_file, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                hdx_id = rec.get("hdx_id")
                if hdx_id:
                    done.add(hdx_id)
            except json.JSONDecodeError:
                pass
    return done


done_ids = load_done_ids(PROGRESS_FILE)
print(f"Progress file: {PROGRESS_FILE}")
print(f"Already processed: {len(done_ids):,} datasets (will be skipped)")


# %% [markdown]
# # Collect Input Files

# %%
all_json_files = sorted(METADATA_DIR.glob("*.json"))
if MAX_DATASETS:
    all_json_files = all_json_files[:MAX_DATASETS]

# NOTE: file stems follow "{uuid}__{slug}" pattern - NOT equal to the hdx_id UUID.
# Done-check happens inside the main loop after reading the id field from JSON.
_approx_remaining = max(0, len(all_json_files) - len(done_ids))
print(f"\nTotal files:         {len(all_json_files):,}")
print(f"Already processed:   ~{len(done_ids):,} (exact check happens in loop)")
print(f"Estimated remaining: ~{_approx_remaining:,}")


# %% [markdown]
# # Initialize Pipeline Components

# %%
# API key
api_key = os.environ.get("ANTHROPIC_API_KEY", "")
if not api_key:
    print("ERROR: ANTHROPIC_API_KEY environment variable not set.")
    sys.exit(1)
print(f"API key: {'*' * 8}{api_key[-6:]}")

# Column cache (read-only - pre-populated by ckan_columns crawler)
col_cache = ColumnCache(COL_CACHE_DIR)
col_data_count, col_none_count = col_cache.count()
print(f"Column cache: {COL_CACHE_DIR}  ({col_data_count:,} with columns, {col_none_count:,} empty)")

# LLM cache (new - keyed by prompt hash, separate from v0.3 cache)
llm_cache = LLMCacheV10(cfg.cache_dir)
print(f"LLM cache:    {cfg.cache_dir}  ({llm_cache.size():,} cached responses)")

# Cost tracker
cost_tracker = CostTracker()


# %% [markdown]
# # Main Processing Loop

# %%
def _tier_dir(tier: str) -> Path:
    return {
        "high":    DIST_HIGH_DIR,
        "medium":  DIST_MEDIUM_DIR,
        "invalid": DIST_INVALID_DIR,
    }[tier]


def _write_record(record: Dict[str, Any], tier: str) -> Path:
    """Write a v1.0 RDLS record to the appropriate tier directory."""
    out_dir = _tier_dir(tier)
    rec_id = record.get("id", f"unknown_{int(time.time())}")
    out_path = out_dir / f"{rec_id}.json"
    wrapped = wrap_datasets_v10(record)
    out_path.write_text(
        json.dumps(wrapped, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return out_path


def _write_not_rdls(hdx_id: str, classification) -> None:
    """Write a lightweight not-RDLS record."""
    out_path = NOT_RDLS_DIR / f"{hdx_id}.json"
    out_path.write_text(
        json.dumps(
            {
                "hdx_id": hdx_id,
                "is_rdls": False,
                "not_rdls_reason": classification.not_rdls_reason,
                "domain": classification.domain,
                "confidence": classification.confidence,
                "reasoning": classification.reasoning,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )


def _append_progress(entry: Dict[str, Any]) -> None:
    """Append one progress record to the JSONL file."""
    with open(PROGRESS_FILE, "a", encoding="utf-8") as fh:
        fh.write(json.dumps(entry, ensure_ascii=False) + "\n")


def _ts() -> str:
    return datetime.now(tz=timezone.utc).isoformat(timespec="seconds")


# --- Loop counters ---
stats = {
    "total":        0,
    "rdls":         0,
    "not_rdls":     0,
    "skipped":      0,   # built record had no resources / base record failed
    "failed":       0,   # exception during processing
    "high":         0,
    "medium":       0,
    "invalid":      0,
    "from_cache":   0,
}

def _progress_bar(current: int, total: int, width: int = 28) -> str:
    """Return an ASCII progress bar: [=====>     ]  42.0%"""
    if total <= 0:
        return f"[{'?' * width}] ??.?%"
    pct = current / total
    filled = int(width * pct)
    arrow = ">" if filled < width else "="
    bar = "=" * max(0, filled - 1) + arrow + " " * max(0, width - filled)
    return f"[{bar}] {pct * 100:5.1f}%"


def _fmt_eta(seconds: float) -> str:
    """Format ETA in a human-friendly way (auto-selects units)."""
    s = int(seconds)
    if s < 0:
        return "--:--"
    if s < 3600:
        return f"{s // 60}m {s % 60:02d}s"
    h = s // 3600
    m = (s % 3600) // 60
    return f"{h}h {m:02d}m"


_start_time = time.time()
_last_heartbeat = time.time()
_PRINT_EVERY = 100      # status line every N processed records
_HEARTBEAT_SECS = 60    # also print if N seconds pass with no output (slow API)
_this_run_processed = 0  # records processed in this run (skips don't count)

print("\n" + "=" * 95)
print("  PROCESSING DATASETS")
print("=" * 95)
# Column header aligned to data lines
#  bar(37) + " " + count(11) + "  " + rdls(5) + " " + not(5) + " " + skip(4) + " " + fail(4)
#  + "  " + h(5) + " " + m(5) + " " + inv(4) + "  " + llm(5) + " " + cache(5) + " " + cost(6) + "  rate + eta
print(
    f"  {'bar + %  processed/total':38s}  "
    f"{'rdls':>5} {'!rds':>5} {'skip':>4} {'fail':>4}  "
    f"{'high':>5} {'med':>5} {'inv':>4}  "
    f"{'LLM':>5} {'cche':>5} {'$cost':>6}  "
    f"{'rate':<7} ETA"
)
print("-" * 95)

for i, json_path in enumerate(all_json_files):
    # --- Load HDX metadata ---
    try:
        hdx_meta = json.loads(json_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as exc:
        # File unreadable - log with stem as fallback id, but only if not done
        _fallback_id = json_path.stem
        if _fallback_id not in done_ids:
            print(f"  SKIP  {json_path.name}: cannot read ({exc})")
            stats["total"] += 1
            stats["failed"] += 1
            _this_run_processed += 1
            _append_progress({
                "hdx_id": _fallback_id,
                "status": "failed",
                "error": str(exc),
                "ts": _ts(),
            })
        continue

    hdx_id = hdx_meta.get("id", json_path.stem)

    # --- Skip if already processed in a prior run ---
    if hdx_id in done_ids:
        continue

    stats["total"] += 1
    _this_run_processed += 1

    # --- Load column headers from disk cache (no API calls) ---
    column_infos = load_columns_for_uuid(hdx_id, hdx_meta, col_cache)

    # --- LLM classify + extract ---
    try:
        clf = classify_v10(hdx_meta, column_infos, llm_cache, cfg, api_key)
    except RuntimeError as exc:
        # Spending limit or persistent API failure
        if "spending" in str(exc).lower() or "usage limit" in str(exc).lower():
            print(f"\nABORT: API spending limit reached after {stats['total']} records.")
            print(f"  {exc}")
            break
        print(f"  FAIL  {hdx_id}: LLM error: {exc}")
        stats["failed"] += 1
        _append_progress({
            "hdx_id": hdx_id,
            "status": "failed",
            "error": str(exc),
            "ts": _ts(),
        })
        continue
    except Exception as exc:
        print(f"  FAIL  {hdx_id}: unexpected error: {exc}")
        stats["failed"] += 1
        _append_progress({
            "hdx_id": hdx_id,
            "status": "failed",
            "error": str(exc),
            "ts": _ts(),
        })
        continue

    # Track cost
    cost_tracker.add(clf.token_usage, clf.from_cache)
    if clf.from_cache:
        stats["from_cache"] += 1

    # Rate limit delay (only for live API calls)
    if not clf.from_cache:
        time.sleep(cfg.rate_limit_delay)

    # Check cost limit
    if cost_tracker.check_limit(cfg):
        print(f"\nABORT: Cost limit ${cfg.max_cost_usd:.2f} exceeded.")
        print(f"  {cost_tracker.summary(cfg)}")
        _append_progress({
            "hdx_id": hdx_id,
            "status": "aborted_cost_limit",
            "ts": _ts(),
        })
        break

    # --- Not RDLS ---
    if not clf.is_rdls:
        stats["not_rdls"] += 1
        if not DRY_RUN:
            _write_not_rdls(hdx_id, clf)
        _append_progress({
            "hdx_id": hdx_id,
            "status": "not_rdls",
            "domain": clf.domain,
            "not_rdls_reason": clf.not_rdls_reason,
            "confidence": clf.confidence,
            "from_cache": clf.from_cache,
            "ts": _ts(),
        })
        continue

    # --- Build v1.0 base record ---
    try:
        base_record = build_base_record_v10(
            hdx_meta=hdx_meta,
            components=clf.components,
            llm_countries=clf.countries,
            llm_scale=clf.spatial_scale,
            llm_contributing_sources=clf.contributing_sources,
            llm_lineage_description=clf.lineage_description,
            naming_config=naming_config,
            spatial_config=spatial_config,
        )
    except Exception as exc:
        print(f"  FAIL  {hdx_id}: build_base_record failed: {exc}")
        stats["failed"] += 1
        _append_progress({
            "hdx_id": hdx_id,
            "status": "failed",
            "error": f"build_base_record: {exc}",
            "ts": _ts(),
        })
        continue

    if base_record is None:
        # Missing required fields (id, title, or no valid resources)
        stats["skipped"] += 1
        _append_progress({
            "hdx_id": hdx_id,
            "status": "skipped",
            "reason": "build_base_record returned None (missing title/resources)",
            "ts": _ts(),
        })
        continue

    # --- Integrate HEVL blocks ---
    try:
        full_record = integrate_hevl_v10(
            base_record=base_record,
            components=clf.components,
            hazard_info=clf.hazard,
            exposure_info=clf.exposure,
            vulnerability_info=clf.vulnerability,
            loss_info=clf.loss,
        )
    except Exception as exc:
        print(f"  FAIL  {hdx_id}: integrate_hevl failed: {exc}")
        stats["failed"] += 1
        _append_progress({
            "hdx_id": hdx_id,
            "status": "failed",
            "error": f"integrate_hevl: {exc}",
            "ts": _ts(),
        })
        continue

    # Apply canonical field order
    full_record = order_record_fields_v10(full_record)

    # --- Schema validation ---
    is_valid, schema_errors = validate_record(full_record, schema)
    record_id = full_record.get("id", hdx_id)

    # --- Determine output tier ---
    if not is_valid:
        tier = "invalid"
        stats["invalid"] += 1
    elif clf.confidence >= CONFIDENCE_HIGH:
        tier = "high"
        stats["high"] += 1
    elif clf.confidence >= CONFIDENCE_MEDIUM:
        tier = "medium"
        stats["medium"] += 1
    else:
        # Low confidence but is_rdls=True and schema-valid: put in medium
        tier = "medium"
        stats["medium"] += 1

    stats["rdls"] += 1

    # --- Write output ---
    if not DRY_RUN:
        _write_record(full_record, tier)
        if tier == "invalid":
            # Write validation errors to a side-car JSONL in reports/
            _errors_path = REPORTS_DIR / "validation_errors.jsonl"
            with open(_errors_path, "a", encoding="utf-8") as fh:
                fh.write(json.dumps({
                    "record_id": record_id,
                    "hdx_id": hdx_id,
                    "errors": schema_errors,
                    "ts": _ts(),
                }, ensure_ascii=False) + "\n")

    # --- Log progress ---
    _append_progress({
        "hdx_id":       hdx_id,
        "status":       "rdls",
        "tier":         tier,
        "record_id":    record_id,
        "components":   clf.components,
        "hazard_type":  (clf.hazard or {}).get("type") if clf.hazard else None,
        "exposure_cat": clf.exposure[0].get("category") if clf.exposure else None,
        "countries":    clf.countries,
        "confidence":   clf.confidence,
        "from_cache":   clf.from_cache,
        "schema_valid": is_valid,
        "schema_errors": schema_errors[:3] if schema_errors else [],
        "ts":           _ts(),
    })

    # --- Periodic status line (every 100 records, or every 60 s as heartbeat) ---
    now = time.time()
    _due = (_this_run_processed % _PRINT_EVERY == 0) or (now - _last_heartbeat >= _HEARTBEAT_SECS)
    if _due and _this_run_processed > 0:
        elapsed = now - _start_time
        rate = _this_run_processed / elapsed if elapsed > 0 else 0
        remaining = max(0, _approx_remaining - _this_run_processed)
        eta_secs = remaining / rate if rate > 0 else 0
        live_calls = cost_tracker.total_calls - cost_tracker.cached_calls
        bar = _progress_bar(_this_run_processed, _approx_remaining)
        rate_str = f"{rate:.1f}/s" if rate >= 0.1 else f"{rate * 60:.1f}/m"
        print(
            f"  {bar} {_this_run_processed:>5}/{_approx_remaining:<5}  "
            f"{stats['rdls']:>5} {stats['not_rdls']:>5} {stats['skipped']:>4} {stats['failed']:>4}  "
            f"{stats['high']:>5} {stats['medium']:>5} {stats['invalid']:>4}  "
            f"{live_calls:>5} {stats['from_cache']:>5} "
            f"{cost_tracker.cost_usd(cfg):>6.2f}  "
            f"{rate_str:<7} {_fmt_eta(eta_secs)}"
        )
        _last_heartbeat = now


# %% [markdown]
# # Final Summary

# %%
elapsed_total = time.time() - _start_time
print("\n" + "=" * 65)
print("  PIPELINE COMPLETE")
print("=" * 65)
print(f"  Total processed:  {stats['total']:,}")
print(f"  RDLS records:     {stats['rdls']:,}")
print(f"    - High:         {stats['high']:,}")
print(f"    - Medium:       {stats['medium']:,}")
print(f"    - Invalid:      {stats['invalid']:,}")
print(f"  Not RDLS:         {stats['not_rdls']:,}")
print(f"  Skipped (no data):{stats['skipped']:,}")
print(f"  Failed:           {stats['failed']:,}")
print(f"\n  {cost_tracker.summary(cfg)}")
print(f"  Elapsed:          {int(elapsed_total // 60)}m {int(elapsed_total % 60):02d}s")
print()

# Write summary JSON
summary = {
    "run_timestamp": _ts(),
    "config": str(CONFIG_PATH),
    "total_input_files": len(all_json_files),
    "previously_done": len(done_ids),
    "processed_this_run": _this_run_processed,
    "stats": stats,
    "cost": {
        "total_input_tokens":  cost_tracker.total_input_tokens,
        "total_output_tokens": cost_tracker.total_output_tokens,
        "total_calls":         cost_tracker.total_calls,
        "cached_calls":        cost_tracker.cached_calls,
        "cost_usd":            cost_tracker.cost_usd(cfg),
    },
    "elapsed_seconds": round(elapsed_total, 1),
}
summary_path = REPORTS_DIR / f"run_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
summary_path.write_text(
    json.dumps(summary, ensure_ascii=False, indent=2),
    encoding="utf-8",
)
print(f"  Summary written: {summary_path}")

# Write distribution CSV for easy inspection
# Reads progress.jsonl and counts by tier, hazard_type, exposure_cat, country
if PROGRESS_FILE.exists():
    _rows: List[Dict[str, Any]] = []
    with open(PROGRESS_FILE, "r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                _rows.append(json.loads(line))
            except json.JSONDecodeError:
                pass

    csv_path = REPORTS_DIR / "pipeline_report.csv"
    _fieldnames = [
        "hdx_id", "status", "tier", "record_id", "components",
        "hazard_type", "exposure_cat", "countries", "confidence",
        "from_cache", "schema_valid", "domain", "not_rdls_reason",
        "error", "ts",
    ]
    with open(csv_path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=_fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row in _rows:
            # Flatten list fields for CSV
            if isinstance(row.get("components"), list):
                row["components"] = "|".join(row["components"])
            if isinstance(row.get("countries"), list):
                row["countries"] = "|".join(row["countries"])
            if isinstance(row.get("schema_errors"), list):
                pass  # drop from CSV (in validation_errors.jsonl)
            writer.writerow(row)

    print(f"  CSV report:      {csv_path}  ({len(_rows):,} rows)")

print("\nOutput locations:")
if not DRY_RUN:
    for label, d in [
        ("High",    DIST_HIGH_DIR),
        ("Medium",  DIST_MEDIUM_DIR),
        ("Invalid", DIST_INVALID_DIR),
        ("Not RDLS",NOT_RDLS_DIR),
    ]:
        count = len(list(d.glob("*.json"))) if d.exists() else 0
        print(f"  {label:<10}: {d}  ({count:,} files)")
