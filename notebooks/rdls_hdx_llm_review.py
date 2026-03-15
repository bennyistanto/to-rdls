"""
rdls_hdx_llm_review.py - LLM-Assisted HEVL Review & Final Distribution
===========================================================================

Pipeline step that follows the regex-based HEVL review (hdx_review.py).
Takes the revised RDLS records, runs LLM classification to fix fabricated
components and flag non-disaster datasets, then validates and distributes.

Full pipeline context:
    NB 01-02: Crawl HDX metadata + OSM exclusion
    NB 03-05: Classify to RDLS components, review & override
    NB 06:    Translate to RDLS JSON (general metadata)
    NB 07:    *** THIS SCRIPT - LLM review + validation + distribution ***
    NB 08-13: HEVL signal analysis & extraction (original notebooks)

Prerequisites:
    1. Regex review completed: output/hdx/revised/ has 12,594 records
    2. Column cache populated: output/column_cache/ (~57K files)
    3. Anthropic API key set: ANTHROPIC_API_KEY environment variable
    4. Python environment: conda activate to-rdls

Usage:
    cd C:\\Users\\benny\\OneDrive\\Documents\\Github\\to-rdls
    set ANTHROPIC_API_KEY=sk-ant-...
    python notebooks/rdls_hdx_llm_review.py

Inputs:
    - output/hdx/revised/          (12,594 RDLS JSON from regex review)
    - output/column_cache/         (CKAN column headers, pre-populated)
    - HDX metadata dump            (26,246 raw HDX metadata files)
    - RDLS schema v0.3             (for final validation)

Outputs:
    - output/llm/revised/          (8,491 RDLS-relevant records)
    - output/llm/not_rdls/         (4,103 non-disaster records)
    - output/llm/dist/high/        (schema-valid records, final)
    - output/llm/dist/invalid/     (records with schema errors)
    - output/llm/reports/          (CSVs, JSONL, summaries)

Cost: ~$22 for 12,594 records (Claude Haiku 4.5, one-time with caching)
Time: ~25 min (Phase 1: 3s cached, Phase 3: ~22 min)

Author: GFDRR / World Bank
Date: 2026-03-12
"""

# %% [markdown]
# # Configuration

# %%
import csv
import json
import re
import shutil
import sys
import time
from collections import Counter
from pathlib import Path

# --- Paths (adjust if your layout differs) ---
PROJECT_DIR = Path(__file__).resolve().parent.parent  # to-rdls/
HDX_CRAWLER_DIR = PROJECT_DIR.parent / "hdx-metadata-crawler"

# Input paths
DIST_DIR = PROJECT_DIR / "output" / "hdx" / "revised"
METADATA_DIR = HDX_CRAWLER_DIR / "hdx_dataset_metadata_dump" / "dataset_metadata"
SCHEMA_PATH = HDX_CRAWLER_DIR / "hdx_dataset_metadata_dump" / "rdls" / "schema" / "rdls_schema_v0.3.json"

# Output paths
OUTPUT_DIR = PROJECT_DIR / "output" / "llm"
REVISED_DIR = OUTPUT_DIR / "revised"
NOT_RDLS_DIR = OUTPUT_DIR / "not_rdls"
DIST_FINAL_DIR = OUTPUT_DIR / "dist"
REPORTS_DIR = OUTPUT_DIR / "reports"

# Verify paths exist
for name, path in [("DIST_DIR", DIST_DIR), ("METADATA_DIR", METADATA_DIR), ("SCHEMA_PATH", SCHEMA_PATH)]:
    if not path.exists():
        print(f"ERROR: {name} not found: {path}")
        sys.exit(1)

print(f"Project:    {PROJECT_DIR}")
print(f"Input:      {DIST_DIR}")
print(f"Metadata:   {METADATA_DIR}")
print(f"Schema:     {SCHEMA_PATH}")
print(f"Output:     {OUTPUT_DIR}")

# %% [markdown]
# # Step 1: Run LLM Review Pipeline (4 phases)
#
# This calls `src.llm_review.run_llm_review()` which orchestrates:
# - **Phase 1**: Signal triage - regex-based HEVL assessment, buckets records
#   into Confident (skip LLM), Borderline, No-signal, and Validation sample.
#   Cached after first run (~3s on subsequent runs vs ~190s fresh).
# - **Phase 2**: Column enrichment - loads cached CKAN column headers.
# - **Phase 3**: LLM classification - sends borderline/no-signal records to
#   Claude Haiku for component validation. Cached per prompt hash.
# - **Phase 4**: Merge - applies LLM decisions, writes revised JSONs.

# %%
from src.llm_review import run_llm_review, load_review_config

config = load_review_config()

print("=" * 60)
print("  STEP 1: LLM REVIEW PIPELINE")
print("=" * 60)

result = run_llm_review(
    dist_dir=str(DIST_DIR),
    metadata_dir=str(METADATA_DIR),
    output_dir=str(OUTPUT_DIR),
    config=config,
    dry_run=False,
    verbose=True,
)

print(f"\nPipeline complete.")
print(f"  Changed:    {result.get('changed', '?')}")
print(f"  Unchanged:  {result.get('unchanged', '?')}")
print(f"  LLM cost:   ${result.get('llm_cost', 0):.2f}")

# %% [markdown]
# # Step 2: Separate Not-RDLS Records
#
# The LLM flags records as "not RDLS relevant" when the dataset is about
# health indicators, education, gender stats, general development, or
# humanitarian operations that don't contain disaster risk data.
#
# These are moved to `output/llm/not_rdls/` to keep the main output clean.
# Domain breakdown (from full run):
# - other (2,029): education, economy, gender, general development
# - health (931): disease surveillance, nutrition, health indicators
# - reference (859): admin boundaries, code lists, gazetteers
# - humanitarian_ops (261): camp surveys, protection monitoring
# - climate (23): general climate data (not disaster-specific)

# %%
print("=" * 60)
print("  STEP 2: SEPARATE NOT-RDLS RECORDS")
print("=" * 60)

report_path = REPORTS_DIR / "review_report.csv"
if not report_path.exists():
    print(f"ERROR: Review report not found: {report_path}")
    print("  Run Step 1 first.")
    sys.exit(1)

# Identify not-RDLS IDs from review report
# Use new_id (renamed) if available, fall back to rdls_id (original)
not_rdls_ids = set()
domain_counts = Counter()
with open(report_path, "r", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        if "not RDLS relevant" in (row.get("changes") or ""):
            file_id = row.get("new_id") or row["rdls_id"]
            not_rdls_ids.add(file_id)
            domain = row.get("llm_domain", "unknown")
            domain_counts[domain] += 1

print(f"Not-RDLS records identified: {len(not_rdls_ids)}")
for domain, count in domain_counts.most_common():
    print(f"  {domain}: {count}")

# Index all JSON files in revised/ (recursive, handles tier subdirs)
NOT_RDLS_DIR.mkdir(parents=True, exist_ok=True)
all_files = {f.stem: f for f in REVISED_DIR.rglob("*.json")}
print(f"\nTotal JSON files in revised/: {len(all_files)}")

# Move matching files
moved = 0
for rdls_id in sorted(not_rdls_ids):
    if rdls_id in all_files:
        src = all_files[rdls_id]
        dst = NOT_RDLS_DIR / src.name
        if not dst.exists():  # Don't re-move if already done
            shutil.move(str(src), str(dst))
            moved += 1

remaining = len(list(REVISED_DIR.rglob("*.json")))
not_rdls_count = len(list(NOT_RDLS_DIR.glob("*.json")))

print(f"\nMoved:     {moved} (new this run)")
print(f"Remaining: {remaining} in revised/ (RDLS-relevant)")
print(f"Not-RDLS:  {not_rdls_count} in not_rdls/")

# %% [markdown]
# # Step 3: Validate Against RDLS Schema & Distribute
#
# Validates each remaining record against the RDLS v0.3 JSON Schema
# and distributes into tiered folders:
# - `dist/high/` - schema-valid records (ready for publication)
# - `dist/invalid/` - records with schema errors (need fixes)
#
# Common validation errors (known issues):
# - `referenced_by.author_names: minItems` - empty citation arrays
# - `referenced_by.doi: minLength` - empty DOI strings
# - `loss.impact_and_losses: required` - missing sub-field in loss blocks
# - `hazard.occurrence: minProperties` - empty occurrence:{} (schema gap)
# - `risk_data_type: minItems` - empty after LLM stripped all components

# %%
from src.utils import load_json, write_json

try:
    from jsonschema import Draft202012Validator
except ImportError:
    from jsonschema import Draft7Validator as Draft202012Validator

print("=" * 60)
print("  STEP 3: VALIDATE & DISTRIBUTE")
print("=" * 60)

schema = load_json(SCHEMA_PATH)
validator = Draft202012Validator(schema)

json_files = sorted(REVISED_DIR.rglob("*.json"))
print(f"Records to validate: {len(json_files)}")

# Clean previous distribution
if DIST_FINAL_DIR.exists():
    shutil.rmtree(DIST_FINAL_DIR, ignore_errors=True)

t0 = time.time()
valid_count = 0
invalid_count = 0
error_counter = Counter()

for i, fp in enumerate(json_files):
    data = load_json(fp)

    # Unwrap {"datasets": [...]} envelope
    if "datasets" in data and isinstance(data["datasets"], list):
        record = data["datasets"][0]
    else:
        record = data

    errors = sorted(validator.iter_errors(record), key=lambda e: list(e.path))

    if not errors:
        valid_count += 1
        tier = "high"
    else:
        invalid_count += 1
        tier = "invalid"
        for err in errors:
            path = ".".join(str(p) for p in err.absolute_path) or "(root)"
            path = re.sub(r"\.\d+\.", ".*.", path)
            error_counter[f"{path}: {err.validator}"] += 1

    tier_dir = DIST_FINAL_DIR / tier
    tier_dir.mkdir(parents=True, exist_ok=True)
    write_json(tier_dir / fp.name, data)

    if (i + 1) % 2000 == 0:
        print(f"  [{i+1}/{len(json_files)}]")

t1 = time.time()

print(f"\nValidation time: {t1-t0:.1f}s")
print(f"\n{'='*40}")
print(f"  VALIDATION RESULTS")
print(f"{'='*40}")
total = valid_count + invalid_count
print(f"  Total:   {total}")
print(f"  Valid:   {valid_count} ({100*valid_count/total:.1f}%)")
print(f"  Invalid: {invalid_count} ({100*invalid_count/total:.1f}%)")

print(f"\n  Distribution:")
for d in sorted(DIST_FINAL_DIR.iterdir()):
    count = len(list(d.glob("*.json")))
    print(f"    {d.name}/: {count}")

if error_counter:
    print(f"\n  Top 10 validation errors:")
    for err, count in error_counter.most_common(10):
        print(f"    {count:5d}  {err}")

# Save validation report
val_report = {
    "total": total,
    "valid": valid_count,
    "invalid": invalid_count,
    "valid_pct": round(100 * valid_count / total, 1),
    "top_errors": dict(error_counter.most_common(20)),
    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
}
write_json(REPORTS_DIR / "validation_report.json", val_report)

# %% [markdown]
# # Step 4: Summary
#
# Print the full pipeline summary with record counts at each stage.

# %%
print()
print("=" * 60)
print("  FULL PIPELINE SUMMARY")
print("=" * 60)
print()

# Count files in each output folder
def count_json(path):
    if not path.exists():
        return 0
    return len(list(path.rglob("*.json")))

n_input = count_json(DIST_DIR)
n_revised = count_json(REVISED_DIR)
n_not_rdls = count_json(NOT_RDLS_DIR)
n_valid = count_json(DIST_FINAL_DIR / "high")
n_invalid = count_json(DIST_FINAL_DIR / "invalid")

print(f"  Input (regex-revised):     {n_input:>6}")
print(f"  After LLM review:")
print(f"    RDLS-relevant:           {n_revised:>6}")
print(f"    Not-RDLS (separated):    {n_not_rdls:>6}")
print(f"  After validation:")
print(f"    Schema-valid (final):    {n_valid:>6}  -> output/llm/dist/high/")
print(f"    Schema-invalid:          {n_invalid:>6}  -> output/llm/dist/invalid/")
print()
print(f"  Reports:  {REPORTS_DIR}")
print(f"  Guide:    docs/llm_review_guide.md")
print("=" * 60)

# %% [markdown]
# # Notes
#
# ## Caching
# - **Phase 1 cache**: `.phase1_cache_*.pkl` in output/llm/ (~370MB).
#   Delete to force re-triage. Auto-invalidates on different dist_dir.
# - **LLM response cache**: output/llm/cache/ - keyed by prompt hash.
#   Re-runs with same records cost $0. Delete to force re-classification.
# - **Column cache**: output/column_cache/ - keyed by resource ID.
#   Persistent across all runs. Only populated via `python -m src.ckan_columns`.
#
# ## Cost Breakdown (March 2026 run)
# - Phase 1 (triage): FREE
# - Phase 2 (columns): FREE (cached)
# - Phase 3 (LLM): $21.98 for 7,761 LLM calls
#   - 11.8M input tokens, 2.0M output tokens
#   - Model: claude-haiku-4-5-20251001
#   - 4 connection errors (0.05% failure rate)
# - Total wall time: ~22 min (with caching: ~3s Phase 1 + ~22 min Phase 3)
#
# ## Known Schema Issues
# - `occurrence: {}` fails `minProperties: 1` - schema team aware, fix pending
# - `referenced_by.author_names/doi` - empty arrays/strings from HDX metadata
# - `risk_data_type: minItems` - records where LLM stripped all components
#   but the record wasn't flagged as not-RDLS (edge case)
#
# ## Re-running
# ```cmd
# # Full re-run (uses all caches, only re-does uncached work):
# python notebooks/rdls_hdx_07_llm_review.py
#
# # Force fresh triage (delete Phase 1 cache):
# del output\llm\.phase1_cache_*.pkl
# python notebooks/rdls_hdx_07_llm_review.py
#
# # Force fresh LLM calls (delete response cache):
# rmdir /s output\llm\cache
# python notebooks/rdls_hdx_07_llm_review.py
# ```
