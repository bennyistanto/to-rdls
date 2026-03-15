# LLM-Assisted HEVL Review Pipeline - Operations Guide

> Last updated: 2026-03-13 (post sanitize + validate)

## Overview

This pipeline classifies 12,594 RDLS records into HEVL components (Hazard, Exposure, Vulnerability, Loss) using a 4-phase approach: regex triage → column enrichment → LLM classification → merge, followed by schema-driven sanitization and validation.

**Important**: The `--dist-dir` should point to the **revised** records from the regex review (`output/hdx/revised/`), NOT the original dist. This way the LLM review builds on top of the regex improvements (2,036 changes from Problems 1-6).

```
Phase 1: Signal Triage (regex, free, ~10 min)
    ↓
Phase 2: Column Enrichment (CKAN API, free, ~8 hrs first run)
    ↓
Phase 3: LLM Classification (Claude Haiku, ~$11.49)
    ↓
Phase 4: Merge + Write (automatic)
    ↓
Step 8: Sanitize + Validate (offline, ~2 min)
```

---

## Pipeline Scripts

| Script | Step | Description |
|--------|------|-------------|
| `src/ckan_columns.py` | 1 | Fetch column headers from HDX CKAN API |
| `src/llm_review.py` | 2-5 | LLM review pipeline (triage → classify → merge) |
| `notebooks/rdls_hdx_07_llm_review.py` | 5-7 | Automated: full run + not-RDLS separation + validation |
| `notebooks/rdls_hdx_08_sanitize_validate.py` | 8 | Rebuild, sanitize, and validate all records |

---

## Prerequisites

### Python Environment

| Step | Conda Env | Why |
|------|-----------|-----|
| Column cache (Phase 2) | `climate` or `to-rdls` | Only needs `requests` |
| LLM review (Phase 1,3,4) | `to-rdls` only | Needs `anthropic`, `PyYAML`, `src.*` |
| Sanitize + Validate (Step 8) | `to-rdls` only | Needs `jsonschema`, `src.*` |

### Dependencies

All listed in `requirements.txt`. Install with:

```cmd
conda run -n to-rdls pip install -r requirements.txt
```

| Package | Version | Used By |
|---------|---------|---------|
| PyYAML | >=6.0 | Config loading (all modules) |
| requests | >=2.28 | CKAN API (`ckan_columns.py`) |
| jsonschema | >=4.20 | Schema validation |
| anthropic | >=0.40 | LLM API (`llm_review.py`) |
| pandas | >=2.0 | Notebook scripts |
| openpyxl | >=3.1 | Excel read/write |
| xlrd | >=2.0 | Legacy .xls read |
| geopandas | >=0.14 | Geospatial (optional) |

### API Keys

| Key | Where to Get | Used By | Required? |
|-----|-------------|---------|-----------|
| HDX API key | https://data.humdata.org/user/ → API Token (36-char UUID) | `ckan_columns.py` | Optional (speeds up Phase 2) |
| Anthropic API key | https://console.anthropic.com/settings/keys (starts with `sk-ant-`) | `llm_review.py` | Required for Phase 3 |

---

## Step-by-Step Commands

### Step 1: Populate Column Cache (Phase 2)

Fetches actual column headers from HDX for each resource. Creates `.json` (has columns) and `.none` (no columns) cache files per resource.

**Test on 10 datasets first (~5 seconds):**

```cmd
cd C:\Users\benny\OneDrive\Documents\Github\to-rdls

conda run -n climate python -m src.ckan_columns ^
  --metadata-dir "C:\Users\benny\OneDrive\Documents\Github\hdx-metadata-crawler\hdx_dataset_metadata_dump\dataset_metadata" ^
  --cache-dir output/column_cache ^
  --max-datasets 10
```

**Full run (~48+ hours):**

```cmd
conda run -n climate python -m src.ckan_columns ^
  --metadata-dir "C:\Users\benny\OneDrive\Documents\Github\hdx-metadata-crawler\hdx_dataset_metadata_dump\dataset_metadata" ^
  --cache-dir output/column_cache ^
  --api-key YOUR_HDX_36_CHAR_UUID
```

**Check cache stats (no fetching):**

```cmd
conda run -n climate python -m src.ckan_columns ^
  --metadata-dir "C:\Users\benny\OneDrive\Documents\Github\hdx-metadata-crawler\hdx_dataset_metadata_dump\dataset_metadata" ^
  --cache-dir output/column_cache ^
  --stats-only
```

**Notes:**
- Resume-safe: if interrupted, re-run the same command. Already-cached resources are skipped.
- **Actual results (2026-03-12):** 88,327 resources crawled across 12,594 datasets. ~55% yielded column headers, ~45% are `.none` sentinels (geo formats, PDFs, etc.).
- **Time: 48+ hours** (with HDX API key). The rate-limited CKAN API is the bottleneck.
- This is a **one-time prerequisite**. Once built, the cache is reused by all subsequent LLM review runs (Phase 2 simply looks up cached headers by resource ID).
- Can use any Python env that has `requests` installed (e.g. `climate`).
- HDX API key is the 36-character UUID from your HDX profile, NOT the longer 197-char token.

---

### Step 2: Set Anthropic API Key

Required for Phase 3 (LLM classification). Pick ONE method:

**Method A: Environment variable (lasts until terminal closes):**

```cmd
set ANTHROPIC_API_KEY=sk-ant-api03-YOUR-KEY-HERE
```

**Method B: `.env` file (persists across sessions):**

```cmd
cd C:\Users\benny\OneDrive\Documents\Github\to-rdls
echo ANTHROPIC_API_KEY=sk-ant-api03-YOUR-KEY-HERE > .env
```

The `.env` file is already in `.gitignore` - your key won't be committed.

**Method C: CLI argument (per-command):**

```cmd
conda run -n to-rdls python -m src.llm_review ... --api-key sk-ant-api03-YOUR-KEY-HERE
```

---

### Step 3: Dry-Run (Phase 1 only, FREE)

Runs signal triage to see bucket distribution and cost estimate. No LLM calls, no API key needed.

**Dry-run on 100 records:**

```cmd
cd C:\Users\benny\OneDrive\Documents\Github\to-rdls

conda run -n to-rdls python -m src.llm_review ^
  --dist-dir output/hdx/revised ^
  --metadata-dir "C:\Users\benny\OneDrive\Documents\Github\hdx-metadata-crawler\hdx_dataset_metadata_dump\dataset_metadata" ^
  --output-dir output/llm ^
  --dry-run ^
  --max-records 100
```

**Dry-run on all 12,594 records (~10 min):**

```cmd
conda run -n to-rdls python -m src.llm_review ^
  --dist-dir output/hdx/revised ^
  --metadata-dir "C:\Users\benny\OneDrive\Documents\Github\hdx-metadata-crawler\hdx_dataset_metadata_dump\dataset_metadata" ^
  --output-dir output/llm ^
  --dry-run
```

**Expected output:**

```
[Phase 1] Signal triage...
  Confident:    5087 (skip LLM)
  Borderline:   7052 (send to LLM)
  No-signal:    455 (send to LLM)
  Validation:   254 (5% cross-check)

  Estimated LLM cost: $11.49
  Records for LLM:   7761
  Cost guardrail:    $15.00
```

---

### Step 4: Pilot Run (100 records, ~$0.10)

First real LLM run. Verifies everything works end-to-end.

```cmd
cd C:\Users\benny\OneDrive\Documents\Github\to-rdls

conda run -n to-rdls python -m src.llm_review ^
  --dist-dir output/hdx/revised ^
  --metadata-dir "C:\Users\benny\OneDrive\Documents\Github\hdx-metadata-crawler\hdx_dataset_metadata_dump\dataset_metadata" ^
  --output-dir output/llm ^
  --max-records 100
```

**Review pilot results:**

```cmd
:: Summary report
type output\llm\reports\review_summary.md

:: Per-record details (CSV)
type output\llm\reports\review_report.csv

:: LLM reasoning audit trail
type output\llm\reports\llm_classifications.jsonl
```

---

### Step 5: Full Run (12,594 records, ~$11.49)

Once pilot looks good, run on all records.

```cmd
cd C:\Users\benny\OneDrive\Documents\Github\to-rdls

conda run -n to-rdls python -m src.llm_review ^
  --dist-dir output/hdx/revised ^
  --metadata-dir "C:\Users\benny\OneDrive\Documents\Github\hdx-metadata-crawler\hdx_dataset_metadata_dump\dataset_metadata" ^
  --output-dir output/llm
```

**Estimated time:** ~25 min (Phase 1: 3s cached / 190s fresh, Phase 3: ~22 min).

**Actual results (2026-03-12 run):**

```
Total:          12594
Changed:        3443 (27.3%)
Unchanged:      9151
Disagreements:  173 (validation sample)
LLM cost:       $21.98
Tokens:         11,830,496 in / 2,028,982 out
Errors:         4 (connection timeouts, 0.05%)
Time:           1343.4s (~22 min)
```

---

### Step 6-7: Automated Post-Processing

`notebooks/rdls_hdx_07_llm_review.py` runs Steps 5-7 (LLM pipeline + not-RDLS separation + basic validation) in sequence. This is the original pipeline script.

---

### Step 8: Sanitize, Rebuild IDs & Validate (FREE, offline)

This is the **recommended post-processing step** after the LLM review. It rebuilds all records from source, applies schema-driven sanitization, corrects IDs/filenames, and validates.

```cmd
cd C:\Users\benny\OneDrive\Documents\Github\to-rdls
conda activate to-rdls
set PYTHONPATH=C:\Users\benny\OneDrive\Documents\Github\to-rdls
python notebooks\rdls_hdx_08_sanitize_validate.py
```

**What it does (6 steps):**

| Step | Action | Details |
|------|--------|---------|
| 1 | Load review report | Reads `review_report.csv` for LLM decisions |
| 2 | Load LLM classifications | Reads `llm_classifications.jsonl` for REMOVE operations |
| 3 | Index source files | Maps all 12,594 source records from `output/hdx/revised/` |
| 4 | Rebuild revised/ and not_rdls/ | Applies LLM changes, sanitizes, rebuilds IDs |
| 5 | Verify filename vs record.id | Cross-checks filename HEVL code matches `risk_data_type` |
| 6 | Validate & distribute | Schema validation, sorts into `dist/high/` and `dist/invalid/` |

**Schema-driven sanitization (Step 4) fixes:**

| Fix | Schema Rule | Action |
|-----|-------------|--------|
| `referenced_by` empty optionals | `author_names` minItems:1, `doi` minLength:1 | Strip empty `[]`/`""` |
| Empty `losses: []` | `losses` minItems:1 | Remove `loss` block |
| Loss entry missing `impact_and_losses` | required field | Drop entry, remove block if all gone |
| Empty `event_sets: []` | `event_sets` minItems:1 | Remove `hazard` block |
| Empty `hazards: []` in event_set | `hazards` minItems:1 | Drop event_set |
| Empty `events: []` in event_set | optional but minItems:1 | Strip empty array |
| Empty `socio_economic: []` | minItems:1 | Strip empty array |
| Empty `functions: []` | minItems:1 | Strip empty array |
| Invalid country codes (XKX) | closed codelist (249 ISO3) | Filter from array |
| No resources | `resources` minItems:1 | Move to `not_rdls/` |
| Empty optional fields | `""`, `[]`, `None` | Strip to avoid minLength/minItems |
| risk_data_type mismatch | Must match HEVL blocks present | Reconcile after block removal |
| Field ordering | Schema property order | Reorder: id...resources...loss...links |
| `occurrence: {}` | minProperties:1 | **Kept as-is** (team will revise schema) |

**Actual results (2026-03-13):**

```
  Written to revised/:  8822
  Written to not_rdls/: 3772
  Renamed:              4087
  HEVL changed:         3426
  Filename/ID mismatch: 0
  Filename/RDT mismatch: 0

  Valid:   6,132  (69.4%)
  Invalid: 2,690  (30.5%)  -- all occurrence:{} only
```

**Time:** ~2 min, **Cost:** $0 (fully offline, no API calls)

**Re-runnable:** Fully idempotent. Rebuilds everything from source + report each time.

---

## Output Files

After Steps 5-8, outputs go to `output/llm/`:

```
output/llm/
├── revised/                         # RDLS-relevant records (sanitized)
│   └── high/                        # All RDLS records (8,822)
├── not_rdls/                        # Non-disaster records (3,772)
├── dist/                            # Final validated distribution
│   ├── high/                        # Schema-valid (6,132)
│   └── invalid/                     # Schema errors (2,690 - all occurrence:{})
├── reports/
│   ├── review_summary.md            # Start here: aggregate stats
│   ├── triage_summary.csv           # Bucket assignment per record
│   ├── review_report.csv            # Per-record: original vs final rdt
│   ├── disagreements.csv            # LLM vs regex conflicts (validation sample)
│   ├── llm_classifications.jsonl    # Full audit: reasoning, confidence, domain
│   ├── validation_report.json       # Schema validation results
│   └── failed_ids.txt              # Records with LLM errors
├── cache/                           # LLM response cache (DO NOT DELETE)
│   └── {prompt_hash}.json
└── .phase1_cache_*.pkl              # Phase 1 triage cache (~370MB)
```

### Key Reports

| File | What It Shows |
|------|---------------|
| `review_summary.md` | Overall stats: total, changed, cost, timing |
| `triage_summary.csv` | Which bucket each record landed in + signal scores |
| `review_report.csv` | Per-record: original_rdt, final_rdt, source (signal/llm), changes |
| `disagreements.csv` | Where LLM disagreed with regex on validation sample |
| `llm_classifications.jsonl` | Full LLM response: components, reasoning, confidence, domain |
| `validation_report.json` | Schema validation: valid/invalid counts, top errors |

---

## Re-Running and Caching

The pipeline is **fully idempotent**:

| Component | Cache Location | Behavior on Re-Run |
|-----------|---------------|---------------------|
| Column headers | `output/column_cache/` | Skips cached resources |
| LLM responses | `output/llm/cache/` | Same prompt = skip API call ($0) |
| Step 8 sanitize | No cache needed | Rebuilds from source each time |

**Re-run cost = $0** if inputs haven't changed. Only new/modified records trigger API calls.

To force re-classification (e.g. after prompt change), delete the LLM cache:

```cmd
:: Delete LLM cache to force re-classification
rmdir /s /q output\llm\cache
```

Column cache should NOT be deleted (expensive to rebuild).

---

## Configuration

All settings in `configs/llm_review.yaml`:

| Setting | Default | Description |
|---------|---------|-------------|
| `triage.confident_score_min` | 5 | Min signal score to skip LLM |
| `triage.max_components_for_confident` | 2 | Max active components for confident |
| `triage.validation_sample_pct` | 0.05 | 5% of confident sent to LLM cross-check |
| `llm.model` | claude-haiku-4-5-20251001 | LLM model |
| `llm.temperature` | 0.0 | Deterministic output |
| `llm.max_concurrent` | 5 | Parallel API calls |
| `llm.max_cost_usd` | 15.0 | Cost guardrail (abort if exceeded) |
| `merge.llm_overrides_signals` | true | LLM wins when confidence >= threshold |
| `merge.disagreement_confidence_min` | 0.7 | Min LLM confidence to override regex |

---

## Troubleshooting

| Error | Fix |
|-------|-----|
| `ANTHROPIC_API_KEY not set` | See Step 2 above (env var, .env, or --api-key) |
| `anthropic package not installed` | `conda run -n to-rdls pip install anthropic` |
| `cost_guardrail_exceeded` | Increase `max_cost_usd` in `configs/llm_review.yaml` |
| Interrupted mid-run | Re-run same command. Cached results reused automatically |
| `ModuleNotFoundError: src` | Make sure you `cd` to `to-rdls/` project root first |
| Wrong conda env | Column cache: `climate` OK. LLM review + Step 8: must use `to-rdls` |
| `PYTHONPATH` not set | `set PYTHONPATH=C:\Users\benny\OneDrive\Documents\Github\to-rdls` |

---

## Cost Summary

| Step | Cost | Time | Repeatable |
|------|------|------|------------|
| Column cache (Step 1) | Free | 48+ hrs with API key (88,327 resources) | Once |
| Dry-run (Step 3) | Free | ~3s cached / ~3 min fresh | Unlimited |
| Pilot 100 records (Step 4) | ~$0.10 | ~2 min | Cached after first run |
| Full 12,594 records (Step 5) | **$21.98** | ~22 min | Cached after first run |
| Sanitize + Validate (Step 8) | Free | ~2 min | Idempotent |
| Any re-run | $0 | ~2 min | Fully cached |

---

## Record Flow Summary

```
HDX crawl (26,246)
  -> OSM excluded (3,649)
  -> RDLS candidates (13,053)
  -> Regex review: output/hdx/revised/ (12,594)
  -> LLM review + sanitize:
       output/llm/revised/ (8,822 RDLS-relevant)
       output/llm/not_rdls/ (3,772 not disaster-relevant)
  -> Validation:
       output/llm/dist/high/ (6,132 schema-valid, 69.4%)
       output/llm/dist/invalid/ (2,690 occurrence:{} only, 30.5%)
```

**Note:** Once the team revises the `occurrence` schema (remove minProperties:1), all 2,690 invalid records should become valid, bringing the total to ~8,822 valid (99.8%).
