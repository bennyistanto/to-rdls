# LLM Review Output - Results & Interpretation Guide

> Last updated: 2026-03-17 | Pipeline run: `notebooks/rdls_hdx_llm_review.py`

## What the LLM Review Does

The LLM review pipeline takes 12,594 regex-classified RDLS records and uses Claude Haiku to:

1. **Fix over-classification (Problem 7)** - Remove fabricated HEVL components that were assigned based on keywords rather than actual data content
2. **Identify non-RDLS datasets** - Flag records that don't contain disaster risk data (health surveys, price indices, education stats, etc.)
3. **Add missing components** - Where LLM identifies genuine components the regex missed
4. **Rename IDs and filenames** - Automatically update the `rdls_` prefix to match new component composition (e.g., `rdls_hevl-` → `rdls_lss-`)

---

## Pipeline Flow

```
Input: 12,594 regex-reviewed records (output/hdx/revised/)
  │
  ├─ Phase 1: Signal Triage ── 4,088 confident (skip LLM)
  │                          ── 8,506 sent to LLM
  ├─ Phase 2: Column Enrichment (adds CKAN column context)
  ├─ Phase 3: LLM Classification (Claude Haiku)
  ├─ Phase 4: Merge + Reconcile blocks + Rename IDs + Write
  │
  ├─ Step 2: Separate not-RDLS ── 4,794 LLM-flagged
  │                             ── 654 empty risk_data_type (no HEVL blocks)
  │
  └─ Step 3: Validate & Distribute
       ├─ dist/high/    3,312 schema-valid (final)
       └─ dist/invalid/ 3,834 schema-invalid
```

---

## Results Summary

### Record Disposition

| Category | Count | % of Input | Location |
|----------|------:|------------|----------|
| **RDLS-relevant** | **7,146** | 56.7% | `output/llm/revised/` |
| Schema-valid (production-ready) | 3,312 | 26.3% | `output/llm/dist/high/` |
| Schema-invalid (needs fixes) | 3,834 | 30.4% | `output/llm/dist/invalid/` |
| **Not-RDLS (removed)** | **5,448** | 43.3% | `output/llm/not_rdls/` |
| LLM-flagged not-RDLS | 4,794 | 38.1% | Semantic classification |
| Empty risk_data_type (no blocks) | 654 | 5.2% | Phantom block reconciliation |
| LLM errors (unprocessed) | 0 | 0% | All retries succeeded |

### What the LLM Changed

Of 12,594 records, **3,511 (27.9%)** were modified:

| Action | Count | Description |
|--------|------:|-------------|
| Flagged as not-RDLS | 4,794 | Dataset unrelated to disaster risk |
| Empty risk_data_type | 654 | No HEVL blocks after phantom reconciliation |
| REMOVE exposure | ~1,680 | Fabricated exposure component removed |
| REMOVE hazard | ~855 | Fabricated hazard component removed |
| REMOVE vulnerability | ~550 | Fabricated vulnerability component removed |
| REMOVE loss | ~120 | Fabricated loss component removed |
| ADD loss | ~1,750 | LLM found genuine loss data the regex missed |
| ADD hazard | ~360 | LLM found genuine hazard data |
| ADD exposure | ~200 | LLM found genuine exposure data |
| ADD vulnerability | ~5 | LLM found genuine vulnerability data |
| **IDs renamed** | **2,425** | Filename prefix updated to match new components |

> **Note on ADD actions**: The LLM may recommend adding a component, but the pipeline only includes it in `risk_data_type` if the corresponding HEVL block actually exists in the JSON. This prevents phantom components (e.g., "loss" in `risk_data_type` with no `loss` block).

### LLM Confidence

| Level | Count | % | Description |
|-------|------:|---|-------------|
| High (≥0.9) | 7,496 | 88.2% | Strong confidence in classification |
| Medium (0.7–0.9) | 994 | 11.7% | Moderate confidence |
| Low (<0.7) | 11 | 0.1% | Uncertain - worth manual review |
| **Average** | | **0.94** | |

### Console Output (First Production Run)

<details>
<summary>Click to view full console output ($21.98, 7,761 records, ~22 min)</summary>

```
❯ python -m src.llm_review --dist-dir output/hdx/revised --metadata-dir "..." --output-dir output/llm
[Phase 1] Loading from cache...
  12594 records loaded from cache
  Confident:    5087 (skip LLM)
  Borderline:   7052 (send to LLM)
  No-signal:    455 (send to LLM)
  Validation:   254 (5% cross-check)
  Time:         3.3s
  Records:      12594

  Estimated LLM cost: $14.36
  Records for LLM:   7761
  Cost guardrail:    $15.00

[Phase 2] Loading column headers from cache...
  With columns:    5093
  Without columns: 2668

[Phase 3] LLM classification (7761 records)...
  [50/7761] cached=0 errors=0 cost=$0.18 (16.2 rec/s, ETA 475s)
  [100/7761] cached=0 errors=0 cost=$0.33 (16.2 rec/s, ETA 473s)
  ...
  [1000/7761] cached=0 errors=0 cost=$3.08 (8.9 rec/s, ETA 761s)
  ...
  [2000/7761] cached=0 errors=0 cost=$6.13 (8.3 rec/s, ETA 695s)
  ...
  [3000/7761] cached=0 errors=0 cost=$8.92 (7.3 rec/s, ETA 656s)
  ...
  [4000/7761] cached=0 errors=0 cost=$11.54 (6.6 rec/s, ETA 569s)
  LLM error for rdls_exp-nic_dhs_nicaraguanationaldemograp: Connection error.
  [4300/7761] cached=0 errors=1 cost=$12.32 (6.1 rec/s, ETA 571s)
  LLM error for rdls_exp-nld_worldbank_economygrowth: Connection error.
  LLM error for rdls_exp-nor_metad4g_highresolutionpopulationd: Connection error.
  ...
  [5000/7761] cached=0 errors=3 cost=$14.17 (5.7 rec/s, ETA 488s)
  ...
  LLM error for rdls_he-ind_heigit_accessibilityindicators: Connection error.
  [5700/7761] cached=0 errors=4 cost=$16.10 (5.5 rec/s, ETA 376s)
  ...
  [6000/7761] cached=0 errors=4 cost=$16.98 (5.5 rec/s, ETA 321s)
  ...
  [7000/7761] cached=0 errors=4 cost=$19.84 (5.8 rec/s, ETA 131s)
  ...
  [7750/7761] cached=0 errors=4 cost=$21.95 (6.1 rec/s, ETA 2s)

  Failed IDs (4): saved to output\llm\reports\failed_ids.txt

  LLM complete: 7757 classified, 4 errors
  Cached:       0
  Tokens:       11,830,496 in / 2,028,982 out
  Cost:         $21.98
  Time:         1266.8s

[Phase 4] Merging results and writing output...

============================================================
  LLM REVIEW COMPLETE
============================================================
  Total:          12594
  Changed:        3443 (27.3%)
  Unchanged:      9151
  Disagreements:  173 (validation sample)
  LLM cost:       $21.98
  Time:           1343.4s
  Reports:        output\llm\reports
  Revised:        output\llm\revised
============================================================
```

</details>

---

## Distribution by Component Prefix

Every output file has an RDLS prefix encoding its component composition. Here is the full breakdown across all output folders:

| Prefix | Components | dist/high | dist/invalid | not_rdls | Total |
|--------|-----------|----------:|-------------:|---------:|------:|
| | **Single component** | | | | |
| `hzd` | Hazard | 12 | 1,001 | 182 | 1,195 |
| `exp` | Exposure | 2,460 | 1,299 | 2,286 | 6,045 |
| `vln` | Vulnerability | 3 | 1 | 11 | 15 |
| `lss` | Loss | 370 | 436 | - | 806 |
| | **Two components** | | | | |
| `he` | Hazard+Exposure | - | 802 | 54 | 856 |
| `hv` | Hazard+Vulnerability | - | - | - | - |
| `hl` | Hazard+Loss | - | 38 | - | 38 |
| `ev` | Exposure+Vulnerability | 348 | 2 | 2,448 | 2,798 |
| `el` | Exposure+Loss | 118 | 226 | 5 | 349 |
| `vl` | Vulnerability+Loss | - | 1 | - | 1 |
| | **Three components** | | | | |
| `hev` | Hazard+Exposure+Vln | - | 2 | 46 | 48 |
| `hel` | Hazard+Exposure+Loss | - | 24 | 7 | 31 |
| `hvl` | Hazard+Vln+Loss | - | - | - | - |
| `evl` | Exposure+Vln+Loss | 1 | 1 | 407 | 409 |
| | **Four components** | | | | |
| `hevl` | Hazard+Exposure+Vln+Loss | - | 1 | 2 | 3 |
| **Total** | | **3,312** | **3,834** | **5,448** | **12,594** |

**Key observations:**

- **Exposure-only (`exp`)** dominates — 6,045 records (48%). Most HDX datasets describe assets/populations at risk.
- **Hazard-only (`hzd`)** has 1,195 records but only 12 schema-valid — most lack required `occurrence` sub-fields.
- **Loss-only (`lss`)** — 806 records of post-event impact data, roughly split between valid and invalid.
- **Vulnerability-only (`vln`)** has 15 files total (3 valid, 1 invalid, 11 not-RDLS). Standalone vulnerability data (fragility curves, damage functions) is rare on HDX.
- **Full HEVL** reduced from thousands (regex over-classification) to just 3 — confirming the LLM correctly fixed Problem 7.
- **`ev` (exposure+vulnerability)** has 2,448 in not_rdls — many were general socioeconomic datasets that the regex misclassified. Only 2 remain in dist/invalid after reconciliation.
- **Phantom component fix**: `risk_data_type` is reconciled with actual HEVL blocks — if a block doesn't exist in the JSON, the component is removed from `risk_data_type` regardless of what the LLM recommended. 654 records with no blocks at all are moved to not_rdls.

---

## Not-RDLS Records

The pipeline identified 5,448 records as not belonging in the RDLS output. These were moved to `output/llm/not_rdls/` and excluded from validation. This includes 4,794 flagged by the LLM as not disaster-relevant, plus 654 with empty `risk_data_type` after phantom block reconciliation.

### Domain Categories

| Domain | Count | Description | Examples |
|--------|------:|-------------|----------|
| **other** | 2,657 | General statistics unrelated to disasters | Price indices, education stats, gender data, trade |
| **health** | 941 | Health/medical data | Disease surveillance, nutrition surveys, COVID tracking |
| **reference** | 894 | Reference/codelist data | Country code lists, administrative boundaries, lookup tables |
| **humanitarian_ops** | 263 | Humanitarian operations | Aid delivery tracking, refugee camp management, 3W/4W |
| **climate** | 39 | Climate data without disaster risk focus | General temperature records, climate normals |

### How to Review Not-RDLS Decisions

Each not-RDLS record has full LLM reasoning available in the reports:

**Quick check** - `review_report.csv`:
```csv
rdls_id,new_id,hdx_link,original_rdt,final_rdt,source,has_change,changes,confidence,llm_confidence,llm_domain
rdls_ev-abw_fao_arubaprices,...,https://data.humdata.org/dataset/...,exposure|vulnerability,...,llm,True,LLM: not RDLS relevant (domain=other),0.98,0.98,other
```

**Full reasoning** - `llm_classifications.jsonl`:
```json
{
  "rdls_id": "rdls_ev-abw_fao_arubaprices",
  "hdx_link": "https://data.humdata.org/dataset/...",
  "is_rdls_relevant": false,
  "components": {"hazard": false, "exposure": false, "vulnerability": false, "loss": false},
  "reasoning": {
    "hazard": "Dataset contains consumer price indices, deflators...",
    "exposure": "Dataset does not enumerate assets, populations...",
    "vulnerability": "Dataset contains no damage functions...",
    "loss": "Dataset contains economic price data, not post-event impact..."
  },
  "confidence": 0.98,
  "domain_category": "other"
}
```

---

## ID and Filename Renaming

When the LLM changes a record's component composition, the pipeline automatically updates both the JSON `id` field and the filename.

### Prefix Mapping

| Components | Prefix | Example |
|-----------|--------|---------|
| Hazard only | `hzd` | `rdls_hzd-ken_...` |
| Exposure only | `exp` | `rdls_exp-bgd_...` |
| Loss only | `lss` | `rdls_lss-ken_ocharosea_droughtrelatedkeyfigures` |
| Vulnerability only | `vln` | `rdls_vln-...` |
| Hazard + Exposure | `he` | `rdls_he-...` |
| Hazard + Exposure + Loss | `hel` | `rdls_hel-...` |
| All four (HEVL) | `hevl` | `rdls_hevl-...` |

Full mapping in `configs/naming.yaml`.

### Example: Kenya Drought Key Figures

```
Before LLM:  rdls_hevl-ken_ocharosea_droughtrelatedkeyfigures.json
              risk_data_type: ["hazard", "exposure", "vulnerability", "loss"]
              (fabricated H, E, V components based on "drought" keyword)

After LLM:   rdls_lss-ken_ocharosea_droughtrelatedkeyfigures.json
              risk_data_type: ["loss"]
              (correctly identified as loss-only: displacement/mortality figures)
```

---

## Validation Errors (Schema-Invalid Records)

3,834 records have schema validation errors. The most common:

| Error | Count | Cause |
|-------|------:|-------|
| `referenced_by.*.author_names: minItems` | 3,587 | Empty author_names array `[]` |
| `referenced_by.*.doi: minLength` | 3,587 | Empty DOI string `""` |
| `loss.losses.*.impact_and_losses: required` | 2,907 | Missing required sub-field in loss block |
| `hazard.event_sets.*.events.*.occurrence: minProperties` | 1,927 | Empty `occurrence: {}` (schema gap) |
| `loss.losses.0: required` | 638 | Malformed loss entry |
| `resources: minItems` | 24 | No resources attached |

These are **data quality issues** from the original HDX metadata, not LLM errors. Records with empty `risk_data_type` (654 files with no HEVL blocks) are now moved to `not_rdls/` rather than remaining in `dist/invalid`. A future sanitization step (NB 08) can address the remaining errors automatically.

---

## Report Files Reference

All reports are in `output/llm/reports/`:

| File | Purpose | Key Columns/Fields |
|------|---------|-------------------|
| **`review_summary.md`** | Quick overview | Totals, cost, timing |
| **`review_report.csv`** | Per-record decisions | `rdls_id`, `new_id`, `hdx_link`, `original_rdt`, `final_rdt`, `changes`, `llm_confidence`, `llm_domain` |
| **`llm_classifications.jsonl`** | Full LLM reasoning | `rdls_id`, `hdx_link`, `components`, `reasoning` (per HEVL), `confidence`, `domain_category`, `is_rdls_relevant` |
| **`triage_summary.csv`** | Phase 1 bucket assignments | Signal scores, bucket (confident/borderline/no-signal) |
| **`disagreements.csv`** | LLM vs regex conflicts | Validation sample where LLM overrode regex |
| **`failed_ids.txt`** | Unprocessed records | IDs that failed all retry attempts |
| **`validation_report.json`** | Schema validation | Valid/invalid counts, error breakdown |

### How to Use the Reports

**"Why was record X classified as not-RDLS?"**
→ Search `review_report.csv` for the ID, check `llm_domain`. For full reasoning, search `llm_classifications.jsonl`.

**"What changed for record X?"**
→ `review_report.csv` → `changes` column shows ADD/REMOVE operations. `original_rdt` vs `final_rdt` shows before/after.

**"Which records have low confidence?"**
→ Filter `review_report.csv` where `llm_confidence < 0.7` (only 12 records).

**"Why did a record fail validation?"**
→ Check `validation_report.json` or re-validate individual files against the schema.

---

## Caching & Re-runs

| Cache | Location | Size | Purpose |
|-------|----------|------|---------|
| Phase 1 triage | `output/llm/.phase1_cache_*.pkl` | ~354 MB | Regex signal scores |
| LLM responses | `output/llm/cache/*.json` | ~13,500 files | Prompt hash → LLM response |
| Column headers | `output/column_cache/` | ~57,000 files | CKAN column data |

**Re-running costs $0** when inputs haven't changed - all LLM responses are cached by prompt hash. Only records with changed prompts (due to code/config changes) trigger new API calls.
