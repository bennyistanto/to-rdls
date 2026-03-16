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
  ├─ Phase 1: Signal Triage ── 4,096 confident (skip LLM)
  │                          ── 8,506 sent to LLM
  ├─ Phase 2: Column Enrichment (adds CKAN column context)
  ├─ Phase 3: LLM Classification (Claude Haiku)
  ├─ Phase 4: Merge + Rename IDs + Write
  │
  ├─ Step 2: Separate not-RDLS ── 4,795 removed
  │
  └─ Step 3: Validate & Distribute
       ├─ dist/high/    3,922 schema-valid (final)
       └─ dist/invalid/ 3,868 schema-invalid
```

---

## Results Summary

### Record Disposition

| Category | Count | % of Input | Location |
|----------|------:|------------|----------|
| **RDLS-relevant** | **7,799** | 61.9% | `output/llm/revised/` |
| Schema-valid (production-ready) | 3,922 | 31.1% | `output/llm/dist/high/` |
| Schema-invalid (needs fixes) | 3,868 | 30.7% | `output/llm/dist/invalid/` |
| **Not-RDLS (removed)** | **4,795** | 38.1% | `output/llm/not_rdls/` |
| LLM errors (unprocessed) | 8 | 0.1% | Listed in `reports/failed_ids.txt` |

### What the LLM Changed

Of 12,594 records, **3,508 (27.9%)** were modified:

| Action | Count | Description |
|--------|------:|-------------|
| Flagged as not-RDLS | 4,795 | Dataset unrelated to disaster risk |
| ADD loss | 1,746 | LLM found genuine loss data the regex missed |
| REMOVE exposure | 1,688 | Fabricated exposure component removed |
| REMOVE hazard | 852 | Fabricated hazard component removed |
| REMOVE vulnerability | 551 | Fabricated vulnerability component removed |
| ADD hazard | 353 | LLM found genuine hazard data |
| ADD exposure | 201 | LLM found genuine exposure data |
| REMOVE loss | 120 | Fabricated loss component removed |
| ADD vulnerability | 5 | LLM found genuine vulnerability data |
| **IDs renamed** | **4,170** | Filename prefix updated to match new components |

### LLM Confidence

| Level | Count | % | Description |
|-------|------:|---|-------------|
| High (≥0.9) | 7,491 | 88.2% | Strong confidence in classification |
| Medium (0.7–0.9) | 995 | 11.7% | Moderate confidence |
| Low (<0.7) | 12 | 0.1% | Uncertain - worth manual review |
| **Average** | | **0.94** | |

---

## Distribution by Component Prefix

Every output file has an RDLS prefix encoding its component composition. Here is the full breakdown across all output folders:

| Prefix | Components | dist/high | dist/invalid | not_rdls | Total |
|--------|-----------|----------:|-------------:|---------:|------:|
| `exp` | Exposure only | 2,024 | 1,238 | 2,704 | 5,966 |
| `ev` | Exposure+Vulnerability | 93 | 1 | 2,197 | 2,291 |
| `lss` | Loss only | 907 | 476 | - | 1,383 |
| `he` | Hazard+Exposure | 176 | 581 | 99 | 856 |
| `hzd` | Hazard only | 11 | 752 | 54 | 817 |
| `el` | Exposure+Loss | 427 | 288 | 61 | 776 |
| `evl` | Exposure+Vln+Loss | 100 | 2 | 422 | 524 |
| `hel` | Hazard+Exposure+Loss | 6 | 318 | 12 | 336 |
| `hl` | Hazard+Loss | 10 | 220 | 1 | 231 |
| `hev` | Hazard+Exposure+Vln | 158 | 3 | 21 | 182 |
| `hevl` | All four (HEVL) | - | 1 | 9 | 10 |
| `vln` | Vulnerability only | - | - | 10 | 10 |
| `vl` | Vulnerability+Loss | 3 | 2 | 3 | 8 |
| `hvl` | Hazard+Vln+Loss | 1 | 1 | - | 2 |
| **Total** | | **3,916** | **3,883** | **5,593** | **13,392** |

**Key observations:**

- **Exposure-only (`exp`)** dominates - 5,966 records (45%). Most HDX datasets describe assets/populations at risk.
- **Loss-only (`lss`)** is the second largest RDLS category - 1,383 records of post-disaster impact data.
- **Vulnerability-only (`vln`)** has 0 files in dist - all 10 were classified as not-RDLS. Standalone vulnerability data (fragility curves, damage functions) without hazard/exposure/loss context is rare on HDX.
- **Full HEVL** reduced from thousands (regex over-classification) to just 10 - confirming the LLM correctly fixed Problem 7.
- **`ev` (exposure+vulnerability)** has 2,197 in not_rdls - many were general socioeconomic datasets that the regex misclassified.

---

## Not-RDLS Records

The LLM identified 4,795 records as not relevant to disaster risk data. These were moved to `output/llm/not_rdls/` and excluded from validation.

### Domain Categories

| Domain | Count | Description | Examples |
|--------|------:|-------------|----------|
| **other** | 2,660 | General statistics unrelated to disasters | Price indices, education stats, gender data, trade |
| **health** | 946 | Health/medical data | Disease surveillance, nutrition surveys, COVID tracking |
| **reference** | 893 | Reference/codelist data | Country code lists, administrative boundaries, lookup tables |
| **humanitarian_ops** | 261 | Humanitarian operations | Aid delivery tracking, refugee camp management, 3W/4W |
| **climate** | 35 | Climate data without disaster risk focus | General temperature records, climate normals |

### How to Review Not-RDLS Decisions

Each not-RDLS record has full LLM reasoning available in the reports:

**Quick check** - `review_report.csv`:
```csv
rdls_id,new_id,original_rdt,final_rdt,source,has_change,changes,confidence,llm_confidence,llm_domain
rdls_ev-abw_fao_arubaprices,...,...,...,llm,True,LLM: not RDLS relevant (domain=other),0.98,0.98,other
```

**Full reasoning** - `llm_classifications.jsonl`:
```json
{
  "rdls_id": "rdls_ev-abw_fao_arubaprices",
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

3,868 records have schema validation errors. The most common:

| Error | Count | Cause |
|-------|------:|-------|
| `referenced_by.*.author_names: minItems` | 3,604 | Empty author_names array `[]` |
| `referenced_by.*.doi: minLength` | 3,604 | Empty DOI string `""` |
| `loss.losses.*.impact_and_losses: required` | 2,917 | Missing required sub-field in loss block |
| `hazard.event_sets.*.events.*.occurrence: minProperties` | 1,914 | Empty `occurrence: {}` object |
| `loss.losses.0: required` | 640 | Malformed loss entry |
| `resources: minItems` | 47 | No resources attached |

These are **data quality issues** from the original HDX metadata, not LLM errors. A future sanitization step (NB 08) can address many of these automatically.

---

## Report Files Reference

All reports are in `output/llm/reports/`:

| File | Purpose | Key Columns/Fields |
|------|---------|-------------------|
| **`review_summary.md`** | Quick overview | Totals, cost, timing |
| **`review_report.csv`** | Per-record decisions | `rdls_id`, `new_id`, `original_rdt`, `final_rdt`, `changes`, `llm_confidence`, `llm_domain` |
| **`llm_classifications.jsonl`** | Full LLM reasoning | `components`, `reasoning` (per HEVL), `confidence`, `domain_category`, `is_rdls_relevant` |
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
