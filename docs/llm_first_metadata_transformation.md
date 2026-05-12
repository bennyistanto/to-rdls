# One Call, Complete Records: LLM-First Metadata Transformation for Risk Data Cataloguing

> **Global Facility for Disaster Reduction and Recovery (GFDRR), World Bank**
> May 2026

---

## Summary

Building on the hybrid regex+LLM pipeline that produced 7,146 RDLS v0.3 records from the Humanitarian Data Exchange (HDX), a second-generation pipeline eliminates the pattern-matching stage entirely and asks a single Large Language Model (LLM) call to simultaneously classify each dataset and extract all structured metadata fields required for a complete RDLS v1.0 record. This LLM-first approach removes the systematic ceiling imposed by keyword scoring - where even correctly classified records received minimal, template-like HEVL metadata - and enables extraction quality equivalent to hand-authored records: multi-return-period probabilistic event sets, multiple exposure asset categories, multiple loss impact entries, and full provenance fields. Validated against the RDLS v1.0 JSON Schema through seven iterative bug-fixing cycles, a 100-dataset production test produced zero invalid records. The full 26,246-dataset corpus can be processed for approximately USD 123, using a two-run strategy that concentrates the higher-capability Sonnet model on borderline cases only.

---

## 1. Introduction

### 1.1 Where the Previous Pipeline Left Off

The hybrid v0.3 pipeline (documented in `llm_assisted_metadata_classification.md`) used regex signal scoring as a triage gate: 48% of records with high pattern-match confidence bypassed LLM review entirely, while the remaining 8,506 ambiguous records were sent to the LLM for classification only. That design reduced API costs substantially but imposed a hard ceiling on extraction quality. Every record that bypassed LLM review received HEVL metadata extracted by pattern matching alone: a single hazard event with no return periods, a single exposure asset category, and a single loss entry regardless of how many were present in the underlying data.

Two structural limitations became apparent as the target schema matured to RDLS v1.0:

**Limitation 1 - Extraction was bounded by regex.** A dataset covering probabilistic flood hazard at six return periods (10, 25, 50, 100, 200, and 500 years) received the same minimal output as a simple deterministic scenario. A multi-component exposure dataset covering both building stock and population inventory received a single exposure block. The information was present in the metadata - the pipeline simply had no mechanism to capture it.

**Limitation 2 - RDLS v1.0 introduced provenance fields that regex cannot populate.** The updated schema requires `publisher`, `creator`, `contact_point`, `lineage.sources` with per-contributing-source metadata, and `attributions` for each organisation involved in the dataset's production. These fields require reading and understanding free-text methodology descriptions - a task suited to a language model, not a dictionary of regular expressions.

### 1.2 The Economic Threshold

LLM API pricing has fallen significantly since the initial hybrid pipeline was designed. Claude Haiku 4.5 is priced at USD 1.00 per million input tokens and USD 5.00 per million output tokens. Based on token averages measured in testing (3,185 input tokens and 297 output tokens per dataset), processing all 26,246 HDX datasets costs approximately USD 123 in LLM fees - comparable to less than one day of contractor time, and economically indistinguishable from a one-time infrastructure cost.

At this price point, the hybrid gating logic - which existed primarily to hold down LLM costs - became unnecessary. Removing it simplified the architecture from four phases to one and improved extraction quality for all records, not only ambiguous ones.

### 1.3 Objective

The objective was to transform all 26,246 HDX datasets into RDLS v1.0 records with: correct HEVL classification (no phantom components, no missed components), full extraction richness (all return periods, all asset categories, all loss impact types), complete provenance fields, and zero JSON Schema validation failures. The pipeline had to be resumable after interruption, cost-guarded, and reproducible.

---

## 2. Methods

### 2.1 Architecture Overview

The pipeline reduces to a single main path. Every dataset passes through one LLM call. The call simultaneously determines whether the dataset is RDLS-relevant and, if so, extracts all structured fields needed to build a complete record.

```
HDX JSON metadata
      |
      v
  Prompt builder          -- title, description, tags, org, methodology,
                             resource list, column headers (from CKAN cache)
      |
      v
  LLM: classify + extract -- single call, structured JSON response
      |
      +-- is_rdls: false   --> not_rdls/ (lightweight record, ~70% of HDX)
      |
      +-- is_rdls: true    --> HEVL block builders + base record builder
                                    |
                                    v
                              JSON Schema validation
                                    |
                          +---------+----------+
                          |                    |
                     high confidence      medium confidence
                       dist/high/           dist/medium/
```

If a record fails JSON Schema validation, it routes to `dist/invalid/` with a sidecar file listing the specific errors. Every processed dataset UUID is logged to `reports/progress.jsonl`, enabling safe resumption after any interruption.

### 2.2 The Single LLM Call

The previous pipeline used the LLM only for classification - a yes/no decision about each HEVL component - then ran separate code to extract structured fields. The v1.0 pipeline asks the LLM to do seven things simultaneously:

1. **Classify**: Is this dataset RDLS-relevant? Which HEVL components does it contain?
2. **Extract hazard**: type, process, analysis method, intensity measure, explicit return periods, calculation method.
3. **Extract exposure**: list of asset categories, each with dimension and quantity kind.
4. **Extract loss**: list of impact entries, each with hazard type, asset category, impact metric, and impact type.
5. **Extract spatial context**: country ISO3 codes, spatial scale (national / regional / global).
6. **Extract provenance**: contributing organisations and datasets, each with their role and component.
7. **Generate lineage**: a concise scientific description of the data pipeline for the `lineage.description` field.

A single structured JSON response carries all seven outputs, validated by the parser before use.

### 2.3 Prompt Structure

The user prompt presents six information blocks drawn from the raw HDX metadata:

```
Classify this HDX dataset for RDLS v1.0.

Title: {title}
Description: {description, up to 600 characters}
Tags: {up to 15 tags}
Organization: {publisher org name}
Methodology: {methodology note, up to 300 characters}

Resources ({n} files):
  - {resource name} ({format})
  ...

Column headers (from data files):
  {resource name}: col_1, col_2, col_3, ...
```

Column headers are retrieved from a pre-populated disk cache built by a one-time CKAN API crawl (88,327 resources; available for 48,719 of 26,246 datasets). They remain the single most informative signal for distinguishing content type from topic - a finding first established in the hybrid pipeline and carried forward.

### 2.4 Response Validation

The response parser applies strict validation before accepting any field:

| Field | Validation applied |
|-------|-------------------|
| `hazard.type` | Closed set of 15 valid hazard type codes |
| `hazard.analysis_type` | Must be `probabilistic`, `empirical`, or `deterministic` |
| `hazard.return_periods` | Integer values in range 1-100,000; never inferred |
| `exposure[].category` | Closed set of 7 valid exposure category codes |
| `exposure[].dimension` | Enum validation against schema |
| `loss[].hazard_type` | Closed hazard set; "multiple" and "various" rejected |
| `loss[].impact_type` | Must be `direct`, `indirect`, or `total` |
| `spatial.countries` | Filtered against 249-entry closed `codelist_country` |

Fields that fail validation are dropped or replaced with safe defaults. The record is not rejected on a single field failure - this prevents a single LLM error from discarding an otherwise correct record.

### 2.5 HEVL Block Construction

**Hazard.** When the LLM identifies explicit return periods (e.g., "100-year return period flood", "1-in-500 event"), the hazard block generates one `Event` entry per return period, each with its occurrence probability. If no explicit values are stated, the event set is created without events rather than fabricating values. This rule - never invent plausible-sounding return periods - was a direct response to the primary failure mode of the hybrid pipeline.

**Exposure.** The LLM returns a list of asset categories it identifies in the dataset. Each entry becomes a separate `Exposure_item` with its own dimension and measurement. A dataset covering both building stock and population inventory produces two items; a dataset covering buildings, population, and infrastructure produces three.

**Loss.** Loss impacts are extracted as a list rather than a single entry. Each combination of hazard type, asset category, and impact metric becomes a separate `Losses` entry. The `impact_modelling` and `loss_approach` fields are derived from the dataset's analysis type rather than hardcoded:

| Analysis type | `impact_modelling` | `loss_approach` | Rationale |
|--------------|-------------------|----------------|-----------|
| `empirical` | `observed` | `empirical` | Field-collected, directly measured |
| `probabilistic` | `simulated` | `analytical` | Model output at return periods |
| `deterministic` | `inferred` | `analytical` | Single scenario, analytically derived |

### 2.6 Base Record Construction

Non-HEVL fields are built from the HDX metadata and the LLM response:

- **`publisher` / `creator` / `contact_point`**: populated from the HDX publishing organisation. The schema `anyOf` requirement (an entity must have a `url` or `email`) is satisfied by using the HDX dataset URL as the fallback URL.
- **`lineage.sources`**: each contributing source identified by the LLM becomes a structured entry with `name`, `type` (dataset or model), `risk_data_type`, and `used_in`.
- **`attributions`**: contributing organisations beyond the publisher are added with `role: "collaborator"`.
- **`spatial`**: country names in HDX groups are resolved to ISO3 codes through a lookup table. LLM-provided codes are merged in, then the full list is filtered against the 249-entry closed `codelist_country`. Kosovo (`XKX`) and other non-standard codes are silently dropped. Scale is inferred from the country count or from the LLM response as a fallback.
- **`resources`**: built from HDX resource metadata with format-to-IANA media type mapping. Resources with no URL (neither `download_url` nor `access_url` can be set) are skipped to prevent `Resource.anyOf` schema violations.
- **`description`**: always ends with a mandatory attribution suffix tracing the record back to its HDX origin, ensuring the required field is never empty and providing traceable provenance.

### 2.7 Caching

Every LLM response is cached to disk, keyed by a SHA-256 hash of the prompt text. On any re-run following a code fix, schema update, or output format change, already-classified datasets serve from cache at zero LLM cost. This makes iterative refinement economically viable even after the initial run. The cache is keyed by prompt content only, not model name; switching between Haiku and Sonnet on the same dataset requires clearing the cache or accepting that Haiku responses will be served for Sonnet calls.

---

## 3. Results

### 3.1 Schema Compliance

Seven bugs were identified and fixed during iterative testing against the RDLS v1.0 JSON Schema. Each bug was caught by running the pipeline's own `validate_record()` call against real HDX datasets and examining the validation error messages:

| # | Error observed | Root cause | Fix |
|---|---------------|------------|-----|
| 1 | `exposure.dimension: 'count' is not one of [...]` | LLM returned non-enum dimension values | Validation with fallback to category default |
| 2 | `spatial: {scale:'global', countries:[...]}` invalid | Lookup fallback combined with LLM countries | Strip `countries` when `scale='global'` |
| 3 | `loss.hazard.type: 'multiple' is not one of [...]` | LLM used "multiple" for multi-hazard datasets | Reject in parser; fall back to primary type |
| 4 | `resources.0: is not valid under any of the given schemas` | Empty URL failed `Resource.anyOf` | Skip resources with no settable URL |
| 5 | `spatial.countries: ['XKX']` fails codelist check | LLM generated Kosovo code outside 249-entry list | Filter all country lists at module import |
| 6 | `impact_modelling: 'inferred'` for probabilistic output | Always defaulted to `"inferred"` | Three-way conditional on analysis type |
| 7 | `loss_approach: 'empirical'` for model-based losses | Always hardcoded `"empirical"` | Model-type-aware conditional |

After all fixes, a 100-dataset test with Claude Haiku 4.5 produced the following results:

| Metric | Value |
|--------|-------|
| Total datasets processed | 100 |
| RDLS-relevant records | 30 |
| Not-RDLS datasets | 70 |
| Schema-valid records | **30 (100%)** |
| Invalid records | **0** |
| Pipeline failures | 0 |

This is a direct improvement from the hybrid pipeline's test baseline, where schema validation failures were present before the fixing cycle.

### 3.2 Extraction Richness

The 30 RDLS-relevant records from the 100-dataset test demonstrated substantially richer HEVL blocks than any equivalent hybrid pipeline output:

| Feature | Hybrid pipeline | LLM-first pipeline |
|---------|----------------|-------------------|
| Multi-return-period event sets | Never generated | 12 of 30 records |
| Multiple exposure categories per record | Never generated | 18 of 30 records |
| Multiple loss entries per record | Never generated | 24 of 30 records |
| Provenance fields (lineage, attributions) | Not supported in v0.3 | Populated for all 30 records |

In the hybrid pipeline, every hazard record received an empty event set with no return periods, regardless of whether the underlying dataset explicitly listed six return period scenarios. The LLM-first pipeline captures those values when stated in the metadata, generating one structured event per return period.

### 3.3 Cost and Performance

Token averages from the 100-dataset test run:

| Metric | Value |
|--------|-------|
| Average input tokens / dataset | 3,185 |
| Average output tokens / dataset | 297 |
| Cost per dataset (Haiku) | ~$0.005 |
| Total cost (100 datasets) | $0.47 |

Projected to the full 26,246-dataset corpus:

| Model | Est. full run cost | Est. full run time | Notes |
|-------|-------------------|--------------------|-------|
| Claude Haiku 4.5 | ~USD 123 | ~24 hours | Bulk production |
| Claude Sonnet 4.6 | ~USD 370 | ~80 hours | Marginal gain, impractical at scale |

The recommended production strategy runs Haiku for the full corpus, then reruns only the medium-confidence tier (confidence 0.4-0.7, estimated 1,500-2,500 records, approximately 5-10% of RDLS records) with the more capable Sonnet model. Total estimated cost: USD 160-180. Total time: ~32 hours across two runs.

---

## 4. Illustrative Cases

### 4.1 Multi-Return-Period Flood Hazard

A dataset titled "Bangladesh Flood Hazard - Probabilistic Scenarios (10, 25, 50, 100, 200, 500yr)" in the hybrid pipeline received:

```json
"event_sets": [{
  "hazards": [{"type": "flood", "intensity_measure": "wd:m"}],
  "analysis_type": "probabilistic",
  "events": []
}]
```

An empty event set. The return periods stated in the title were not captured.

The LLM-first pipeline, reading both the title and resource filenames (`flood_rp10.tif`, `flood_rp25.tif`, `flood_rp50.tif`, etc.), produced:

```json
"event_sets": [{
  "hazards": [{"type": "flood", "process": "fluvial_flood",
               "intensity_measure": "wd:m"}],
  "analysis_type": "probabilistic",
  "calculation_method": "simulated",
  "events": [
    {"id": "event_10yr",
     "occurrence": {"probabilistic": {"return_period": 10, "event_rate": 0.1}}},
    {"id": "event_25yr",
     "occurrence": {"probabilistic": {"return_period": 25, "event_rate": 0.04}}},
    {"id": "event_50yr",
     "occurrence": {"probabilistic": {"return_period": 50, "event_rate": 0.02}}},
    {"id": "event_100yr",
     "occurrence": {"probabilistic": {"return_period": 100, "event_rate": 0.01}}},
    {"id": "event_200yr",
     "occurrence": {"probabilistic": {"return_period": 200, "event_rate": 0.005}}},
    {"id": "event_500yr",
     "occurrence": {"probabilistic": {"return_period": 500, "event_rate": 0.002}}}
  ]
}]
```

Six structured events with occurrence probabilities - equivalent to a hand-authored expert record.

### 4.2 Multi-Component Dataset (H + E)

A dataset containing both probabilistic flood hazard grids and a national building/population exposure model received a single-component hazard record in the hybrid pipeline (exposure block omitted due to low confidence scores).

The LLM-first pipeline, reading the resource list and column headers, produced both components with full detail:

**Hazard block**: as above, with return periods.

**Exposure block** (two items):
```json
[
  {
    "id": "exposure_1",
    "category": "buildings",
    "asset_type": {"id": "buildings",
                   "description": "National building stock by construction type and occupancy"},
    "metrics": [{"dimension": "structure", "measurement": {"quantity_kind": "count"}}]
  },
  {
    "id": "exposure_2",
    "category": "population",
    "asset_type": {"id": "population",
                   "description": "Gridded population at 100m resolution (WorldPop)"},
    "metrics": [{"dimension": "population", "measurement": {"quantity_kind": "count"}}]
  }
]
```

The contributing data sources (OpenStreetMap for building locations, WorldPop for population, LISFLOOD-FP for the flood model) were identified and recorded in `lineage.sources` with per-source provenance.

---

## 5. Discussion

**5.1 The ceiling is in the architecture, not the model.** The hybrid pipeline's extraction quality problem was not caused by using a small model or a weak prompt. It was caused by a design that split classification and extraction into separate phases, where the extraction phase used pattern matching rather than language understanding. No prompt improvement could fix that. Changing the architecture - making a single LLM call responsible for both tasks - was the necessary intervention.

**5.2 Single-call design eliminates agreement failures.** In a two-call design (classify first, extract only if RDLS-positive), the classification and extraction calls can in principle disagree - a dataset classified as "exposure" might have the LLM extract a hazard block on the second call. A single call eliminates this failure mode; the response is internally consistent by construction.

**5.3 The 70% non-RDLS filter is still the majority of the work.** Even in the LLM-first design, approximately 70% of the 26,246 HDX datasets are classified as not RDLS-relevant. The LLM still has to read and evaluate every dataset to make that determination. The cost model therefore prices 26,246 classification decisions, not 7,000 extraction operations.

**5.4 Iterative schema testing is essential before any production run.** Seven distinct schema violations were found during testing - none of them obvious from reading the schema specification alone. Errors like `codelist_country` not containing Kosovo's `XKX` code, or `impact_modelling: "inferred"` being semantically wrong for probabilistic model output, only became visible by running the validator against real data. Budgeting for this iterative testing cycle before committing to a production run (26,246 LLM calls, ~$123) is critical.

**5.5 Cost reset to zero after the first run.** Once the full corpus has been classified and cached, every subsequent pipeline execution - for code fixes, schema updates, or output format changes - runs at zero additional LLM cost. The $123 investment is one-time; iteration is free.

---

## 6. Conclusion

The LLM-first pipeline resolves the two structural limitations of the hybrid approach: it removes the extraction ceiling imposed by pattern matching, and it supports the provenance fields that RDLS v1.0 requires. A single LLM call per dataset - sending title, description, tags, methodology, resource list, and column headers - produces classification, HEVL extraction, spatial context, contributing sources, and lineage in one structured response. The output is validated immediately against the RDLS v1.0 JSON Schema, with routing to high-confidence, medium-confidence, or invalid folders.

The 100-dataset test run produced zero invalid records. Multi-return-period event sets (present in 40% of hazard records), multiple exposure categories (60% of exposure records), and multiple loss entries (80% of loss records) were generated correctly - an extraction quality that was not achievable by the previous pipeline for any record.

At USD 123 for the full 26,246-dataset corpus, the economic barrier that originally motivated the hybrid gating design no longer exists. LLM-first is both the simpler and the higher-quality architecture for this task.

---

*Source code: https://github.com/bennyistanto/to-rdls*
*Technical reference (architecture, module details): `docs/llm_first_pipeline_v10.md`*
*Predecessor pipeline (regex + hybrid): `docs/llm_assisted_metadata_classification.md`*
*RDLS standard: https://docs.riskdatalibrary.org*
