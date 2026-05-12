---
marp: true
---

# LLM-First Metadata Transformation: HDX to RDLS v1.0

> **Global Facility for Disaster Reduction and Recovery (GFDRR), World Bank**
> May 2026

---

## Summary

Following the hybrid regex+LLM pipeline that produced 7,146 RDLS v0.3 records from HDX (documented in `llm_assisted_metadata_classification.md`), a second-generation pipeline was developed targeting RDLS v1.0 and the full 26,246-dataset HDX corpus. The new design eliminates the regex pre-screening stage entirely: a single LLM call per dataset simultaneously classifies the dataset's RDLS component types and extracts all structured metadata fields needed to construct a complete RDLS v1.0 record. This LLM-first approach removes the systematic ceiling imposed by pattern matching and enables richer HEVL block construction - including multi-return-period event sets, multiple exposure asset categories, and multiple loss impact entries per record - equivalent to hand-authored records. Validated against the RDLS v1.0 JSON Schema, a 100-dataset test run produced zero invalid records.

---

## 1. Background and Motivation

### 1.1 What the Previous Pipeline Could Not Do

The hybrid v0.3 pipeline (described in `llm_assisted_metadata_classification.md`) used regex signal scoring as a first-pass triage gate. Records with high signal scores bypassed the LLM entirely; only ambiguous records received semantic review. This design reduced LLM costs by 48% but introduced two structural limitations that became apparent as the target schema matured to v1.0:

**Limitation 1 - Extraction quality was bounded by regex.** High-confidence records that bypassed LLM review received HEVL metadata extracted purely by pattern matching: a single hazard event with no return periods, a single exposure category, a single loss entry. Where the underlying dataset contained probabilistic hazard data at six return periods or exposure data for both buildings and population, the output captured only a fraction of the available information.

**Limitation 2 - v1.0 schema requires fields the regex pipeline could not produce.** RDLS v1.0 introduced new top-level fields (`publisher`, `creator`, `contact_point`, `lineage.sources` with per-source provenance, `attributions` for contributing organisations) that have no analogue in metadata tag patterns. Populating these fields correctly requires reading and understanding free-text methodology descriptions - a task suited to an LLM but not to a dictionary of regex patterns.

### 1.2 The LLM-First Decision

With LLM API costs having fallen substantially since the initial hybrid design (Claude Haiku 4.5 at USD 1.00/MTok input, USD 5.00/MTok output), it became economically viable to send all 26,246 datasets through the LLM at an estimated total cost of ~USD 123 - comparable to a one-day contractor task. This made the hybrid gating logic unnecessary. Removing it simplified the architecture from four phases to one and improved extraction quality across all records, not only ambiguous ones.

---

## 2. Pipeline Architecture

### 2.1 Overview

```
HDX JSON (raw)
    |
    v
build_prompt_v10()          -- title + description + tags + org + methodology
    |                          + resources list + column headers (from CKAN cache)
    v
LLM: classify_v10()         -- single Claude call: classify AND extract
    |                          returns V10Classification dataclass
    |
    +-- is_rdls: false  -->  not_rdls/  (lightweight JSON, no schema record built)
    |
    +-- is_rdls: true   -->
            |
            v
    build_base_record_v10()  -- publisher, creator, contact_point, spatial,
            |                   license, lineage, attributions, resources, links
            v
    integrate_hevl_v10()     -- hazard block (with events[]),
            |                   exposure block (list),
            |                   loss block (list of losses)
            v
    validate_record()        -- JSON Schema v1.0 validation
            |
            +-- valid + high confidence  -->  dist/high/
            +-- valid + medium confidence -->  dist/medium/
            +-- invalid                   -->  dist/invalid/
                                               (+ validation_errors.jsonl sidecar)
```

### 2.2 Key Modules

| Module | Role |
|--------|------|
| `src/llm_classify_v10.py` | Prompt builder, LLM API call, response parser, disk cache |
| `src/translate_v10.py` | Base record builder: all non-HEVL v1.0 fields |
| `src/extract_v10.py` | HEVL block builders: hazard, exposure, vulnerability, loss |
| `notebooks/rdls_hdx_v10.py` | Pipeline orchestrator: resumable loop, cost guard, progress tracking |
| `configs/llm_review_v10.yaml` | All runtime settings: model, pricing, thresholds, paths |

### 2.3 Resumability

Every processed dataset is logged by its UUID to `reports/progress.jsonl`. Re-running the pipeline after any interruption (network error, cost limit, manual stop) automatically skips already-processed datasets by checking this file. This allows safe long-running execution across multiple sessions without reprocessing or duplicating records.

---

## 3. The Single-Phase LLM Call

### 3.1 What the LLM Does in One Call

The previous pipeline used the LLM for classification only, then ran separate code to extract structured fields. The v1.0 pipeline asks the LLM to do both simultaneously:

1. **Classify**: Is this dataset RDLS-relevant? Which components does it contain (hazard / exposure / vulnerability / loss)?
2. **Extract hazard details**: type, process, analysis method, intensity measure, return periods, calculation method
3. **Extract exposure details**: list of asset categories with dimension and quantity kind per category
4. **Extract loss details**: list of impact combinations with hazard type, asset category, impact metric, and impact type per entry
5. **Extract spatial context**: country ISO3 codes, spatial scale
6. **Extract provenance**: contributing organisations with their role in the dataset
7. **Generate lineage**: a scientific description of the data pipeline for the lineage field

All seven outputs are returned in a single structured JSON response, validated by `_parse_response()` before use.

### 3.2 Prompt Structure

The user prompt contains six information blocks drawn from the raw HDX metadata:

```
Classify this HDX dataset for RDLS v1.0.

Title: {title}
Description: {description, truncated to 600 chars}
Tags: {up to 15 tags}
Organization: {publisher org name}
Data source: {dataset_source, if different from org}
Methodology: {methodology, truncated to 300 chars}

Resources (N files):
  - {resource name} ({format})
  ...

Column headers (from data files):
  {resource name}: col1, col2, col3, ...
```

Column headers are retrieved from a pre-populated disk cache built by a one-time CKAN API crawl (88,327 resources; available for 48,719 of 26,246 datasets). They remain the single most informative signal for distinguishing content type from topic.

### 3.3 Response Schema

The LLM returns a JSON object with this structure:

```json
{
  "is_rdls": true,
  "components": ["hazard", "exposure"],
  "hazard": {
    "type": "flood",
    "process": "fluvial_flood",
    "analysis_type": "probabilistic",
    "imt": "wd:m",
    "calculation_method": "simulated",
    "return_periods": [10, 25, 50, 100, 200, 500],
    "description": "Probabilistic fluvial flood hazard at 6 return periods"
  },
  "exposure": [
    {
      "category": "buildings",
      "dimension": "structure",
      "quantity_kind": "count",
      "description": "Residential and commercial building stock by construction type"
    },
    {
      "category": "population",
      "dimension": "population",
      "quantity_kind": "count",
      "description": "Gridded population at 100m resolution"
    }
  ],
  "vulnerability": null,
  "loss": [
    {
      "hazard_type": "flood",
      "asset_category": "buildings",
      "impact_metric": "buildings_damaged_count",
      "impact_type": "direct",
      "imt": null,
      "description": "Direct structural damage to buildings from fluvial flooding"
    }
  ],
  "spatial_scale": "national",
  "countries": ["BGD"],
  "contributing_sources": [
    {
      "name": "OpenStreetMap",
      "used_in": "exposure",
      "type": "dataset",
      "description": "Building footprint data used to construct the exposure layer"
    }
  ],
  "lineage_description": "Flood hazard layers derived from hydrodynamic modelling...",
  "confidence": 0.92,
  "not_rdls_reason": null,
  "domain": "disaster_risk",
  "reasoning": "Dataset contains probabilistic flood inundation depths..."
}
```

### 3.4 Response Validation

`_parse_response()` applies strict validation before accepting any field:

| Field | Validation applied |
|-------|-------------------|
| `hazard.type` | Must be in closed `_VALID_HAZARD_TYPES` set (15 codes) |
| `hazard.analysis_type` | Must be `probabilistic`, `empirical`, or `deterministic` |
| `hazard.calculation_method` | Must be `simulated`, `observed`, or `inferred` |
| `hazard.return_periods` | Each value cast to `int`; must be in range 1-100,000 |
| `exposure[].category` | Must be in closed `_VALID_EXPOSURE_CATS` set (7 codes) |
| `exposure[].dimension` | Validated against closed enum in `build_exposure_block()` |
| `loss[].hazard_type` | Must be in `_VALID_HAZARD_TYPES`; "multiple"/"various" rejected |
| `loss[].impact_type` | Must be `direct`, `indirect`, or `total` |
| `contributing_sources[].type` | Must be `dataset` or `model` |
| `contributing_sources[].used_in` | Must be a valid RDLS component |
| `spatial.countries` | Each code filtered against 249-entry closed `codelist_country` |

Fields that fail validation are dropped or replaced with safe defaults rather than causing record failure.

---

## 4. HEVL Block Construction

### 4.1 Hazard Block - Multi-Return-Period Events

When the LLM identifies a probabilistic hazard dataset and provides explicit return periods, the hazard block generates one `Event` entry per return period:

```json
{
  "event_sets": [{
    "id": "event_set_1",
    "hazards": [{
      "id": "hazard_1",
      "type": "flood",
      "process": "fluvial_flood",
      "intensity_measure": "wd:m"
    }],
    "analysis_type": "probabilistic",
    "calculation_method": "simulated",
    "events": [
      {
        "id": "event_10yr",
        "calculation_method": "simulated",
        "hazard": {"type": "flood", "process": "fluvial_flood", "intensity_measure": "wd:m"},
        "occurrence": {"probabilistic": {"return_period": 10, "event_rate": 0.1}},
        "description": "Probabilistic fluvial flood hazard at 6 return periods"
      },
      {
        "id": "event_25yr",  "occurrence": {"probabilistic": {"return_period": 25, "event_rate": 0.04}}
      },
      {
        "id": "event_50yr",  "occurrence": {"probabilistic": {"return_period": 50, "event_rate": 0.02}}
      },
      {
        "id": "event_100yr", "occurrence": {"probabilistic": {"return_period": 100, "event_rate": 0.01}}
      },
      {
        "id": "event_200yr", "occurrence": {"probabilistic": {"return_period": 200, "event_rate": 0.005}}
      },
      {
        "id": "event_500yr", "occurrence": {"probabilistic": {"return_period": 500, "event_rate": 0.002}}
      }
    ]
  }]
}
```

This is equivalent to what a subject-matter expert would write by hand for a probabilistic hazard catalogue entry.

**Return period rule**: return periods are only populated when the LLM finds values explicitly stated in the metadata (e.g., *"100-year return period flood"*, *"1-in-500 event"*). The LLM is instructed never to invent plausible-sounding values.

### 4.2 Exposure Block - Multiple Asset Categories

Where a dataset covers more than one asset type, the exposure block produces one `Exposure_item` per category:

```json
[
  {
    "id": "exposure_1",
    "category": "buildings",
    "asset_type": {
      "id": "buildings",
      "description": "Residential and commercial building stock by construction type"
    },
    "metrics": [{"id": "metric_1", "dimension": "structure", "measurement": {"quantity_kind": "count"}}]
  },
  {
    "id": "exposure_2",
    "category": "population",
    "asset_type": {
      "id": "population",
      "description": "Gridded population at 100m resolution"
    },
    "metrics": [{"id": "metric_1", "dimension": "population", "measurement": {"quantity_kind": "count"}}]
  }
]
```

### 4.3 Loss Block - Multiple Impact Entries

Where a dataset records multiple impact types (e.g., building damage AND displacement), each combination becomes a separate `Losses` entry:

```json
{
  "losses": [
    {
      "id": "loss_1",
      "hazard": {"type": "flood", "intensity_measure": "wd:m"},
      "asset_category": "buildings",
      "asset_dimension": "structure",
      "description": "Direct structural damage to buildings from fluvial flooding",
      "impact_and_losses": {
        "impact_type": "direct",
        "impact_modelling": "simulated",
        "impact_metric": "buildings_damaged_count",
        "measurement": {"quantity_kind": "count"},
        "loss_type": "ground_up",
        "loss_approach": "analytical",
        "loss_frequency_type": "probabilistic"
      }
    },
    {
      "id": "loss_2",
      "hazard": {"type": "flood", "intensity_measure": "wd:m"},
      "asset_category": "population",
      "asset_dimension": "population",
      "description": "Flood-induced displacement of residential population",
      "impact_and_losses": {
        "impact_type": "indirect",
        "impact_modelling": "simulated",
        "impact_metric": "displaced_count",
        "measurement": {"quantity_kind": "count"},
        "loss_type": "ground_up",
        "loss_approach": "analytical",
        "loss_frequency_type": "probabilistic"
      }
    }
  ]
}
```

### 4.4 Impact Modelling and Loss Approach

Both fields are derived from the dataset's analysis type rather than hardcoded:

| Analysis type | `impact_modelling` | `loss_approach` | Rationale |
|--------------|-------------------|----------------|-----------|
| `empirical` | `observed` | `empirical` | Field-collected impact observations |
| `probabilistic` | `simulated` | `analytical` | Model output at defined return periods |
| `deterministic` | `inferred` | `analytical` | Single scenario, analytically derived |

---

## 5. Base Record: v1.0 Field Mapping

### 5.1 Entity Fields

RDLS v1.0 separates `publisher`, `creator`, and `contact_point` as top-level entity objects (v0.3 used a single `attributions` list). All three are populated from the HDX publishing organisation:

```json
"publisher":      {"name": "UNOSAT", "url": "https://data.humdata.org/dataset/{slug}"},
"creator":        {"name": "UNOSAT", "url": "https://data.humdata.org/dataset/{slug}"},
"contact_point":  {"name": "UNOSAT", "url": "https://data.humdata.org/dataset/{slug}"}
```

Contributing organisations identified by the LLM are added to `attributions` with `role: "collaborator"`. The schema `anyOf` requirement (entity must have `url` or `email`) is satisfied by using the HDX dataset URL as a fallback when no organisation URL is available.

### 5.2 Lineage Sources

Where the LLM identifies contributing sources, each becomes a separate entry in `lineage.sources` with per-source metadata:

```json
"lineage": {
  "description": "Flood hazard layers derived from hydrodynamic modelling...",
  "sources": [
    {
      "id": "source_1",
      "name": "OpenStreetMap",
      "type": "dataset",
      "risk_data_type": ["exposure"],
      "used_in": "exposure",
      "description": "Building footprint data used to construct the exposure layer"
    },
    {
      "id": "source_2",
      "name": "LISFLOOD-FP",
      "type": "model",
      "risk_data_type": ["hazard"],
      "used_in": "hazard",
      "description": "Hydrodynamic model used to generate inundation depth grids"
    }
  ]
}
```

### 5.3 Spatial Inference

Spatial scale and country codes are derived from three sources, applied in priority order:

1. **HDX `groups` field**: country names in HDX groups are resolved to ISO3 codes via a spatial lookup table. Scale is inferred from count (1 country -> national, 2+ -> regional).
2. **LLM country extraction**: supplementary ISO3 codes from LLM response are merged if not already present from groups resolution.
3. **Fallback**: if neither source resolves a country, the LLM's `spatial_scale` field is used directly. If scale remains unknown, `"global"` is the final fallback.

All country codes are filtered against the 249-entry closed `codelist_country` before being written to the record. Invalid codes (e.g., `XKX` for Kosovo, `ANT` for Netherlands Antilles) are silently dropped.

Schema rule enforced: `scale: "global"` must not have a `countries` field. When inferred scale is global but LLM provided specific country codes, the scale is corrected to `national` (single country) or `regional` (multiple countries).

### 5.4 Resources

Resources are built from HDX resource metadata with format-to-IANA media_type mapping for 50+ format strings. Service resources (WMS, WFS, OGC API, STAC) receive `access_url` instead of `download_url` and a `conforms_to` URI.

Resources that resolve to neither `download_url` nor `access_url` (empty URL in HDX metadata) are silently skipped. This prevents `Resource.anyOf` schema violations. If all resources for a dataset fail this check, `build_resources_v10()` returns an empty list and the record is skipped entirely (logged as `status: "skipped"`).

### 5.5 Description and Attribution Suffix

Every record description always ends with a mandatory source attribution:

```
[Source: This metadata record was automatically extracted from the
Humanitarian Data Exchange (HDX); Original dataset: {hdx_url}]
```

This guarantees the `description` field is never empty (required by v1.0) and provides traceable provenance from every record back to its HDX origin.

---

## 6. Schema Compliance

### 6.1 Validation Architecture

Every record is validated against the RDLS v1.0 JSON Schema (`rdl-standard/schema/rdls_schema.json`) immediately after HEVL integration. Validation failures route the record to `dist/invalid/` with a sidecar entry in `reports/validation_errors.jsonl` recording the specific errors.

The schema is loaded once at pipeline startup and reused across all records. The `validate_record()` function validates a single unwrapped record dict (never the `{"datasets": [...]}` envelope).

### 6.2 Bug Fixes Applied

The following bugs were found during iterative testing and are fixed in the current code:

| # | Error observed | Root cause | Fix |
|---|---------------|------------|-----|
| 1 | `exposure.dimension: 'count' is not one of [...]` | LLM returns non-enum dimension values | `_VALID_DIMENSIONS` check in `build_exposure_block()` with fallback to category default |
| 2 | `spatial: {scale:'global', countries:[...]} should not be valid` | infer_spatial() fallback + LLM countries combined | Strip `countries` when `scale='global'`; infer scale from country count when groups lookup fails |
| 3 | `loss.hazard.type: 'multiple' is not one of [...]` | LLM describes multi-hazard datasets with `"multiple"` | Validate in `_parse_response()`; fall back to primary hazard type |
| 4 | `resources.0: is not valid under any of the given schemas` | Resources with empty URL fail `Resource.anyOf` | Skip resources where neither `download_url` nor `access_url` can be set |
| 5 | `spatial.countries: ['XKX']` fails codelist check | LLM generates codes not in closed 249-entry codelist | Filter all country lists against `_VALID_COUNTRY_CODES` frozenset at import time |
| 6 | `impact_modelling: 'inferred'` for probabilistic model output | Always defaulted to `"inferred"` | Use `"simulated"` for probabilistic, `"observed"` for empirical, `"inferred"` for deterministic |
| 7 | `loss_approach: 'empirical'` for model-based losses | Always hardcoded `"empirical"` | Use `"analytical"` for probabilistic/deterministic, `"empirical"` for empirical analysis |

### 6.3 Test Results

A 100-dataset test run with Claude Haiku 4.5 against the current code produced:

| Metric | Value |
|--------|-------|
| Total datasets processed | 100 |
| Schema-valid records | 30 (100% of RDLS records) |
| Invalid records | **0** |
| Not-RDLS datasets | 70 |
| Failures (pipeline errors) | 0 |
| Multi-exposure records | 18 |
| Multi-loss records | 24 |

---

## 7. Cost and Performance

### 7.1 Model Options

The pipeline supports any Anthropic Claude model via the `model` field in `configs/llm_review_v10.yaml`.

| Model | Input $/MTok | Output $/MTok | Est. full run cost | Est. full run time |
|-------|-------------|--------------|--------------------|--------------------|
| `claude-haiku-4-5-20251001` | $1.00 | $5.00 | ~$123 | ~24 hours |
| `claude-sonnet-4-6` | $3.00 | $15.00 | ~$370 | ~80 hours |

Token averages from a 100-file test run: 3,185 input tokens/file, 297 output tokens/file.

### 7.2 Recommended Strategy

Given the time and cost trade-offs, the recommended production run strategy is:

1. **Haiku for all 26,246 datasets** - ~24 hours, ~$123. Produces schema-valid records with correct HEVL classification across the full corpus.
2. **Sonnet re-run for medium-confidence tier only** - records with confidence 0.4-0.7 (~5-10% of RDLS records, estimated 1,500-2,500 datasets). Replaces Haiku outputs with richer Sonnet extraction for borderline cases. ~$35-60, ~8 hours.

This hybrid strategy achieves near-Sonnet quality on borderline records at a fraction of the cost and time of a full Sonnet run.

### 7.3 Cache Strategy

All LLM responses are cached to disk at `output/hdx/v1.0/cache/` keyed by a SHA-256 hash of the prompt text. Cache hits bypass the API call entirely. This means:
- Re-running the pipeline after code fixes costs USD 0 in LLM fees for already-processed datasets
- Iterative refinement of translation and extraction code is free once classifications are cached

**Important**: the cache is keyed by prompt content, not model name. Switching from Haiku to Sonnet on the same dataset will serve the Haiku response from cache unless the cache is explicitly cleared.

---

## 8. Key Design Decisions

### 8.1 Why Remove Regex Pre-Screening?

The hybrid v0.3 approach gated ~48% of records from LLM review using regex confidence scores. This worked because those records had clear-cut, unambiguous metadata. However:
- Clear-cut classification still produced poor extraction (single event, single exposure category)
- v1.0 provenance fields (lineage, contributing sources, attributions) cannot be populated by regex at all
- At Haiku pricing, the cost of sending all 26,246 records (~USD 123) is comparable to a one-time manual task

Removing the gate simplifies the architecture to a single code path and improves extraction quality for all records, not just ambiguous ones.

### 8.2 Why a Single LLM Call for Both Classification and Extraction?

A two-call design (classify first, extract only if RDLS-positive) would reduce output costs for non-RDLS datasets (~70% of HDX). However:
- The dominant cost is input tokens (the prompt), which are identical for both calls
- A second extraction call adds latency without improving classification accuracy
- A single call eliminates the risk of extraction disagreeing with classification

### 8.3 Why RDLS v1.0?

The v1.0 schema adds explicit provenance fields (`lineage.sources`, `attributions`), richer hazard structure (multi-event event_sets), and IANA media types for resources. For a fresh bulk conversion of HDX metadata, targeting v1.0 directly avoids a subsequent schema migration step.

---

## 9. Limitations

**Return periods not inferred.** The pipeline only populates `events[]` when the LLM finds return period values explicitly stated in the metadata. Datasets that describe probabilistic analysis without specifying return periods produce an empty events array.

**Process type not schema-validated.** The `process` field on hazard objects accepts any string. The pipeline prompts the LLM with the correct closed codelist values, but invalid process codes are not caught by the JSON Schema validator and pass through silently.

**Exposure dimension defaults.** When the LLM returns an invalid dimension value, the pipeline falls back to a per-category default (e.g., `"structure"` for buildings). This is schema-safe but may not reflect the actual dataset content.

**Not-RDLS classification is final.** Datasets classified as not-RDLS are written to `not_rdls/` and not re-evaluated. A small fraction of these may be borderline RDLS datasets that a more capable model or different prompt would classify differently.

---

*Source code: https://github.com/bennyistanto/to-rdls*
*Prior pipeline documentation: `docs/llm_assisted_metadata_classification.md`*
*RDLS v1.0 specification: https://docs.riskdatalibrary.org/en/1__0__0/*
