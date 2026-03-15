# to-rdls Architecture

Technical architecture reference for the RDLS v0.3 metadata transformation toolkit.

---

## 1. Design Principles

**Source-independent core.** Every module from `classify.py` onward operates on a
common field dictionary, not on raw API responses. Source-specific logic lives
exclusively in `src/sources/`. Adding a new source requires no changes to
classification, translation, extraction, integration, or validation.

**Config-driven behaviour.** Runtime decisions (thresholds, codelist values,
pattern dictionaries, format aliases, naming conventions) are loaded from YAML
files in `configs/`. No codelist values or scoring weights are hard-coded in
Python modules.

**Cascade extraction.** HEVL extractors use tiered field scanning: high-trust
fields (title, tags) can introduce new component values; lower-trust fields
(notes, methodology) can only corroborate or serve as fallback. This reduces
false positives from verbose description text.

**Constraint validation.** Records pass through JSON Schema validation,
business-rule checking, and a 5-pass auto-fix engine before distribution.
Confidence scoring combines extraction, classification, and validation signals
into a single composite score used for tiered output.

**Modular composition.** Each pipeline stage is a standalone module with its own
config loader, data classes, and public API. Stages communicate through plain
Python dicts and dataclasses, not shared global state.

---

## 2. Pipeline Overview

```
Source API ──► Source Adapter ──► Common Fields ──► Classification
                                                        │
                                                        ▼
                                                   Translation
                                                        │
                                                        ▼
                                              HEVL Extraction
                                           (Hazard / Exposure /
                                          Vulnerability / Loss)
                                                        │
                                                        ▼
                                                  Integration
                                                        │
                                                        ▼
                                                    Naming
                                                        │
                                                        ▼
                                                  Validation
                                                  (QA + Auto-fix)
                                                        │
                                                        ▼
                                                  Distribution
                                               (Tiered output)
```

| Stage | Module | Purpose |
|-------|--------|---------|
| Source Adapter | `sources/hdx.py`, `sources/geonode.py` | Fetch raw metadata, normalize to common field dict |
| Common Fields | (dict interface) | Flat dict with keys: `id`, `name`, `title`, `notes`, `methodology`, `organization`, `tags`, `resources`, `groups`, etc. |
| Classification | `classify.py` | Score datasets against tag weights, keyword patterns, and org hints to determine HEVL components |
| Translation | `translate.py` | Build base RDLS record: format mapping, license mapping, attribution, resource building, spatial block |
| HEVL Extraction | `extract_hazard.py`, `extract_exposure.py`, `extract_vulnloss.py` | Pattern-match metadata text against signal dictionary to populate component blocks |
| Integration | `integrate.py` | Merge HEVL blocks into base record, reconcile `risk_data_type`, validate component combinations |
| Naming | `naming.py` | Generate structured record IDs: `rdls_{types}-{iso3}{org}_{titleslug}` with collision detection |
| Validation | `validate_qa.py`, `schema.py` | JSON Schema validation, business rules, 5-pass auto-fix, confidence scoring |
| Distribution | `validate_qa.py` | Route records to tiered output folders (high/medium/low x valid/invalid) |

---

## 3. Module Dependency Graph

Grouped by pipeline phase. Arrows show `import` relationships between `src/` modules.

```
┌─────────────────────────────────────────────────────────────┐
│  FOUNDATION                                                  │
│                                                              │
│  utils.py  ◄──── (imported by every other module)           │
│    │                                                         │
│    └──► spatial.py                                           │
│                                                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  SOURCE ADAPTERS                                             │
│                                                              │
│  sources/hdx.py ────► utils (load_yaml, norm_str)           │
│  sources/geonode.py ─► utils (load_yaml)                    │
│                                                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  CLASSIFICATION + TRANSLATION                                │
│                                                              │
│  classify.py ──────► utils (as_list, load_yaml,             │
│                             normalize_text)                   │
│                                                              │
│  translate.py ─────► utils (as_list, load_yaml,             │
│                        sanitize_text, slugify_token, ...)    │
│                  ──► spatial (country_name_to_iso3,          │
│                        infer_spatial, load_spatial_config)    │
│                                                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  HEVL EXTRACTION                                             │
│                                                              │
│  extract_hazard.py ──► utils (load_yaml, normalize_text)    │
│  extract_exposure.py ► utils (load_yaml, normalize_text)    │
│  extract_vulnloss.py ► utils (normalize_text)               │
│                                                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  INTEGRATION + NAMING                                        │
│                                                              │
│  integrate.py ─────► naming (build_rdls_id,                 │
│                        encode_component_types)               │
│                  ──► utils (load_yaml)                       │
│                                                              │
│  naming.py ────────► utils (load_yaml, slugify_token)       │
│                                                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  VALIDATION + QA                                             │
│                                                              │
│  schema.py ────────► utils (load_json, load_yaml)           │
│                                                              │
│  validate_qa.py ───► schema (SchemaContext, validate_record)│
│                  ──► utils (load_json, load_yaml, write_json│
│                        navigate_path, remove_at_path,        │
│                        set_at_path)                          │
│                                                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  DATA REVIEW (file inspection)                               │
│                                                              │
│  inventory.py ─────► (stdlib only)                          │
│  zipaccess.py ─────► (stdlib only)                          │
│  review.py ────────► inventory, utils, zipaccess            │
│                                                              │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│  LLM REVIEW PIPELINE                                         │
│                                                              │
│  ckan_columns.py ──► (stdlib + requests)                    │
│  hdx_review.py ────► utils, review                          │
│  llm_review.py ────► ckan_columns, hdx_review,             │
│                       naming, utils                          │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 4. Source Adapter Pattern

### Common Field Dict Interface

Every source adapter must produce a flat dictionary with these keys:

```python
{
    "id":           str,   # Source-native dataset ID
    "name":         str,   # URL-safe slug
    "title":        str,   # Human-readable title
    "notes":        str,   # Description / abstract
    "methodology":  str,   # Methodology text (if available)
    "organization": str,   # Publishing org name
    "license_title": str,  # License string
    "groups":       list,  # Country/region group dicts
    "tags":         list,  # Tag/keyword dicts
    "resources":    list,  # Resource/download dicts
    "dataset_date": str,   # Temporal coverage
    "url":          str,   # Canonical URL
}
```

### HDX Adapter (`sources/hdx.py`)

- `HDXCrawlerConfig` / `HDXClient`: CKAN API client with rate limiting, retries,
  exponential backoff.
- `normalize_dataset_record(raw)`: Unwraps CKAN package_show response.
- `extract_hdx_fields(ds)`: Maps CKAN fields to common dict. Handles OSM policy
  detection, resource format inference, and HDX-specific metadata quirks.

### GeoNode Stub (`sources/geonode.py`)

- `GeoNodeConfig`, `GeoNodeClient`: Placeholder classes matching the HDX pattern.
- `normalize_geonode_record()`, `extract_geonode_fields()`: Stub functions
  returning the same common field dict keys. `GeoNodeClient.__init__` raises
  `NotImplementedError`.

### Adding a New Source

1. Create `src/sources/your_source.py` with a config class, client class,
   `normalize_record()`, and `extract_fields()` returning the common dict.
2. Create `configs/sources/your_source.yaml` with API endpoints and
   source-specific overrides.
3. All downstream stages (classification through distribution) work unchanged.

---

## 5. HEVL Extraction Cascade

### Hazard Extraction (2-Tier)

| Tier | Fields | Behaviour |
|------|--------|-----------|
| Tier 1 | title, name, tags, resources | Can **introduce** new hazard types |
| Tier 2 | notes, methodology | Can only **corroborate** Tier 1 findings, or serve as fallback if Tier 1 found nothing |

`HazardExtractor` extracts: `hazard_type`, `process_type`, `analysis_type`,
`return_periods`, `intensity_measures`, `calculation_method`.

### Exposure Extraction (3-Tier)

| Tier | Fields | Behaviour |
|------|--------|-----------|
| Tier 1 | title, name, tags | Highest confidence, introduces categories |
| Tier 2 | resources | Medium confidence, can introduce new categories |
| Tier 3 | notes, methodology | Lowest confidence, fallback only |

`ExposureExtractor` extracts: `categories`, `metrics` (dimension, quantity_kind),
`taxonomy`, `currency`.

### Vulnerability + Loss Extraction

- **VulnerabilityExtractor**: function types, approaches, relationships, intensity
  measures, impact metrics with constraint validation.
- **LossExtractor**: 8 signal types with full defaults, impact modelling, approach,
  frequency, currency, temporal references.

### Signal Dictionary (`configs/signal_dictionary.yaml`)

Top-level sections mapping to RDLS codelists:

| Section | Maps To |
|---------|---------|
| `hazard_type` | RDLS `hazard_type` closed codelist |
| `process_type` | RDLS `process_type` closed codelist |
| `exposure_category` | RDLS exposure category |
| `analysis_type` | RDLS `analysis_type` |
| `return_period` | Return period extraction |
| `spatial_scale` | Spatial scale hints |
| `vulnerability_indicators` | Vulnerability signal patterns |
| `loss_indicators` | Loss signal patterns |
| `format_hints` | Data format detection |
| `organization_hints` | Org-based classification hints |
| `exclusion_patterns` | False-positive filters |

Each entry contains regex patterns (case-insensitive) with confidence levels
(high = 0.9, medium = 0.7, low = 0.5). The `exclusion_patterns` section filters
out false positives (e.g., "flood" in "blood flood bank" contexts).

---

## 6. Validation Engine

### 5-Pass Auto-Fix (`AutoFixer` in `validate_qa.py`)

| Pass | Name | Action |
|------|------|--------|
| Pass 0 | Structural Repair | Fix wrong JSON types (e.g., string where object expected) |
| Pass 1 | Error-Driven Fixes | Remove empty values, coerce types, correct codelist values via fuzzy matching |
| Pass 2 | Deep Clean | Remove remaining empty strings/objects/arrays in non-required fields |
| Pass 3 | Structural Inference | Rebuild missing or empty required fields from context in the record |
| Pass 4 | Additional Properties | Remove fields not defined in the JSON Schema |

### Confidence Scoring (`compute_composite_confidence`)

Combines signals from extraction confidence, classification score, and validation
pass/fail status into a single 0.0--1.0 composite score.

### Tiered Distribution (`distribute_records`)

Records are routed to output folders based on composite confidence and schema
validity:

| Tier | Confidence | Schema Valid |
|------|-----------|--------------|
| `high` | >= 0.8 | Yes |
| `medium` | >= 0.5 | Yes |
| `low` | < 0.5 | Yes |
| `invalid/high` | >= 0.8 | No |
| `invalid/medium` | >= 0.5 | No |
| `invalid/low` | < 0.5 | No |

### Supporting Checks

- **JSON Schema validation** (`schema.py`): `SchemaContext` bundles all
  schema-derived lookups (enum values, field aliases, required fields, allowed
  properties) built once from the RDLS v0.3 JSON Schema.
- **Business rules** (`validate_qa.py`): attribution role coverage, schema link
  presence, component consistency checks beyond what JSON Schema can express.

---

## 7. LLM Review Pipeline

4-phase architecture in `llm_review.py`, orchestrated by `run_llm_review()`.

### Phase 1: Signal Triage (free, fast)

Re-runs existing regex-based HEVL assessment (`hdx_review.assess_hevl`) on all
records and buckets them:

- **Confident**: high signal score (>= threshold), <= 2 components. Skips LLM.
- **Borderline**: some signals but ambiguous. Sent to LLM.
- **No-signal**: no regex matches. Sent to LLM.
- **Validation sample**: 5% random subset of confident records, cross-checked by
  LLM.

Results are pickle-cached for reuse across runs.

### Phase 2: Column Enrichment (cached)

Loads CKAN resource column headers from disk cache (`ColumnCache` in
`ckan_columns.py`). Provides actual CSV/XLSX/GeoJSON field names to the LLM
prompt. Column data is fetched separately via `ckan_columns.py` (CKAN
`resource_show` API, parses `fs_check_info` and `shape_info`).

### Phase 3: LLM Classification (Claude Haiku)

- Builds structured prompts with metadata, column headers, and current HEVL
  assessment.
- Calls Claude Haiku via `anthropic` SDK with `ThreadPoolExecutor` concurrency.
- Response cache (`LLMResponseCache`) avoids re-classifying unchanged records.
- Cost guardrail aborts if estimated cost exceeds `max_cost_usd`.
- Rate limiting: 1.5s pause between batches to stay under 50K tokens/min.
- Returns `LLMClassification` with per-component booleans, reasoning, confidence,
  and domain category.

### Phase 4: Merge + Write

- Merges LLM classifications into existing HEVL assessments
  (`merge_classification_into_assessment`).
- When LLM reclassifies `risk_data_type`, rebuilds record ID and filename
  (`_rebuild_id_for_new_rdt`) with collision detection.
- Separates not-RDLS records to `output/llm/not_rdls/`.
- Writes revised JSON records, review report CSV, disagreement log, LLM audit
  trail, and summary markdown.

---

## 8. MCP Server

`mcp_server.py` exposes the toolkit as a Model Context Protocol server using
`FastMCP`. Runs in the `to-rdls` conda environment (GDAL, rasterio, fiona,
geopandas).

Start command: `conda run --no-banner -n to-rdls python mcp_server.py`

### Tools

| Tool | Description |
|------|-------------|
| `inventory_folder` | Scan a folder or ZIP. List files with sizes, formats, MIME types, modification dates. ZIP contents listed without extraction. Returns JSON with stats and markdown summary. |
| `review_folder` | Full automated data review. Inspects files (including inside ZIPs), classifies into HEVL components, analyzes gaps against RDLS schema, extracts naming patterns, suggests dataset structure. |
| `inspect_file` | Inspect a single file's metadata and content. Supports GeoTIFF, Shapefile, GeoJSON, GeoPackage, XLSX, CSV, JSON, PDF, DOCX, NetCDF, text. Supports `archive.zip::inner/path` syntax. |
| `validate_record` | Validate an RDLS JSON record against the v0.3 schema. Handles both single records and `{"datasets": [...]}` wrappers. |
| `inspect_folder_for_llm` | Return raw inspection data without automated classification. Designed for LLM-driven semantic classification: returns folder summary, file groups, file inspections, README extractions, and RDLS context. |

---

## 9. Config Architecture

### Loading Pattern

All config files are YAML. Modules load them through `utils.load_yaml()` and
typically expose a dedicated `load_*_config()` function that normalizes keys,
compiles regex patterns, and returns a structured dict.

Configs are loaded once at pipeline start and passed as arguments to processing
functions (no global config singletons).

### Config File Categories

| File | Category | Used By |
|------|----------|---------|
| `pipeline.yaml` | Orchestration | Output mode, confidence thresholds, tier definitions, processing limits, directory structure |
| `signal_dictionary.yaml` | Extraction | HEVL pattern matching (hazard/exposure/vuln/loss signals, exclusion patterns) |
| `classification.yaml` | Classification | Tag weights, keyword patterns, org hints, scoring parameters |
| `rdls_schema.yaml` | Schema | RDLS codelist enum values (closed + open codelists) |
| `rdls_defaults.yaml` | Defaults | Default mappings (hazard-to-process, metrics, weights) |
| `spatial.yaml` | Spatial | Region-to-countries mapping, country name fixes, non-country groups |
| `format_mapping.yaml` | Translation | Format aliases, skip list, service URL patterns |
| `license_mapping.yaml` | Translation | License string to RDLS license code mapping |
| `naming.yaml` | Naming | ID format rules, org abbreviations, component code mappings |
| `review_knowledge.yaml` | Review | File inspection knowledge base for automated review |
| `llm_review.yaml` | LLM Review | Triage thresholds, CKAN settings, LLM model/cost config |
| `country_bbox.yaml` | Spatial | Country bounding boxes |
| `geonames_country_ids.yaml` | Spatial | GeoNames country ID lookup |
| `desinventar_mapping.yaml` | Source-specific | DesInventar field mappings |
| `sources/hdx.yaml` | Source-specific | HDX API endpoints, OSM markers, format overrides |
| `sources/geonode.yaml` | Source-specific | GeoNode API settings (stub) |
