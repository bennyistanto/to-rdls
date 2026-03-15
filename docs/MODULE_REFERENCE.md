# Module Reference

This document describes each Python module in `src/`, grouped by pipeline phase. For each module: purpose, key public functions, key dataclasses, and dependencies on other `src/` modules.

---

## Utilities

### `utils.py` - Text processing, file I/O, temporal parsing

General-purpose utilities used across all modules. No dependencies on other `src/` modules.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `sanitize_text()` | Clean text for JSON: fix mojibake, strip HTML, normalize characters |
| `normalize_text()` | Lowercase + strip for pattern matching |
| `slugify()` / `slugify_token()` | Generate URL-safe slugs from text |
| `load_json()` / `write_json()` | Atomic JSON file I/O |
| `append_jsonl()` | Append record to JSONL file |
| `load_yaml()` | YAML config loading |
| `iter_json_files()` | Iterate JSON files in a directory |
| `parse_hdx_temporal()` | Parse HDX `dataset_date` into ISO 8601 temporal fields |
| `navigate_path()` / `set_at_path()` / `remove_at_path()` | Nested dict/list navigation and mutation |
| `as_list()` / `split_semicolon_list()` | Normalize values to lists |
| `looks_like_url()` | Heuristic URL detection |
| `norm_str()` | Normalize string for comparison (lowercase, strip, collapse whitespace) |

**Dependencies:** PyYAML (external only)

---

## Schema and Spatial

### `schema.py` - RDLS schema loading, validation, codelist operations

Loads the RDLS v0.3 JSON Schema, extracts codelists, validates records, and provides `SchemaContext` for fast lookup and auto-fix.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `load_rdls_schema()` | Load JSON schema from file |
| `load_codelists()` | Load codelist enum values from `rdls_schema.yaml` |
| `load_codelists_from_schema()` | Extract enum values directly from a JSON schema |
| `validate_record()` | Validate a single record against the schema (Draft 2020-12 / Draft 7 fallback) |
| `check_required_fields()` | Quick check for mandatory RDLS fields |
| `summarize_errors()` | Categorize validation errors into human-readable groups |

**Key Classes:**

| Class | Description |
|-------|-------------|
| `SchemaContext` | Bundles all schema-derived lookup structures: `enum_lookup`, `field_aliases`, `required_lookup`, `allowed_props`, `property_to_def`. Methods: `fuzzy_codelist_fix()` (find closest valid codelist value), `is_field_required()` |

**Dependencies:** `utils`

---

### `spatial.py` - Country/region resolution and spatial block inference

Converts country names to ISO3 codes, expands regions to country lists, and builds RDLS spatial blocks.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `load_spatial_config()` | Load spatial config (regions, country fixes, non-country groups) |
| `country_name_to_iso3()` | Resolve country name via fixes dict, CSV table, or pycountry fallback |
| `infer_spatial()` | Build RDLS spatial block (scale + countries) from group names |
| `infer_scale()` | Infer spatial scale (global/national/regional) from country count |

**Dependencies:** `utils`

---

## Classification

### `classify.py` - Dataset classification for RDLS components

Scores datasets against tag weights, keyword patterns, and organization hints to determine which HEVL components are relevant.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `load_classification_config()` | Load tag weights, keyword patterns, org hints, scoring params |
| `load_exclusion_patterns()` | Load exclusion patterns from signal_dictionary.yaml |
| `classify_dataset()` | Score a dataset and determine RDLS components |
| `apply_overrides()` | Apply manual classification overrides per dataset ID |
| `enforce_component_deps()` | Enforce vulnerability/loss dependency rules (require H or E) |

**Key Classes:**

| Class | Fields |
|-------|--------|
| `Classification` | `scores` (dict), `components` (list), `rdls_candidate` (bool), `confidence` (str: high/medium/low), `top_signals` (list) |

**Dependencies:** `utils`

---

## Translation

### `translate.py` - RDLS record builder

Translates source metadata into base RDLS v0.3 records. Handles format mapping, license mapping, attribution building, resource assembly, and temporal annotation.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `load_format_config()` | Load format aliases, skip list, service URL patterns |
| `load_license_config()` | Load license string to RDLS code mapping |
| `detect_service_url()` | Detect WMS/WFS/OGC service URLs |
| `infer_format_from_name()` | Infer data format from filename keywords |
| `map_data_format()` | Translate source format to RDLS `data_format` |
| `map_license()` | Translate license string to RDLS license code |
| `build_rdls_record()` | Assemble complete base RDLS record (format, license, attributions, resources, temporal) |

**Dependencies:** `utils`, `spatial`

---

## Extraction

### `extract_hazard.py` - Hazard block extraction (2-tier cascade)

Extracts hazard types, process types, analysis types, return periods, and intensity measures from metadata using pattern matching against the Signal Dictionary.

**Key Classes:**

| Class | Fields |
|-------|--------|
| `ExtractionMatch` | `value`, `confidence`, `source_field`, `matched_text`, `pattern` |
| `HazardExtraction` | `hazard_types`, `process_types`, `analysis_type`, `return_periods`, `intensity_measures`, `overall_confidence`, `calculation_method`, `description` |
| `HazardExtractor` | Main extractor class. Constructor takes `signal_dict` and `defaults`. Method: `extract(dataset)` |

**Key Functions:**

| Function | Description |
|----------|-------------|
| `build_hazard_block()` | Convert `HazardExtraction` into RDLS-schema hazard block dict |

**Dependencies:** `utils`

---

### `extract_exposure.py` - Exposure block extraction (3-tier cascade)

Extracts exposure categories, metric dimensions, quantity kinds, taxonomy hints, and currency from metadata.

**Key Classes:**

| Class | Fields |
|-------|--------|
| `ExtractionMatch` | `value`, `confidence`, `source_field`, `matched_text`, `pattern` |
| `MetricExtraction` | `dimension`, `quantity_kind` per exposure category |
| `ExposureExtraction` | `categories`, `metrics` (dict), `taxonomy_hint`, `currency`, `overall_confidence` |
| `ExposureExtractor` | Main extractor. Constructor takes `signal_dict` and `defaults`. Method: `extract(dataset)` |

**Key Functions:**

| Function | Description |
|----------|-------------|
| `build_exposure_block()` | Convert `ExposureExtraction` into RDLS-schema exposure block dict |

**Dependencies:** `utils`

---

### `extract_vulnloss.py` - Vulnerability and Loss extraction

Extracts vulnerability functions (fragility, engineering demand, etc.) and loss entries (8 signal types).

**Key Classes:**

| Class | Fields |
|-------|--------|
| `FunctionExtraction` | `function_type`, `approach`, `relationship`, `hazard_primary`, `intensity_measure`, `category`, `dimension`, `quantity_kind`, `impact_type`, `impact_modelling`, `impact_metric`, `confidence` |
| `SocioEconomicExtraction` | `indicator_name`, `indicator_code`, `scheme`, `description`, `confidence` |
| `VulnerabilityExtraction` | `functions` (list), `socio_economic` (list), `overall_confidence` |
| `LossEntryExtraction` | Signal type, hazard, asset, impact, frequency, currency, confidence |
| `LossExtraction` | `entries` (list), `overall_confidence` |
| `VulnerabilityExtractor` | Main extractor. Method: `extract(dataset)` |
| `LossExtractor` | Main extractor. Method: `extract(dataset)` |

**Key Functions:**

| Function | Description |
|----------|-------------|
| `build_vulnerability_block()` | Convert `VulnerabilityExtraction` into RDLS-schema vulnerability block dict |
| `build_loss_block()` | Convert `LossExtraction` into RDLS-schema loss block dict |

**Dependencies:** `utils`

---

## Integration

### `integrate.py` - HEVL merge and risk_data_type reconciliation

Merges hazard, exposure, vulnerability, and loss blocks into base RDLS records. Reconciles `risk_data_type` arrays and validates component combinations.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `validate_component_combination()` | Check logical validity of component combinations (V/L may require H or E) |
| `determine_risk_data_types()` | Reconcile base types with HEVL extraction flags |
| `merge_hevl_into_record()` | Merge HEVL blocks into a base record dict |
| `integrate_record()` | End-to-end: merge blocks + reconcile types + generate ID |
| `append_provenance()` | Add extraction metadata (version, timestamp, signals used) |

**Dependencies:** `naming`, `utils`

---

## Naming

### `naming.py` - RDLS ID and filename generation

Builds structured record IDs in the format `rdls_{types}-{iso3}{org}_{titleslug}`. Handles component encoding, collision detection, and ID parsing for rebuild after reclassification.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `load_naming_config()` | Load naming convention from `configs/naming.yaml` |
| `encode_component_types()` | Convert component list to type code (single: `hzd`/`exp`/`vln`/`lss`; multi: `he`/`hev`/`hevl`) |
| `encode_countries()` | Encode ISO3 codes into ID segment (lowercased, concatenated) |
| `build_rdls_id()` | Build full ID from components, countries, org, title |
| `build_rdls_id_with_collision()` | Build ID with collision detection (appends UUID suffix if duplicate) |
| `parse_rdls_id()` | Parse existing ID back into segments (for ID rebuild after LLM reclassification) |
| `resolve_shortname()` | Resolve org abbreviation from config lookup or auto-truncate |
| `slugify_title()` | Sanitize title for slug (remove country/org names, stop words, max chars) |
| `is_valid_iso3()` | Check if string is a valid ISO 3166-1 alpha-3 code |

**Dependencies:** `utils`

---

## Validation

### `validate_qa.py` - Schema validation, auto-fix, confidence scoring, distribution

Validates records against the RDLS v0.3 JSON Schema, applies a 5-pass auto-fix engine, computes confidence scores, and distributes records to quality tiers.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `validate_against_schema()` | Run JSON Schema validation, return rich error dicts with path, message, category |
| `categorize_error()` | Assign human-readable category to validation errors (empty_string, missing_required, invalid_codelist, etc.) |
| `check_business_rules()` | Check RDLS rules beyond JSON Schema (attribution roles, resource URLs, entity contacts, schema links, type consistency) |
| `auto_fix_record()` | 5-pass auto-fix engine (enum fuzzy-match, required field inference, type coercion, structural cleanup, orphaned removal) |
| `compute_composite_confidence()` | Score record confidence (0.0-1.0) from completeness, consistency, and signal strength |
| `validate_and_score()` | End-to-end: validate + auto-fix + score |
| `distribute_records()` | Distribute records to quality tiers (high/medium/low/invalid) based on confidence and validity |
| `create_validation_report()` | Generate CSV/JSON validation report |

**Dependencies:** `schema`, `utils`

---

## Inventory and Review

### `inventory.py` - Folder/ZIP inventory generator

Generates structured inventories of data delivery folders or ZIP files. Standard library only (no external dependencies). Also runnable as CLI via `python -m src`.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `scan_target()` | Recursively scan directory or ZIP file for all files |
| `inventory_folder()` | Generate complete inventory (Markdown + metadata rows + stats) |
| `render_and_write()` | Render and write outputs in requested formats (md, csv, json) |
| `human_size()` | Format bytes as human-readable string |
| `iso_time()` | Format timestamp as ISO 8601 |

**Key Classes:**

| Class | Fields |
|-------|--------|
| `InventoryConfig` | `target`, `output_dir`, `formats`, `include_hash`, `exclude_patterns`, `max_depth`, `follow_symlinks`, `inspect_zips` |

**Dependencies:** None (stdlib only)

---

### `review.py` - File inspection, HEVL classification, gap analysis

Full automated data review: inspects geospatial/tabular/document files, classifies into HEVL components, analyzes gaps against RDLS schema, and suggests dataset structure.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `load_review_config()` | Load review knowledge base (signal patterns, column detection rules) |
| `review_folder()` | End-to-end: scan, inspect, group, classify, analyze gaps |
| `inspect_file()` | Inspect a single file's metadata and content |
| `render_review_markdown()` | Render review results as Markdown report |
| `analyze_naming_patterns()` | Detect naming patterns in file groups (scenarios, return periods, countries) |
| `extract_readme_metadata()` | Extract project metadata from README/text files |

**Key Classes:**

| Class | Fields |
|-------|--------|
| `FileInspection` | `path`, `format`, `inspection` (dict of metadata details) |
| `FileGroup` | `name`, `files`, `formats`, `total_size_bytes`, HEVL assessment |
| `GapAnalysis` | Missing RDLS fields relative to inspection results |
| `ReviewResult` | `target`, `file_groups`, `inspections`, `suggested_datasets`, `quality_issues`, `stats` |

**Dependencies:** `utils`, `inventory` (for `human_size`, `iso_time`). Optional: geopandas, rasterio, fiona, pandas, openpyxl, Pillow, python-docx, netCDF4/xarray

---

### `zipaccess.py` - ZIP member extraction

Extract individual ZIP members to temporary files for inspection without extracting the whole archive. Supports nested ZIPs (ZIP-in-ZIP).

**Key Functions:**

| Function | Description |
|----------|-------------|
| `parse_zip_spec()` | Parse `archive.zip::inner/path/file` syntax into (archive, member) |
| `open_zip_member()` | Context manager: extract member to temp file, yield path, cleanup on exit |
| `resolve_and_open()` | Handle nested ZIPs with two-level temp extraction |

**Dependencies:** None (stdlib only)

---

## LLM Pipeline

### `hdx_review.py` - Second-pass HEVL review

Re-analyzes RDLS JSON files from the HDX crawler using improved signal matching (column detection, resource names) and cross-references with original HDX metadata. Phase 1 of the LLM pipeline uses this for signal triage.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `build_hdx_index()` | Index HDX metadata JSON files by dataset UUID |
| `load_rdls_record()` | Load RDLS JSON from dist folder |
| `assess_hevl()` | Re-assess HEVL with improved signal matching |
| `revise_record()` | Update record with new HEVL blocks + risk_data_types |
| `_scan_dist_tiers()` | Scan dist folder tiers (high, invalid/high, etc.) |

**Key Classes:**

| Class | Fields |
|-------|--------|
| `ReviewableRecord` | `filepath`, `record`, `rdls_id`, `hdx_uuid`, `current_rdt`, `current_blocks`, `dist_tier` |
| `HEVLAssessment` | `current_hevl` vs `assessed_hevl`, signal evidence, component scores, verdict |

**Dependencies:** `utils`, `review`, `integrate`, `classify`

---

### `ckan_columns.py` - CKAN column header fetcher with cache

Fetches actual column headers from HDX resources via CKAN `resource_show` API. Parses `fs_check_info` and `shape_info` fields. Caches results to disk for reuse.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `load_columns_for_uuid()` | Load cached column headers for a dataset UUID |
| `fetch_columns_batch()` | Batch fetch from CKAN API with rate limiting and caching |

**Key Classes:**

| Class | Fields |
|-------|--------|
| `ColumnInfo` | `resource_id`, `resource_name`, `format`, `columns`, `column_types`, `hxl_tags`, `sheet_name`, `n_rows`, `n_cols`, `source` |
| `ColumnCache` | Disk-backed cache. Methods: `get()`, `put()`, `has()`, `stats()` |
| `FetchStats` | Batch run statistics (fetched, cached, failed, skipped) |

**Dependencies:** `utils`, requests

---

### `llm_review.py` - 4-phase LLM classification pipeline

Full LLM-assisted HEVL classification: Phase 1 (signal triage), Phase 2 (column enrichment), Phase 3 (LLM classification via Claude Haiku), Phase 4 (validation + merge + ID rebuild).

**Key Functions:**

| Function | Description |
|----------|-------------|
| `run_llm_review()` | End-to-end 4-phase pipeline orchestrator |
| `run_phase_1()` | Signal triage via hdx_review (free) |
| `run_phase_2()` | Enrich records with CKAN column headers (cached) |
| `run_phase_3()` | Batch LLM classification with rate limiting and cost guardrails |
| `run_phase_4()` | Validate, merge, rebuild IDs, write outputs |
| `_rebuild_id_for_new_rdt()` | Parse old ID, swap type prefix when risk_data_type changes |

**Key Classes:**

| Class | Fields |
|-------|--------|
| `LLMClassification` | `rdls_id`, `is_rdls_relevant`, `components` (dict), `component_reasoning` (dict), `overall_reasoning`, `confidence`, `domain_category`, `llm_model`, `prompt_hash`, `token_usage` |
| `TriageBucket` | `confident`, `borderline`, `no_signal` (lists of rdls_ids) |

**Dependencies:** `hdx_review`, `ckan_columns`, `naming`, `validate_qa`, `utils`, anthropic (external)

---

## Source Adapters

### `sources/hdx.py` - HDX CKAN API client

HDX-specific crawling, metadata normalization, field extraction, and OSM policy detection.

**Key Functions:**

| Function | Description |
|----------|-------------|
| `extract_hdx_fields()` | Extract standardized fields from HDX dataset JSON (title, notes, tags, organization, groups, resources, etc.) |
| `normalize_dataset_record()` | Unwrap/normalize HDX API response |
| `detect_osm_dataset()` | Detect OpenStreetMap datasets (tags + notes scan) |

**Key Classes:**

| Class | Fields |
|-------|--------|
| `HDXCrawlerConfig` | `base_url`, `rows_per_page`, `requests_per_second`, `max_retries`, `timeout`, `max_datasets`, `slug_max_length`. Class method: `from_yaml()` |
| `HDXClient` | HTTP client with rate limiting, retries, bot-check detection. Methods: `get_json()`, `ckan_action()` |

**Dependencies:** `utils`, requests

---

### `sources/geonode.py` - GeoNode adapter (stub)

Placeholder for future GeoNode support. Interface defined but not implemented. Follow the `sources/hdx.py` pattern to implement.

**Dependencies:** `utils`

---

## Entry Points

### `__init__.py` - Convenience imports

Re-exports ~60 public functions and classes from all submodules for easier `from src import X` usage.

### `__main__.py` - CLI entry point

Runs the inventory module as a CLI tool: `python -m src path/to/folder`.
