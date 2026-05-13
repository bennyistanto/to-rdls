# to-rdls Module Reference

Complete reference for all modules, their public API, key internals, and inter-dependencies.

## Module dependency graph

```
utils.py ← (all modules)
spatial.py ← translate.py, translate_v03.py, naming.py
schema.py ← validate_qa.py
codelists.py ← translate.py, extract.py, validate_v10.py
naming.py ← integrate.py, translate.py, translate_v03.py

--- v1.0 pipeline (canonical) ---
llm_classify.py ← (v1.0 pipeline entry - single-phase LLM; uses sources/ckan_columns.py)
translate.py ← (v1.0 base record builder)
extract.py ← (v1.0 HEVL block builders)
validate_v10.py ← (v1.0 3-layer audit: schema + codelist + semantic)

--- v0.3 pipeline (legacy) ---
classify.py ← (v0.3 pipeline entry - tag/keyword scoring)
translate_v03.py ← (v0.3 record builder)
extract_hazard.py ← (v0.3 pipeline)
extract_exposure.py ← (v0.3 pipeline)
extract_vulnloss.py ← (v0.3 pipeline)
validate_v03.py ← (v0.3 semantic validation logic)

--- shared pipeline ---
integrate.py ← (HEVL merge - used by both pipelines)
validate_qa.py ← (pipeline-time autofix + scoring - used by both pipelines)

--- standalone tools ---
inventory.py ← (no to-rdls dependencies, stdlib only)
review.py ← inventory.py, configs/review_knowledge.yaml (requires geospatial env)
zipaccess.py ← review.py (ZIP member inspection)
__main__.py ← inventory.py (CLI entry)

--- sources/ (all source-specific code) ---
sources/hdx.py ← (HDX/CKAN source adapter - reference implementation)
sources/geonode.py ← (GeoNode source adapter)
sources/ckan_columns.py ← (CKAN column fetcher; used by llm_classify.py + sources/hdx_llm_review.py)
sources/hdx_review.py ← utils, review, integrate (HDX HEVL re-scoring)
sources/hdx_llm_review.py ← sources/hdx_review, sources/ckan_columns, naming, utils
                            (v0.3 4-phase LLM pipeline entry; config: configs/sources/hdx_llm_review.yaml)
```

## utils.py - Text & I/O utilities

**Text processing**:
- `sanitize_text(text)` → Clean mojibake, HTML tags, smart quotes, control chars
- `slugify(s, max_len=80)` → URL-safe hyphenated slug
- `slugify_token(s, max_len=80)` → Underscore-separated token
- `norm_str(x)` → NFKD normalize + lowercase
- `normalize_text(s)` → Lowercase + whitespace collapse
- `short_text(s, max_len=100)` → Truncate with ellipsis
- `split_semicolon_list(s)` → Parse delimited strings
- `looks_like_url(s)` → URL detection
- `as_list(x)` → Coerce to list

**File I/O**:
- `load_json(path)`, `write_json(path, obj, pretty=True)`
- `append_jsonl(path, obj)`
- `load_yaml(path)`, `write_yaml(path, obj)`
- `iter_json_files(folder)` → sorted list of .json files

**Directory**:
- `clean_directory(directory, label, mode="replace")` → replace/skip/abort

**Nested dict**:
- `navigate_path(obj, parts)` → (parent, key)
- `set_at_path(obj, parts, value)`, `remove_at_path(obj, parts)`

## schema.py - Schema operations

- `load_rdls_schema(schema_path)` → parsed JSON Schema dict
- `load_codelists(yaml_path)` → Dict[name → list of values]
- `load_codelists_from_schema(schema)` → extract enums from $defs
- `validate_record(record, schema)` → (is_valid, error_messages)
- `summarize_errors(errors)` → Counter by category
- `check_required_fields(record)` → list of missing fields
- `SchemaContext(schema, codelist_config)`:
  - `.enum_lookup[field_name]` → set of valid values
  - `.field_aliases[prop_name]` → $defs enum name
  - `.required_lookup[def_name]` → set of required fields
  - `.allowed_props[def_name]` → set of allowed properties
  - `.fuzzy_codelist_fix(bad_value, field_name)` → best match or None
  - `.is_field_required(parts)` → bool (heuristic)
  - Internal builders: `_build_enum_lookup()`, `_build_field_aliases()`, `_build_required_lookup()`, `_build_allowed_props()`, `_build_property_to_def()`

## spatial.py - Geography

- `load_spatial_config(yaml_path)` → config with regions, fixes, non-country groups
- `load_country_iso3_table(csv_path)` → country name → ISO3 mapping from CSV
- `country_name_to_iso3(name, fixes, iso3_table)` → ISO3 code
  - Resolution: already ISO3? → country_name_fixes → iso3_table → pycountry fallback
- `infer_spatial(groups, ...)` → `{"scale": "national|regional|global", "countries": [...]}`
- `infer_scale(countries)` → scale from country count (0→global, 1→national, 2+→regional)
- `_norm_country_key(s)` → normalize for lookup
- `_try_pycountry(name)` → fallback resolution

## classify.py - Dataset classification

- `Classification(scores, components, rdls_candidate, confidence, top_signals)`
- `load_classification_config(yaml_path)` → scoring config
- `load_exclusion_patterns(signal_dict_path)` → component → regex list
- `classify_dataset(meta, config, keywords, exclusions)` → Classification
- `apply_overrides(classification, overrides, dataset_id)`
- `enforce_component_deps(components, rules)` → enforce V/L require H or E
- Thresholds: high≥7, medium≥4, candidate≥5

## translate.py - v1.0 record builder (canonical)

- `build_entity(name, url, email)` → entity dict with anyOf (url or email)
- `build_resources_v10(hdx_resources)` → RDLS v1.0 resources array (media_type + format)
- `build_base_record_v10(fields, classification, spatial_config)` → base RDLS v1.0 record
- `order_record_fields_v10(record)` → field-ordered dict for clean JSON output
- `wrap_datasets_v10(record)` → `{"datasets": [record]}` wrapper
- `map_license_url(license_title, license_id, license_url)` → SPDX URL
- `map_media_type(data_format)` → IANA media type string
- `parse_hdx_date(date_str)` → ISO date string

## translate_v03.py - v0.3 record builder (legacy)

- `load_format_config(yaml_path)`, `load_license_config(yaml_path)`
- `detect_service_url(url, patterns)` → (data_format, access_modality)
- `infer_format_from_name(name, url)` → format string
- `map_data_format(source_fmt, url, name, config)` → RDLS data_format
- `map_license(license_str, config)` → RDLS license code
- `build_attributions(fields, source_url)` → attributions array
- `build_resources(fields, format_config)` → resources array
- `build_rdls_record(fields, components, ...)` → base RDLS v0.3 record
- `wrap_datasets(record)` → `{"datasets": [record]}` wrapper

## extract_hazard.py - Hazard extraction

**Dataclasses**:
- `ExtractionMatch(value, confidence, source_field, matched_text, pattern)`
- `HazardExtraction(hazard_types, process_types, analysis_type, return_periods, intensity_measures, overall_confidence, calculation_method)`

**Key constants**:
- `TIER1_FIELDS` = {"title", "name", "tags", "resources"}
- `TIER2_FIELDS` = {"notes", "methodology"}
- `TIER2_FALSE_POSITIVE_PATTERNS` - compiled regex list to filter FP in Tier 2
- `RP_PATTERNS` - return period regex patterns (N-year, RP-N, 1-in-N)
- `IM_TEXT_PATTERNS` - intensity measure codes → pattern lists (PGA:g, PGV:m/s, MMI:-, wd:m, etc.)
- `SIMULATED_PATTERNS`, `OBSERVED_PATTERNS`, `INFERRED_PATTERNS` - calculation method detection
- `CONFIDENCE_MAP` = {"high": 0.9, "medium": 0.7, "low": 0.5}

**Public API**:
- `HazardExtractor(signal_dict, defaults_config)`:
  - `.extract(record)` → HazardExtraction
  - 2-tier cascade, false-positive filtering
  - Extracts: hazard_types, process_types, analysis_type, return_periods, intensity_measures
  - Internal: `_compile_patterns()`, `_extract_text_fields()`, `_match_hazard_types()`, `_has_false_positive_context()`, `_match_process_types()`, `_match_analysis_type()`, `_extract_return_periods()`, `_extract_intensity_measures()`, `_infer_calculation_method()`
- `build_hazard_block(extraction)` → RDLS hazard JSON with event_sets

## extract_exposure.py - Exposure extraction

**Dataclasses**:
- `ExposureExtraction(categories, metrics, taxonomy_hint, currency, overall_confidence)`
- `MetricExtraction(dimension, quantity_kind, confidence, source_hint)`

**Key constants**:
- `DIMENSION_PATTERNS` - regex for structure, content, product, disruption, population, index
- `QUANTITY_KIND_PATTERNS` - regex for count, area, length, monetary, time
- `CURRENCY_PATTERNS` - (regex, currency_code) tuples for currency detection
- `COMMON_CURRENCIES` - 80+ ISO 4217 codes for fallback
- `TAXONOMY_PATTERNS` - regex for GED4ALL, MOVER, GLIDE, EMDAT, OED, HAZUS, etc.
- `CORROBORATION_BOOST` = 0.05

**Public API**:
- `ExposureExtractor(signal_dict, defaults_config)`:
  - `.extract(record)` → ExposureExtraction
  - 3-tier cascade with corroboration boost (+0.05)
  - Validates against valid_triplets constraint table
  - Internal: `_scan_tier()`, `_infer_metrics()`, `_detect_taxonomy()`, `_detect_currency()`
- `build_exposure_block(extraction)` → RDLS exposure JSON array

## extract_vulnloss.py - Vulnerability & Loss

**Dataclasses**:
- `FunctionExtraction(function_type, approach, relationship, hazard_primary, impact_type, impact_metric, quantity_kind, confidence)`
- `SocioEconomicExtraction(indicator_name, indicator_code, scheme, description, confidence)`
- `VulnerabilityExtraction(functions, socioeconomic_indices, overall_confidence)`
- `LossEntryExtraction(loss_signal_type, hazard_type, impact_metric, loss_frequency_type, currency, reference_year, is_insured, asset_category, asset_dimension, impact_type, quantity_kind, loss_type, approach)`
- `LossExtraction(entries, overall_confidence)`

**Key constants (Vulnerability)**:
- `FUNCTION_TYPE_PATTERNS` - regex for vulnerability, fragility, damage_to_loss, engineering_demand
- `APPROACH_PATTERNS` - regex for analytical, empirical, hybrid, judgement
- `RELATIONSHIP_PATTERNS` - regex for math_parametric, math_bespoke, discrete
- `IMPACT_TYPE_PATTERNS` - regex for direct, indirect, total
- `IMPACT_MODELLING_PATTERNS` - regex for simulated, observed, inferred

**Key constants (Loss)**:
- `LOSS_SIGNAL_PATTERNS` - 8 signal types with regex patterns
- `LOSS_EXCLUSION_PATTERNS` - compiled FP filters ("data loss", "weight loss", "profit & loss")
- `INSURED_LOSS_PATTERNS` - insured loss detection
- `LOSS_APPROACH_PATTERNS`, `LOSS_FREQUENCY_PATTERNS`
- `CURRENCY_PATTERNS` - 21+ currencies with regex
- `YEAR_PATTERN` - reference year extraction (1900-2099)

**Public API**:
- `VulnerabilityExtractor(defaults_config)`:
  - `.extract(record)` → VulnerabilityExtraction
  - Detects function_type, approach, relationship, impact_metric
  - 18 socioeconomic indicators
  - Internal: `_load_socio_patterns()`, `_detect_approach()`, `_detect_relationship()`, `_detect_impact_type()`, `_validate_metric()`
- `LossExtractor(defaults_config)`:
  - `.extract(record)` → LossExtraction
  - 8 signal types with defaults from config
  - Internal: `_is_excluded()`, `_detect_currency()`, `_detect_insured()`, `_detect_loss_approach()`, `_detect_loss_frequency()`, `_validate_loss_entry()`
- `build_vulnerability_block(extraction)` → RDLS vulnerability JSON
- `build_loss_block(extraction)` → RDLS loss JSON

## naming.py - ID generation

- `load_naming_config(yaml_path)`
- `encode_component_types(components, config)` → type segment (hzd/exp/he/hevl)
- `encode_countries(iso3_codes, config)` → country segment (max 5)
- `resolve_shortname(org_name, org_slug, config)` → org abbreviation
- `slugify_title(title, ...)` → max 25 char slug
- `build_rdls_id(...)` → `rdls_{type}-{iso3}{org}_{slug}`
- `build_rdls_id_with_collision(...)` → with `__{uuid8}` suffix
- `parse_rdls_id(rdls_id, config)` → parsed components
- `is_valid_iso3(code, config)` → bool
- `_iso3_to_names(iso3_codes, config)` → lowercase names for slug stripping
- `_ID_PATTERN` - compiled regex for parsing RDLS IDs

## integrate.py - HEVL merge

- `merge_hevl_into_record(base_record, hevl_blocks)` → merged record
- `integrate_record(...)` → full integration with validation + ID rebuild
- `append_provenance(record, note)` → add to description
- `validate_component_combination(components, ...)` → (is_valid, reason)
- `determine_risk_data_types(base_types, hevl_flags)` → updated list
- `build_integrated_id(components, iso3_codes, org_name, org_slug, naming_config, title)` → wrapper for naming
- `extract_hazard_types_from_block(hazard_block)` → list of hazard_type values
- `extract_exposure_categories_from_block(exposure_block)` → list of category values
- `extract_iso3_from_spatial(spatial)` → list of ISO3 codes
- `extract_org_from_attributions(attributions)` → publisher org name

## validate_qa.py - Validation & QA

**Classes**:
- `ScoredRecord(record, validation_status, error_count, fix_count, warnings, composite_confidence, auto_fixed)`
- `AutoFixer(ctx, defaults, schema_gap_fields)` - 5-pass auto-fix engine:
  1. `_structural_repair()` - fix JSON type mismatches (exposure object→array, etc.)
  2. Error-driven codelist fixes via `SchemaContext.fuzzy_codelist_fix()`
  3. `_deep_clean_empties()` - remove empty strings/dicts/arrays from non-required fields
  4. `_infer_missing_required()` - fill missing required by context inference
  5. `_clean_non_schema_fields()` - remove additional properties not in schema
  - Hazard-specific: `_repair_hazard_obj()`, `_infer_hazard_process_from_events()`, `_build_occurrence_placeholder()`

**Public API**:
- `validate_against_schema(record, schema)` → rich error dicts with path, message, category, validator
- `categorize_error(err, path)` → human-readable category
- `check_business_rules(record, required_roles, schema_link_pattern)` → RDLS-specific checks (attribution roles, resource URLs, entity contact, schema link, risk_data_type consistency, spatial validity, currency)
- `validate_and_score(records, schema, context)` → list of ScoredRecord
- `apply_autofixes(record, errors, schema)` → auto-fixed record
- `compute_composite_confidence(record, context)` → 0.0–1.0
- `distribute_records(scored, thresholds, output_dir)` → tiered output
- `create_validation_report(scored)` → JSON summary
- `generate_validation_csv(scored)` → detailed CSV export

## inventory.py - Delivery folder/ZIP inventory

**Standalone module** - stdlib only, no to-rdls dependencies. Scans folders and ZIP archives without extraction.

**Public API**:
- `inventory_folder(target, *, output_dir=None, formats="json,md,csv", include_hash=False, inspect_zips=True, verbose=True)` → `(markdown, rows, stats)` - high-level convenience function
- `scan_target(cfg: InventoryConfig)` → `(rows, stats)` - core scanner
- `render_and_write(cfg: InventoryConfig)` → `(markdown, rows, stats)` - scan + render + write outputs

**Config dataclass**: `InventoryConfig(target, write_markdown_path, write_csv_path, write_json_path, include_hash, inspect_zips, zip_max, excludes, max_depth, follow_symlinks, verbose)`

**Row dict fields**: `container`, `path`, `name`, `ext`, `mime`, `size_bytes`, `size_human`, `modified_utc`, `is_in_zip`, `sha256`

**Stats dict fields**: `target`, `files`, `dirs`, `total_bytes`, `total_human`, `generated_utc`, `zip_entries`

**CLI**: `python -m src.inventory /path/to/folder [-o OUTPUT_DIR] [--formats json,md,csv] [--hash] [--no-zip-inspect] [-q]`

**Internal helpers**: `human_size()`, `iso_time()`, `sha256_file()`, `mime_from_name()`, `matches_any_glob()`, `iter_dir()`, `list_zip_members()`, `file_row()`, `build_tree_lines()`, `markdown_report()`, `write_csv()`, `write_json()`

## review.py - Automated data review

Inspects delivery folders, classifies files by HEVL, identifies metadata gaps, and generates structured review reports. Requires the `to-rdls` conda environment (GDAL, rasterio, fiona, geopandas, PyMuPDF, python-docx).

All HEVL signal patterns, file filtering rules, model software definitions, naming patterns, and column detection rules are loaded from `configs/review_knowledge.yaml` (not hardcoded). New model software (MIKE FLOOD, TUFLOW, Delft3D, etc.) can be added by editing the YAML without touching Python code.

**Pipeline phases**: Inventory → Group files → Filter intermediates → Inspect representative files → Classify HEVL → Gap analysis → Write report

**Entry points**:
- `review_folder(target, *, output_dir=None, max_inspect=30, verbose=True) → ReviewResult` - full review with HEVL classification
- `_inspect_pipeline(target, *, max_inspect=30, verbose=False) → _PipelineResult` - shared Steps 1-3 (inventory → group → filter intermediates → split → inspect), used by both `review_folder()` and MCP's `inspect_folder_for_llm()`

**Config loader**:
- `load_review_config(yaml_path=None) → Dict` - loads `review_knowledge.yaml`, caches at module level, compiles regex patterns, converts lists to sets for O(1) lookup. Falls back to `_builtin_defaults()` with `warnings.warn` if YAML missing.
- `_compile_config(cfg) → Dict` - compiles regex strings to `re.Pattern` for model_software, naming, and readme pattern sections
- `_get_config() → Dict` - lazy accessor for cached config

**Dataclasses**:
- `_PipelineResult(groups, inspections, stats, rows, intermediate_summary)` - shared pipeline output for Steps 1-3
- `FileInspection(path, format, inspection)` - raw inspection result dict per file
- `FileGroup(name, files, formats, total_size_bytes, hevl, hazard_types, exposure_categories, confidence, evidence, inspections)` - logical file grouping with HEVL classification
- `GapAnalysis(group, severity, field, status, missing_required, missing_recommended, actions)` - gap assessment per group
- `ReviewResult(target, generated_utc, stats, file_groups, inspections, gap_analyses, suggested_datasets)` - complete review output

**File inspectors** (dispatch by extension):
- `inspect_geotiff(path)` - rasterio primary (CRS, bounds, bands, resolution, dtype), PIL fallback
- `inspect_vector(path)` - geopandas (CRS, bounds, columns, geometry type, sample row)
- `inspect_fgdb(path)` - fiona layer list + geopandas first layer schema
- `inspect_xlsx(path)` - openpyxl (sheets, columns, row counts)
- `inspect_csv(path)` - pandas (columns, dtypes, row count, sample)
- `inspect_json_data(path)` - JSON structure (array/object, GeoJSON detection, fields)
- `inspect_text(path)` - text excerpt up to 2000 chars
- `_inspect_pdf(path)` - PyMuPDF text extraction
- `_inspect_docx(path)` - python-docx paragraph text

**Grouping**: `group_files(rows)` - groups inventory rows by top-level folder or ZIP container name

**Classification**: `classify_group(group)` - matches file paths and inspection content against HEVL signal patterns from `review_knowledge.yaml` (loaded at module init, compiled to regex)

**Gap analysis**: `analyze_gaps(groups)` - checks available fields against RDLS required/recommended fields

**Dataset mapping**: `suggest_datasets(groups)` - maps groups to RDLS dataset records; splits multi-component groups (e.g., EHL → separate E, H, L records)

**Report**: `render_review_markdown(review)` - generates human-readable markdown with summary table, group classifications, file inspections, gap analysis, and suggested datasets

**Output**: writes `review_{timestamp}.json` + `review_{timestamp}.md` to `_rdls_review/` subfolder

**CLI**: `python -m src.review /path/to/folder [-o OUTPUT_DIR] [--max-inspect 30] [-q]`

## sources/ - Source adapters and HDX pipeline extensions

All source-specific code lives in `src/sources/`. Rule: if a module only makes sense
for one data source, it belongs here. General pipeline modules stay in `src/` root.

### sources/hdx.py - HDX/CKAN source adapter (reference implementation)

- `HDXCrawlerConfig.from_yaml(yaml_path)`
- `HDXClient(config)` - rate-limited HTTP client with retry
- `iter_datasets(client, config, query)` → generator of dataset dicts
- `download_dataset_metadata(client, config, id)` → (metadata, source)
- `normalize_dataset_record(raw)` → unwrapped record
- `extract_hdx_fields(ds)` → common field dict (the interface all adapters must match)
- `detect_osm(ds, markers, threshold)` → OSMDetectionResult

### sources/geonode.py - GeoNode source adapter

- `GeoNodeConfig` dataclass with `from_yaml()`
- `normalize_geonode_record(raw)` → normalized record
- `extract_geonode_fields(ds)` → common field dict (same shape as HDX adapter)

### sources/ckan_columns.py - CKAN column header fetcher

- `ColumnCache(cache_dir)` - disk-backed cache for column headers
- `ColumnInfo(uuid, resource_id, columns, fetch_time, error)` dataclass
- `fetch_resource_columns(resource_id, timeout)` → list of column names
- `load_columns_for_uuid(dataset_uuid, cache)` → list of ColumnInfo

### sources/hdx_review.py - HDX second-pass HEVL review

- `ReviewableRecord` dataclass - RDLS record + HDX metadata + column info
- `HEVLAssessment` dataclass - scored HEVL components with evidence
- `assess_hevl(record, column_data)` → HEVLAssessment
- `build_hdx_index(records)` → lookup dict for HDX cross-referencing
- `_init_extractors()` - lazy init of v0.3 extractors

### sources/hdx_llm_review.py - HDX v0.3 LLM pipeline (4-phase)
Config: `configs/sources/hdx_llm_review.yaml`

- `ReviewConfig` dataclass - loaded from YAML (model, thresholds, cost cap, rate limits)
- `load_review_config(yaml_path)` → ReviewConfig
- `TriageBucket(SKIP, ACCEPT, LLM)` enum
- `triage_records(records, config)` → bucketed records
- `build_classification_prompt(record, columns)` → (system, user) tuple
- `parse_llm_response(response_text)` → LLMClassification
- `run_llm_review(records, config, ...)` → reviewed records

### Common field dict (interface contract for source adapters)
All source adapters must produce a dict with these keys:
`id`, `name`, `title`, `notes`, `methodology`, `organization`, `org_name`, `org_description`, `license_title`, `license_url`, `groups`, `tags`, `resources`, `dataset_date`, `dataset_source`, `maintainer`, `url`

## scripts/ and notebooks/

**scripts/** - executable entry points. One file = one runnable action.
All scripts use `Path(__file__).parent.parent` to locate project root.

| Script | Purpose |
|--------|---------|
| `rdls_hdx_pipeline.py` | v1.0 LLM-first HDX pipeline (canonical) |
| `rdls_hdx_llm_review.py` | v0.3 HDX LLM review pipeline (4 phases) |
| `rdls_hdx_sanitize_validate.py` | Post-LLM sanitization + validation |
| `rdls_geonode_pipeline.py` | GeoNode v0.3 pipeline |
| `rdls_desinventar_01_generate_records.py` | DesInventar loss record generation |
| `rdls_nismod_00a/00b_*.py` | NISMOD preprocessing (country bbox, GeoNames) |
| `rdls_nismod_01_generate_icra_records.py` | NISMOD ICRA record generation |
| `validate_records.py` | v1.0 three-layer validation CLI (wraps src/validate_v10.py) |
| `validate_records_v03.py` | v0.3 semantic validation CLI (wraps src/validate_v03.py) |
| `convert_v03_to_v10.py` | Schema version conversion |
| `post_convert_enrich.py` | Post-conversion enrichment + validation |

**notebooks/** - interactive Jupyter notebooks only (.ipynb)

| Notebook | Purpose |
|----------|---------|
| `rdls_validate_metadata.ipynb` | Interactive metadata validator |
| `rdls_data_inventory_contents.ipynb` | Interactive delivery inventory |
| `rdls_ind_gobs_csv2gpkg.ipynb` | India GOBS CSV → GeoPackage converter |

## Documentation (to-rdls/docs/)

| File | Purpose |
|---|---|
| `delta_vs_rdls_schema_comparison.md` | Field-by-field DELTA database vs RDLS v0.3 mapping |
| `delta_vs_rdls_system_comparison.md` | Architectural comparison of DELTA vs RDLS systems |
| `github_issue_19_revision.md` | Revision notes for GFDRR issue (impact_metric, process_type fixes) |
| `jkan_issue_loss_display.md` | Issue tracking for loss display UI in RDL-JKAN portal |

## MCP server (mcp_server.py)

FastMCP-based server exposing review and validation tools for Claude-assisted workflows.

**Tools**:
- `inventory_folder(path)` → file inventory JSON (rows, stats, format breakdown)
- `review_folder(path, max_inspect=30)` → full `ReviewResult` as JSON (groups, inspections, gaps, suggested datasets)
- `validate_record(record_json)` → validation result (is_valid, errors, fixes applied)
- `lookup_codelist(codelist_name)` → list of valid values for any RDLS codelist
- `inspect_folder_for_llm(path, max_inspect=30)` → structured inspection data optimized for LLM consumption:
  - `folder_summary`: total files, format distribution, intermediate files excluded
  - `file_groups[]`: name, file count, formats, sample filenames, naming patterns (scenarios, return periods, hazard codes)
  - `file_inspections[]`: path, format, metadata (CRS, bounds, columns, band stats, geometry type)
  - `readme_extractions`: project title, provider, financer (from README/reports)
  - `rdls_context`: required fields, valid hazard types, valid exposure categories

`inspect_folder_for_llm` deliberately omits HEVL classification - Claude applies domain knowledge to the structured inspection data. Internally calls `_inspect_pipeline()` (shared with `review_folder()`) then `analyze_naming_patterns()` per group and `extract_readme_metadata()`.

---

## zipaccess.py - ZIP member extraction

**stdlib only** (zipfile, tempfile, pathlib, os). Provides context managers for extracting individual files from ZIP archives to temp paths.

- `parse_zip_spec(fpath)` → `(zip_path, member_name)` - Split `archive.zip::inner/path/file.tif`
- `open_zip_member(zip_path, member_name)` → context manager → yields temp `Path`
- Handles nested ZIPs (ZIP-in-ZIP) with two-level extraction
- Critical for multi-GB ZIPs: only extracts the requested member

**Dependencies**: stdlib only (no to-rdls imports)

---

## hdx_review.py - HDX second-pass HEVL review

Re-analyzes RDLS JSON files using improved signal matching (column detection, resource-name signals) and cross-references with original HDX metadata.

**Dataclasses**:
- `ReviewableRecord(filepath, record, rdls_id, hdx_uuid, current_rdt, current_blocks, dist_tier)`
- `HEVLAssessment(rdls_id, old_components, new_components, changes, evidence, confidence)`

**Functions**:
- `build_hdx_index(metadata_dir)` → `Dict[uuid, metadata]` - index HDX dataset_metadata by UUID
- `load_rdls_record(filepath)` → `ReviewableRecord` - load and parse RDLS JSON with HDX cross-ref
- `_scan_dist_tiers(dist_dir)` → list of (filepath, tier) tuples - scan tier directories
- `assess_hevl(record, hdx_meta, config)` → `HEVLAssessment` - re-score HEVL using column patterns
- `revise_record(record, assessment)` → revised dict - apply HEVL changes to record

**Dependencies**: `utils.py`, `review.py` (signal patterns), `integrate.py` (merge_hevl_into_record, determine_risk_data_types)

**Config**: `configs/review_knowledge.yaml` (HEVL signals, column detection patterns)

---

## ckan_columns.py - CKAN column header fetcher

Fetches actual column headers from HDX resources via CKAN resource_show API without downloading data files.

**Dataclasses**:
- `ColumnInfo(resource_id, resource_name, format, columns, column_types, hxl_tags, sheet_name, n_rows, n_cols, source)`
- `FetchStats(total_datasets, total_resources, cached, fetched, with_columns, without_columns, errors, skipped_formats, elapsed_seconds)`

**Classes**:
- `ColumnCache(cache_dir)` - disk-backed cache
  - `.get(resource_id)` → `List[ColumnInfo] | None`
  - `.put(resource_id, infos)` - save to `{resource_id}.json`
  - `.put_none(resource_id)` - sentinel `{resource_id}.none`
  - `.has(resource_id)` → bool

**Functions**:
- `load_columns_for_uuid(uuid, metadata_dir, cache, api_key)` → `List[ColumnInfo]`
- `fetch_resource_columns(resource_id, api_key)` → `List[ColumnInfo]`
- Parses `fs_check_info` (CSV/XLSX) and `shape_info` (GeoJSON/SHP)

**Dependencies**: `requests` (external). No to-rdls module imports.

---

## llm_review.py - LLM-assisted HEVL classification pipeline

4-phase pipeline solving content-blind over-classification (Problem 7).

**Dataclasses**:
- `LLMClassification(rdls_id, is_rdls_relevant, components, component_reasoning, overall_reasoning, confidence, domain_category, llm_model, prompt_hash, token_usage)`
- `TriageBucket(confident, borderline, no_signal, validation_sample)`
- `ReviewConfig` - 20+ fields loaded from `configs/llm_review.yaml` via `from_yaml()`

**Key functions**:
- `run_llm_review(dist_dir, metadata_dir, output_dir, config)` - main entry, runs all 4 phases
- `load_review_config(yaml_path)` → `ReviewConfig`
- `_rebuild_id_for_new_rdt(old_id, new_components, naming_cfg)` - swap type prefix in record ID when LLM reclassifies

**4-phase architecture**:
1. Signal triage: `_phase1_triage()` - re-score with regex, bucket into confident/borderline/no_signal
2. Column enrichment: `_phase2_columns()` - fetch CKAN headers via `ColumnCache`
3. LLM classification: `_phase3_llm()` - Claude Haiku with structured prompt, cost guardrails
4. Merge + write: `_phase4_merge()` - apply LLM decisions, rebuild IDs, separate not-RDLS, validate

**Dependencies**: `hdx_review.py`, `ckan_columns.py`, `naming.py`, `utils.py`

**Config**: `configs/llm_review.yaml`

---

## __main__.py - CLI entry point

Allows running inventory as: `python -m src /path/to/folder`

Delegates to `inventory.main()`.

---

## Key dataclasses (all modules)

```python
# classify.py
Classification(scores, components, rdls_candidate, confidence, top_signals)

# extract_*.py (common)
ExtractionMatch(value, confidence, source_field, matched_text, pattern)

# extract_hazard.py
HazardExtraction(hazard_types, process_types, analysis_type, return_periods, intensity_measures, overall_confidence)

# extract_exposure.py
ExposureExtraction(categories, metrics, taxonomy_hint, currency, overall_confidence)

# extract_vulnloss.py
FunctionExtraction(function_type, approach, relationship, hazard_primary, impact_type, impact_metric, quantity_kind, confidence)
LossEntryExtraction(loss_signal_type, hazard_type, impact_metric, loss_frequency_type, currency, reference_year, is_insured)

# validate_qa.py
ScoredRecord(record, validation_status, error_count, fix_count, warnings, composite_confidence, auto_fixed)

# review.py
_PipelineResult(groups, inspections, stats, rows, intermediate_summary)

# llm_review.py
LLMClassification(rdls_id, is_rdls_relevant, components, component_reasoning, overall_reasoning, confidence, domain_category, llm_model, prompt_hash, token_usage)
ReviewConfig(confident_score_min, max_components_for_confident, validation_sample_pct, ckan_*, llm_*, max_cost_usd, llm_overrides_signals, disagreement_confidence_min)
TriageBucket(confident, borderline, no_signal, validation_sample)

# hdx_review.py
ReviewableRecord(filepath, record, rdls_id, hdx_uuid, current_rdt, current_blocks, dist_tier)
HEVLAssessment(rdls_id, old_components, new_components, changes, evidence, confidence)

# ckan_columns.py
ColumnInfo(resource_id, resource_name, format, columns, column_types, hxl_tags, sheet_name, n_rows, n_cols, source)
ColumnCache(cache_dir) - disk-backed cache: {resource_id}.json or {resource_id}.none sentinel
```

---

## HEVL extraction cascade

### Hazard (2-tier, HazardExtractor)
- **Tier 1** (title, name, tags, resources): Can INTRODUCE hazard_types — high authority
- **Tier 2** (notes, methodology): CORROBORATE only, or fallback if Tier 1 found nothing
- False-positive filter on Tier 2 (suppresses "earthquake risk reduction", "flood preparedness")
- Also extracts: return_periods, intensity_measures, analysis_type, calculation_method

### Exposure (3-tier, ExposureExtractor)
- **Tier 1** (title, name, tags): Introduce categories
- **Tier 2** (resources): Introduce new or corroborate (+0.05 confidence boost)
- **Tier 3** (notes, methodology): Corroborate only
- Validates metrics against exposure_valid_triplets constraint table

### Vulnerability (VulnerabilityExtractor)
- Extracts function_type, approach, relationship, impact_metric
- Detects 18 socioeconomic indicators (POV_HEADCOUNT, HDI, SVI, FOOD_SECURITY, etc.)
- Validates against function_type_constraints table

### Loss (LossExtractor)
- 8 signal types: human_loss, displacement, affected_population, economic_loss, structural_damage, agricultural_loss, catastrophe_model, general_loss
- Each has default impact_metric, asset_category, quantity_kind from rdls_defaults.yaml
- Exclusion patterns filter false positives ("data loss", "profit & loss")
- Detects insured loss, currency, reference_year

---

## Validation & QA (validate_qa.py)

5-pass autofix engine:
1. Codelist fuzzy matching (case-insensitive, substring, fuzzy)
2. Enum fixes for nested properties
3. Component validation
4. Type coercion
5. Defaults for missing required fields

Confidence scoring: composite from data completeness, attribution variety, resource format quality, spatial precision, component confidence. Weights: hazard 0.3, exposure 0.3, vulnerability 0.2, loss 0.2.

Distribution tiers: high (≥0.8 valid), medium (≥0.5 valid), low (<0.5 valid), plus invalid variants.

---

## LLM-Assisted Review (llm_review.py)

Solves the content-blind over-classification problem (Problem 7).

4-phase pipeline:
1. **Signal triage** — Re-scores each record; buckets into `confident` (skip LLM), `borderline`, `no_signal`. 5% validation sample from confident cross-checked.
2. **Column enrichment** — Fetches actual column headers from CKAN resource_show API via `ColumnCache`. ~88K resources, ~55% have headers. 48+ hours for full cache build.
3. **LLM classification** — Claude Haiku 4.5. Cost guardrail (`max_cost_usd`), rate limiting (1.5s between batches for 50K tokens/min), disk-cached responses. Returns `LLMClassification` with per-component reasoning.
4. **Merge + write** — When LLM disagrees (confidence ≥ 0.7), LLM wins. Rebuilds record ID if risk_data_type changes. Separates non-RDLS records to `output/llm/not_rdls/`. Validates remaining.

Production results (12,594 HDX records): $21.98, 22 min. 3,443 reclassified, 4,103 separated as non-RDLS. 8,822 RDLS-relevant → 6,132 valid, 2,690 blocked by `occurrence:{}` schema gap.

### HDX review (hdx_review.py)
Second-pass HEVL review using improved signal matching (column detection, resource-name signals). Functions: `build_hdx_index()`, `assess_hevl()`, `revise_record()`, `_scan_dist_tiers()`.

### CKAN columns (ckan_columns.py)
Fetches column headers from HDX resources via CKAN resource_show API. Parses `fs_check_info` (CSV/XLSX) and `shape_info` (GeoJSON/SHP). Disk-backed `ColumnCache` with sentinel files for resources without columns.

---

## GeoNode source adapter (sources/geonode.py)

**Implemented** (not a stub). Key components:
- `GeoNodeConfig(portals, title_humanize_config, ...)` — loaded from `configs/sources/geonode.yaml`
- `GeoNodeClient(portal)` — HTTP session with rate limiting and retries
- `iter_datasets(client, portal, max_datasets)` — paginated dataset generator
- `normalize_geonode_record(raw)` — handles GeoNode 4.x `{"dataset": {...}}` wrapper
- `extract_geonode_fields(ds, portal_name, portal_base_url, ..., title_humanize_config)` — returns common 17-key dict + underscore extras: `_source_portal`, `_geonode_spatial`, `_geonode_category`, `_region_iso3_codes`, `_slug_title`
- `_humanize_title(title, config)` — decodes machine-code titles using config regex patterns (e.g., `CK_EQ_HazardMap_03_100_MRP` → `Cook Islands Earthquake Hazard Map, 0.3s SA, 100-year Return Period`)

**Key internal fields** (stripped before final output):
- `_slug_title`: original technical title used for unique ID slug — stripped in `integrate.py`
- `_region_iso3_codes`: authoritative ISO3 codes from GeoNode region `code` fields, filtered through `_NON_ISO3_REGION_CODES` blocklist (PAC, GLO, ASI, EAS, SEA, AFR, NAF, WAF, EAF, CAF, SAF, EUR, CAM, SAM, NAM, CAR, MDE)
