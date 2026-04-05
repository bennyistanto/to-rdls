# RDLS Project - Claude Code Instructions

## Project overview

A source-independent toolkit for transforming dataset metadata from any data catalog into RDLS (Risk Data Library Standard) v0.3 schema. Designed to work with multiple sources: HDX, GeoNode, CKAN, World Bank Data Catalog, and others. Maintained under GFDRR/World Bank's Digital Earth team. License: MPL-2.0.

Pipeline: **Source Crawl → Filter → Classify → Translate → HEVL Extract → Integrate → Validate → Distribute**

## Architecture

The `to-rdls/` module is the core: portable, source-independent Python modules + YAML configs + notebooks. Originally extracted from an HDX-specific notebook pipeline, now designed to support any metadata source via pluggable source adapters.

### to-rdls module structure
```
to-rdls/
├── src/                    # Python modules
│   ├── utils.py            # Text sanitization, file I/O, nested dict navigation
│   ├── schema.py           # JSON Schema loading, validation, SchemaContext
│   ├── spatial.py          # Country name→ISO3, region expansion, spatial block
│   ├── classify.py         # Tag-weighted HEVL classification → Classification
│   ├── translate.py        # Source metadata → base RDLS record builder
│   ├── extract_hazard.py   # 2-tier cascade → HazardExtraction
│   ├── extract_exposure.py # 3-tier cascade → ExposureExtraction
│   ├── extract_vulnloss.py # Vulnerability + Loss extractors
│   ├── integrate.py        # Merge HEVL blocks into base record
│   ├── naming.py           # Structured ID: rdls_{type}-{iso3}{org}_{slug}
│   ├── validate_qa.py      # 5-pass autofix, confidence scoring, distribution
│   ├── inventory.py        # Delivery folder/ZIP inventory (standalone, stdlib only)
│   ├── zipaccess.py        # ZIP member extraction with temp-file context managers
│   ├── review.py           # Automated data review: inspect, HEVL classify, gap analysis
│   ├── hdx_review.py       # HDX second-pass HEVL review with column detection
│   ├── ckan_columns.py     # CKAN column header fetcher with disk-backed cache
│   ├── llm_review.py       # LLM-assisted 4-phase HEVL classification pipeline
│   ├── __main__.py         # CLI entry: python -m src /path/to/folder
│   ├── __init__.py          # Package init
│   └── sources/
│       ├── hdx.py          # HDX/CKAN source adapter (reference implementation)
│       └── geonode.py      # GeoNode source adapter (stub - template for new sources)
├── configs/                # 15 YAML config files (see below)
├── schema/                 # RDLS v0.3 JSON Schema + template
└── notebooks/              # Pipeline notebooks + generators
```

### Data flow
```
Source (any catalog) → source adapter → extract_fields() → common dict
  → classify.classify_dataset() → Classification
  → translate.build_rdls_record() → base RDLS record
  → HazardExtractor.extract() → HazardExtraction → build_hazard_block()
  → ExposureExtractor.extract() → ExposureExtraction → build_exposure_block()
  → VulnerabilityExtractor.extract() → build_vulnerability_block()
  → LossExtractor.extract() → build_loss_block()
  → integrate.integrate_record() → merged record
  → validate_qa.validate_and_score() → ScoredRecord (with autofix)
  → validate_qa.distribute_records() → tiered output

# LLM-assisted review (optional, for HDX source)
  → hdx_review.assess_hevl() → HEVLAssessment (re-scores with column signals)
  → ckan_columns.load_columns_for_uuid() → ColumnInfo (actual column headers)
  → llm_review.run_llm_review() → 4-phase pipeline:
      Phase 1: Signal triage (regex, free) → confident/borderline/no_signal
      Phase 2: Column enrichment (CKAN API, cached)
      Phase 3: LLM classification (Claude Haiku, ~$7/12K records)
      Phase 4: Merge + validate + write (renamed IDs if reclassified)
```

### MCP server (mcp_server.py)

FastMCP-based server exposing 5 tools for Claude-assisted workflows:

| Tool | Purpose |
|------|---------|
| `inventory_folder(path)` | Scan delivery folder, return file inventory with stats |
| `review_folder(path, max_inspect)` | Full automated review: inspect, classify HEVL, gap analysis |
| `validate_record(record_json)` | Validate single RDLS record against schema |
| `lookup_codelist(codelist_name)` | Look up valid values for any RDLS codelist |
| `inspect_folder_for_llm(path, max_inspect)` | Structured inspection data for LLM-assisted classification |

`inspect_folder_for_llm` returns structured JSON (folder summary, file groups with naming patterns, file inspections with CRS/bounds/columns/band stats, README extractions, RDLS context) **without** HEVL classification - designed for Claude to do semantic classification using domain knowledge. It shares `_inspect_pipeline()` with `review_folder()` for Steps 1-3 (inventory → group → filter intermediates → split → inspect).

## Key dataclasses

```python
Classification(scores, components, rdls_candidate, confidence, top_signals)
ExtractionMatch(value, confidence, source_field, matched_text, pattern)
HazardExtraction(hazard_types, process_types, analysis_type, return_periods, intensity_measures, overall_confidence)
ExposureExtraction(categories, metrics, taxonomy_hint, currency, overall_confidence)
FunctionExtraction(function_type, approach, relationship, hazard_primary, impact_type, impact_metric, quantity_kind, confidence)
LossEntryExtraction(loss_signal_type, hazard_type, impact_metric, loss_frequency_type, currency, reference_year, is_insured)
ScoredRecord(record, validation_status, error_count, fix_count, warnings, composite_confidence, auto_fixed)
_PipelineResult(groups, inspections, stats, rows, intermediate_summary)
# LLM review pipeline
LLMClassification(rdls_id, is_rdls_relevant, components, component_reasoning, overall_reasoning, confidence, domain_category, llm_model, prompt_hash, token_usage)
ReviewConfig(confident_score_min, max_components_for_confident, validation_sample_pct, ckan_*, llm_*, max_cost_usd, llm_overrides_signals, disagreement_confidence_min)
TriageBucket(confident, borderline, no_signal, validation_sample)
# HDX review
ReviewableRecord(filepath, record, rdls_id, hdx_uuid, current_rdt, current_blocks, dist_tier)
HEVLAssessment(rdls_id, old_components, new_components, changes, evidence, confidence)
# CKAN columns
ColumnInfo(resource_id, resource_name, format, columns, column_types, hxl_tags, sheet_name, n_rows, n_cols, source)
ColumnCache(cache_dir) - disk-backed cache: {resource_id}.json or {resource_id}.none sentinel
```

## Config files reference

| File | Purpose | Key contents |
|------|---------|-------------|
| `rdls_schema.yaml` | Codelists & required fields | All closed/open codelists, 195+ country codes |
| `signal_dictionary.yaml` | HEVL extraction patterns | 100+ regex patterns with confidence levels |
| `rdls_defaults.yaml` | Default values & constraints | process defaults, valid triplets, loss signal defaults, socioeconomic indicators |
| `classification.yaml` | Scoring rules | tag_weights, keyword_patterns, org_hints, thresholds (high≥7, medium≥4, candidate≥5) |
| `naming.yaml` | ID generation | 265 org abbreviations, 249 ISO3 codes, component codes, title slug rules |
| `format_mapping.yaml` | Format aliases | 81 format aliases, 12 skip formats, 6 service URL patterns |
| `license_mapping.yaml` | License normalization | 33 license variants → RDLS codes |
| `spatial.yaml` | Geography | 77 country name fixes, region→country maps |
| `pipeline.yaml` | Runtime settings | Thresholds (high≥0.8, medium≥0.5), output paths, batch settings |
| `desinventar_mapping.yaml` | DesInventar→RDLS | 31 event type mappings, 14 loss columns, 16 datasets |
| `country_bbox.yaml` | Bounding boxes | ~250 ISO3→[minlon,minlat,maxlon,maxlat] |
| `geonames_country_ids.yaml` | GeoNames IDs | ~250 ISO3→{geoname_id, name} |
| `review_knowledge.yaml` | Review patterns & model software | HEVL signals, file filtering, model software (FIAT/HEC-RAS/general), naming patterns, README patterns, column detection - config-driven, extensible via YAML for new models |
| `sources/hdx.yaml` | HDX source adapter config | Rate limiting, field paths, OSM markers (reference implementation) |
| `sources/geonode.yaml` | GeoNode source adapter config | Stub - template for new source adapters |
| `llm_review.yaml` | LLM review pipeline settings | Phase 1 triage thresholds, Phase 2 CKAN settings, Phase 3 LLM model/cost/concurrency, Phase 4 merge strategy, prompt limits |

## Schema (schema/)

- `rdls_schema_v0.3.json` - Full JSON Schema (3,280 lines) with all $defs, codelists, constraints
- `rdls_template_v0.3.json` - Complete template record with example data (Aruba ICRA)

### Required dataset fields
`id`, `title`, `risk_data_type`, `attributions` (publisher+creator+contact_point), `spatial`, `license`, `resources` (id+title+description+data_format)

### Closed codelists (must match exactly)
- **risk_data_type**: hazard, exposure, vulnerability, loss
- **hazard_type**: coastal_flood, convective_storm, drought, earthquake, extreme_temperature, flood, landslide, strong_wind, tsunami, volcanic, wildfire
- **process_type**: 32 values (fluvial_flood, pluvial_flood, ground_motion, liquefaction, storm_surge, tropical_cyclone, tornado, agricultural_drought, etc.)
- **exposure_category**: agriculture, buildings, infrastructure, population, natural_environment, economic_indicator, development_index
- **analysis_type**: probabilistic, deterministic, empirical
- **function_approach**: analytical, empirical, hybrid, judgement
- **metric_dimension**: structure, content, product, disruption, population, index
- **impact_metric**: 21 values (damage_ratio, loss_ratio, casualty_count, economic_loss_value, etc.)
- **data_format**: 20+ values (GeoTIFF, NetCDF, CSV, GeoJSON, Shapefile, GeoPackage, etc.)
- **access_modality**: file_download, download_page, API, OGC_API, GEE_collection, WMS, WFS, WCS, STAC, REST, dashboard

## Companion reference docs

Detailed lookup tables are in separate files (deployed to `.claude/` alongside this CLAUDE.md):

| File | Contents |
|------|----------|
| `.claude/module-reference.md` | All 20 modules with function signatures, dataclasses, key constants, internal methods, notebooks, docs |
| `.claude/schema-reference.md` | Full RDLS v0.3 JSON Schema $defs: Event_set, Event, VulnerabilityFunction, Losses, Resource, Attribution, Location structures |
| `.claude/constraints-reference.md` | All constraint tables: function_type_constraints, loss signal defaults, valid asset triplets, impact metric constraints, compound tags |
| `.claude/naming-reference.md` | ID format, component codes, hazard/exposure item codes, slug rules, org shortname rules, classification thresholds |
| `.claude/signals-reference.md` | Hazard/exposure signal patterns, exclusion patterns, tag weights, socioeconomic indicators |
| `.claude/configs-detail-reference.md` | Format mapping details (service URLs, ZIP inference, skip formats), region→country mappings, DesInventar event/loss mappings, org_hints, HDX field paths, OSM detection |

## Key constraint tables (quick reference)

### hazard_type → valid process_types
- flood → fluvial_flood, pluvial_flood, groundwater_flood, coastal_flood
- earthquake → primary_rupture, ground_motion, liquefaction
- coastal_flood → storm_surge, coastal_flood
- convective_storm → tropical_cyclone, tornado, extratropical_cyclone
- strong_wind → tropical_cyclone, tornado, extratropical_cyclone
- landslide → landslide, debris_flow, rock_fall, shallow_landslide, slow_moving_landslide
- drought → agricultural_drought, hydrological_drought, meteorological_drought, socioeconomic_drought
- extreme_temperature → extreme_cold, extreme_heat
- volcanic → volcanic_eruption, ash_fall, lahar, pyroclastic_flow, lava_flow
- tsunami → tsunami
- wildfire → wildfire

### exposure_category → valid (dimension, quantity_kind) triplets
- agriculture: (product, area), (product, monetary), (product, count)
- buildings: (structure, count), (structure, area), (content, monetary), (structure, monetary)
- infrastructure: (structure, count), (structure, length), (structure, monetary)
- population: (population, count)
- natural_environment: (product, area)
- economic_indicator: (index, monetary), (index, count)
- development_index: (index, count)

For vulnerability function constraints, loss signal defaults, impact metric constraints, and all other tables → see `.claude/constraints-reference.md`

## HEVL extraction cascade

### Hazard (2-tier, HazardExtractor)
- **Tier 1** (title, name, tags, resources): Can INTRODUCE hazard_types - high authority
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

## Validation & QA (validate_qa.py)

5-pass autofix engine:
1. Codelist fuzzy matching (case-insensitive, substring, fuzzy)
2. Enum fixes for nested properties
3. Component validation
4. Type coercion
5. Defaults for missing required fields

Confidence scoring: composite from data completeness, attribution variety, resource format quality, spatial precision, component confidence. Weights: hazard 0.3, exposure 0.3, vulnerability 0.2, loss 0.2.

Distribution tiers: high (≥0.8 valid), medium (≥0.5 valid), low (<0.5 valid), plus invalid variants.

## LLM-Assisted Review (llm_review.py)

Solves the content-blind over-classification problem (Problem 7): the regex pipeline classifies based on metadata text only, so "data ABOUT earthquakes" and "data CONTAINING earthquake measurements" score identically.

4-phase pipeline:
1. **Signal triage** (Phase 1) - Re-scores each record with improved signal matching including column detection patterns. Buckets into `confident` (skip LLM), `borderline` (send to LLM), `no_signal` (send to LLM). 5% validation sample from confident sent for cross-check.
2. **Column enrichment** (Phase 2) - Fetches actual column headers from CKAN resource_show API via `ckan_columns.ColumnCache`. Disk-backed cache (`{resource_id}.json`). ~88K resources, ~55% have headers. 48+ hours for full cache build.
3. **LLM classification** (Phase 3) - Claude Haiku 4.5 with structured prompt. Cost guardrail (`max_cost_usd`), rate limiting (1.5s between batches for 50K tokens/min), disk-cached responses. Returns `LLMClassification` with per-component reasoning.
4. **Merge + write** (Phase 4) - When LLM disagrees with signals (confidence ≥ 0.7), LLM wins. Rebuilds record ID if risk_data_type changes (`_rebuild_id_for_new_rdt()`). Separates non-RDLS records to `output/llm/not_rdls/`. Validates remaining against schema.

Production results (12,594 HDX records, $21.98, 22 min):
- 3,443 reclassified, 4,103 separated as non-RDLS
- 8,822 RDLS-relevant → 6,132 valid, 2,690 blocked by `occurrence:{}` schema gap

### HDX review (hdx_review.py)

Second-pass HEVL review that re-analyzes RDLS JSON files using improved signal matching (column detection from `review_knowledge.yaml`, resource-name signals) and cross-references with original HDX metadata. Functions: `build_hdx_index()`, `assess_hevl()`, `revise_record()`, `_scan_dist_tiers()`.

### CKAN columns (ckan_columns.py)

Fetches column headers from HDX resources via CKAN resource_show API. Parses `fs_check_info` (CSV/XLSX) and `shape_info` (GeoJSON/SHP). Disk-backed `ColumnCache` with sentinel files for resources without columns.

## Execution environment

### Running to-rdls scripts
```bash
# Always use the to-rdls conda env — it has GDAL, rasterio, fiona, geopandas, PyMuPDF, python-docx, openpyxl, fastmcp
C:/Users/benny/miniforge3/Scripts/conda.exe run -n to-rdls python <script.py>

# MCP server
C:/Users/benny/miniforge3/Scripts/conda.exe run -n to-rdls python mcp_server.py
```

### Gotchas
- **Never** use `--no-banner` flag with `conda run` — not supported, causes error
- **Never** use `python -c "multiline"` with `conda run` — triggers assertion error. Write to a temp `.py` file and run that instead.
- Working directory: `C:/Users/benny/OneDrive/Documents/Github/hdx-metadata-crawler/to-rdls`
- Test baseline: 12 datasets (TC-chattogram, TC-coxsbazar, TC-daressalaam, TC-istanbul, TC-khokana, TC-nablus, TC-nairobi, TC-nakuru, TC-quito, TC-rapti, PhuQuoc, SLE-Freetown) — all must produce identical output after any config/code change

## Coding conventions

- Python 3.10+, type hints on all functions, dataclasses for structured data
- Config-driven: all patterns, mappings, thresholds in YAML - never hardcoded
- Text: `sanitize_text()` (mojibake, HTML, smart quotes), `norm_str()` (NFKD+lowercase), `slugify()`
- File I/O: `load_json/yaml()`, `write_json()` (atomic via temp+rename), `append_jsonl()`
- Nested dict ops: `navigate_path()`, `set_at_path()`, `remove_at_path()`
- Validation: `(is_valid, errors_list)` tuples
- Extractions: `ExtractionMatch(value, confidence, source_field, matched_text, pattern)`
- IDs: `rdls_{type}-{iso3}{org}_{titleslug}` with collision suffix `__{uuid8}`

## Common schema pitfalls

These validation failures appear frequently across sources - check for them proactively:

| Issue | Schema rule | Fix |
|-------|-------------|-----|
| `referenced_by.author_names: []` | minItems: 1 | Remove empty `author_names` and `doi: ""` from referenced_by |
| Loss entry missing `impact_and_losses` | required field in loss component | Wrap loss metrics inside `impact_and_losses` object |
| Empty arrays (`losses: []`, `hazards: []`, `events: []`, `event_sets: []`) | minItems: 1 on each | Remove empty arrays entirely - optional fields should be absent, not empty |
| `resources: []` | minItems: 1 | Record cannot be valid without resources - move to non-RDLS |
| Country code `XKX` (Kosovo) | Not in ISO 3166-1 alpha-3 (249 codes) | Filter or remap - not our schema to change |
| `occurrence: {}` | minProperties: 1 | Known schema issue - team will revise; records blocked until then |

## RDLS v1.0 (draft — GCA data only)

v1.0 is used exclusively for GCA climate hazard data. Do NOT apply v1.0 to other datasets. Do NOT break the existing v0.3 setup.

### v1.0 Schema and Validation

- Schema: `schema/rdls_schema_v1.0.json`
- Template: `schema/rdls_template_v1.0.json` (annotated with type, requirement level, cross-field rules)
- Validator: `schema/validate_v1.0.py` (three-layer: JSON Schema → codelist CSV → semantic cross-field)
- Codelists: local clone at `C:\Users\benny\OneDrive\Documents\Github\rdl-standard\schema\codelists` (`closed/` and `open/` subdirs)

### v1.0 JSON Metadata Rules

**Structure**
- Every JSON MUST start with `{"datasets": [{"id": ...}]}` wrapper — never unwrapped
- If a field has no confirmed value, SKIP it — do not add placeholders or invented values
- Do not create fields outside the schema/template — fields are fixed
- Only use values sourced from: data review, report, data files, or codelists
- For `details` field: content from data review/report NOT already in `description`

**Entity fields** (publisher, contact_point, creator, attributions.entity)
- `name` always REQUIRED
- At least one of `email` or `url` REQUIRED (schema `anyOf`)
- Do not invent email addresses

**Source fields** (lineage.sources)
- Schema field is `name`, NOT `title` — v0.3 uses `title`, v1.0 uses `name`
- Optional enrichment: `type` (dataset/model), `risk_data_type`, `used_in`, `license`

### v1.0 Key Differences from v0.3

| Feature | v0.3 | v1.0 |
|---------|------|------|
| `publisher/creator/contact_point` | Inside `attributions` array | Separate required top-level fields |
| `project` | Object `{name, url}` | Simple string |
| `sources` | Top-level `sources[]` | Inside `lineage.sources[]` |
| `license` | String codelist code | IRI string (URL) |
| `data_format` (resources) | Closed codelist | Replaced by `media_type` + `format` |
| `hazard_type` codelist | 11 types | +`convective_storm`, `dust_sand_storm`, `erosion`, `pest_infestation`, `sea_level_rise` |
| `process_type` codelist | No lightning | +`lightning`, `thunderstorm`, `hail`, `glacial_lake_outburst`, `coastal_erosion`, `soil_erosion`, `subsidence_uplift` |
| `occurrence` types | `probabilistic` only | +`empirical`, `deterministic` with `index_criteria` |
| `climate` on resources | Not available | `{model, scenario, percentile}` |
| `baseline_period` | Not available | Period object on resources |
| `spatial_aggregation` | Not available | Free text on resources |

### v1.0 Codelist Rules

**Closed** (must match exactly): `hazard_type`, `process_type`, `climate_scenario`, `analysis_type`, `frequency_distribution`, `risk_data_type`, `spatial_scale`, `geometry_type`, `data_calculation_type`, `source_type`

**Open** (standard preferred, custom allowed): `imt_*.csv`, `IMT.csv`, `unit_*.csv`, `roles.csv`, `license.csv`, `media_type.csv`, `location_gazetteers.csv`, `classification_scheme.csv`

**IMT codes**: Format is `metric:unit` (e.g., `AirTemp:C`, `PGWS:m/s`, `pptn24:mm`, `HD:-`). Master `IMT.csv` has `Hazard` column linking codes to types, plus `universal` entries. Per-type files `imt_[type].csv` have type-specific codes. Validator checks both combined.

**Climate scenario**: Single string enum — cannot hold multiple scenarios. For multi-scenario resources: omit `scenario`, use `model` and `description`. Only set when resource covers exactly one scenario.

### v1.0 Cross-Field Rules (8 rules)

1. **type → process**: Process must match hazard type (full mappings in template)
2. **type → IMT**: IMT codelist switches per type, all open
3. **quantity_kind → unit**: Unit codelist per quantity_kind. Currency is CLOSED (ISO 4217)
4. **scale → countries**: global=none, regional=min 2, national/sub-national/urban=min 1
5. **analysis_type → occurrence**: probabilistic/empirical/deterministic must match. Do NOT mix within one event_set — split into separate event_sets
6. **Entity contact**: name + (email or url) required
7. **risk_data_type → sections**: hazard/exposure/vulnerability/loss sections match risk_data_type
8. **climate.scenario → baseline_period**: scenario present → baseline_period expected

### v1.0 Event Set Design

- Split by analysis type: deterministic (means), probabilistic (return periods), empirical (observations) in separate event_sets
- `occurrence_range`: only for `analysis_type: "probabilistic"` — schema guidance
- `event_count`: only when events array is NOT populated
- Mean values: use deterministic event_set with `index_criteria`, or probabilistic with `return_period: 1`

### v1.0 Common Pitfalls

| Issue | Fix |
|-------|-----|
| `sources[].title` instead of `name` | v1.0 uses `name`, not `title` |
| `climate.scenario` with multiple RCPs | Omit scenario, describe in `description` |
| Custom IMT like `deg_C` instead of `AirTemp:C` | Use standard codelist codes from IMT.csv |
| Mixed occurrence types in one event_set | Split into separate event_sets per analysis_type |
| `occurrence_range` on deterministic sets | Remove — only for probabilistic |
| Missing `{"datasets": [...]}` wrapper | Always wrap |

## When modifying code

- Check `configs/rdls_schema.yaml` for valid codelist values before adding patterns
- New extraction patterns → `configs/signal_dictionary.yaml`, not Python code
- New review patterns (HEVL signals, model software, naming) → `configs/review_knowledge.yaml`, not Python code
- New format aliases → `configs/format_mapping.yaml`
- New org abbreviations → `configs/naming.yaml` under org_abbreviations
- Test constraint validity against tables in rdls_defaults.yaml
- Run `validate_record()` after any changes to record structure
- Preserve cascade tiering - Tier 2/3 must not introduce values without Tier 1 evidence
- Use `SchemaContext.fuzzy_codelist_fix()` for auto-correction, not manual string matching
