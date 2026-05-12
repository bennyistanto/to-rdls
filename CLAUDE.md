# RDLS Project - Claude Code Instructions

## Project overview

A source-independent toolkit for transforming dataset metadata from any data catalog into RDLS (Risk Data Library Standard) v0.3 schema. Sources: HDX, GeoNode, CKAN, World Bank Data Catalog, and others. Maintained under GFDRR/World Bank's Digital Earth team. License: MPL-2.0.

Pipeline: **Source Crawl → Filter → Classify → Translate → HEVL Extract → Integrate → Validate → Distribute**

## Architecture

```
to-rdls/
├── src/                    # Python modules
│   ├── utils.py            # Text sanitization, file I/O, nested dict navigation
│   ├── codelists.py        # v1.0 codelist utilities (AUTHORITATIVE): load_codelists_v10(),
│   │                       #   normalise_unit(), normalise_source_type(), VALID_* sets.
│   │                       #   Loaded from rdl-standard/schema/codelists/ CSV files.
│   │                       #   Import: from src.codelists import normalise_unit
│   ├── schema.py           # JSON Schema loading, validation, SchemaContext
│   │                       #   Re-exports all of src/codelists.py for convenience
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
│   └── sources/
│       ├── hdx.py          # HDX/CKAN source adapter (reference implementation)
│       └── geonode.py      # GeoNode source adapter (implemented)
├── configs/                # 15 YAML config files (see below)
├── schema/                 # RDLS v0.3 JSON Schema + template
└── notebooks/              # Pipeline notebooks + generators
```

Data flow: `source adapter → extract_fields() → classify → translate → HEVL extract → integrate → validate_and_score → distribute_records`

Full dataclasses, extraction cascade details, and LLM pipeline → see `.claude/module-reference.md`

## Config files reference

| File | Purpose | Key contents |
|------|---------|-------------|
| `rdls_schema.yaml` | Codelists & required fields | All closed/open codelists, 195+ country codes |
| `signal_dictionary.yaml` | HEVL extraction patterns | 100+ regex patterns with confidence levels |
| `rdls_defaults.yaml` | Default values & constraints | process defaults, valid triplets, loss signal defaults |
| `classification.yaml` | Scoring rules | tag_weights, keyword_patterns, org_hints, thresholds |
| `naming.yaml` | ID generation | 265 org abbreviations, 249 ISO3 codes, component codes |
| `format_mapping.yaml` | Format aliases | 81 format aliases, 12 skip formats |
| `license_mapping.yaml` | License normalization | 33 license variants → RDLS codes |
| `spatial.yaml` | Geography | country name fixes, region→country maps |
| `pipeline.yaml` | Runtime settings | thresholds, output paths |
| `sources/geonode.yaml` | GeoNode adapter config | title_humanize, category_tag_map, skip_link_types |
| `sources/hdx.yaml` | HDX adapter config | rate limiting, field paths, OSM markers |
| `llm_review.yaml` | LLM review settings | phase thresholds, model, cost cap, rate limits |

## Schema

**v0.3** (all sources): `schema/rdls_schema_v0.3.json` + `schema/rdls_template_v0.3.json` — **consult template before writing any field**

**v1.0** (GCA climate data only):
- Schema (authoritative): `C:\Users\benny\OneDrive\Documents\Github\rdl-standard\schema\rdls_schema.json` — regularly synced; prefer over local snapshot
- Template: `schema/rdls_template_v1.0.json` — annotated, no template in rdl-standard
- Codelists: `C:\Users\benny\OneDrive\Documents\Github\rdl-standard\schema\codelists\`

### Required dataset fields
`id`, `title`, `risk_data_type`, `attributions` (publisher+creator+contact_point), `spatial`, `license`, `resources` (id+title+description+data_format)

### Closed codelists (must match exactly)
- **risk_data_type**: hazard, exposure, vulnerability, loss
- **hazard_type**: coastal_flood, convective_storm, drought, earthquake, extreme_temperature, flood, landslide, strong_wind, tsunami, volcanic, wildfire
- **process_type**: 32 values (fluvial_flood, pluvial_flood, ground_motion, liquefaction, storm_surge, tropical_cyclone, tornado, agricultural_drought, etc.)
- **exposure_category**: agriculture, buildings, infrastructure, population, natural_environment, economic_indicator, development_index
- **analysis_type**: probabilistic, deterministic, empirical
- **impact_metric**: 21 values (damage_ratio, loss_ratio, casualty_count, economic_loss_value, etc.)
- **data_format**: 20+ values (GeoTIFF, NetCDF, CSV, GeoJSON, Shapefile, GeoPackage, etc.)
- **access_modality**: file_download, download_page, API, OGC_API, WMS, WFS, WCS, STAC, REST, dashboard

## Companion reference docs (.claude/)

| File | Contents |
|------|----------|
| `module-reference.md` | All modules: function signatures, dataclasses, HEVL cascade, LLM pipeline, GeoNode adapter |
| `schema-reference.md` | Full JSON Schema $defs + Layer 3 closed codelist validation table |
| `constraints-reference.md` | function_type_constraints, loss signal defaults, valid asset triplets, impact metric constraints |
| `naming-reference.md` | ID format, component codes, hazard/exposure item codes, slug rules, org shortname rules |
| `signals-reference.md` | Hazard/exposure signal patterns, exclusion patterns, tag weights, socioeconomic indicators |
| `configs-detail-reference.md` | Format mapping details, region→country mappings, DesInventar mappings, org_hints |
| `v1.0-reference.md` | Full RDLS v1.0 spec (GCA data only): differences, codelist rules, cross-field rules |

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
- tsunami → tsunami  |  wildfire → wildfire

### exposure_category → valid (dimension, quantity_kind) triplets
- agriculture: (product, area/monetary/count)  |  buildings: (structure, count/area/monetary), (content, monetary)
- infrastructure: (structure, count/length/monetary)  |  population: (population, count)
- natural_environment: (product, area)  |  economic_indicator: (index, monetary/count)  |  development_index: (index, count)

Full tables → `.claude/constraints-reference.md`

## Execution environment

```bash
# Always use this python — has GDAL, rasterio, fiona, geopandas, PyMuPDF, fastmcp
C:/Users/benny/miniforge3/envs/to-rdls/python.exe <script.py>
C:/Users/benny/miniforge3/envs/to-rdls/python.exe mcp_server.py
```

- **Never** use `conda run`, `python`, `py` — the `to-rdls` env is the only one with all dependencies
- **Never** use `python -c "multiline"` — write to a temp `.py` file first
- Working directory: `C:/Users/benny/OneDrive/Documents/Github/to-rdls`
- Test baseline: 12 datasets (TC-chattogram, TC-coxsbazar, TC-daressalaam, TC-istanbul, TC-khokana, TC-nablus, TC-nairobi, TC-nakuru, TC-quito, TC-rapti, PhuQuoc, SLE-Freetown) — must produce identical output after any config/code change
- `temp/` scripts in subdirs: path is `parent.parent.parent` not `parent.parent` to reach project root

## Coding conventions

- Python 3.10+, type hints on all functions, dataclasses for structured data
- Config-driven: all patterns, mappings, thresholds in YAML — never hardcoded
- Text: `sanitize_text()`, `norm_str()` (NFKD+lowercase), `slugify()`
- File I/O: `load_json/yaml()`, `write_json()` (atomic via temp+rename), `append_jsonl()`
- Nested dict ops: `navigate_path()`, `set_at_path()`, `remove_at_path()`
- IDs: `rdls_{type}-{iso3}{org}_{titleslug}` with collision suffix `__{uuid8}`

## CRITICAL: Schema field name mapping (v0.3)

**NEVER guess field names. ALWAYS use this table.** The JSON schema `$defs` use different property names than the concept names. Getting these wrong causes silent validation failures.

### $defs/Hazard (inside event_sets[].hazards[] AND events[].hazard)
| Schema property | NOT this | Required |
|----------------|----------|----------|
| `id` | — | YES |
| `type` | ~~hazard_type~~ | YES |
| `hazard_process` | ~~process_type~~ | YES |
| `intensity_measure` | — | no |

### $defs/Event_set (inside hazard.event_sets[])
| Schema property | Required |
|----------------|----------|
| `id` | YES |
| `hazards` | YES (array of Hazard objects) |
| `analysis_type` | YES (probabilistic/deterministic/empirical) |

### $defs/Event (inside event_sets[].events[])
| Schema property | Required |
|----------------|----------|
| `id` | YES |
| `calculation_method` | YES (inferred/observed/simulated) |
| `hazard` | YES (full Hazard object: {id, type, hazard_process}) |
| `occurrence` | YES (minProperties:1, needs sub-object) |

### $defs/Losses (inside loss.losses[])
| Schema property | NOT this | Required |
|----------------|----------|----------|
| `id`, `hazard_type`, `asset_category`, `asset_dimension` | — | YES |
| `impact_and_losses` | — | YES (object, see below) |

### impact_and_losses (inside each loss entry)
| Schema property | NOT this | Required |
|----------------|----------|----------|
| `impact_type` | ~~type~~ | YES (direct/indirect/total) |
| `impact_modelling` | — | YES (inferred/observed/simulated) |
| `impact_metric` | ~~metric~~ | YES |
| `quantity_kind`, `loss_type`, `loss_approach`, `loss_frequency_type` | — | YES |

### $defs/Exposure_item (inside exposure[])
`id` (required), `category` (required), `metrics` (no)

### Validation architecture
- `schema.py:validate_record()` validates a **single unwrapped record** — NEVER wrap before validating
- `notebooks/rdls_validate_semantic.py` — Layer 2 semantic validation (open codelists, cross-field)
- `validate_qa.py:validate_and_score()` runs validate_record() then applies 5-pass autofix

## Common schema pitfalls

| Issue | Schema rule | Fix |
|-------|-------------|-----|
| `referenced_by.author_names: []` | minItems: 1 | Remove empty `author_names` and `doi: ""` |
| Empty arrays (`losses: []`, `hazards: []`, etc.) | minItems: 1 | Remove empty arrays — optional fields must be absent |
| `resources: []` | minItems: 1 | Record without resources → move to non-RDLS |
| `occurrence: {}` | minProperties: 1 | Known schema gap — team will revise |
| Hazard entry uses `hazard_type` | Schema property is `type` | Use `type` in $defs/Hazard |
| Loss `impact_and_losses` uses `type`/`metric` | Schema uses `impact_type`/`impact_metric` | Use prefixed names |

### Common value mistakes
- `risk_data_type: "hazard"` → must be array: `["hazard"]`
- `spatial.countries: [{"iso_3": "COK"}]` → must be plain strings: `["COK"]`
- `loss_frequency_type: "event"` or `"average_annual"` → not valid; use `deterministic`/`probabilistic`/`empirical`
- `vulnerability.functions: [...]` → `functions` is an object, not array: `{"vulnerability": [...]}`
- `impact_modelling: "empirical"` → not valid; use `inferred`, `observed`, or `simulated`
- Entity missing `url` or `email` → schema `anyOf` requires at least one
- Resources missing `download_url` or `access_url` → schema `anyOf` requires at least one

## ⛔ CRITICAL: No non-schema fields in RDLS records

**NEVER add fields to RDLS record output that are not in the JSON Schema.** This includes `_source`, `overall_confidence`, any `_`-prefixed fields, any pipeline-internal state.

`build_*_block()` functions must return **only** schema-defined fields. Internal state (like `overall_confidence` on dataclasses) stays on the dataclass — NEVER in dict output.

## RDLS v1.0

v1.0 is used for **all published and converted datasets** (Tomorrow Cities, NISMOD, MDG, Mombasa, and future). Do NOT break the v0.3 pipeline (still used for source ingestion).

Key differences: separate top-level `publisher`/`creator`/`contact_point`, `lineage.sources[].name` (not `title`), `media_type`+`format` (not `data_format`), `measurement.unit` codelist codes, `climate` field on resources.

**Unit values**: always use exact codelist codes from `src/codelists.py:normalise_unit()`.
Common codes: `square_metre`, `hectare`, `metre`, `kilometre`, `kilogram`, `kilowatt_hour`.
NEVER use abbreviations (`m2`, `ha`, `m`) directly — they are not codelist codes.

Full v1.0 spec → `.claude/v1.0-reference.md`

### CRITICAL: Post-conversion enrichment (run after EVERY v0.3 -> v1.0 conversion)

```bash
C:/Users/benny/miniforge3/envs/to-rdls/python.exe scripts/post_convert_enrich.py "output/<collection>/**/*.json"
```

Applies automatically: `unit=count` for qk=count, GED4ALL URI fix, remove invalid `scheme="Custom"`,
remove Commercial licenses, `intensity_measure=wd:m` for flood losses, GED4ALL scheme restoration.

Then manually fix what the script flags: `asset_type.id` (must be per-item, not scheme name),
`title`, `description`, `scheme` (GED4ALL where fits, absent otherwise), `uri`.

Full checklist with media_type rules, backup convention, validation steps:
`.claude/v1.0-reference.md` section "Post-conversion checklist"

## CRITICAL: Metadata record validation requirement

**Every RDLS record MUST pass ALL THREE layers before it is done.**

### Template reference — consult before writing any field
- `schema/rdls_template_v0.3.json` — v0.3 (all pipeline sources)
- `schema/rdls_template_v1.0.json` — v1.0 (GCA climate data only)

### Layer 1 — JSON Schema
```python
from src.schema import validate_record, load_json as load_schema_json
SCHEMA = load_schema_json("schema/rdls_schema_v0.3.json")
is_valid, errors = validate_record(record, SCHEMA)
assert is_valid, errors
```

### Layer 2 — Semantic validation
```bash
C:/Users/benny/miniforge3/envs/to-rdls/python.exe notebooks/rdls_validate_semantic.py path/to/record.json schema/rdls_schema_v0.3.json
```
Required output: `PASSED` (zero errors). Every record must include:
```python
"links": [{"href": "https://docs.riskdatalibrary.org/en/0__3__0/rdls_schema.json", "rel": "describedby"}]
```

### Layer 3 — Closed codelist check
After schema validation, verify closed-codelist fields are valid (schema may accept strings not in the codelist). Full table → `.claude/schema-reference.md`

### One record per file rule
Each record saved as `{record_id}.json` wrapped with `{"datasets": [{...}]}`. Never merge into one file as primary output.

## When modifying code

- New extraction patterns → `configs/signal_dictionary.yaml`, not Python code
- New review patterns → `configs/review_knowledge.yaml`, not Python code
- New format aliases → `configs/format_mapping.yaml`
- New org abbreviations → `configs/naming.yaml`
- Run `validate_record()` after any changes to record structure
- Preserve cascade tiering — Tier 2/3 must not introduce values without Tier 1 evidence

### CRITICAL: Do NOT break working pipeline code
- **NEVER change field names in `build_hazard_block()`, `build_loss_block()`, `build_exposure_block()`, or `build_vulnerability_block()`** without verifying against the schema field name mapping table. Wrong field names = silent validation failures across ALL sources.
- **NEVER modify `schema.py:validate_record()` validation logic.**
- **Core modules are source-independent** (`extract_*.py`, `translate.py`, `integrate.py`, `validate_qa.py`, `schema.py`) — changes affect HDX, GeoNode, and all other sources. Test against baseline 12 datasets before and after.
- **Source adapters are the ONLY source-specific code** — new source quirks go in the adapter's `extract_*_fields()`, not in core pipeline modules.
