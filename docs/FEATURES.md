# Features

to-rdls is a modular toolkit for transforming metadata from multiple sources into RDLS v0.3 JSON records. This document describes what the toolkit can do today.

---

## Multi-Source Metadata Transformation

The core pipeline is source-independent. Each data source has a pluggable adapter that normalizes metadata into a common field dictionary, after which the same classification, translation, extraction, validation, and naming modules process every record identically.

### Supported Sources

| Source | Adapter | Status | Scale |
|--------|---------|--------|-------|
| **HDX (Humanitarian Data Exchange)** | `src/sources/hdx.py` | Complete | 26,246 datasets crawled, 13,053 RDLS candidates produced |
| **DesInventar (UNDRR)** | Notebook script | Complete | Loss records from national disaster loss databases (16 countries) |
| **NISMOD ICRA** | Notebook script | Complete | Template-based hazard+exposure records from irv-datapkg (per-country Zenodo DOI) |
| **GeoNode** | `src/sources/geonode.py` | Stub | Interface defined, implementation pending |

### Adding a New Source

Adding support for a new metadata source requires:

1. **Source adapter** (`src/sources/your_source.py`) with `extract_fields()` returning the common field dictionary (title, notes, tags, organization, groups, resources, etc.)
2. **Source config** (`configs/sources/your_source.yaml`) with API endpoints, format overrides, and field path mappings
3. No changes needed to the rest of the pipeline

---

## HEVL Extraction Pipeline

The pipeline extracts four RDLS component types from metadata text fields using regex pattern matching against a curated Signal Dictionary (`configs/signal_dictionary.yaml`).

### Components

| Component | Module | Cascade | Key Outputs |
|-----------|--------|---------|-------------|
| **Hazard** | `extract_hazard.py` | 2-tier | hazard_type, process_type, analysis_type, return_periods, intensity_measures |
| **Exposure** | `extract_exposure.py` | 3-tier | category, metric (dimension + quantity_kind), taxonomy_hint, currency |
| **Vulnerability** | `extract_vulnloss.py` | Pattern | function_type, approach, impact_metric, socio-economic indicators |
| **Loss** | `extract_vulnloss.py` | Pattern | signal type, hazard, asset, impact, frequency, currency |

### Cascade Logic

Extraction uses tiered authority levels to control false positives:

- **Tier 1** (title, name, tags, resources): Highest authority. Can **introduce** new component types.
- **Tier 2** (resources for exposure; notes/methodology for hazard): Medium authority. Can **corroborate** Tier 1 findings, or serve as fallback if Tier 1 found nothing.
- **Tier 3** (notes, methodology for exposure): Lowest authority. Fallback only.

### Constraint Enforcement

- `VALID_TRIPLETS` in `configs/rdls_defaults.yaml` restricts which hazard_type + process_type + analysis_type combinations are allowed
- Component dependency rules: vulnerability and loss require either hazard or exposure to be present
- Auto-repair (M5): standalone vulnerability/loss datasets automatically receive an exposure component
- No fabricated values: every output traces to a matched text signal or a schema default

---

## LLM-Assisted Classification

The pipeline includes a 4-phase LLM review system (`src/llm_review.py`) that solves the content-blind over-classification problem (Problem 7).

### The Problem

The regex pipeline classifies based on metadata keywords only. When a title says "Earthquake Health Facility Status," the pipeline infers hazard (earthquake) + exposure (infrastructure). But the actual data is a post-event damage assessment (loss data), not seismic measurements. This affected 2,313 records (82% of all records with a hazard component).

### 4-Phase Solution

| Phase | What It Does | Cost |
|-------|-------------|------|
| **Phase 1: Signal Triage** | Re-score all records using existing regex. Bucket into confident / borderline / no_signal. | Free (cached regex) |
| **Phase 2: Column Enrichment** | Fetch actual column headers from CKAN API via `src/ckan_columns.py`. Add as LLM context. | Free (cached, ~48h first build) |
| **Phase 3: LLM Classification** | Send borderline + no_signal records to Claude Haiku 4.5 with strict RDLS component definitions. | ~$22 for 12,594 records |
| **Phase 4: Merge and Rename** | Apply LLM decisions, rebuild record IDs when risk_data_type changes, validate, distribute. | Free |

### Production Results

| Metric | Count |
|--------|------:|
| Records processed | 12,594 |
| Reclassified by LLM | 3,443 (27.3%) |
| Non-disaster datasets separated | 4,103 (health, education, admin boundaries) |
| RDLS-relevant after review | 8,822 |
| Schema-valid (publication-ready) | 3,998 |
| Schema-invalid (occurrence:{} gap) | 4,493 |
| Total cost | $21.98 |
| Processing time | ~22 minutes |

### Key Capabilities

- Distinguishes "data ABOUT earthquakes" from "data CONTAINING earthquake measurements"
- Never fabricates intensity measures (no more synthetic `PGA:g`, `SPI:-`)
- Provides per-component reasoning (auditable explanations)
- LLM cache prevents re-classification costs on re-runs ($0 for cached records)
- ID rebuild: when LLM changes risk_data_type, the record ID and filename are automatically updated (e.g., `rdls_hevl-*` becomes `rdls_lss-*`)

---

## Schema Validation and Auto-Fix

The validation engine (`src/validate_qa.py`) validates records against the RDLS v0.3 JSON Schema and applies a 5-pass auto-fix engine before scoring confidence and distributing to quality tiers.

### 5-Pass Auto-Fix Engine

| Pass | Action |
|------|--------|
| 1. Enum fixes | Fuzzy-match invalid codelist values to closest valid entry (case-insensitive, substring, Levenshtein) |
| 2. Required fields | Infer missing required fields from context (e.g., add `contact_point` role from publisher entity) |
| 3. Type coercion | Convert mistyped values (string numbers to integers, single items to arrays) |
| 4. Structural cleanup | Remove empty strings, empty arrays, empty objects from non-required fields |
| 5. Orphaned removal | Remove fields not defined in the schema (`additionalProperties: false`) |

### Business Rules (Beyond JSON Schema)

- Attribution roles: `publisher`, `creator`, `contact_point` must all be present
- Resources: each must have at least `download_url` or `access_url`
- Entity validation: each entity must have at least `email` or `url`
- Schema link: `links[]` must contain a `describedby` entry pointing to the RDLS schema
- risk_data_type consistency: declared types must match actual HEVL blocks present

### Confidence Scoring

Records receive a composite confidence score (0.0-1.0) based on completeness, consistency, and signal strength. Records are distributed to quality tiers:

- **high**: Schema-valid, all business rules pass, confidence above threshold
- **medium**: Schema-valid but missing some business rules
- **invalid**: Schema validation fails

---

## Data Inventory and Review

### Inventory (`src/inventory.py`)

Generates structured inventories of data delivery folders or ZIP files. No external dependencies (stdlib only).

- Recursive folder traversal with configurable depth and exclusion patterns
- ZIP content listing without extraction
- Output formats: Markdown tree, CSV, JSON
- File metadata: size, format, MIME type, modification date, optional SHA256 checksums
- Runnable as CLI: `python -m src path/to/folder`

### Review (`src/review.py`)

Full automated data review for RDLS metadata creation. Inspects actual file content and classifies into HEVL components.

**Supported file formats:**

| Format | Inspection Details |
|--------|--------------------|
| GeoTIFF | CRS, bounds, band count, resolution, data type, statistics |
| Shapefile / GeoPackage / GeoJSON | CRS, bounds, feature count, geometry type, attribute schema |
| CSV / XLSX | Column headers, row count, HXL tags, data type inference |
| PDF | Page count, text extraction (first pages) |
| DOCX | Text extraction, paragraph count |
| NetCDF | Variables, dimensions, global attributes, CRS |

**Review pipeline:**
1. Scan and group files by naming patterns (scenarios, return periods, countries)
2. Inspect individual files (up to `max_inspect` limit)
3. Classify file groups into HEVL components using review knowledge base
4. Analyze gaps against RDLS schema requirements
5. Suggest dataset structure and naming

---

## Naming Convention

The naming module (`src/naming.py`) generates structured RDLS record IDs and filenames in the format:

```
rdls_{types}-{iso3}{org}_{titleslug}
```

### Segments

| Segment | Rules |
|---------|-------|
| `types` | Single component: 3-letter code (`hzd`, `exp`, `vln`, `lss`). Multiple: single-letter concat in HEVL order (`he`, `hev`, `hevl`) |
| `iso3` | Lowercase ISO 3166-1 alpha-3 codes, concatenated without separator. Omitted for regional/global datasets exceeding `max_countries` |
| `org` | Organization abbreviation from `configs/naming.yaml` lookup, or auto-truncated |
| `titleslug` | Slugified title with country names, org names, and stop words removed (max 20 chars) |

### Collision Detection

`build_rdls_id_with_collision()` detects when two different datasets would produce the same ID and appends a UUID suffix to ensure uniqueness. `parse_rdls_id()` can decompose an existing ID back into its segments for ID rebuild after reclassification.

---

## MCP Server

The toolkit includes an MCP (Model Context Protocol) server (`mcp_server.py`) that exposes 5 tools for Claude-assisted workflows:

| Tool | Purpose |
|------|---------|
| `inventory_folder` | Scan a data folder or ZIP and return file listing with stats |
| `review_folder` | Full automated HEVL classification with gap analysis and dataset suggestions |
| `inspect_file` | Inspect a single file's metadata and content (supports `archive.zip::inner/path` syntax) |
| `validate_record` | Validate an RDLS JSON record against the v0.3 schema |
| `inspect_folder_for_llm` | Return raw inspection data WITHOUT classification, for Claude to do semantic HEVL classification using domain knowledge |

**Start command:**
```bash
conda run --no-banner -n to-rdls python mcp_server.py
```

The `inspect_folder_for_llm` tool is designed for a different workflow than `review_folder`: it provides structured file metadata (columns, CRS, naming patterns) without automated classification, allowing Claude to apply semantic understanding of RDLS, geospatial standards, and risk data to classify components.

---

## Config-Driven Design

All behavior is driven by 14 YAML configuration files in `configs/`. No patterns, mappings, or thresholds are hardcoded in Python. Changing extraction behavior, adding new format mappings, or adjusting scoring weights requires only YAML edits.

| Config | Controls |
|--------|----------|
| `signal_dictionary.yaml` | HEVL extraction patterns (regex → RDLS codelist values) |
| `rdls_defaults.yaml` | Constraint tables, default mappings, component dependency rules |
| `rdls_schema.yaml` | RDLS codelists (hazard_type, process_type, category, etc.) |
| `classification.yaml` | Tag weights, keyword patterns, org hints, scoring thresholds |
| `naming.yaml` | ID format, component codes, org abbreviations, stop words |
| `pipeline.yaml` | Runtime thresholds, output modes, distribution settings |
| `format_mapping.yaml` | Data format aliases, skip list, service URL patterns |
| `license_mapping.yaml` | License string to RDLS license code mapping |
| `spatial.yaml` | Region-to-countries mapping, country name fixes, non-country groups |
| `llm_review.yaml` | LLM model, phase thresholds, cost guardrails, merge strategy |
| `review_knowledge.yaml` | File inspection patterns, HEVL signal rules for review module |
| `desinventar_mapping.yaml` | DesInventar event type to RDLS hazard/process mapping |
| `country_bbox.yaml` | Country bounding boxes (generated by setup script) |
| `geonames_country_ids.yaml` | GeoNames country ID lookup (generated by setup script) |

See [CONFIG_REFERENCE.md](CONFIG_REFERENCE.md) for detailed documentation of each config file.
