# Configuration Reference

All pipeline behavior is driven by YAML configuration files in `configs/`. No patterns, mappings, or thresholds are hardcoded in Python code. This document describes each config file, its structure, and how to modify it.

---

## Overview

| Config File | Purpose | Loaded By |
|------------|---------|-----------|
| `signal_dictionary.yaml` | HEVL extraction patterns (regex to RDLS codelist) | `extract_hazard`, `extract_exposure`, `extract_vulnloss` |
| `rdls_defaults.yaml` | Default mappings and constraint tables | `extract_*`, `integrate` |
| `rdls_schema.yaml` | RDLS codelists (hazard_type, process_type, etc.) | `schema` |
| `classification.yaml` | Scoring weights, thresholds, tag/keyword/org rules | `classify` |
| `naming.yaml` | Record ID format, component codes, org abbreviations | `naming` |
| `pipeline.yaml` | Runtime thresholds, output modes, distribution tiers | `validate` |
| `format_mapping.yaml` | Data format aliases, skip list, service URL patterns | `translate` |
| `license_mapping.yaml` | License string to RDLS license code | `translate` |
| `spatial.yaml` | Region-to-countries mapping, country name fixes | `spatial` |
| `llm_review.yaml` | LLM model, phase thresholds, cost guardrails | `llm_review` |
| `review_knowledge.yaml` | File inspection patterns, HEVL signal rules | `review`, `hdx_review` |
| `desinventar_mapping.yaml` | DesInventar event type to RDLS hazard/process | Notebook script |
| `country_bbox.yaml` | Country bounding boxes (generated) | Notebook scripts |
| `geonames_country_ids.yaml` | GeoNames country ID lookup (generated) | Notebook scripts |

---

## Core Extraction Configs

### `signal_dictionary.yaml`

Maps metadata text patterns to RDLS v0.3 codelist values. Used by the three HEVL extraction modules.

**Structure:**
```yaml
hazard_type:
  flood:
    patterns:
      - '\b(flood|flooding|inundation)\b'
      - '\b(fluvial|pluvial|riverine)\b'
    confidence: high    # high=0.9, medium=0.7, low=0.5

process_type:
  fluvial_flood:
    patterns:
      - '\b(river|fluvial)\s*(flood|inundation)\b'
    confidence: high

exposure_category:
  buildings:
    patterns:
      - '\b(building|structure|dwelling|house)\b'
    confidence: high

# Similar sections for: analysis_type, intensity_measure,
# vulnerability patterns, loss signal types, exclusion patterns
```

**Key sections:**
- `hazard_type` - 11 hazard types with regex patterns
- `process_type` - 30+ process types mapped to hazard parents
- `exposure_category` - 7 categories (buildings, population, infrastructure, etc.)
- `exposure_dimension` / `exposure_quantity_kind` - Metric patterns
- `vulnerability_function` / `loss_signal` - V and L extraction patterns
- `exclusion_patterns` - Negative patterns that reduce classification scores

**How to add a new pattern:** Add a regex entry under the appropriate codelist value. Patterns are case-insensitive. Use `\b` word boundaries to avoid false matches.

---

### `rdls_defaults.yaml`

Default values and constraint tables used when source metadata lacks specifics.

**Key sections:**

```yaml
# hazard_type → default process_type
hazard_process_defaults:
  flood: fluvial_flood
  earthquake: ground_motion
  tsunami: tsunami
  # ...

# hazard_type → default intensity_measure
default_intensity_measures:
  earthquake: "PGA:g"
  flood: "wd:m"
  # ...

# exposure_category → default (dimension, quantity_kind)
exposure_metric_defaults:
  buildings:
    dimension: structure
    quantity_kind: count
  population:
    dimension: people
    quantity_kind: count
  # ...

# Valid (category, dimension, quantity_kind) triplets
valid_triplets:
  buildings:
    - [structure, count]
    - [structure, currency]
    - [content, currency]
  # ...

# Loss signal type → default fields
loss_signal_defaults:
  deaths:
    impact_type: direct
    loss_type: ground_up
    # ...
```

**Constraint enforcement:** The `valid_triplets` table restricts which exposure metric combinations are allowed. If an extraction produces an invalid triplet, it falls back to the default for that category.

---

### `classification.yaml`

Scoring rules for classifying datasets into RDLS components.

**Structure:**
```yaml
scoring:
  keyword_hit_weight: 2        # Points per keyword match
  candidate_min_score: 5       # Minimum score to be RDLS candidate
  confidence_thresholds:
    high: 7                    # Score >= 7 = high confidence
    medium: 4                  # Score >= 4 = medium confidence

components:
  - hazard
  - exposure
  - vulnerability_proxy
  - loss_impact

tag_weights:
  hazard:
    flooding: 5
    drought: 5
    "earthquake-tsunami": 5
    # ...
  exposure:
    population: 5
    "affected-population": 5
    # ...

keyword_patterns:
  hazard:
    - '\b(flood|earthquake|tsunami|drought)\b'
    # ...

org_hints:
  hazard:
    - "pacific disaster center"
    # ...
```

**How scoring works:** Each dataset accumulates points from tag weights (exact tag matches), keyword patterns (regex against title/notes), and org hints (organization name matches). The total score determines `rdls_candidate` status and confidence level.

---

## Naming Config

### `naming.yaml`

Defines the structure of RDLS record IDs and filenames.

**Structure:**
```yaml
component_codes:
  single:
    hazard: hzd
    exposure: exp
    vulnerability: vln
    loss: lss
  letter:
    hazard: h
    exposure: e
    vulnerability: v
    loss: l
  order: [hazard, exposure, vulnerability, loss]

hazard_item_codes:
  flood: fl
  earthquake: eq
  # ...

org_abbreviations:
  "world bank": wb
  "united nations": un
  # ...

slug:
  max_length: 25
  stop_words: [the, and, for, from, with, ...]

country:
  max_countries: 5     # Beyond this, omit ISO3 from ID
```

**ID format:** `rdls_{types}-{iso3}{org}_{titleslug}`
- Single component uses 3-letter code (`rdls_hzd-...`)
- Multiple components use single-letter concat in HEVL order (`rdls_he-...`, `rdls_hevl-...`)

---

## Pipeline Config

### `pipeline.yaml`

Runtime settings for the transformation pipeline.

```yaml
output:
  mode: in_place          # "in_place" or "run_folder" (timestamped dirs)
  write_pretty_json: true
  clean_before_run: true
  cleanup_mode: replace   # replace | prompt | skip | abort

thresholds:
  high: 0.8               # Confidence >= 0.8 = high tier
  medium: 0.5             # Confidence >= 0.5 = medium tier

distribution:
  tiers:
    - name: high
      min_confidence: 0.8
      schema_valid: true
    - name: medium
      min_confidence: 0.5
      schema_valid: true
    - name: low
      min_confidence: 0.0
      schema_valid: true
    - name: invalid/high
      min_confidence: 0.8
      schema_valid: false
    # ...
```

---

## LLM Review Config

### `llm_review.yaml`

Settings for the 4-phase LLM-assisted classification pipeline.

```yaml
# Phase 1: Signal triage
triage:
  confident_score_min: 5           # Min max-component score to skip LLM
  max_components_for_confident: 2  # >2 active components = borderline
  validation_sample_pct: 0.05     # 5% of confident sent to LLM for cross-check

# Phase 2: CKAN column enrichment
ckan:
  base_url: "https://data.humdata.org/api/3/action"
  delay_seconds: 0.5              # Rate limiting (0.1s with API key)
  cache_dir: "output/column_cache"
  max_resources_per_dataset: 10

# Phase 3: LLM classification
llm:
  model: "claude-haiku-4-5-20251001"
  temperature: 0.0                # Deterministic
  max_tokens: 400
  max_concurrent: 2
  max_cost_usd: 15.0             # Cost guardrail
  cost_per_mtok_input: 1.00      # Haiku 4.5 pricing
  cost_per_mtok_output: 5.00
  cache_dir: "output/llm_review/cache"

# Phase 4: Merge strategy
merge:
  llm_overrides_signals: true     # LLM wins over regex when they disagree
  disagreement_confidence_min: 0.7 # LLM must be >0.7 to override

# Prompt truncation limits
prompt:
  description_max_chars: 500
  methodology_max_chars: 300
  max_resources_shown: 20
  max_columns_shown: 50
```

**Cost guardrail:** If cumulative LLM cost exceeds `max_cost_usd`, Phase 3 stops and reports remaining records as unprocessed.

---

## Source-Specific Configs

### `format_mapping.yaml`

Maps source data format names to RDLS `data_format` values.

**Key sections:**
- `format_aliases` - Source format string to RDLS format (e.g., `"GEOTIFF"` to `"geotiff"`, `"Shapefile"` to `"shapefile"`)
- `skip_formats` - Formats to exclude from resources (e.g., preview images, web apps)
- `service_url_patterns` - Regex patterns to detect WMS/WFS/OGC services and assign format + access modality
- `service_formats` - Direct service format mapping (e.g., `WMS` to `(wms, ogc_api)`)
- `zip_inner_formats` - Formats expected inside ZIP archives

### `license_mapping.yaml`

Maps source license strings to RDLS license codes using two tiers:
1. **Pattern matching** - Regex patterns tried in order
2. **Dictionary fallback** - Exact string lookup

### `spatial.yaml`

- `region_to_countries` - Maps region names (e.g., "East Africa") to lists of ISO3 codes
- `country_name_fixes` - Maps non-standard country names to correct ISO3 codes (e.g., "Viet Nam" to "VNM")
- `non_country_groups` - Group names that are not countries (e.g., "World", "Global")

---

## Setup Configs (Generated)

These files are generated by one-time setup scripts and should not be edited manually.

### `country_bbox.yaml`

Generated by `notebooks/rdls_nismod_00a_generate_country_bbox.py`. Maps ISO3 codes to bounding boxes `[min_lon, min_lat, max_lon, max_lat]`.

### `geonames_country_ids.yaml`

Generated by `notebooks/rdls_nismod_00b_generate_geonames_lookup.py`. Maps ISO3 codes to GeoNames country IDs and names.

### `desinventar_mapping.yaml`

Maps DesInventar event type codes to RDLS hazard_type and process_type values. Used by the DesInventar notebook script.

---

## Config Loading Pattern

All configs are loaded via `src/utils.load_yaml()` and cached at the module level. The standard pattern in `src/` modules:

```python
def load_my_config(yaml_path):
    """Load config from YAML file."""
    cfg = load_yaml(yaml_path)
    # Process/normalize config values
    return processed_config
```

Configs are passed to functions as arguments, never imported as global state. This keeps modules testable and source-independent.


---

# Detailed config sections (complementary)

> Merged from the former .claude/configs-detail-reference.md

# RDLS Config Detail Reference

Detailed config sections not covered in the main CLAUDE.md. Complements the config files table.

## format_mapping.yaml - Extended details

### service_url_patterns (URL → format + access_modality)
| URL pattern | data_format | access_modality |
|---|---|---|
| `arcgis.com/.*/rest/services/` | GeoJSON | REST |
| `/rest/services/.*(?:Feature\|Map)Server` | GeoJSON | REST |
| `geoserver.*(?:/wms\|/wfs\|/ows)` | GeoJSON | WFS |
| `/wms\b` | XML | WMS |
| `/wfs\b` | GeoJSON | WFS |
| `/wcs\b` | GeoTIFF | WCS |

### zip_inner_formats (ZIP contents → format inference)
| File extension inside ZIP | Inferred data_format |
|---|---|
| `.shp` | Shapefile |
| `.geojson` | GeoJSON |
| `.gpkg` | GeoPackage |
| `.tif` | GeoTIFF |
| `.csv` | CSV |
| `.kml` | KML |
| `.gdb` | File Geodatabase |
| `.nc` | NetCDF |

### service_formats (source format string → format + modality)
| Source format | data_format | access_modality |
|---|---|---|
| GEOSERVICE | GeoJSON | REST |
| API | JSON | API |
| WEB APP | CSV | download_page |

### skip_formats (non-data, excluded from resources)
HTML, PNG, JPEG, JPG, GIF, EMF, SVG, RAR, QGIS, ESRI ARCMAP PROJECT FILE, MXD, APR, STYLE, SLD

## spatial.yaml - Region mappings

### region_to_countries (31 regions → ISO3 codes)

**Africa regions:**
- East Africa (18): BDI, COM, DJI, ERI, ETH, KEN, MDG, MWI, MUS, MOZ, RWA, SYC, SOM, SSD, TZA, UGA, ZMB, ZWE
- West Africa (16): BEN, BFA, CPV, CIV, GMB, GHA, GIN, GNB, LBR, MLI, MRT, NER, NGA, SEN, SLE, TGO
- North Africa (6): DZA, EGY, LBY, MAR, SDN, TUN
- Central Africa (8): CMR, CAF, TCD, COG, COD, GNQ, GAB, STP
- Southern Africa (8): BWA, SWZ, LSO, MOZ, NAM, ZAF, ZMB, ZWE
- Horn of Africa (4): DJI, ERI, ETH, SOM
- Sahel (9): BFA, TCD, MLI, MRT, NER, SEN, GMB, NGA, CMR

**Asia regions:**
- Southeast Asia (10): BRN, KHM, IDN, LAO, MYS, MMR, PHL, SGP, THA, VNM
- South Asia (7): AFG, BGD, BTN, IND, MDV, NPL, PAK, LKA
- Central Asia (5): KAZ, KGZ, TJK, TKM, UZB
- East Asia (6): CHN, JPN, MNG, PRK, KOR, TWN

**Europe regions:**
- Eastern, Western, Northern, Southern Europe (47 total)

**Americas regions:**
- Caribbean (8), Central America (7), South America (13), Latin America (18), North America (3)

**Other:**
- Middle East (15), Oceania (11), Pacific (11)

## desinventar_mapping.yaml

### event_type_mapping (DesInventar → RDLS hazard+process)

**High confidence (11):**
| DesInventar event | hazard_type | process_type |
|---|---|---|
| FLOOD | flood | fluvial_flood |
| FLASH FLOOD | flood | pluvial_flood |
| LANDSLIDE | landslide | landslide_general |
| EARTHQUAKE | earthquake | ground_motion |
| DROUGHT | drought | meteorological_drought |
| FOREST FIRE | wildfire | wildfire |
| AVALANCHE | landslide | snow_avalanche |
| SURGE | coastal_flood | storm_surge |
| HEAT WAVE | extreme_temperature | extreme_heat |
| COLD WAVE | extreme_temperature | extreme_cold |
| TSUNAMI | tsunami | tsunami |

**Medium confidence (17):**
RAINS→flood, STORM→strong_wind/extratropical_cyclone, WINDSTORM→strong_wind/extratropical_cyclone, CYCLONE/TYPHOON/HURRICANE→strong_wind/tropical_cyclone, VOLCANIC ERUPTION→volcanic/ashfall, MUDSLIDE→landslide/landslide_mudflow, ROCKSLIDE→landslide/landslide_rockslide, COASTAL FLOOD→coastal_flood, and others.

**Not mappable (20):** EPIDEMIC, PLAGUE, ACCIDENT, CONTAMINATION, EXPLOSION, FOG, BIOLOGICAL, FAMINE, etc. (set to null)

### loss_column_mapping (15 DesInventar columns)
| Column | impact_metric | asset_category | asset_dimension | quantity_kind |
|---|---|---|---|---|
| Deaths | casualty_count | population | population | count |
| Injured | casualty_count | population | population | count |
| Missing | casualty_count | population | population | count |
| Affected | exposure_to_hazard | population | population | count |
| Victims | exposure_to_hazard | population | population | count |
| Evacuated | displaced_count | population | population | count |
| Relocated | displaced_count | population | population | count |
| Houses Destroyed | asset_loss | buildings | structure | count |
| Houses Damaged | damage_ratio | buildings | structure | count |
| Losses $USD | economic_loss_value | buildings | content | monetary |
| Losses $Local | economic_loss_value | buildings | content | monetary |
| Damages in crops Ha. | asset_loss | agriculture | product | area |
| Lost Cattle | asset_loss | agriculture | product | count |
| Education centers | asset_loss | infrastructure | structure | count |
| Hospitals | asset_loss | infrastructure | structure | count |

## classification.yaml - org_hints

Organizations with automatic component score boosts:
| Organization | Component | Boost |
|---|---|---|
| Food and Agriculture Organization | vulnerability_proxy | +3 |
| The DHS Program | vulnerability_proxy | +4 |
| UNICEF | vulnerability_proxy | +3 |
| World Bank Group | vulnerability_proxy | +2 |

## sources/hdx.yaml - Source adapter config

### field_paths (source JSON → common field mapping)
```
id → "id"
name → "name"
title → "title"
notes → "notes"
methodology → "methodology"
organization → "organization.title"     (nested path)
org_name → "organization.name"          (nested path)
org_description → "organization.description"
license_title → "license_title"
license_id → "license_id"
license_url → "license_url"
groups → "groups"                       (list of {name, title})
tags → "tags"                           (list of {name})
resources → "resources"                 (list of resource dicts)
dataset_date → "dataset_date"
data_update_frequency → "data_update_frequency"
maintainer → "maintainer"
dataset_source → "dataset_source"
url → "url"
```

### osm_detection (OpenStreetMap dataset filtering)
- **supporting_evidence_threshold**: 2 (requires 2+ markers)
- **fast_markers** (6): "openstreetmap contributors", "odbl", "hotosm", etc.
- **url_markers** (5): openstreetmap.org, hotosm.org, export.hotosm.org, etc.
- **org_markers** (3): humanitarian openstreetmap, hotosm, openstreetmap
- **title_markers** (3): openstreetmap export, (openstreetmap export), openstreetmap
- **notes_markers** (3): openstreetmap, wiki.openstreetmap.org, osm

## llm_review.yaml - LLM-Assisted HEVL Review

Used by `src/llm_review.py` (`run_llm_review()`).

### triage (Phase 1 - signal-based bucketing)
| Key | Default | Purpose |
|-----|---------|---------|
| `confident_score_min` | 5 | Min max-component score to skip LLM |
| `max_components_for_confident` | 2 | >2 active components = borderline (send to LLM) |
| `validation_sample_pct` | 0.05 | 5% of confident records cross-checked by LLM |

### ckan (Phase 2 - column header enrichment)
| Key | Default | Purpose |
|-----|---------|---------|
| `base_url` | `https://data.humdata.org/api/3/action` | CKAN API base |
| `delay_seconds` | 0.5 | Rate limit (0.5s without API key, 0.1s with key) |
| `timeout_seconds` | 15 | HTTP request timeout |
| `cache_dir` | `output/column_cache` | Disk cache for column headers |
| `max_resources_per_dataset` | 10 | Max resources to fetch per dataset |

### llm (Phase 3 - LLM classification)
| Key | Default | Purpose |
|-----|---------|---------|
| `model` | `claude-haiku-4-5-20251001` | Anthropic model ID |
| `temperature` | 0.0 | Deterministic output |
| `max_tokens` | 400 | Max response tokens |
| `max_concurrent` | 2 | Concurrent LLM requests |
| `max_retries` | 3 | Retry on failure |
| `timeout_seconds` | 30 | Per-request timeout |
| `cache_dir` | `output/llm_review/cache` | Disk cache for LLM responses |
| `max_cost_usd` | 15.0 | Cost guardrail - stops if exceeded |
| `cost_per_mtok_input` | 1.00 | Haiku 4.5 input pricing ($/MTok) |
| `cost_per_mtok_output` | 5.00 | Haiku 4.5 output pricing ($/MTok) |

### merge (Phase 4 - conflict resolution)
| Key | Default | Purpose |
|-----|---------|---------|
| `llm_overrides_signals` | true | LLM wins when disagreeing with regex signals |
| `disagreement_confidence_min` | 0.7 | LLM must be ≥0.7 to override |

### prompt (context limits for LLM input)
| Key | Default | Purpose |
|-----|---------|---------|
| `description_max_chars` | 500 | Truncate description in prompt |
| `methodology_max_chars` | 300 | Truncate methodology in prompt |
| `max_resources_shown` | 20 | Max resources in prompt |
| `max_columns_shown` | 50 | Max column names in prompt |
