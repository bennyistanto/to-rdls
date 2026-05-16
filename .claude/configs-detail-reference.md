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
