# RDLS Signal Dictionary Reference

Key patterns from `to-rdls/configs/signal_dictionary.yaml` used by HEVL extractors.

## Hazard type patterns (key patterns per type)

| hazard_type | key regex patterns | confidence |
|---|---|---|
| flood | `\b(flood\|flooding\|inundation)\b`, `\b(fluvial\|pluvial\|riverine)\b` | high |
| coastal_flood | `\b(coastal.?flood\|storm.?surge\|tidal.?flood)\b`, `\b(sea.?level.?rise\|slr)\b` | high |
| earthquake | `\b(earthquake\|seismic\|quake\|tremor)\b`, `\b(ground.?motion\|pga\|pgv\|psa)\b` | high |
| tsunami | `\b(tsunami\|tidal.?wave)\b`, `\b(run.?up\|wave.?height)\b` | high |
| landslide | `\b(landslide\|mudslide\|rockfall)\b`, `\b(debris.?flow\|mass.?movement)\b` | high |
| volcanic | `\b(volcan\|eruption\|lava)\b`, `\b(pyroclastic\|ash.?fall\|tephra)\b` | high |
| convective_storm | `\b(cyclone\|typhoon\|hurricane)\b`, `\b(tornado\|twister\|waterspout)\b` | high |
| strong_wind | `\b(wind.?gust\|gale)\b`, `\b(windstorm\|derecho)\b` | high |
| extreme_temperature | `\b(heat.?wave\|cold.?wave\|cold.?spell)\b`, `\b(extreme.?temperature\|thermal.?stress)\b` | high |
| drought | `\b(drought\|water.?scarcity\|aridity)\b`, `\b(dry.?spell\|precipitation.?deficit)\b` | high |
| wildfire | `\b(wildfire\|wild.?fire\|forest.?fire)\b`, `\b(bushfire\|brush.?fire\|grassfire)\b` | high |

## Exposure category patterns (key patterns per type)

| category | key regex patterns |
|---|---|
| buildings | `\b(building\|dwelling)\b`, `\b(house\|housing\|residential)\b` |
| infrastructure | `\b(infrastructure\|road\|highway\|street)\b`, `\b(bridge\|railway\|rail.?line)\b` |
| population | `\b(population\|people\|inhabitant)\b`, `\b(census.?(?:data\|population\|survey))\b` |
| agriculture | `\b(agriculture\|agricultural\|farming)\b`, `\b(crop\|cropland\|harvest\|yield)\b` |
| natural_environment | `\b(ecosystem\|habitat\|biodiversity)\b`, `\b(forest\|woodland\|wetland)\b` |
| economic_indicator | `\b(gdp\|gross.?domestic.?product)\b`, `\b(economic.?loss\|damage.?cost)\b` |
| development_index | `\b(hdi\|human.?development.?index)\b`, `\b(poverty.?index\|vulnerability.?index)\b` |

## Exclusion patterns (false positive filters)

| category | patterns |
|---|---|
| flood_false_positives | `flood.?of.?(data\|information\|requests)`, `flood.?(light\|fill)` |
| population_false_positives | `population.?(health\|nutrition\|immunization)`, `target.?population` |
| infrastructure_false_positives | `data.?infrastructure`, `it.?infrastructure`, `digital.?infrastructure` |

## Tag weights (classification scoring)

### Hazard tags
| tag | weight |
|---|---|
| flooding | 5 |
| drought | 5 |
| cyclones-hurricanes-typhoons | 5 |
| earthquake-tsunami | 5 |
| climate hazards | 4 |
| hydrology | 3 |
| natural disasters | 3 |
| hazards and risk | 3 |
| forecasting | 2 |
| topography | 2 |

### Exposure tags
| tag | weight |
|---|---|
| facilities-infrastructure | 5 |
| populated places-settlements | 4 |
| population | 4 |
| roads | 4 |
| education facilities-schools | 4 |
| health facilities | 4 |
| ports, railways, aviation, energy | 3 |
| geodata, gazetteer | 2 |

### Vulnerability proxy tags
| tag | weight |
|---|---|
| demographics, poverty, socioeconomics | 4 |
| disability, gender, food security | 3 |
| nutrition, livelihoods, health, education | 2 |

### Loss/impact tags
| tag | weight |
|---|---|
| damage assessment, casualties, fatalities | 5 |
| mortality | 4 |
| affected population, affected area | 4 |
| people in need-pin, severity | 3 |

## Socioeconomic indicators (18)

Used by VulnerabilityExtractor to detect socioeconomic vulnerability indices:

| code | key patterns |
|---|---|
| POV_HEADCOUNT | poverty headcount, poverty ratio, below poverty line |
| HDI | human development index, hdi |
| SVI_OVERALL | social vulnerability index, svi |
| FOOD_SECURITY | food security, food insecurity, ipc phase |
| POP_DENSITY | population density |
| AGE_65_PLUS | elderly population, aging population |
| EDU_ATTAINMENT | education attainment, literacy rate |
| HEALTH_ACCESS | health access, health facility |
| INFORM_RISK | inform risk, inform index |
| DISPLACEMENT | displaced, displacement, idp |
| COPING_CAPACITY | coping capacity, adaptive capacity |
| RESILIENCE | resilience index, community resilience |
| DEPRIVATION | deprivation index, multi dimensional poverty |
| MALNUTRITION | malnutrition, stunting, wasting |
| VULN_INDEX | vulnerability index, climate vulnerability |
| GENDER_INEQUALITY | gender inequality, gender index |
| DISABILITY | disability prevalence, persons with disabilities |
| LIVELIHOOD | livelihood zone, livelihood vulnerability |
