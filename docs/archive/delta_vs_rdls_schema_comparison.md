# DELTA Data Model vs RDLS v0.3 Schema - Field-Level Comparison

## Purpose

An apples-to-apples comparison of the **DELTA data model** (entities and fields from the ER diagram and user guide) against the **RDLS v0.3 JSON Schema** (fields and codelists). For each DELTA entity and field, we assess the closest RDLS equivalent and rate the fit:

- **Good fit** - direct or near-direct mapping exists
- **Needs adjustment** - concept exists in both but structure or granularity differs
- **No equivalent** - one schema has it, the other doesn't

Reference materials:
- DELTA ER Diagram: `to-rdls/references/delta/DELTA_ER_DIAGRAM-1.pdf`
- DELTA User Guide: `to-rdls/references/delta/DELTA-resilience-user-guide.pdf`
- DELTA Source: [github.com/unisdr/delta](https://github.com/unisdr/delta) (`app/drizzle/schema/`)
- RDLS v0.3 Schema: `hdx_dataset_metadata_dump/rdls/schema/rdls_schema_v0.3.json`

---

## 1. Hazardous Event → RDLS Hazard

The DELTA **Hazardous Event** describes a single physical phenomenon (a flood, an earthquake, a storm). The RDLS **Hazard** component describes a dataset containing hazard data, with nested event_sets and events.

| # | DELTA Field | DELTA Type | RDLS Equivalent | Fit |
|---|-------------|-----------|-----------------|-----|
| 1 | id (UUID) | uuid | `hazard.event_sets[].events[].id` | **Good fit** - both have event-level IDs |
| 2 | name | text | `hazard.event_sets[].events[].description` | **Needs adjustment** - RDLS has no event `name`, only `description` |
| 3 | hipTypeId (HIP Type) | FK → hipType | *(no equivalent)* | **No equivalent** - RDLS has no top-level hazard grouping (e.g. "Meteorological") |
| 4 | hipClusterId (HIP Cluster) | FK → hipCluster | *(no equivalent)* | **No equivalent** - RDLS has no mid-level cluster (e.g. "Flood") |
| 5 | hipHazardId (Specific Hazard) | FK → hipHazard | `hazard.event_sets[].hazards[].type` (hazard_type enum) | **Needs adjustment** - both identify the hazard, but DELTA uses HIP codes (MH0004), RDLS uses snake_case enum (coastal_flood). Mapping table needed. |
| 6 | *(implied from hipHazard)* | - | `hazard.event_sets[].hazards[].hazard_process` (process_type enum) | **Needs adjustment** - DELTA's HIP hazard maps to RDLS hazard_type + process_type pair. DELTA is 3-level (type→cluster→hazard), RDLS is 2-level (hazard_type→process_type). |
| 7 | hazardousEventStatus | enum: forecasted, ongoing, passed | *(no equivalent)* | **No equivalent** - RDLS doesn't track event lifecycle status |
| 8 | startDate | text (yyyy / yyyy-mm / yyyy-mm-dd) | `hazard.event_sets[].events[].occurrence.empirical.temporal.start` | **Good fit** - both support flexible date precision |
| 9 | endDate | text | `hazard.event_sets[].events[].occurrence.empirical.temporal.end` | **Good fit** |
| 10 | magnitude | text | `hazard.event_sets[].hazards[].intensity_measure` | **Needs adjustment** - DELTA stores magnitude as free text; RDLS uses structured "measure:unit" format (e.g. "PGA:g", "wd:m") |
| 11 | description | text | `hazard.event_sets[].events[].description` | **Good fit** |
| 12 | chainsExplanation | text | *(no equivalent)* | **No equivalent** - RDLS has no cascading event concept |
| 13 | spatialFootprint | jsonb (GeoJSON) | `spatial.bbox` / `spatial.geometry` | **Needs adjustment** - DELTA stores full GeoJSON per event; RDLS stores bounding box per dataset |
| 14 | dataSource | text | `sources[].name` | **Needs adjustment** - DELTA is free text per event; RDLS is structured Source object per dataset |
| 15 | recordOriginator | text | `attributions[].entity` (role: creator) | **Needs adjustment** - DELTA is free text; RDLS requires structured entity (name + email/url) with role |
| 16 | attachments | jsonb | `resources[]` | **Needs adjustment** - DELTA stores file attachments as JSON blob; RDLS models resources with format, URL, description |
| 17 | nationalSpecification | text | *(no equivalent)* | **No equivalent** |
| 18 | *(event relationships: caused_by)* | FK → eventRelationshipTable | *(no equivalent)* | **No equivalent** - RDLS has `trigger` object on hazard (type + hazard_process) but no full event chain |

### RDLS Hazard fields with no DELTA equivalent:

| RDLS Field | Description | Note |
|------------|-------------|------|
| `event_sets[].analysis_type` | probabilistic / deterministic / empirical | DELTA only tracks observed (empirical) events |
| `event_sets[].frequency_distribution` | poisson, negative_binomial, etc. | Probabilistic modeling - outside DELTA scope |
| `event_sets[].calculation_method` | inferred / observed / simulated | DELTA events are always observed |
| `event_sets[].occurrence_range` | "Return periods from 10 to 10,000 years" | Probabilistic concept - outside DELTA scope |
| `event_sets[].events[].occurrence.probabilistic` | return_period, event_rate, probability | Probabilistic modeling - outside DELTA scope |
| `hazards[].intensity_measure` | Structured "measure:unit" (100+ predefined) | DELTA has free text `magnitude` |
| `hazards[].trigger` | Secondary hazard trigger (type + process) | DELTA uses event chain relationship instead |

---

## 2. Disaster Event → RDLS Dataset (metadata level)

The DELTA **Disaster Event** represents the societal disruption caused by hazardous events - declarations, warnings, assessments, response, and financial totals. RDLS has no direct equivalent entity; the closest mapping is at the dataset level.

| # | DELTA Field | DELTA Type | RDLS Equivalent | Fit |
|---|-------------|-----------|-----------------|-----|
| 1 | id (UUID) | uuid | `id` (dataset identifier) | **Good fit** - both are unique identifiers |
| 2 | nameNational | text | `title` | **Good fit** |
| 3 | nameGlobalOrRegional | text | *(no equivalent)* | **No equivalent** - RDLS has single `title` |
| 4 | startDate / endDate | text | `resources[].temporal.start` / `.end` | **Needs adjustment** - DELTA is per disaster event; RDLS temporal is per resource |
| 5 | durationDays | bigint | `resources[].temporal.duration` | **Needs adjustment** - RDLS uses ISO 8601 duration format |
| 6 | glide | text (GLIDE number) | *(no equivalent)* | **No equivalent** |
| 7 | nationalDisasterId | text | *(no equivalent)* | **No equivalent** |
| 8 | otherId1/2/3 | text | *(no equivalent)* | **No equivalent** |
| 9 | hipTypeId / hipClusterId / hipHazardId | FK (optional) | `risk_data_type` + hazard component | **Needs adjustment** - DELTA optionally classifies the disaster by hazard; RDLS classifies the dataset |
| 10 | disasterDeclaration | enum: yes/no/unknown | *(no equivalent)* | **No equivalent** |
| 11 | disasterDeclarationType (×5) | text + timestamp | *(no equivalent)* | **No equivalent** |
| 12 | hadOfficialWarning | boolean | *(no equivalent)* | **No equivalent** |
| 13 | earlyActionDescription (×5) | text + timestamp | *(no equivalent)* | **No equivalent** |
| 14 | rapidAssessment (×5) | text + timestamp | *(no equivalent)* | **No equivalent** |
| 15 | postDisasterAssessment (×5) | text + timestamp | *(no equivalent)* | **No equivalent** |
| 16 | responseOperations | text | *(no equivalent)* | **No equivalent** |
| 17 | effectsTotalUsd | money | *(no equivalent)* | **No equivalent** - RDLS describes the dataset, not aggregate financial totals |
| 18 | damagesSubtotalLocalCurrency | money | *(no equivalent)* | **No equivalent** |
| 19 | lossesSubtotalUSD | money | *(no equivalent)* | **No equivalent** |
| 20 | responseCostTotal (local + USD) | money | *(no equivalent)* | **No equivalent** |
| 21 | humanitarianNeeds (local + USD) | money | *(no equivalent)* | **No equivalent** |
| 22 | rehabilitation/repair/replacement/recoveryCosts | money | *(no equivalent)* | **No equivalent** |
| 23 | recordingInstitution | text | `attributions[].entity` (role: creator) | **Needs adjustment** |
| 24 | dataSource | text | `sources[].name` | **Needs adjustment** |
| 25 | spatialFootprint | jsonb (GeoJSON) | `spatial` | **Needs adjustment** - same as hazardous event |
| 26 | attachments | jsonb | `resources[]` | **Needs adjustment** - same as hazardous event |
| 27 | hazardousEventId | FK → hazardousEvent | *(structural)* | **No equivalent** - RDLS doesn't link loss datasets to specific hazard events |
| 28 | legacyData | jsonb | *(no equivalent)* | **No equivalent** - DELTA-specific migration field |

**Summary**: The Disaster Event entity is heavily operational (declarations, warnings, assessments, response costs). These fields have **no equivalent** in RDLS because RDLS describes datasets, not disaster management activities. Only the basic identification/temporal/spatial fields have rough RDLS counterparts.

---

## 3. Disaster Record → RDLS Loss

The DELTA **Disaster Record** is the granular impact record - the core data unit for loss tracking. This maps most closely to RDLS's **Loss** component, but at very different granularity levels.

| # | DELTA Field | DELTA Type | RDLS Equivalent | Fit |
|---|-------------|-----------|-----------------|-----|
| 1 | id (UUID) | uuid | `loss.losses[].id` | **Good fit** |
| 2 | disasterEventId | FK → disasterEvent | *(structural)* | **No equivalent** - RDLS loss entries are not linked to specific disaster events |
| 3 | hipTypeId / hipClusterId / hipHazardId | FK → HIP tables | `loss.losses[].hazard_type` + `loss.losses[].hazard_process` | **Needs adjustment** - HIP 3-level → RDLS 2-level, mapping table needed |
| 4 | approvalStatus | enum: draft/waiting/revision/validated/published | *(no equivalent)* | **No equivalent** |
| 5 | locationDesc | text | `loss.losses[].description` (embedded) | **Needs adjustment** - DELTA has dedicated location field; RDLS uses free text |
| 6 | startDate / endDate | text | *(no equivalent at loss level)* | **No equivalent** - RDLS loss entries don't have individual dates |
| 7 | primaryDataSource | text | `sources[].name` | **Needs adjustment** - per-record in DELTA, per-dataset in RDLS |
| 8 | otherDataSource | text | `sources[]` | **Needs adjustment** |
| 9 | fieldAssessDate | timestamp | *(no equivalent)* | **No equivalent** |
| 10 | assessmentModes | text | *(no equivalent)* | **No equivalent** |
| 11 | originatorRecorderInst | text | `attributions[].entity` | **Needs adjustment** - per-record in DELTA, per-dataset in RDLS |
| 12 | validatedBy | text | *(no equivalent)* | **No equivalent** |
| 13 | checkedBy | text | *(no equivalent)* | **No equivalent** |
| 14 | dataCollector | text | *(no equivalent)* | **No equivalent** |
| 15 | spatialFootprint | jsonb (GeoJSON) | `spatial` (dataset level) | **Needs adjustment** - per-record vs per-dataset |
| 16 | attachments | jsonb | `resources[]` | **Needs adjustment** |

---

## 4. Human Effects → RDLS Loss (impact metrics)

DELTA tracks human effects through dedicated tables per metric, each with full disaggregation. RDLS represents human impacts as `impact_metric` enum values.

| # | DELTA Entity/Field | Disaggregation | RDLS Equivalent | Fit |
|---|-------------------|---------------|-----------------|-----|
| 1 | **deathsTable**.deaths | sex, age, disability, globalPovertyLine, nationalPovertyLine | `loss.losses[].impact_and_losses.impact_metric: casualty_count` | **Needs adjustment** - RDLS has the metric concept but no disaggregation. DELTA stores actual counts per demographic breakdown. |
| 2 | **injuredTable**.injured | same disaggregation | `loss.losses[].impact_and_losses.impact_metric: casualty_count` | **Needs adjustment** - RDLS uses same metric for deaths and injuries (no distinction) |
| 3 | **missingTable**.missing | same disaggregation | *(no equivalent)* | **No equivalent** - RDLS has no "missing persons" metric |
| 4 | **affectedTable**.direct | same disaggregation | `loss.losses[].impact_and_losses.impact_metric: exposure_to_hazard` | **Needs adjustment** - RDLS has the concept but no direct/indirect split at metric level |
| 5 | **affectedTable**.indirect | same disaggregation | `loss.losses[].impact_and_losses.impact_type: indirect` | **Needs adjustment** - RDLS separates direct/indirect at `impact_type` level, not as separate counts |
| 6 | **displacedTable**.displaced | same disaggregation + assisted (enum), timing (enum), duration (enum), asOf (timestamp) | `loss.losses[].impact_and_losses.impact_metric: displaced_count` | **Needs adjustment** - RDLS has the metric but none of the rich displacement enums (assisted/not, pre-emptive/reactive, short/medium/long/permanent) |
| 7 | **humanDsgTable** disaggregation: sex, age, disability, poverty | dimensions per record | *(no equivalent)* | **No equivalent** - RDLS has no disaggregation dimensions at all |
| 8 | Custom disaggregation (JSON) | jsonb | *(no equivalent)* | **No equivalent** |

### Fit summary for Human Effects:
RDLS can represent *that* a dataset contains casualty or displacement data (via `impact_metric` enum), but **cannot represent** the granular per-event counts with demographic disaggregation that DELTA stores. The concepts overlap at a high level, but the granularity gap is fundamental - this reflects the metadata-vs-data difference.

---

## 5. Sector Effects - Damages → RDLS Loss

DELTA tracks physical damage per sector, per asset, distinguishing partially damaged from totally destroyed, with repair/replacement/recovery costs. RDLS has no structural equivalent.

| # | DELTA Field | RDLS Equivalent | Fit |
|---|-------------|-----------------|-----|
| 1 | sectorId (FK → sectorTable) | `loss.losses[].asset_category` (enum: agriculture, buildings, infrastructure, population, natural_environment) | **Needs adjustment** - DELTA has hierarchical, user-defined sectors; RDLS has a 7-value fixed enum. Concepts overlap but granularity differs. |
| 2 | assetId (FK → assetTable: name, category, nationalId) | *(no equivalent)* | **No equivalent** - RDLS doesn't track individual assets |
| 3 | pdDamageAmount (partially damaged count) | *(no equivalent)* | **No equivalent** - RDLS has no damaged/destroyed distinction |
| 4 | tdDamageAmount (totally destroyed count) | *(no equivalent)* | **No equivalent** |
| 5 | pdRepairCostUnit + pdRepairCostTotal | *(no equivalent)* | **No equivalent** - RDLS doesn't model repair costs |
| 6 | tdReplacementCostUnit + tdReplacementCostTotal | *(no equivalent)* | **No equivalent** |
| 7 | pdRecoveryCostUnit + pdRecoveryCostTotal | *(no equivalent)* | **No equivalent** |
| 8 | tdRecoveryCostUnit + tdRecoveryCostTotal | *(no equivalent)* | **No equivalent** |
| 9 | pdDisruptionDurationDays/Hours | *(no equivalent)* | **No equivalent** |
| 10 | pdDisruptionUsersAffected / PeopleAffected | *(no equivalent)* | **No equivalent** |
| 11 | unit (enum: units type) | `loss.losses[].impact_and_losses.quantity_kind` | **Needs adjustment** - different unit systems |
| 12 | totalDamageAmount | `loss.losses[].impact_and_losses.impact_metric: damage_ratio` or `economic_loss_value` | **Needs adjustment** - RDLS can describe damage metrics but not individual damage amounts |
| 13 | totalRepairReplacement (money) | *(no equivalent)* | **No equivalent** |
| 14 | totalRecovery (money) | *(no equivalent)* | **No equivalent** |
| 15 | spatialFootprint | `spatial` (dataset level) | **Needs adjustment** |

### Fit summary for Damages:
RDLS can describe *what kind* of damage data a dataset contains (via `asset_category`, `impact_metric`, `quantity_kind`), but **cannot represent** the actual damage records with partially-damaged/destroyed counts, per-asset costs, or repair/recovery figures. Almost entirely **no equivalent**.

---

## 6. Sector Effects - Losses → RDLS Loss

DELTA tracks economic losses per sector with public/private split and per-unit costs.

| # | DELTA Field | RDLS Equivalent | Fit |
|---|-------------|-----------------|-----|
| 1 | sectorId (FK → sectorTable) | `loss.losses[].asset_category` | **Needs adjustment** - same as damages |
| 2 | sectorIsAgriculture | *(implied by asset_category: agriculture)* | **Good fit** |
| 3 | typeAgriculture / typeNotAgriculture | *(no equivalent)* | **No equivalent** - RDLS has no sub-type within agriculture |
| 4 | publicUnit (enum) / publicUnits (count) | *(no equivalent)* | **No equivalent** - RDLS has no public/private split |
| 5 | publicCostUnit + publicCostTotal | *(no equivalent)* | **No equivalent** |
| 6 | privateCostUnit + privateCostTotal | *(no equivalent)* | **No equivalent** |
| 7 | publicCostUnitCurrency / privateCostUnitCurrency | `loss.losses[].impact_and_losses.currency` (ISO 4217) | **Good fit** - both use currency codes |
| 8 | description | `loss.losses[].description` | **Good fit** |

---

## 7. Sector Effects - Disruption → RDLS Loss

| # | DELTA Field | RDLS Equivalent | Fit |
|---|-------------|-----------------|-----|
| 1 | durationDays / durationHours | `loss.losses[].asset_dimension: disruption` | **Needs adjustment** - RDLS flags that disruption data exists but doesn't store actual duration values |
| 2 | usersAffected / peopleAffected | `loss.losses[].impact_and_losses.impact_metric: downtime_loss` | **Needs adjustment** - conceptually similar but RDLS is metadata description, not actual count |

---

## 8. Non-Economic Losses → RDLS Loss

| # | DELTA Field | RDLS Equivalent | Fit |
|---|-------------|-----------------|-----|
| 1 | categoryId (FK → categoriesTable) | `loss.losses[].asset_category` | **Needs adjustment** - DELTA has user-defined categories; RDLS has fixed enum |
| 2 | description | `loss.losses[].description` | **Good fit** |

---

## 9. Spatial & Administrative → RDLS Spatial

| # | DELTA Entity/Field | RDLS Equivalent | Fit |
|---|-------------------|-----------------|-----|
| 1 | divisionTable (hierarchical: name, parentId, level) | `spatial.gazetteer_entries[]` (scheme: ISO 3166-2, NUTS, GEONAMES) | **Needs adjustment** - DELTA has a custom hierarchy per country; RDLS references external gazetteers |
| 2 | countriesTable (ISO codes) | `spatial.countries[]` (ISO 3166-1 alpha-3) | **Good fit** |
| 3 | countryAccounts (tenant isolation) | *(no equivalent)* | **No equivalent** - operational concern |
| 4 | spatialFootprint (GeoJSON, per event/record) | `spatial.bbox` + `spatial.geometry` (per dataset) | **Needs adjustment** - DELTA is per-record GeoJSON; RDLS is per-dataset bounding box |

---

## 10. Attribution & Provenance → RDLS Attributions

| # | DELTA Field/Entity | RDLS Equivalent | Fit |
|---|-------------------|-----------------|-----|
| 1 | organizationTable (id, name) | `attributions[].entity` (name, email/url) | **Needs adjustment** - DELTA has a separate org table; RDLS embeds entity inline with role |
| 2 | recordOriginator (per event) | `attributions[]` with role: creator | **Needs adjustment** - per-event vs per-dataset |
| 3 | validatedBy (per record) | *(no equivalent)* | **No equivalent** |
| 4 | dataCollector (per record) | *(no equivalent)* | **No equivalent** - RDLS has no data collector role |
| 5 | primaryDataSource (per record) | `sources[].name` (per dataset) | **Needs adjustment** |
| 6 | approvalFields (status, workflow) | *(no equivalent)* | **No equivalent** |
| 7 | *(no equivalent)* | `attributions[].role` - 21 role types (publisher, contact_point, funder, etc.) | **No equivalent in DELTA** - DELTA has simple text fields, RDLS has rich role taxonomy |

---

## 11. RDLS Components with No DELTA Equivalent

These RDLS components have no structural counterpart in the DELTA data model:

| RDLS Component | Description | Why No DELTA Equivalent |
|----------------|-------------|------------------------|
| **Exposure** (entire component) | Categories, taxonomy, metrics, dimensions | DELTA treats exposure as external context data, not core collection |
| **Vulnerability** (entire component) | 4 function types (vulnerability, fragility, damage-to-loss, engineering demand) + socio-economic indices | DELTA focuses on observed impacts, not risk modeling functions |
| **Resources** (structured) | 24 data formats, 11 access modalities, spatial resolution, CRS | DELTA is the database itself; RDLS describes external data files |
| **Probabilistic occurrence** | Return periods, event rates, probability distributions | DELTA tracks real observed events only |
| **Intensity measures** | 100+ predefined measure:unit pairs | DELTA has free text `magnitude` |
| **Dataset lineage** | hazard→exposure→vulnerability→loss chain | DELTA links via FK relationships between its own tables |
| **License** | Codelist with CC-BY, ODbL, etc. | DELTA uses Apache 2.0 for its software; data licensing is per-country policy |

---

## 12. Overall Fit Assessment Summary

| DELTA Domain | Fields Mapped | Good Fit | Needs Adjustment | No Equivalent |
|-------------|---------------|----------|------------------|---------------|
| **Hazardous Event** (18 fields) | 18 | 4 (22%) | 8 (44%) | 6 (33%) |
| **Disaster Event** (28 fields) | 28 | 2 (7%) | 5 (18%) | 21 (75%) |
| **Disaster Record** (16 fields) | 16 | 1 (6%) | 7 (44%) | 8 (50%) |
| **Human Effects** (8 categories) | 8 | 0 (0%) | 6 (75%) | 2 (25%) |
| **Damages** (15 fields) | 15 | 0 (0%) | 3 (20%) | 12 (80%) |
| **Losses** (8 fields) | 8 | 3 (38%) | 1 (13%) | 4 (50%) |
| **Disruption** (2 fields) | 2 | 0 (0%) | 2 (100%) | 0 (0%) |
| **Spatial** (4 entities) | 4 | 1 (25%) | 2 (50%) | 1 (25%) |
| **Attribution** (7 fields) | 7 | 0 (0%) | 3 (43%) | 4 (57%) |
| **TOTAL** | **106** | **11 (10%)** | **37 (35%)** | **58 (55%)** |

---

## 13. Conclusions

### Where they align well (Good Fit - 10%)
- Basic identifiers (UUID ↔ string ID)
- Country-level spatial (ISO codes)
- Hazard event dates and descriptions
- Currency codes
- Loss descriptions

### Where they overlap but differ in structure (Needs Adjustment - 35%)
- **Hazard classification**: HIP 3-level hierarchy ↔ RDLS 2-level codelist - a mapping table (HIP code → hazard_type + process_type) would bridge the gap
- **Human impact metrics**: DELTA disaggregated counts ↔ RDLS impact_metric enum - RDLS can describe the *type* of data but not the *values* with breakdown
- **Spatial**: DELTA per-record GeoJSON ↔ RDLS per-dataset bbox - aggregation needed
- **Attribution/provenance**: DELTA per-record text ↔ RDLS per-dataset structured entities with roles
- **Sectors**: DELTA hierarchical user-defined ↔ RDLS 7-value fixed enum

### Where they are completely different (No Equivalent - 55%)
- DELTA's **disaster management fields** (declarations, warnings, early actions, assessments, response operations) - entirely operational, no RDLS counterpart
- DELTA's **disaggregation dimensions** (sex, age, disability, poverty) - RDLS has no concept of this
- DELTA's **damage granularity** (partially damaged vs destroyed, repair/replacement/recovery costs) - RDLS operates at metadata level
- DELTA's **financial aggregation** (effects totals, response costs, humanitarian needs in USD + local currency) - RDLS doesn't store actual financial figures
- DELTA's **approval workflows** - operational concern outside RDLS scope
- RDLS's **exposure and vulnerability components** - outside DELTA scope (DELTA treats these as external context)
- RDLS's **probabilistic modeling** (return periods, event rates) - DELTA only tracks observed events
- RDLS's **resource metadata** (24 data formats, spatial resolution, CRS) - DELTA is the database itself

### Bottom line

The 55% "no equivalent" rate reflects the fundamental design difference: **DELTA is an operational database for collecting and storing granular disaster impact data**; **RDLS is a metadata catalog for describing risk datasets**. They are complementary, not competing. A DELTA instance could be the *source* that an RDLS record *describes* - the RDLS record would say "this dataset contains loss data from DELTA with casualty counts, damage records, and economic losses for floods in Albania", while the DELTA database would contain the actual 4,659 event records with disaggregated figures.

The 35% "needs adjustment" represents the practical bridging work for a DELTA→RDLS adapter: hazard code mapping, spatial aggregation, attribution restructuring, and metric-level summarization.

---

## 14. Practical Mapping: DELTA Extract → RDLS Loss Record

For building a DELTA→RDLS adapter (one RDLS JSON per country multi-hazard extract), this is the corrected field mapping using actual RDLS v0.3 codelist values:

| DELTA Entity / Field | RDLS Mapping | Fit | Notes |
|---|---|---|---|
| **Hazardous Event** hipHazardId (e.g. MH0004) | `loss.losses[].hazard_type` + `hazard_process` | Needs adjustment | HIP 3-level (type→cluster→hazard) → RDLS 2-level. Mapping table needed (e.g. MH0004 → coastal_flood + storm_surge). |
| **deathsTable**.deaths (disaggregated) | `impact_metric: casualty_count`, `quantity_kind: count` | Needs adjustment | RDLS can't distinguish deaths from injuries - same metric for both. Disaggregation (sex, age, disability, poverty) is lost entirely. |
| **injuredTable**.injured (disaggregated) | `impact_metric: casualty_count`, `quantity_kind: count` | Needs adjustment | Same metric as deaths - lossy mapping. Would need separate loss entry or note in description. |
| **missingTable**.missing | *(no equivalent)* | No equivalent | RDLS has no missing persons metric. Could note in `description`. |
| **affectedTable**.direct | `impact_metric: exposure_to_hazard`, `impact_type: direct` | Needs adjustment | |
| **affectedTable**.indirect | `impact_metric: exposure_to_hazard`, `impact_type: indirect` | Needs adjustment | |
| **displacedTable**.displaced (+ enums) | `impact_metric: displaced_count`, `quantity_kind: count` | Needs adjustment | Loses DELTA's rich enums: assisted/not_assisted, pre-emptive/reactive, duration (short/medium/long/permanent). |
| **damagesTable** (pd/td per asset) | `asset_category` + `impact_metric: economic_loss_value` | Needs adjustment | Loses partially-damaged vs totally-destroyed distinction, per-asset detail, repair/replacement/recovery cost breakdown. |
| **lossesTable** (public/private per sector) | `asset_category` + `quantity_kind: monetary` + `currency` | Needs adjustment | Loses public/private split. `asset_category` maps DELTA sectors to 7-value enum (agriculture, buildings, infrastructure, population, natural_environment, economic_indicator, development_index). |
| **disruptionTable** (duration, people) | `asset_dimension: disruption` + `impact_metric: downtime_loss` | Needs adjustment | RDLS flags the concept, cannot store actual duration values. |
| **divisionTable** + spatialFootprint | `spatial.countries[]` + `spatial.bbox` + `spatial.gazetteer_entries[]` | Needs adjustment | Per-record GeoJSON aggregated to per-dataset bbox. Division hierarchy maps to gazetteer scheme (ISO 3166-2 or GEONAMES). |
| Hazardous Event startDate/endDate | `resources[].temporal.start` / `.end` | Good fit | Aggregate to dataset temporal range. |
| recordingInstitution, dataSource | `attributions[]` (role: creator) + `sources[].name` | Needs adjustment | Per-record text → per-dataset structured entities. |
| approvalStatus (draft→validated→published) | *(no equivalent)* | No equivalent | Could note in `details` field. |
| Disaster declarations, warnings, response | *(no equivalent)* | No equivalent | Operational fields - outside RDLS scope. |
| Sendai indicators (implied by structure) | `purpose` / `details` | No dedicated field | Targets A-D align with RDLS impact_metric groups but no formal Sendai field in RDLS. |

### Key data losses in DELTA→RDLS transformation:
1. **Disaggregation** - all demographic breakdowns (sex, age, disability, poverty) are lost
2. **Deaths vs Injured** - collapsed into single `casualty_count` metric
3. **Missing persons** - no RDLS equivalent
4. **Damage granularity** - partially damaged / totally destroyed distinction is lost
5. **Public/private split** - economic losses merged
6. **Displacement details** - assisted, timing, duration enums lost
7. **Per-record spatial** - GeoJSON footprints aggregated to bounding box
8. **Operational context** - declarations, warnings, assessments, response all dropped
