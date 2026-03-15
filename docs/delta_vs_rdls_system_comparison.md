# DELTA vs RDLS v0.3 - System-Level Comparison

## Context

This is a research comparison between two disaster risk data standards. **DELTA Resilience** (UNDRR/UNDP/WMO) is an operational system for tracking hazardous events, losses and damages. **RDLS v0.3** (GFDRR) is a metadata standard for cataloging risk datasets. Understanding their differences and overlaps is key for interoperability - especially since our DesInventar pipeline already produces RDLS records, and DELTA is the successor to DesInventar.

---

## 1. Fundamental Design Philosophy

| Aspect | DELTA Resilience | RDLS v0.3 |
|--------|-----------------|-----------|
| **Purpose** | Operational disaster tracking system (data collection & analysis) | Metadata catalog standard (describing risk datasets) |
| **Data level** | Individual event records with granular impact data | Dataset-level metadata describing collections of data |
| **Architecture** | PostgreSQL/PostGIS relational database (40 tables) | JSON Schema (Draft 2020-12) for document validation |
| **Granularity** | Single event → single loss record → disaggregated metrics | One metadata record per dataset (may summarize thousands of events) |
| **Governance** | UNDRR/UNDP/WMO tripartite, country-owned instances | GFDRR-led open standard |
| **Tech stack** | TypeScript/Remix, Drizzle ORM, Node.js, PostgreSQL 16 | Schema-only (JSON), implementation-agnostic |
| **License** | Apache 2.0 (open-source software) | Open standard (schema definition) |

**Key insight**: DELTA stores the actual disaster data; RDLS describes datasets that contain such data. They operate at different levels of abstraction - DELTA is the database, RDLS is the catalog card.

---

## 2. Entity/Component Architecture

### DELTA: 3-tier Event Hierarchy + Impact Records

```
eventTable (base: id, name, description)
├── hazardousEventTable (physical phenomenon)
│     └── HIP classification (type → cluster → hazard)
├── disasterEventTable (societal disruption)
│     └── disasterRecordsTable (granular impact per location)
│           ├── Human Effects (deaths, injured, missing, affected, displaced)
│           ├── Sector Effects (damages per asset, losses per sector)
│           └── Non-economic Losses (per category)
└── eventRelationshipTable (parent↔child, "caused_by" cascading)
```

**40 database tables** including: eventTable, hazardousEventTable, disasterEventTable, disasterRecordsTable, deathsTable, injuredTable, missingTable, affectedTable, displacedTable, damagesTable, lossesTable, disruptionTable, nonecoLossesTable, sectorTable, assetTable, categoriesTable, hipTypeTable, hipClusterTable, hipHazardTable, divisionTable, countriesTable, countryAccounts, organizationTable, + admin/auth tables.

### RDLS: Flat Dataset with 4 HEVL Components

```
dataset (root metadata object)
├── hazard
│     └── event_sets[] → events[] → hazards[] + occurrence
├── exposure[]
│     └── category + metrics[]
├── vulnerability
│     ├── functions (vulnerability, fragility, damage_to_loss, engineering_demand)
│     └── socio_economic[]
├── loss
│     └── losses[] → impact_and_losses + lineage
├── attributions[] (publisher, creator, contact_point, ...)
├── resources[] (download_url, data_format, ...)
└── spatial (countries, bbox, gazetteer_entries)
```

---

## 3. Hazard Classification

| Aspect | DELTA (HIP) | RDLS v0.3 |
|--------|-------------|-----------|
| **Standard** | WMO-CHE + UNDRR/ISC Hazard Information Profiles | RDLS closed codelists |
| **Structure** | 3-level hierarchy: Type → Cluster → Hazard | 2-level flat: hazard_type → process_type |
| **Hierarchy** | hipType (e.g. "Meteorological") → hipCluster (e.g. "Flood") → hipHazard (e.g. "MH0004 Coastal Flood") | hazard_type (e.g. "flood") → process_type (e.g. "fluvial_flood") |
| **Extensibility** | Database-driven (add rows), hierarchy-free flexible analysis | Closed enum (code change to extend) |
| **Scope** | Weather, climate, water, space weather, environmental | Natural hazards only (11 types, 30 processes) |
| **Codes** | Alphanumeric IDs (e.g. "MH0004", "GH0001") | Snake_case strings (e.g. "coastal_flood") |
| **Cascading** | Native support via eventRelationshipTable (caused_by) | No native cascade support |
| **Compound events** | Parent-child event linking | Not modeled |

### RDLS hazard_type codelist (11 types):
coastal_flood, convective_storm, drought, earthquake, extreme_temperature, flood, landslide, strong_wind, tsunami, volcanic, wildfire

### DELTA HIP hierarchy (examples from code comments):
- Type: Meteorological and Hydrological, Extraterrestrial, Geohazards
- Cluster: Flood, Temperature-Related, ...
- Hazard: MH0004 (Coastal Flood), GH0001 (Earthquake), ...
- Full taxonomy loaded as data (not hardcoded in schema)

---

## 4. Loss & Impact Data Model

This is where the schemas differ most significantly.

### DELTA: Granular, Disaggregated, Record-Level

**Human Effects** - tracked per disaster record through disaggregation table (`humanDsgTable`):
- Disaggregation dimensions: sex, age, disability, globalPovertyLine, nationalPovertyLine
- Plus custom disaggregation stored as JSON
- Metric tables: `deathsTable`, `injuredTable`, `missingTable`, `affectedTable` (direct + indirect), `displacedTable`
- Displaced has enums: assisted/not_assisted, pre-emptive/reactive, duration (short/medium_short/medium_long/long/permanent)

**Sector Effects** - per sector, per asset, per disaster record:
- `damagesTable`: partially damaged (pd*) and totally destroyed (td*) subcategories
  - Each with: damage amount, repair/replacement cost (unit + total), recovery cost, disruption (duration + people affected)
  - Repair and replacement costs in local currency
- `lossesTable`: per sector, public/private split
  - Each with: unit type, unit count, cost per unit, total cost (with currency)
  - Agriculture vs non-agriculture type distinction
- `disruptionTable`: duration (days/hours), users/people affected
- `assetTable`: named assets with categories and national identifiers

**Non-economic Losses**:
- `nonecoLossesTable`: category + description per disaster record

**Financial Aggregation** (at disaster event level):
- effects total (USD), damages subtotal (local currency), losses subtotal (USD)
- Response costs, humanitarian needs, rehabilitation/repair/replacement/recovery costs
- Both local currency and USD figures, with calculated and override options

### RDLS: Metadata-Level Aggregation

**Loss component** - describes the dataset, not individual records:
- `losses[]`: array of loss objects, each with:
  - hazard_type, hazard_process (what hazard caused the loss)
  - asset_category: exposure_category enum (agriculture, buildings, infrastructure, population, natural_environment, economic_indicator, development_index)
  - asset_dimension: metric_dimension enum (structure, content, product, disruption, population, index)
  - impact_and_losses:
    - impact_type: direct/indirect/total
    - impact_modelling: inferred/observed/simulated
    - impact_metric: closed codelist (20 values: damage_ratio, economic_loss_value, casualty_count, displaced_count, etc.)
    - quantity_kind: open (area, count, monetary, length, time)
    - loss_type: ground_up/insured/gross/count/net_precat/net_postcat
    - loss_approach: analytical/empirical/hybrid/judgement
    - loss_frequency_type: probabilistic/deterministic/empirical
    - currency: ISO 4217 (optional)
  - lineage: links to hazard/exposure/vulnerability datasets
  - description: free text

### Comparison Table - Loss Data

| Aspect | DELTA | RDLS v0.3 |
|--------|-------|-----------|
| **Granularity** | Per event, per location, per asset | Per dataset (aggregate description) |
| **Human impact** | 5 dedicated metric tables + disaggregation | Single `impact_metric` enum value |
| **Disaggregation** | sex, age, disability, poverty (+ custom JSON) | Not supported |
| **Sector breakdown** | Hierarchical sector table + per-sector records | `asset_category` enum (7 values) |
| **Damage vs loss** | Separate tables (damages=physical, losses=economic) | Single `loss_type` field |
| **Public/private** | Explicit split in losses table | Not distinguished |
| **Currency** | Local currency + USD with override | ISO 4217 code (optional) |
| **Non-economic** | Dedicated table with categories | Covered by `impact_metric` enum |
| **Spatial detail** | GeoJSON footprint per record | Bounding box per dataset |
| **Temporal detail** | Start/end per event (yyyy/yyyy-mm/yyyy-mm-dd) | Period per dataset |
| **Data provenance** | Data source, collector, validator, recorder per record | `lineage` links to other datasets |

---

## 5. Exposure & Vulnerability

### DELTA Approach
- Exposure and vulnerability are **context info** (ingested from external sources, not core data collection)
- The data model diagram shows "Reference/pre-disaster conditions", "Vulnerability", "Exposure - key statistics" as context data feeding into analysis
- No dedicated exposure/vulnerability tables in the database schema - these are external inputs

### RDLS Approach
- **Exposure**: First-class component with categories, taxonomy, metrics (dimension, quantity_kind)
  - 7 categories: agriculture, buildings, infrastructure, population, natural_environment, economic_indicator, development_index
  - 12 taxonomy standards: GED4ALL, MOVER, HAZUS, EMS-98, etc.
- **Vulnerability**: Detailed function definitions (4 types) + socio-economic indices
  - Functions: vulnerability, fragility, damage-to-loss, engineering demand
  - Each with approach, relationship, intensity measure, impact modeling
  - Socio-economic: indicator name/code, reference year, threshold

| Aspect | DELTA | RDLS v0.3 |
|--------|-------|-----------|
| **Exposure** | External context data (not core) | First-class component with metrics |
| **Vulnerability** | External context data (not core) | Detailed function definitions (4 types) |
| **Focus** | Observed impacts (what happened) | Risk modeling inputs (what could happen) |

---

## 6. Spatial & Temporal

| Aspect | DELTA | RDLS v0.3 |
|--------|-------|-----------|
| **Spatial model** | GeoJSON footprints (jsonb) per event/record + administrative divisions | countries[], bbox, centroid, gazetteer_entries |
| **Admin hierarchy** | divisionTable (hierarchical, self-referencing) | Gazetteer entries (ISO 3166-2, NUTS, GEONAMES, OSMN/R) |
| **Resolution** | Individual event location | Dataset-level coverage |
| **Geometry** | Full GeoJSON in jsonb columns | Optional geometry object (GeoJSON types) |
| **Temporal model** | start/end dates per event (flexible precision: yyyy, yyyy-mm, yyyy-mm-dd) | Period object (start, end, duration, resolution) |
| **Temporal scope** | Individual event timeline | Dataset temporal coverage |

---

## 7. Identity & Attribution

| Aspect | DELTA | RDLS v0.3 |
|--------|-------|-----------|
| **Identifiers** | UUID v4 (gen_random_uuid()) | String (HTTP URI/URN/DOI recommended) |
| **Attribution** | organizationTable, recordOriginator, validatedBy, dataCollector | attributions[] (min 3: publisher, creator, contact_point, + 18 other roles) |
| **Data quality** | Approval workflow (pending/approved), validation assignments | Not modeled |
| **Multi-tenancy** | Country accounts with tenant isolation | Not applicable (single records) |
| **GLIDE numbers** | Native field (glide) on disaster events | Not present |
| **National IDs** | nationalDisasterId + otherId1/2/3 | Not present |

---

## 8. Resources & Data Access

| Aspect | DELTA | RDLS v0.3 |
|--------|-------|-----------|
| **Data access** | REST API + CSV import/export + dashboards | resources[] with download_url/access_url |
| **Formats** | JSON API, CSV, GeoJSON (spatial footprints) | 24 data_format enum values (GeoTIFF, NetCDF, GeoPackage, Shapefile, etc.) |
| **Access modes** | API endpoints, role-based access | 11 access_modality values (file_download, API, OGC_API, WMS, STAC, etc.) |
| **Attachments** | jsonb attachment fields on events/records | resources[] array with metadata |
| **Resolution** | Not applicable (event-level data) | spatial_resolution (meters), coordinate_system (EPSG/ESRI) |
| **Legacy import** | DesInventar ETL migration API (DIX format) | Not applicable |

---

## 9. Key Differences Summary

### DELTA does that RDLS doesn't:
1. **Disaggregated human impacts** (sex, age, disability, poverty line)
2. **Cascading/compound event chains** (parent-child event relationships)
3. **Public vs private loss split**
4. **Partially damaged vs totally destroyed** distinction
5. **Displaced population tracking** with duration/assistance/timing enums
6. **Approval/validation workflow** (draft → approved, validator assignments)
7. **Multi-tenancy** (country-owned instances with tenant isolation)
8. **Asset-level granularity** (named assets with national IDs)
9. **Non-economic loss categories**
10. **GLIDE and national disaster identifiers**

### RDLS does that DELTA doesn't:
1. **Exposure modeling metadata** (categories, taxonomy, metrics, dimensions)
2. **Vulnerability function definitions** (4 types with mathematical relationships)
3. **Probabilistic risk modeling** (return periods, event rates, probability)
4. **Rich resource metadata** (24 data formats, 11 access modalities, spatial resolution, CRS)
5. **Standardized attribution** (21 role types, mandatory publisher/creator/contact)
6. **Intensity measures** (100+ predefined measures with definitions)
7. **Loss frequency analysis** (probabilistic/deterministic/empirical)
8. **Dataset lineage** (hazard→exposure→vulnerability→loss chain)
9. **Open codelist extensibility** (license, intensity_measure, quantity_kind)

---

## 10. Interoperability Mapping (DesInventar → DELTA → RDLS)

Since DesInventar is DELTA's predecessor and our pipeline already transforms DesInventar→RDLS, the mapping chain matters:

| DesInventar Field | DELTA Entity/Field | RDLS v0.3 Field |
|-------------------|--------------------|-----------------|
| Event type | hipHazard (MH0004, GH0001...) | hazard_type (coastal_flood, earthquake...) |
| Event date | hazardousEvent.startDate/endDate | loss.losses[].description (embedded) |
| Deaths | deathsTable.deaths (disaggregated) | loss.impact_metric: casualty_count |
| Injured | injuredTable.injured (disaggregated) | loss.impact_metric: casualty_count |
| Missing | missingTable.missing | Not directly mapped |
| Affected | affectedTable.direct/indirect | loss.impact_metric: exposure_to_hazard |
| Displaced | displacedTable.displaced (with enums) | loss.impact_metric: displaced_count |
| Houses destroyed | damagesTable.tdDamageAmount | loss.asset_category: buildings |
| Houses damaged | damagesTable.pdDamageAmount | loss.asset_category: buildings |
| Crop damage | lossesTable (agriculture sector) | loss.asset_category: agriculture |
| Economic losses | disasterEvent financial fields | loss.impact_metric: economic_loss_value |
| Location | divisionTable + spatialFootprint | spatial.countries, spatial.bbox |
| Source | primaryDataSource, otherDataSource | attributions[].entity |

---

## 11. Implications for Our Pipeline

1. **DELTA as a future data source**: When countries migrate from DesInventar to DELTA, our pipeline will need a DELTA→RDLS adapter (like the existing HDX and DesInventar adapters in `to-rdls/sources/`)

2. **Richer loss data available**: DELTA's disaggregated human effects and public/private loss splits could produce much richer RDLS records than DesInventar currently allows

3. **Hazard classification mapping**: Need a HIP→RDLS hazard_type mapping table (HIP codes like MH0004 → RDLS codes like "coastal_flood")

4. **Cascading events**: DELTA's event chain model has no RDLS equivalent - would need to flatten or reference multiple hazard_types

5. **The RDLS `vulnerability` and `exposure` components remain unique** - DELTA doesn't replace these, it complements them with observed impact data

---

## Sources

- [DELTA Resilience - UNDRR](https://www.undrr.org/building-risk-knowledge/disaster-losses-and-damages-tracking-system-delta-resilience)
- [DELTA GitHub Repository](https://github.com/unisdr/delta) - schema in `app/drizzle/schema/` (40 tables)
- [DELTA User Guide PDF](../references/delta/DELTA-resilience-user-guide.pdf) - local reference
- [DELTA ER Diagram](../references/delta/DELTA_ER_DIAGRAM-1.pdf) - local reference
- [DELTA Data Model Diagram](../references/delta/DELTA-Data-model-updated.png) - local reference
- [WMO-CHE](https://community.wmo.int/en/activity-areas/drr/che) - hazard classification standard
- RDLS v0.3 Schema - `hdx_dataset_metadata_dump/rdls/schema/rdls_schema_v0.3.json`
- [UN Stats DELTA Presentation](https://unstats.un.org/sdgs/files/meetings/iaeg-sdgs-meeting-16/5c_DELTA-Resilience.pdf)
- [CEPAL DELTA Presentation](https://www.cepal.org/sites/default/files/events/files/s_3_delta_resilience_undrr.pdf)
