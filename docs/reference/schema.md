# RDLS v0.3 JSON Schema Reference

Complex object structures from `to-rdls/schema/rdls_schema_v0.3.json`.

## Top-level dataset structure

```
Dataset (root)
├── id (string, required) - URI, URN, or DOI
├── title (string, required)
├── description (string)
├── risk_data_type (array, required) - ["hazard", "exposure", ...]
├── version (string)
├── purpose (string)
├── project {name, url}
├── details (string)
├── spatial → Location (required)
├── license (string, required) - from codelist
├── license_url (string) - required when license="Custom"
├── attributions[] → Attribution (required, min 3 roles)
├── sources[] → Source
├── referenced_by[] → Related_resource
├── resources[] → Resource (required)
├── hazard → Hazard (if risk_data_type includes "hazard")
├── exposure[] → Exposure_item (array, if "exposure")
├── vulnerability → Vulnerability (if "vulnerability")
├── loss → Loss (if "loss")
└── links[] → Link (first must be schema describedby link)
```

## Location object

```
Location
├── scale - global, regional, national, sub-national, urban
├── countries[] - ISO 3166-1 alpha-3 codes
├── bbox - [minLon, minLat, maxLon, maxLat]
├── centroid - [lon, lat]
├── geometry - GeoJSON geometry object
└── gazetteer_entries[]
    ├── scheme - ISO 3166-2, NUTS, ISO 3166-1, GEONAMES, OSMN, OSMR
    ├── id (string)
    └── name (string)
```

## Attribution object

```
Attribution
├── id (string)
├── role (required) - publisher, creator, contact_point, sponsor, funder, custodian, processor, etc. (25 roles)
├── entity (required)
│   ├── name (string, required)
│   ├── email (string)
│   └── url (string)
├── date (string)
└── details (string)
```

Required roles: publisher, creator, contact_point (minimum 3 attributions)

## Resource object

```
Resource
├── id (string, required)
├── title (string, required)
├── description (string, required)
├── data_format (string, required) - from data_format codelist
├── access_modality - file_download, API, WMS, WFS, WCS, STAC, etc.
├── access_url (string)
├── download_url (string)
├── coordinate_system (string) - e.g., "EPSG:4326"
├── spatial_resolution (string) - e.g., "90m", "0.25deg"
└── temporal
    ├── start (date)
    ├── end (date)
    ├── duration (string)
    └── resolution (string)
```

## Hazard block

**CRITICAL: Hazard entries use `type` NOT `hazard_type`, and `hazard_process` NOT `process_type`.**

```
Hazard
└── event_sets[] → Event_set (required: id, hazards, analysis_type)
    ├── id (string, REQUIRED)
    ├── hazards[] (REQUIRED, min 1)
    │   ├── id (string, REQUIRED)
    │   ├── type (REQUIRED) - from hazard_type codelist ⚠️ NOT "hazard_type"
    │   ├── hazard_process (REQUIRED) - from process_type codelist ⚠️ NOT "process_type"
    │   ├── intensity_measure (string) - e.g., "PGA:g", "wd:m"
    │   └── trigger {type, hazard_process}
    ├── analysis_type (REQUIRED) - probabilistic, deterministic, empirical
    ├── frequency_distribution - poisson, negative_binomial, etc.
    ├── seasonality - uniform, user_defined
    ├── calculation_method - simulated, observed, inferred
    ├── event_count (integer)
    ├── occurrence_range
    │   ├── start_date, end_date
    │   └── return_period (number, years)
    └── events[] → Event (required: id, calculation_method, hazard, occurrence)
        ├── id (string, REQUIRED)
        ├── calculation_method (REQUIRED) - simulated, observed, inferred
        ├── hazard (REQUIRED) - full Hazard object, same structure as above:
        │   ├── id (REQUIRED)
        │   ├── type (REQUIRED) ⚠️ NOT "hazard_type"
        │   └── hazard_process (REQUIRED) ⚠️ NOT "process_type"
        ├── occurrence (REQUIRED, minProperties: 1)
        │   ├── probabilistic (use when analysis_type=probabilistic)
        │   │   ├── return_period (number)
        │   │   ├── probability (number)
        │   │   └── span (number) - required when probability is used
        │   ├── empirical (use when analysis_type=empirical)
        │   │   ├── temporal {start, end}
        │   │   └── return_period (number)
        │   └── deterministic (use when analysis_type=deterministic)
        │       ├── index_criteria (string)
        │       └── thresholds[] (string array)
        ├── disaster_identifiers[]
        │   ├── scheme - GLIDE, EMDAT, USGS_EHP, Custom
        │   └── id (string)
        └── description (string)
```

## Exposure block (array)

```
Exposure[] → Exposure_item (required: id, category)
├── id (string, REQUIRED)
├── category (REQUIRED) - from exposure_category codelist
├── taxonomy - GED4ALL, MOVER, GLIDE, EMDAT, OED, HAZUS, etc.
└── metrics[] → Metric (required: id, dimension, quantity_kind)
    ├── id (string, REQUIRED)
    ├── dimension (REQUIRED) - structure, content, product, disruption, population, index
    └── quantity_kind (REQUIRED) - count, area, length, monetary, time
```

## Vulnerability block

```
Vulnerability
├── functions[]
│   └── VulnerabilityFunction
│       ├── approach - analytical, empirical, hybrid, judgement
│       ├── relationship - math_parametric, math_bespoke, discrete
│       ├── hazard_primary (required) - from hazard_type codelist
│       ├── hazard_secondary
│       ├── hazard_process_primary - from process_type codelist
│       ├── hazard_process_secondary
│       ├── hazard_analysis_type - probabilistic, deterministic, empirical
│       ├── intensity_measure (string)
│       ├── category - from exposure_category codelist
│       ├── impact_type - direct, indirect, total
│       ├── impact_modelling - simulated, observed, inferred
│       ├── impact_metric - from impact_metric codelist (21 values)
│       ├── quantity_kind - count, area, length, monetary, time, ratio
│       ├── taxonomy - from taxonomy codelist
│       └── analysis_details (string)
└── socioeconomic_vulnerability_indices[]
    ├── indicator_name (string)
    ├── indicator_code (string) - e.g., POV_HEADCOUNT, HDI, SVI
    ├── classification_scheme (string) - e.g., INFORM, IPC
    ├── description (string)
    └── data_source (string)
```

## Loss block

**CRITICAL: impact_and_losses uses `impact_type` NOT `type`, `impact_metric` NOT `metric`, `loss_approach` NOT `approach`.**

```
Loss
└── losses[] → Losses (required: id, hazard_type, asset_category, asset_dimension, impact_and_losses)
    ├── id (string, REQUIRED)
    ├── hazard_type (REQUIRED) - from hazard_type codelist
    ├── hazard_process - from process_type codelist
    ├── asset_category (REQUIRED) - from exposure_category codelist
    ├── asset_dimension (REQUIRED) - structure, content, product, disruption, population, index
    ├── impact_and_losses (REQUIRED, all 7 sub-fields required)
    │   ├── impact_type (REQUIRED) - direct, indirect, total ⚠️ NOT "type"
    │   ├── impact_modelling (REQUIRED) - inferred, observed, simulated
    │   ├── impact_metric (REQUIRED) - from impact_metric codelist ⚠️ NOT "metric"
    │   ├── quantity_kind (REQUIRED) - count, area, monetary, ratio, time
    │   ├── loss_type (REQUIRED) - ground_up, insured, gross, count, net_precat, net_postcat
    │   ├── loss_approach (REQUIRED) - analytical, empirical, hybrid, judgement ⚠️ NOT "approach"
    │   ├── loss_frequency_type (REQUIRED) - probabilistic, deterministic, empirical
    │   └── currency (string) - ISO 4217, required if quantity_kind is monetary
    ├── reference_year (integer)
    ├── lineage (string)
    └── description (string)
```

## Links (required first entry)

```
links[] → Link
├── href (required) - URL to schema or related resource
├── rel (required) - "describedby" for schema link
└── type - media type, e.g., "application/json"
```

First link must be: `{"href": "...", "rel": "describedby", "type": "application/json"}`

---

## Layer 3 — Closed codelist fields to verify after schema validation

The JSON Schema may accept strings that are NOT in the closed codelist. Always check these fields proactively:

| Field | Location | Valid values |
|-------|----------|-------------|
| `risk_data_type` | root (array) | `["hazard"]`, `["exposure"]`, `["vulnerability"]`, `["loss"]` |
| `hazard.event_sets[].hazards[].type` | hazard block | 11 hazard_type values |
| `hazard.event_sets[].hazards[].hazard_process` | hazard block | 32 process_type values |
| `hazard.event_sets[].analysis_type` | event_set | `probabilistic`, `deterministic`, `empirical` |
| `exposure[].category` | exposure block | 7 exposure_category values |
| `vulnerability.functions.vulnerability[].approach` | vuln block | `analytical`, `empirical`, `hybrid`, `judgement` |
| `vulnerability.functions.vulnerability[].relationship` | vuln block | `math_parametric`, `math_bespoke`, `discrete` |
| `vulnerability.functions.vulnerability[].hazard_analysis_type` | vuln block | `probabilistic`, `deterministic`, `empirical` |
| `vulnerability.functions.vulnerability[].category` | vuln block | 7 exposure_category values |
| `vulnerability.functions.vulnerability[].impact_type` | vuln block | `direct`, `indirect`, `total` |
| `vulnerability.functions.vulnerability[].impact_modelling` | vuln block | `inferred`, `observed`, `simulated` |
| `vulnerability.functions.vulnerability[].impact_metric` | vuln block | 21 impact_metric values |
| `loss.losses[].hazard_type` | loss block | 11 hazard_type values |
| `loss.losses[].impact_and_losses.impact_type` | loss block | `direct`, `indirect`, `total` |
| `loss.losses[].impact_and_losses.impact_modelling` | loss block | `inferred`, `observed`, `simulated` |
| `loss.losses[].impact_and_losses.impact_metric` | loss block | 21 impact_metric values |
| `loss.losses[].impact_and_losses.loss_frequency_type` | loss block | `probabilistic`, `deterministic`, `empirical` |
| `resources[].data_format` | resources | 20+ values from data_format codelist |
| `license` | root | CC-BY-4.0, CC-BY-SA-4.0, CC0-1.0, ODbL-1.0, etc. |
