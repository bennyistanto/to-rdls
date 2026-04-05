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

```
Hazard
└── event_sets[] → Event_set
    ├── id (string)
    ├── hazards[]
    │   ├── hazard_type (required) - from hazard_type codelist
    │   ├── hazard_process (required) - from process_type codelist
    │   └── intensity_measure (string) - e.g., "PGA:g", "wd:m"
    ├── analysis_type - probabilistic, deterministic, empirical
    ├── frequency_distribution - poisson, negative_binomial, etc.
    ├── seasonality - uniform, user_defined
    ├── calculation_method - simulated, observed, inferred
    ├── event_count (integer)
    ├── occurrence_range
    │   ├── start_date, end_date
    │   └── return_period (number, years)
    └── events[] → Event
        ├── id (string)
        ├── disaster_identifiers[]
        │   ├── scheme - GLIDE, EMDAT, USGS_EHP, Custom
        │   └── id (string)
        ├── calculation_method - simulated, observed, inferred
        ├── hazard
        │   ├── hazard_type, hazard_process
        │   └── intensity_measure
        ├── occurrence
        │   ├── time
        │   │   ├── start, end, duration
        │   │   └── date (string)
        │   └── probability
        │       ├── return_period (number)
        │       └── occurrence_probability (number)
        ├── description (string)
        └── footprint
            ├── bbox (required) - [minLon, minLat, maxLon, maxLat]
            ├── centroid - [lon, lat]
            └── geometry - GeoJSON
```

## Exposure block (array)

```
Exposure[] → Exposure_item
├── category (required) - from exposure_category codelist
├── taxonomy - GED4ALL, MOVER, GLIDE, EMDAT, OED, HAZUS, etc.
└── metrics[]
    ├── dimension (required) - structure, content, product, disruption, population, index
    └── quantity_kind (required) - count, area, length, monetary, time
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

```
Loss
└── losses[] → Losses
    ├── hazard_type - from hazard_type codelist
    ├── hazard_process - from process_type codelist
    ├── asset_category - from exposure_category codelist
    ├── asset_dimension - structure, content, product, disruption, population, index
    ├── impact_and_losses (required)
    │   ├── impact_type (required) - direct, indirect, total
    │   ├── impact_metric (required) - from impact_metric codelist
    │   ├── quantity_kind (required) - count, area, monetary, ratio, time
    │   ├── loss_type (required) - ground_up, insured, gross, count, net_precat, net_postcat
    │   ├── approach (required) - analytical, empirical, hybrid, judgement
    │   ├── currency (string) - ISO 4217, required if quantity_kind is monetary
    │   └── reference_year (integer) - year of monetary values
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
