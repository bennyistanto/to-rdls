# RDLS Translate

Help translate a source dataset's metadata into an RDLS v0.3 record.

## Input
$ARGUMENTS - path to a source metadata file (JSON from any data catalog: HDX, GeoNode, CKAN, World Bank Data Catalog, etc.) or a description of the dataset

## Instructions

1. Read the source metadata and identify available fields
2. Map source fields to RDLS fields:
   - `title` → `title`
   - `notes`/`description` → `description`
   - `organization` → `attributions[].entity`
   - `license_title`/`license_id` → `license` (normalize using license_mapping)
   - `groups`/country tags → `spatial.countries[]` (ISO3 codes)
   - `resources` → `resources[]` with format, access_url, media_type

3. Classify the dataset:
   - Analyze title, tags, description for HEVL signals
   - Determine `risk_data_type` (hazard, exposure, vulnerability, loss, or multiple)
   - Extract component-specific metadata (hazard_type, exposure_category, etc.)

4. Build the RDLS record:
   - Generate a slug-based `id`
   - Fill all required fields
   - Add HEVL component blocks based on classification
   - Validate against schema before presenting

5. Present the draft record as formatted JSON with comments explaining mapping decisions
6. Flag any fields that need human review (low-confidence extractions, ambiguous mappings)
