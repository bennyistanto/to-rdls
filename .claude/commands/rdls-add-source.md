# RDLS Add Source

Help add a new data source adapter to the to-rdls pipeline (following the source adapter pattern).

## Input
$ARGUMENTS - name of the new source (e.g., "geonode", "datacatalog", "ckan", "worldbank", "stac")

## Instructions

1. Study the existing source adapter pattern:
   - Read `to-rdls/src/sources/hdx.py` for the reference implementation (most complete adapter)
   - Read `to-rdls/configs/sources/hdx.yaml` for the config structure to follow
   - Read `to-rdls/src/sources/geonode.py` for the stub template (starting point for new adapters)

2. The new source adapter needs:
   - **Config YAML** (`configs/sources/{name}.yaml`):
     - api: base_url, rows_per_page, timeout, max_retries, rate_limit
     - field_paths: mapping from source field names to common field names
     - resource_fields: resource-level field paths
     - format_overrides: source-specific format mappings
   - **Python module** (`src/sources/{name}.py`):
     - `{Name}Config` dataclass with `from_yaml()` classmethod
     - `{Name}Client` class with rate limiting and retry logic
     - `normalize_{name}_record(raw)` → unwrap source response
     - `extract_{name}_fields(ds)` → common field dict with keys: id, name, title, notes, methodology, organization, org_name, license_title, groups, tags, resources, dataset_date, url

3. The common field dict must match what `classify.classify_dataset()` and `translate.build_rdls_record()` expect.

4. Generate the config YAML and Python module, following the source adapter pattern.

5. Update `src/sources/__init__.py` to expose the new adapter.
