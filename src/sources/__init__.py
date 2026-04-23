"""
Source adapters for metadata crawling.

Each source module provides:
- A config dataclass loaded from configs/sources/{name}.yaml
- A client class for API interaction with rate limiting and retries
- normalize_record() to unwrap source-specific JSON
- extract_fields() to map source fields to a common dict structure
- Source-specific policy detection (e.g. OSM exclusion for HDX)

Available adapters:
- hdx: HDX/CKAN API (reference implementation)
- geonode: GeoNode REST API v2 (multi-portal, ISO 19115 metadata)
"""
