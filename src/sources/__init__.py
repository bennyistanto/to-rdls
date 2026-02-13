"""
Source adapters for metadata crawling.

Each source module provides:
- A client class for API interaction
- normalize_record() to unwrap source-specific JSON
- extract_fields() to map source fields to a common structure
- Source-specific policy detection (e.g. OSM exclusion for HDX)
"""
