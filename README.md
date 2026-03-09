# to-rdls: RDLS v0.3 Metadata Transformation Toolkit

Transform metadata from various sources (HDX, GeoNode, etc.) into
[Risk Data Library Standard (RDLS)](https://docs.riskdatalibrary.org/) v0.3 JSON records.

## Overview

This is a modular, config-driven toolkit extracted from the HDX-RDLS Metadata
Crawler pipeline. It is **not** a Python package &mdash; it is a portable folder
of scripts and YAML configs that you can copy to any project.

## Structure

```
to-rdls/
├── src/                          # Python modules
│   ├── __init__.py               # Convenience imports
│   ├── utils.py                  # Text processing, file I/O, slug generation
│   ├── spatial.py                # Country/region → ISO3, spatial block inference
│   ├── schema.py                 # RDLS schema loading, validation
│   ├── classify.py               # Dataset classification (tag/keyword/org scoring)
│   ├── translate.py              # RDLS record builder (format, license, attributions)
│   ├── extract_hazard.py         # Hazard block extraction (2-tier cascade)
│   ├── extract_exposure.py       # Exposure block extraction (3-tier cascade)
│   ├── extract_vulnloss.py       # Vulnerability + Loss extraction
│   ├── integrate.py              # HEVL merge, risk_data_type reconciliation
│   ├── validate_qa.py            # Validation, confidence scoring, distribution
│   └── sources/
│       ├── hdx.py                # HDX: CKAN API client, OSM detection, field extraction
│       └── geonode.py            # GeoNode: stub for future implementation
│
├── configs/                      # YAML configuration files
│   ├── rdls_schema.yaml          # RDLS codelists (hazard_type, process_type, etc.)
│   ├── rdls_defaults.yaml        # Default mappings (hazard→process, metrics, weights)
│   ├── spatial.yaml              # Region→countries, country name fixes
│   ├── format_mapping.yaml       # Data format aliases, skip list, service URL patterns
│   ├── license_mapping.yaml      # License string → RDLS license code
│   ├── signal_dictionary.yaml    # HEVL extraction patterns
│   ├── classification.yaml       # Tag weights, keyword patterns, org hints
│   ├── pipeline.yaml             # Runtime settings (thresholds, output modes)
│   └── sources/
│       ├── hdx.yaml              # HDX-specific (API, OSM markers, format overrides)
│       └── geonode.yaml          # GeoNode-specific (stub)
│
└── notebooks/                    # Streamlined notebooks (future)
```

## Quick Start

```python
import sys
from pathlib import Path

# Add to-rdls to path
sys.path.insert(0, str(Path("to-rdls")))

from src.utils import load_json, load_yaml
from src.spatial import load_spatial_config
from src.schema import load_rdls_schema, validate_record
from src.translate import build_rdls_record, load_format_config, load_license_config
from src.extract_hazard import HazardExtractor, build_hazard_block
from src.sources.hdx import extract_hdx_fields, normalize_dataset_record

# Load configs
CONFIGS = Path("to-rdls/configs")
spatial_cfg = load_spatial_config(CONFIGS / "spatial.yaml")
format_cfg = load_format_config(CONFIGS / "format_mapping.yaml")
license_cfg = load_yaml(CONFIGS / "license_mapping.yaml")
signal_dict = load_yaml(CONFIGS / "signal_dictionary.yaml")
defaults = load_yaml(CONFIGS / "rdls_defaults.yaml")
schema = load_rdls_schema("path/to/rdls_schema_v0.3.json")

# Process a dataset
raw = load_json("path/to/hdx_dataset.json")
ds = normalize_dataset_record(raw)
fields = extract_hdx_fields(ds)

# Build base RDLS record
record = build_rdls_record(
    fields=fields,
    components=["hazard", "exposure"],
    spatial_config=spatial_cfg,
    format_config=format_cfg,
    license_config=license_cfg,
    source_base_url="https://data.humdata.org",
)

# Extract hazard block
extractor = HazardExtractor(signal_dict, defaults)
hazard_extraction = extractor.extract(ds)
hazard_block = build_hazard_block(hazard_extraction)

# Validate
is_valid, errors = validate_record(record, schema)
```

## Adding a New Source

To add support for a new metadata source (e.g., GeoNode):

1. Create `src/sources/your_source.py` with:
   - A client class for API interaction
   - `normalize_record()` to unwrap source-specific JSON
   - `extract_fields()` returning the same keys as `extract_hdx_fields()`

2. Create `configs/sources/your_source.yaml` with:
   - API endpoints and settings
   - Source-specific format name overrides
   - Source-specific field path mappings

3. The rest of the pipeline (classification, translation, HEVL extraction,
   validation) works identically with no changes needed.

## Dependencies

- Python 3.10+
- `pyyaml` (YAML config loading)
- `requests` (HTTP client for crawling)
- `jsonschema` (RDLS schema validation)
- `pycountry` (optional, country name resolution fallback)
- `pandas` (optional, CSV loading for country tables)
- `tqdm` (optional, progress bars)

## License

This project is licensed under the Mozilla Public License 2.0.
See LICENSE for the full license text or visit [https://www.mozilla.org/en-US/MPL/2.0/](https://www.mozilla.org/en-US/MPL/2.0/).

[![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-FF7139.svg?style=for-the-badge)](https://www.mozilla.org/en-US/MPL/2.0/)
