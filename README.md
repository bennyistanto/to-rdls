# to-rdls: RDLS v0.3 Metadata Transformation Toolkit

Transform metadata from various sources (HDX, GeoNode, etc.) into
[Risk Data Library Standard (RDLS)](https://docs.riskdatalibrary.org/) v0.3 JSON records.

## Overview

This is a modular, config-driven toolkit that evolved from the [HDX-RDLS Metadata Crawler](https://github.com/your-org/hdx-metadata-crawler) pipeline. While the crawler focused exclusively on HDX with a notebook-based approach, to-rdls redesigns the pipeline as a modular library supporting any metadata source.

It is **not** a Python package &mdash; it is a portable folder of scripts and YAML configs that you can copy alongside any project.

## Key Capabilities

- **Multi-source metadata transformation** &mdash; HDX (complete), DesInventar, NISMOD ICRA, GeoNode (stub)
- **HEVL extraction pipeline** &mdash; Regex-based signal detection with 2/3-tier cascades for Hazard, Exposure, Vulnerability, and Loss
- **LLM-assisted classification** &mdash; 4-phase pipeline solving content-blind over-classification via Claude Haiku
- **Schema validation and auto-fix** &mdash; 5-pass engine with confidence scoring and tiered distribution
- **Data inventory and review** &mdash; Folder/ZIP inspection with automated HEVL classification and gap analysis
- **MCP server** &mdash; 5 tools for Claude-assisted data review and metadata creation workflows
- **Config-driven design** &mdash; 14 YAML config files, no hardcoded patterns or mappings
- **Structured naming** &mdash; RDLS record IDs with component encoding, collision detection, and rebuild support

## Structure

```
to-rdls/
├── src/                          # Python modules (20 files)
│   ├── utils.py                  # Text processing, file I/O, slug generation
│   ├── spatial.py                # Country/region -> ISO3, spatial block inference
│   ├── schema.py                 # RDLS schema loading, validation, SchemaContext
│   ├── classify.py               # Dataset classification (tag/keyword/org scoring)
│   ├── translate.py              # RDLS record builder (format, license, attributions)
│   ├── extract_hazard.py         # Hazard block extraction (2-tier cascade)
│   ├── extract_exposure.py       # Exposure block extraction (3-tier cascade)
│   ├── extract_vulnloss.py       # Vulnerability + Loss extraction
│   ├── integrate.py              # HEVL merge, risk_data_type reconciliation
│   ├── naming.py                 # RDLS ID + filename generation, collision detection
│   ├── validate_qa.py            # Validation, auto-fix, confidence scoring, distribution
│   ├── inventory.py              # Folder/ZIP inventory generator (stdlib only)
│   ├── review.py                 # File inspection, HEVL classification, gap analysis
│   ├── zipaccess.py              # ZIP member extraction (supports nested ZIPs)
│   ├── hdx_review.py             # Second-pass HEVL review (RDLS + HDX cross-ref)
│   ├── ckan_columns.py           # CKAN column header fetcher with disk cache
│   ├── llm_review.py             # 4-phase LLM classification pipeline
│   └── sources/
│       ├── hdx.py                # HDX: CKAN API client, OSM detection, field extraction
│       └── geonode.py            # GeoNode: stub for future implementation
│
├── configs/                      # YAML configuration files (14 files)
│   ├── signal_dictionary.yaml    # HEVL extraction patterns (regex -> RDLS codelist)
│   ├── rdls_defaults.yaml        # Default mappings, constraint tables
│   ├── rdls_schema.yaml          # RDLS codelists (hazard_type, process_type, etc.)
│   ├── classification.yaml       # Tag weights, keyword patterns, org hints
│   ├── naming.yaml               # Record ID format, component codes, org abbreviations
│   ├── pipeline.yaml             # Runtime thresholds, output modes, distribution tiers
│   ├── format_mapping.yaml       # Data format aliases, skip list, service URL patterns
│   ├── license_mapping.yaml      # License string -> RDLS license code
│   ├── spatial.yaml              # Region->countries, country name fixes
│   ├── llm_review.yaml           # LLM model, phase thresholds, cost guardrails
│   ├── review_knowledge.yaml     # File inspection patterns for review module
│   └── sources/
│       ├── hdx.yaml              # HDX-specific (API, OSM markers, format overrides)
│       └── geonode.yaml          # GeoNode-specific (stub)
│
├── notebooks/                    # Pipeline scripts and interactive notebooks
│   ├── rdls_hdx_llm_review.py          # LLM review pipeline (4 phases)
│   ├── rdls_hdx_sanitize_validate.py   # Post-LLM sanitization and validation
│   ├── rdls_desinventar_01_*.py        # DesInventar loss record generation
│   ├── rdls_nismod_01_*.py             # NISMOD ICRA record generation
│   ├── rdls_validate_metadata.ipynb    # Interactive metadata validator
│   └── rdls_data_inventory_contents.ipynb  # Data inventory notebook
│
├── schema/                       # RDLS v0.3 JSON Schema
├── mcp_server.py                 # MCP server (5 tools for Claude workflows)
├── environment.yml               # Conda environment (Python 3.12, geospatial stack)
└── requirements.txt              # Pip dependencies
```

## Documentation

| Document | Description |
|----------|-------------|
| [Getting Started](docs/GETTING_STARTED.md) | Installation, setup, first pipeline run |
| [Features](docs/FEATURES.md) | Complete capability overview |
| [Architecture](docs/ARCHITECTURE.md) | Design principles, pipeline data flow, extension points |
| [Module Reference](docs/MODULE_REFERENCE.md) | Each src/ module: purpose, functions, dataclasses |
| [Config Reference](docs/CONFIG_REFERENCE.md) | Each YAML config: structure, fields, how to modify |
| [Limitations and Roadmap](docs/LIMITATIONS_AND_ROADMAP.md) | Current gaps, pending work, near-term roadmap |
| [LLM Review Guide](docs/llm_review_guide.md) | Operations guide for the LLM classification pipeline |
| [DELTA vs RDLS Comparison](docs/delta_vs_rdls_system_comparison.md) | System-level comparison with UNDRR DELTA |

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

### Core (pip)
- `pyyaml>=6.0` &mdash; YAML config loading
- `requests>=2.28` &mdash; HTTP client for API crawling
- `jsonschema>=4.20` &mdash; RDLS schema validation (Draft 2020-12)
- `anthropic>=0.40` &mdash; Claude API client (LLM review pipeline)
- `rapidfuzz>=3.5` &mdash; Fuzzy codelist matching

### Geospatial (conda)
- `gdal>=3.8`, `rasterio>=1.3`, `fiona>=1.9`, `geopandas>=0.14` &mdash; Geospatial file inspection
- `shapely>=2.0`, `pyproj>=3.6` &mdash; Geometry and projection handling

### Data and Documents (conda + pip)
- `pandas>=2.1`, `openpyxl>=3.1`, `xlrd>=2.0` &mdash; Tabular data handling
- `netcdf4>=1.6`, `xarray>=2024.1` &mdash; Scientific data formats
- `pillow>=10.0`, `python-docx>=1.1`, `PyMuPDF>=1.24` &mdash; Document reading (images, DOCX, PDF)

### MCP Server
- `mcp>=1.2.0` &mdash; Claude Code MCP server SDK

## License

This project is licensed under the Mozilla Public License 2.0.
See LICENSE for the full license text or visit [https://www.mozilla.org/en-US/MPL/2.0/](https://www.mozilla.org/en-US/MPL/2.0/).

[![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-FF7139.svg?style=for-the-badge)](https://www.mozilla.org/en-US/MPL/2.0/)
