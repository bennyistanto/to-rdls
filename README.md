# to-rdls: RDLS Metadata Transformation Toolkit

Transform dataset metadata from any data catalog into
[Risk Data Library Standard (RDLS)](https://docs.riskdatalibrary.org/) v1.0 (or legacy v0.3) JSON records, validated against what real consumers enforce.

> **Two pipeline generations.** **v1.0 (canonical)** - LLM-first, single-call classify + extract, producing RDLS v1.0 records. **v0.3 (legacy)** - regex + LLM hybrid, still used for some source ingestion. See [docs/pipelines/](docs/pipelines/v1.0-llm-first.md).

Maintained under GFDRR / World Bank's Digital Earth team. License: MPL-2.0.

## Overview

A modular, config-driven toolkit that evolved from the [HDX-RDLS Metadata Crawler](https://github.com/bennyistanto/hdx-metadata-crawler). It is **not** a Python package - it is a portable folder of `src/` modules, `scripts/` entry points, and YAML configs you can run alongside any project.

To date it has produced roughly **9,600 RDLS v1.0 records** across HDX (~8,300), GeoNode (10 portals), STAC/CoCliCo, and program datasets (NISMOD, Tomorrow Cities, DesInventar, MDG, WBG-UFRA, and the converted JKAN legacy catalogue).

## Key capabilities

- **Multi-source transformation** - HDX, GeoNode, STAC, CoCliCo, DesInventar, NISMOD (ICRA + SDK), MDG, WBG-UFRA, and v0.3 -> v1.0 conversion of legacy catalogues.
- **LLM-first pipeline (v1.0)** - a single Claude call classifies the RDLS components and extracts all HEVL fields in one pass, building complete v1.0 records (multi-return-period event sets, multiple exposure items, multiple loss entries).
- **HEVL extraction pipeline (v0.3)** - regex signal detection with 2/3-tier cascades for Hazard, Exposure, Vulnerability, Loss.
- **5-layer audit** (`src/audit.py`) - validates every record against (1) the published JSON Schema, (2) codelist membership, (3) semantic / cross-field rules, (4) resource media-type vs URL, and (5) Metadata-Editor + consumer (JKAN) business rules.
- **Validate + enrich** - `scripts/validate_records.py --enrich` applies safe post-conversion fixes (units, GED4ALL codes, media types, licenses) and reports what needs a human.
- **Data inventory and review** - folder/ZIP inspection with automated HEVL classification and gap analysis.
- **MCP server** - 5 tools for Claude-assisted data review and metadata authoring.
- **Config-driven** - all patterns, mappings, and thresholds live in YAML; no hardcoded rules.
- **Structured naming** - RDLS IDs `rdls_{types}-{iso3}_{org}_{slug}` with component encoding and collision handling.

## Structure

```
to-rdls/
├── src/                    # Importable library (no argparse, no side effects)
│   ├── utils.py            # Text processing, file I/O, nested-dict navigation
│   ├── codelists.py        # v1.0 codelist utilities (AUTHORITATIVE): normalise_unit(), VALID_* sets
│   ├── schema.py           # Schema loading, validate_record(), SchemaContext
│   ├── spatial.py          # Country/region -> ISO3, spatial block inference
│   ├── naming.py           # RDLS ID + filename generation, collision handling
│   ├── classify.py         # [v0.3] tag/keyword/org classification
│   ├── translate.py        # [v1.0] base record builder (entities, resources, field ordering)
│   ├── translate_v03.py    # [v0.3] base record builder
│   ├── llm_classify.py     # [v1.0] LLM-first classify + HEVL extract (single phase)
│   ├── extract.py          # [v1.0] HEVL block builders from the LLM response
│   ├── extract_hazard.py / extract_exposure.py / extract_vulnloss.py  # [v0.3] cascades
│   ├── integrate.py        # Shared: HEVL merge + risk_data_type reconciliation
│   ├── validate.py         # Pipeline-time autofix, confidence scoring, distribution
│   ├── audit.py            # v1.0: the 5-layer audit validator
│   ├── enrich.py           # Post-conversion enrichment fixes
│   ├── inventory.py / review.py / zipaccess.py   # Standalone data inspection
│   └── sources/            # Source adapters (hdx, geonode) + HDX pipeline extensions
├── configs/                # YAML configs (pipeline at root; per-source in configs/sources/)
├── scripts/                # Executable entry points (one file = one action)
├── schema/                 # RDLS v0.3 + v1.0 JSON Schemas and templates
├── notebooks/              # Interactive Jupyter notebooks only
├── docs/                   # Documentation (see below)
└── mcp_server.py           # MCP server (5 tools)
```

## Documentation

Start at **[docs/GETTING_STARTED.md](docs/GETTING_STARTED.md)**. Full map:

| Area | Document |
|------|----------|
| Setup + first run | [Getting Started](docs/GETTING_STARTED.md) |
| Capability overview | [Features](docs/FEATURES.md) |
| System design + data flow | [Architecture](docs/ARCHITECTURE.md) |
| **Pipelines** | [v1.0 LLM-first](docs/pipelines/v1.0-llm-first.md) · [v0.3 hybrid](docs/pipelines/v0.3-hybrid.md) · [Source adapters](docs/pipelines/sources.md) |
| **Reference** | [Modules](docs/reference/modules.md) · [Config](docs/reference/config.md) · [Schema (v0.3)](docs/reference/schema.md) · [v1.0 spec](docs/reference/v1.0-spec.md) · [Naming](docs/reference/naming.md) · [Constraints](docs/reference/constraints.md) · [Codelists](docs/reference/codelists.md) · [Signals](docs/reference/signals.md) |
| **Validation** | [5-layer audit](docs/validation/audit-layers.md) · [Validate + enrich](docs/validation/validate-and-enrich.md) |
| **Workflows** | [Convert a source](docs/workflows/convert-a-source.md) · [JKAN upload](docs/workflows/jkan-upload.md) |
| Gaps + roadmap | [Limitations and Roadmap](docs/LIMITATIONS_AND_ROADMAP.md) |
| Historical | [docs/archive/](docs/archive/) |

## Quick start

Set up the environment (see [Getting Started](docs/GETTING_STARTED.md) for the full geospatial stack), then:

```bash
# Run the v1.0 pipeline over a folder of source dataset JSON
python -m src path/to/folder

# Validate + audit v1.0 records (all 5 layers) and apply post-conversion enrichment
python scripts/validate_records.py --enrich "output/<collection>/**/*.json"
```

Validate a single record from Python:

```python
from src.schema import validate_record, load_json
schema = load_json("schema/rdls_schema_v1.0.json")
ok, errors = validate_record(record, schema)   # never wrap the record before validating
```

## Adding a new source

1. Create `src/sources/your_source.py` with a client class, `normalize_record()`, and `extract_fields()` returning the same keys as `extract_hdx_fields()`.
2. Create `configs/sources/your_source.yaml` with endpoints, format overrides, and field-path mappings.
3. The rest of the pipeline (classify, translate, HEVL extract, validate) runs unchanged.

See [docs/workflows/convert-a-source.md](docs/workflows/convert-a-source.md) for the end-to-end walkthrough.

## Dependencies

- **Core (pip)**: `pyyaml`, `requests`, `jsonschema` (Draft 2020-12), `anthropic`, `rapidfuzz`
- **Geospatial (conda)**: `gdal`, `rasterio`, `fiona`, `geopandas`, `shapely`, `pyproj`
- **Data + documents**: `pandas`, `openpyxl`, `netcdf4`, `xarray`, `pillow`, `python-docx`, `PyMuPDF`
- **MCP server**: `mcp`

Full environment in `environment.yml` / `requirements.txt`.

## License

Mozilla Public License 2.0. See LICENSE or [mozilla.org/MPL/2.0](https://www.mozilla.org/en-US/MPL/2.0/).

[![License: MPL 2.0](https://img.shields.io/badge/License-MPL_2.0-FF7139.svg?style=for-the-badge)](https://www.mozilla.org/en-US/MPL/2.0/)
