# Getting Started

This guide covers how to set up and run the to-rdls toolkit.

---

## Prerequisites

- **Python 3.10+** (3.12 recommended)
- **conda** (Miniforge or Miniconda) for the geospatial stack (GDAL, rasterio, fiona, geopandas)
- **Anthropic API key** (only needed for the LLM review pipeline)

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/your-org/to-rdls.git
cd to-rdls
```

### 2. Create the conda environment

The `environment.yml` includes the full geospatial and document processing stack:

```bash
conda env create -f environment.yml
conda activate to-rdls
```

This installs: GDAL, rasterio, fiona, geopandas, pandas, numpy, openpyxl, NetCDF4, xarray, Pillow, python-docx, PyMuPDF, PyYAML, jsonschema, rapidfuzz, and MCP server SDK.

### 3. Install additional pip dependencies

```bash
pip install -r requirements.txt
```

This adds: requests, anthropic (Claude API client).

### 4. Set PYTHONPATH

The toolkit is not a Python package. You need to add the repo root to your Python path:

**Linux/macOS:**
```bash
export PYTHONPATH="/path/to/to-rdls:$PYTHONPATH"
```

**Windows (Command Prompt):**
```cmd
set PYTHONPATH=C:\path\to\to-rdls;%PYTHONPATH%
```

**Windows (PowerShell):**
```powershell
$env:PYTHONPATH = "C:\path\to\to-rdls;$env:PYTHONPATH"
```

Alternatively, add `sys.path.insert(0, "path/to/to-rdls")` at the top of your scripts.

---

## Running the HDX Pipeline

The HDX pipeline processes metadata from the Humanitarian Data Exchange into RDLS v0.3 JSON records. The pipeline runs as a sequence of notebook scripts.

### Step 1: Prepare inputs

You need:
- HDX dataset metadata (from hdx-metadata-crawler or CKAN API dump)
- RDLS v0.3 JSON Schema (in `schema/rdls_schema_v0.3.json`)

### Step 2: Run the pipeline scripts

```bash
# Classification, translation, HEVL extraction, integration, validation
# (These correspond to hdx-metadata-crawler notebooks 06-13,
#  now implemented as src/ modules)
python notebooks/rdls_hdx_sanitize_validate.py
```

### Step 3: Run LLM review (optional)

The LLM review pipeline reclassifies borderline records using Claude Haiku:

```bash
# Set API key
export ANTHROPIC_API_KEY="sk-ant-..."

# Run 4-phase pipeline
python notebooks/rdls_hdx_llm_review.py
```

This will:
1. Triage records via regex scoring (free)
2. Load cached column headers (free after first build)
3. Classify borderline records via Claude Haiku (~$22 for 12,594 records)
4. Merge results, rebuild IDs, validate, and distribute

See [llm_review_guide.md](llm_review_guide.md) for detailed step-by-step instructions.

### Step 4: Validate and distribute

```bash
python notebooks/rdls_hdx_sanitize_validate.py
```

This sanitizes schema violations, renames files per updated risk_data_type, and distributes to `output/llm/dist/high/` (valid) and `output/llm/dist/invalid/` (needs fixes).

---

## Running Standalone Tools

### Data Inventory (CLI)

Generate a Markdown + CSV/JSON inventory of any data folder or ZIP:

```bash
python -m src path/to/data/folder
```

Options:
- `--formats json,md,csv` — Output formats to generate
- `--hash` — Include SHA256 checksums
- `--no-zip` — Skip ZIP inspection

### Metadata Validator (Notebook)

Open `notebooks/rdls_validate_metadata.ipynb` in Jupyter for interactive validation of RDLS JSON records with auto-fix suggestions.

### MCP Server

Start the MCP server for Claude-assisted workflows:

```bash
conda run --no-banner -n to-rdls python mcp_server.py
```

Or configure in `.mcp.json` for Claude Code:

```json
{
  "mcpServers": {
    "to-rdls": {
      "command": "C:\\path\\to\\conda.exe",
      "args": ["run", "--no-banner", "-n", "to-rdls", "python", "C:\\path\\to\\mcp_server.py"]
    }
  }
}
```

---

## Running Non-HDX Sources

### DesInventar Loss Records

Generate RDLS loss records from UNDRR DesInventar national disaster loss databases:

```bash
python notebooks/rdls_desinventar_01_generate_records.py
```

Output: per-country loss records in `output/desinventar/metadata/`.

### NISMOD ICRA Records

Generate RDLS hazard+exposure records for all countries using the NISMOD ICRA template:

```bash
# One-time setup: generate country bounding boxes and GeoNames lookup
python notebooks/rdls_nismod_00a_generate_country_bbox.py
python notebooks/rdls_nismod_00b_generate_geonames_lookup.py

# Generate records
python notebooks/rdls_nismod_01_generate_icra_records.py
```

Output: per-country NISMOD ICRA records in `output/nismod_icra/`.

---

## Output Structure

Pipeline outputs are organized in `output/`:

```
output/
├── hdx/                    # HDX pipeline outputs
│   ├── revised/            # RDLS JSON records (post-LLM if applicable)
│   └── dist/               # Validated + distributed
│       ├── high/           # Schema-valid, high confidence
│       ├── medium/         # Schema-valid, medium confidence
│       └── invalid/        # Schema-invalid
│
├── llm/                    # LLM review outputs
│   ├── revised/            # Reclassified records
│   ├── not_rdls/           # Non-disaster datasets
│   ├── dist/               # Validated distribution
│   └── reports/            # Review reports, disagreement logs
│
├── column_cache/           # CKAN column header cache
├── desinventar/            # DesInventar outputs
└── nismod_icra/            # NISMOD ICRA outputs
```

---

## Common Issues

### PYTHONPATH not set

```
ModuleNotFoundError: No module named 'src'
```

**Fix:** Set `PYTHONPATH` to include the to-rdls root directory (see Installation step 4).

### OneDrive file locks (Windows)

OneDrive may lock files during sync, causing `PermissionError` on write operations.

**Fix:** Pause OneDrive sync during pipeline runs, or use a non-OneDrive directory for output.

### XKX (Kosovo) country code

Kosovo uses `XKX` which is not in ISO 3166-1 alpha-3. The pipeline handles it via `configs/spatial.yaml` country name fixes, but `pycountry` lookups will return `None`.

**Fix:** Already handled in config. No action needed unless adding new Kosovo datasets.

### LLM API errors

```
anthropic.NotFoundError: 404 model not found
```

**Fix:** Use `claude-haiku-4-5-20251001` (not the older `-20250414` version which was retired). Check `configs/llm_review.yaml` for the correct model name.

### Rate limiting

```
anthropic.RateLimitError: 429
```

**Fix:** The pipeline includes a 1.5-second pause between batches. If still hitting limits, increase `delay_seconds` in `configs/llm_review.yaml`.
