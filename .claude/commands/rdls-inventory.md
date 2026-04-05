# RDLS Inventory

Scan a data delivery folder or ZIP archive and produce a structured inventory for RDLS metadata planning.

## Input
$ARGUMENTS - path to a folder or .zip file containing project deliverable data

## Instructions

### MCP shortcut (preferred)

If the MCP server is available, use `inventory_folder(path)` to scan the folder and return a structured inventory with file counts, format breakdown, and size stats.

For deeper inspection with HEVL classification prep, use `inspect_folder_for_llm(path)` which adds CRS, bounds, columns, band stats, README extractions, and naming pattern detection.

### Manual approach

1. Run the inventory module:
   ```python
   import sys
   sys.path.insert(0, "path/to/to-rdls")
   from src.inventory import inventory_folder
   md, rows, stats = inventory_folder("$ARGUMENTS", formats="json,md")
   ```
   Or via CLI:
   ```bash
   python -m src "$ARGUMENTS"
   ```

2. Review the inventory output:
   - **File count by format**: GeoTIFF, Shapefile, CSV, XLSX, NetCDF, PDF, etc.
   - **Total size** and per-format breakdown
   - **Folder structure tree** showing logical organization
   - **ZIP contents**: members listed without extraction

3. Group files by logical dataset:
   - Look for naming patterns (scenarios, return periods, country codes)
   - Identify intermediate/temporary files vs final outputs
   - Flag README, metadata, and documentation files

4. For each group, determine:
   - Likely RDLS risk_data_type (H/E/V/L)
   - Geographic scope (from file names or folder structure)
   - Temporal scope (from dates in names or metadata)

5. Output a summary table:
   | Group | Files | Format | Size | Likely HEVL | Notes |

6. Suggest next steps:
   - Use `/rdls-review-folder` for full inspection + HEVL classification + gap analysis
   - Use the `data-reviewer` agent for semantic classification with domain knowledge
   - Use `/rdls-llm-review` if these are HDX-sourced records needing reclassification
