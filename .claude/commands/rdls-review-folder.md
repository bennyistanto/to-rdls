# RDLS Review Folder

Review a World Bank project deliverable folder: inventory its contents, inspect documents, classify data by HEVL theme, identify metadata gaps, and generate draft RDLS JSON metadata.

## Input
$ARGUMENTS - path to a folder (or .zip file) containing project deliverable data

## Instructions

Execute these phases in sequence. After each phase, show the user the key findings before proceeding.

### MCP shortcut (preferred)

If the MCP server is available, use `inspect_folder_for_llm(path)` to run Phases 1-2 in a single call. This returns structured JSON with folder summary, file groups (with naming patterns like scenarios, return periods, hazard codes), file inspections (CRS, bounds, columns, band stats), README extractions, and RDLS context - but **no HEVL classification**, so you do the semantic classification yourself using the inspection data. Then skip to Phase 3.

If MCP is not available, fall back to the manual phases below.

### Phase 1: Inventory

1. Run the inventory module to scan the folder:
   ```python
   import sys
   sys.path.insert(0, "path/to/to-rdls")
   from src.inventory import inventory_folder
   md, rows, stats = inventory_folder("$ARGUMENTS", formats="json,md")
   ```
   If `src/inventory.py` is not available, run the scan manually: walk the directory tree, list all files with name, extension, size, modified date. Peek inside .zip files without extracting.

2. Present a summary: total files, total size, file format breakdown (by extension), folder structure tree.

### Phase 2: Content inspection & HEVL classification

For each file or logical group of files, determine what it contains and which RDLS component it belongs to:

1. **Geospatial files** (GeoTIFF, Shapefile, GeoJSON, GeoPackage, FGDB, NetCDF):
   - Read headers/metadata using rasterio, fiona, or GDAL (CRS, bounds, bands/layers, resolution, field schemas)
   - Classify: hazard rasters (flood depth, PGA, wind speed), exposure vectors (buildings, population, infrastructure), loss/impact results

2. **Tabular data** (XLSX, CSV):
   - Read headers and first few rows using pandas or openpyxl
   - Look for: vulnerability/fragility function columns (damage states, intensity measures), loss columns (economic loss, casualties), exposure attributes (building types, population counts)

3. **Documents** (PDF, DOCX):
   - Read text content to understand project context, methodology, data descriptions
   - Extract: project name, location, hazard types, time periods, data sources, licensing info

4. **Archive contents** (ZIP members):
   - Group by logical dataset based on naming patterns and folder structure

5. **Classify each group** using RDLS signal patterns:
   - **Hazard (H)**: flood depth, earthquake ground motion, wind speed, storm surge, drought indicators
   - **Exposure (E)**: building footprints, population, infrastructure, agriculture, land use
   - **Vulnerability (V)**: fragility curves, damage functions, vulnerability indices
   - **Loss (L)**: impact results, damage assessments, economic losses, casualty counts

6. Present results as a classification table:
   | Dataset/Group | Files | Format | HEVL | Confidence | Key Evidence |

### Phase 3: Gap analysis & resource grouping

1. **Identify metadata gaps** - for each dataset group, check what RDLS requires vs what's available:
   - Spatial coverage (country, bounding box) - can we determine from the data?
   - Temporal coverage (date range, reference year) - documented anywhere?
   - License - stated in reports or source?
   - Return periods / scenarios - for hazard data
   - Taxonomy / building typology - for exposure data
   - Currency and reference year - for loss/vulnerability data
   - Attribution (publisher, creator, contact point)

2. **Suggest resource grouping** - how to organize files into RDLS datasets:
   - Each logical dataset becomes one RDLS record with one or more resources
   - Group by: theme (H/E/V/L), geographic scope, scenario, and format
   - Name suggestion following RDLS naming convention: `rdls_{type}-{iso3}{org}_{slug}`

3. Present as a gap table:
   | Dataset | Missing Required | Missing Recommended | Action Needed |

### Phase 4: Draft RDLS metadata

For each identified dataset group:

1. **Build a draft RDLS JSON record** using the template structure:
   - `id`: generate using naming convention
   - `title`: derive from folder/file names and document context
   - `description`: summarize based on inspected content
   - `risk_data_type`: from HEVL classification
   - `spatial`: from geospatial file metadata (bbox, country)
   - `resources`: one per file/format in the group
   - `attributions`: from document context if available
   - Fill hazard/exposure/vulnerability/loss component blocks as appropriate

2. **Validate each record** against RDLS v0.3 schema:
   - Check if `to-rdls/schema/rdls_schema_v0.3.json` exists, use it
   - Otherwise validate against known required fields and codelists from CLAUDE.md
   - Report: valid fields, errors, warnings

3. **Write outputs** to `{folder}/_rdls_review/`:
   - `data_review_{timestamp}.md` - the full review document (like github_issue format)
   - `rdls_metadata_{dataset_name}.json` - one per dataset group
   - `review_summary.json` - machine-readable summary (datasets found, gaps, confidence)

### Output format

The data review markdown should follow this structure:
```markdown
# [Data Review] {Project Name} - {Location}

## Summary
- Data Provider, Location, License, Dataset count

## Dataset Inventory
| Dataset | Size | Formats | Description |

## Data Structure Details
### 1. Hazard Data
### 2. Exposure Data
### 3. Vulnerability Data
### 4. Loss/Impact Data

## Issues and Gaps Identified
### Critical Metadata Gaps
### Data Quality Issues
### Documentation Gaps

## RDLS Metadata Status
| Dataset | ID | Valid | Errors | Confidence |

## Recommended Actions
- Prioritized list of what to fix / request from data provider
```
