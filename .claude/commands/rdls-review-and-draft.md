# Review and Draft RDLS Metadata

Full end-to-end workflow: inventory data, review content, classify HEVL components, and draft RDLS v0.3 JSON metadata records.

## Input
$ARGUMENTS - path to a data folder or ZIP file containing project deliverable data

## Instructions

Execute these phases in sequence. After each phase, show the user key findings before proceeding.

### Phase 1: Inventory and automated review

Run both MCP tools in sequence:

1. **Inventory** - get the file listing:
   ```
   inventory_folder(path="$ARGUMENTS")
   ```

2. **Automated review** - classify with column-level detection:
   ```
   review_folder(path="$ARGUMENTS", max_inspect=30)
   ```
   The automated review now includes column-level HEVL detection and conflict flags.
   Pay attention to the "Column-Level Classification Evidence" section and any conflicts.

3. Present the automated review summary to the user.

### Phase 2: LLM validation of classification

1. **Raw inspection** - get unclassified data for your own analysis:
   ```
   inspect_folder_for_llm(path="$ARGUMENTS", max_inspect=30)
   ```

2. For each file group, validate the automated HEVL classification:
   - Check column headers against domain knowledge (see classification rules below)
   - Check if README/document context supports or contradicts the classification
   - Check for the common misclassification patterns:
     - Post-disaster assessment data misclassified as Hazard+Exposure (actually Loss)
     - "Key figures" / situation reports misclassified as all-HEVL (usually Loss)
     - Infrastructure datasets with operational status → Exposure + Loss (not just Exposure)

3. If disagreements exist, use your semantic classification. Document why.

**Classification priority**: Column headers > File content > Filename signals > Folder structure

### Phase 3: Draft RDLS metadata JSON

For each validated dataset group, build a draft RDLS JSON record following the v0.3 schema:

```json
{
  "datasets": [
    {
      "id": "rdls_{type}-{iso3}{org}_{slug}",
      "title": "...",
      "description": "...",
      "risk_data_type": ["hazard"],
      "spatial": {
        "scale": "...",
        "countries": ["ISO3"],
        "bbox": [west, south, east, north]
      },
      "license": "...",
      "attributions": [
        {"id": "attribution_publisher", "role": "publisher", "entity": {"name": "...", "url": "..."}},
        {"id": "attribution_creator", "role": "creator", "entity": {"name": "...", "url": "..."}},
        {"id": "attribution_contact", "role": "contact_point", "entity": {"name": "...", "url": "..."}}
      ],
      "resources": [...],
      "hazard": {...},
      "exposure": [...],
      "vulnerability": {...},
      "loss": {...}
    }
  ]
}
```

**ID naming convention**: `rdls_{type}-{iso3}{org}_{slug}`
- `{type}`: `hzd` (hazard), `exp` (exposure), `vln` (vulnerability), `lss` (loss)
- `{iso3}`: ISO 3166-1 alpha-3 country code, lowercase
- `{org}`: data provider abbreviation
- `{slug}`: short dataset descriptor

**Required fields**: id, title, risk_data_type, attributions (min 3: publisher, creator, contact_point), spatial, license, resources

**HEVL component blocks**:
- Only include the component blocks that match the classification
- `risk_data_type` values: `hazard`, `exposure`, `vulnerability`, `loss`
- Use RDLS codelists for hazard_type, process_type, exposure_category, etc.

Fill what you can from:
- Geospatial metadata (CRS → coordinate_system, bounds → bbox, resolution → spatial_resolution)
- Column headers (field names → metrics, dimensions)
- README/document text (project name, provider, contact, license)
- Filename patterns (return periods, scenarios, hazard types)

Mark unknown fields with placeholder comments like `"TODO: confirm with data provider"`.

### Phase 4: Validate and report

1. **Validate each draft** against the RDLS v0.3 schema:
   ```
   validate_record(record_path="path/to/draft.json")
   ```

2. **Write outputs** to `{folder}/_rdls_review/`:
   - `data_review.md` - full review document
   - `rdls_metadata_{dataset_name}.json` - one per dataset group
   - `review_summary.json` - machine-readable summary

3. Present a final summary:

```markdown
# Review Complete: {folder_name}

## Datasets Drafted
| # | ID | Type | HEVL | Valid | Errors | Confidence |

## Metadata Gaps
| Dataset | Missing Required | Action Needed |

## Classification Notes
- Any disagreements between automated and semantic classification
- Any conflicts flagged by column-level detection

## Next Steps
- Fields that need data provider confirmation
- Additional data or documentation needed
```
