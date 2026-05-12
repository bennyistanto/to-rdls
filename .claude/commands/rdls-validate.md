# RDLS Validate

Validate one or more RDLS records against the JSON Schema (auto-detects v0.3 or v1.0 from the `links` field).

## Input
$ARGUMENTS - path to a JSON file (single record or array) or a directory of JSON files

## Schema detection
- v1.0: `links[].href` contains `1__0__0` -> use `rdl-standard/schema/rdls_schema.json`
- v0.3: `links[].href` contains `0__3__0` or no links -> use `schema/rdls_schema_v0.3.json`
- Codelists for v1.0: load via `from src.codelists import load_codelists_v10` (reads rdl-standard CSVs)
- Codelists for v0.3: load from `configs/rdls_schema.yaml`

## Known schema quirk (v1.0)
The local `rdl-standard/schema/rdls_schema.json` has `links.href const: "0__2__0"` - a dev placeholder.
Our files correctly use `1__0__0`. Patch the schema before validating:
```python
href_node = schema["properties"]["links"]["prefixItems"][0]["properties"]["href"]
if href_node.get("const", "").endswith("0__2__0/rdls_schema.json"):
    href_node["const"] = "https://docs.riskdatalibrary.org/en/1__0__0/rdls_schema.json"
```

## Instructions

1. Detect schema version from the record's `links` field. Load appropriate schema.

2. Load and validate each record. For each, check:
   - **Schema compliance**: All required fields present, correct types, valid enum values
   - **Codelist compliance (v1.0)**: validate against live CSV codelists via `src.codelists`
     - `hazard_type`: `VALID_HAZARD_TYPES`
     - `exposure[].category`: `VALID_EXPOSURE_CATEGORIES`
     - `exposure[].metrics[].measurement.unit`: must be in `VALID_UNIT_CODES` (not an abbreviation like `m` or `ha`)
     - `lineage.sources[].type`: `VALID_SOURCE_TYPES` (dataset|model)
   - **Codelist compliance (v0.3)**: `hazard_type`, `process_type`, `exposure_category`, `risk_data_type`
   - **Constraint compliance**: process matches hazard type, metric_dimension valid for exposure_category
   - **Completeness**: Which optional but recommended fields are missing

3. Report results:
   - Total records checked, valid count, invalid count
   - For invalid records: list each error with JSON path, expected vs actual
   - Group errors by category (missing required, invalid enum, constraint violation, type mismatch)
   - Suggest specific fixes for each error

4. If validating a directory, produce a summary table:
   | File | Valid | Errors | Top Issue |

5. **Check for known structural issues** before reporting - these are common across sources and can be auto-fixed:
   - `referenced_by` with empty `author_names: []` or `doi: ""` → remove empty optionals (schema: minItems:1, minLength:1)
   - Loss entries missing required `impact_and_losses` wrapper object
   - Empty arrays (`losses: []`, `hazards: []`, `events: []`, `event_sets: []`) → remove entirely (schema: minItems:1)
   - `resources: []` → record can never be valid without resources
   - `occurrence: {}` → known schema issue (minProperties:1), flag but note pending schema revision
   - Country code `XKX` (Kosovo) → not in ISO 3166-1 alpha-3 codelist (249 codes)

   When suggesting fixes, recommend a **two-stage approach**: Stage A (structural sanitization - empty arrays, missing wrappers, invalid optionals) then Stage B (semantic autofix via `AutoFixer` - codelist matching, type coercion, defaults).
