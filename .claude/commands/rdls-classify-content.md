# Classify Content

Content-driven HEVL classification: inspect actual data content (column headers, field names, band metadata) and compare with the automated classifier output.

## Input
$ARGUMENTS - path to a data folder or ZIP file

## Instructions

### Step 1: Run automated inspection (raw data, no classification)

Use the `inspect_folder_for_llm` MCP tool:
```
inspect_folder_for_llm(path="$ARGUMENTS", max_inspect=30)
```

This returns structured inspection data WITHOUT automated classification - column headers, CRS, bounds, band stats, naming patterns, and README extractions.

### Step 2: Run automated review (with classification)

Use the `review_folder` MCP tool:
```
review_folder(path="$ARGUMENTS", max_inspect=30)
```

This returns the automated HEVL classification using YAML signal patterns, including the new column-level detection and conflict flags.

### Step 3: Semantic classification from column headers

For each file group, analyze the actual column headers/field names returned by `inspect_folder_for_llm` and classify using these domain rules:

**Loss indicators** (most commonly misclassified):
- Post-disaster assessment columns: `damage_status`, `building_damaged`, `open_closed`, `status_post_earthquake` → **Loss**
- Mortality/displacement: `death`, `killed`, `injured`, `displaced`, `total_affected`, `idp`, `evacuated` → **Loss**
- Economic impact: `economic_loss`, `damage_cost`, `repair_cost`, `insured_loss` → **Loss**
- Agricultural loss: `crop_loss`, `livestock_death`, `food_insecurity`, `harvest_loss` → **Loss**
- Key figures / situation reports: almost always **Loss** even if title mentions hazard name
- Catastrophe model outputs: `ds1-ds4`, `total_damage`, `loss_ratio`, `aal` → **Loss**

**Exposure indicators**:
- Building attributes: `bldid`, `repvalue`, `occupancy`, `construction_type`, `floor_area`, `tiv` → **Exposure**
- Population: `hhid`, `household`, `population`, `census`, `demographic` → **Exposure**
- Infrastructure: `facility_name`, `facility_type`, `hospital_name`, `bed_capacity` → **Exposure**
- Agriculture: `crop_type`, `land_use`, `hectare`, `livestock_count` → **Exposure**

**Hazard indicators**:
- Intensity measures: `water_depth`, `hmax`, `pga`, `wind_speed`, `wave_height`, `velocity` → **Hazard**
- Probability: `return_period`, `annual_probability`, `rp10`, `rp50`, `rp100` → **Hazard**
- Classification: `hazard_type`, `event_type`, `flood_zone`, `susceptibility` → **Hazard**

**Vulnerability indicators**:
- Fragility parameters: `muds1-4`, `sigmads`, `damage_ratio`, `fragility`, `limit_state` → **Vulnerability**
- Capacity indices: `coping_capacity`, `vulnerability_index`, `adaptive_capacity`, `svi` → **Vulnerability**

**Critical rule**: Column headers > filename signals when they conflict. A file titled "earthquake_health_facility" with columns like `Building_Damaged_Severe`, `Status_Post_Earthquake`, `Open_Closed` is **Loss** data, not Hazard+Exposure.

### Step 4: Compare and highlight disagreements

For each file group, compare:
1. **Automated classification** (from `review_folder`) - HEVL, confidence, evidence
2. **Your semantic classification** (from Step 3) - HEVL, reasoning

Present as a comparison table:
| Group | Automated HEVL | Your HEVL | Agree? | Disagreement Details |

For any disagreements:
- Explain WHY the automated classifier got it wrong (e.g., "title contains 'earthquake' → H, but columns show post-disaster damage assessment → L")
- Note which source is more reliable (columns vs filename vs README context)
- Flag the group's `conflicts` field from the automated review if present

### Step 5: Generate pattern feedback

For any misclassifications or gaps found, suggest new YAML patterns that would fix them:

```yaml
# Suggested additions to configs/review_knowledge.yaml → column_detection
new_group_name:
  component: L  # or H, E, V
  weight: high
  label: "Descriptive label"
  patterns:
    - "regex_pattern_1"
    - "regex_pattern_2"
```

### Output format

```markdown
# Content-Driven Classification: {folder_name}

## Automated vs Semantic Comparison
| Group | Auto HEVL | Semantic HEVL | Match | Notes |

## Disagreements (if any)
### {Group Name}
- **Automated says**: ...
- **Columns say**: ...
- **Correct classification**: ...
- **Root cause**: ...

## Suggested Pattern Updates
(YAML snippets for review_knowledge.yaml)

## Summary
- Groups analyzed: N
- Agreements: N
- Disagreements: N
- New patterns suggested: N
```
