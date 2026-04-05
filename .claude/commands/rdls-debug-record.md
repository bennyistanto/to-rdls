# RDLS Debug Record

Debug why an RDLS record failed validation or has low confidence.

## Input
$ARGUMENTS - path to an RDLS JSON record file, or a dataset ID

## Instructions

1. Load the RDLS record
2. Run schema validation against `to-rdls/schema/rdls_schema_v0.3.json`
3. For each error, trace the root cause:

**Schema validation errors**:
- Missing required field → check which pipeline step should have set it
- Invalid enum value → check against closed codelists in `configs/rdls_schema.yaml`
- Wrong type → check the translate or extract module that produced it
- Additional properties → field name typo or schema version mismatch

**Low confidence diagnosis**:
- Check `risk_data_type` - is the classification correct?
- Check HEVL blocks - are hazard_type/exposure_category populated?
- Check attributions - are publisher, creator, contact_point present?
- Check resources - do they have data_format and access_url?
- Check spatial - are countries set with valid ISO3 codes?

4. Trace back through the pipeline:
   - Was the source metadata sufficient? (check the original source record if available)
   - Did classification assign the right components?
   - Did extraction find the right signals? Check signal_dictionary patterns.
   - Did integration merge correctly?

5. **Check against known failure patterns** - these are the most common issues discovered across thousands of records:

   | Pattern | Root cause | Fix |
   |---------|-----------|-----|
   | `referenced_by.author_names` minItems | Empty optional arrays left in record | Remove `author_names: []` and `doi: ""` from `referenced_by` |
   | Loss `impact_and_losses` required | Loss entry built without required wrapper | Wrap loss metrics inside `impact_and_losses` object |
   | `losses/hazards/events/event_sets: []` | Optional arrays present but empty | Remove empty arrays entirely (minItems:1) |
   | `resources: []` | Record has no downloadable resources | Cannot be valid - move to non-RDLS or add resources |
   | `occurrence: {}` | Empty occurrence object in events | Known schema issue (minProperties:1) - pending schema revision, 2,690+ records blocked |
   | Country code `XKX` | Kosovo not in ISO 3166-1 alpha-3 | Filter or use closest valid code |

6. Suggest specific fixes:
   - Config changes (new patterns, mappings)
   - Code changes (module, function, line)
   - Data overrides (manual corrections)
   - Structural sanitization (can be automated via `_sanitize_record()` in the pipeline)
