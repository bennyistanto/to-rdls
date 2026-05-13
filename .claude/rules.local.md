# Local Project Rules
> Persistent rules and lessons learned from working in this repo.
> Read this at session start alongside HANDOFF.md.
> Unlike HANDOFF.md (session state), this file accumulates knowledge across sessions.
> Unlike CLAUDE.md (deployed from ai-config), this is local and never overwritten.

## Session Management Rules

- **HANDOFF.md is a master index** — task-specific state goes in `HANDOFF-{task}.md` files (portal, metadata, llm, review, triage, mcp)
- **Never re-explore** what's documented in any HANDOFF-*.md file
- **Validate as you go** — run Layer 1 + Layer 2 immediately after drafting any record; never batch all validation at the end
- **Fix one thing, verify immediately** — when fixing a bug, run the pipeline and check for regressions before moving on
- **temp/ subdirs**: geonode/, pcrafi/, llm/, catastrophi/, audit/, test/, reports/, notes/ — scripts in subdirs use `parent.parent.parent` for PROJECT_DIR

## Cross-Repo Sync

- **Source of truth for Claude Code configs**: `ai-config` repo (`claude-code/projects/rdls/`)
- **Deploy command**: `bash deploy.sh C:\Users\benny\OneDrive\Documents\Github\to-rdls`
- **What gets deployed**: CLAUDE.md, 13 commands, 6 agents, 6 reference docs
- **What stays local**: This file (`rules.local.md`), `settings.local.json`
- **Workflow**: Edit configs in ai-config first, then deploy here. Never edit deployed files in-place.

---

## JSON Output Rules

1. **Always wrap** in `{"datasets": [{"id": ...}]}` — never output unwrapped JSON
2. **Skip empty fields** — if a field has no confirmed value, omit it entirely
3. **No invented values** — only use data from: data review, report, data files, or codelists
4. **No new fields** — fields in template and schema are fixed; do not create fields outside them
5. **Format with json.dump** — use `json.dump(data, f, indent=2, ensure_ascii=False)` for consistent formatting

## Schema Version Scope

- **v0.3**: All datasets except GCA
- **v1.0**: GCA climate hazard data only
- Never break v0.3 setup when working on v1.0

## v1.0 Lessons Learned

### Source Fields
- v1.0 schema uses `lineage.sources[].name`, NOT `title` — this was a recurring bug

### Climate Scenario
- `climate.scenario` is a single string enum, not an array
- Resources covering multiple scenarios (e.g., both RCP4.5 and RCP8.5): omit `scenario`, use `model` and describe in `description`
- Only set `scenario` when resource covers exactly one scenario (e.g., `res_solar_radiation` with RCP8.5 only)

### IMT Codes
- Use standard codes from `IMT.csv` or `imt_[type].csv` — format is `metric:unit`
- Examples: `AirTemp:C` (not `deg_C`), `PGWS:m/s` (not `m/s`), `pptn24:mm` (not `mm/day`), `HD:-` (hot days count), `HI:-` (heat index), `PNP:%` (percent normal precip), `Ltng:count/km/y` (lightning density)
- Master `IMT.csv` has a `Hazard` column plus `universal` entries — validator checks both per-type and master

### Event Set Design
- Do NOT mix occurrence types in one event_set — split into separate event_sets:
  - Deterministic (ensemble means, index values) → `analysis_type: "deterministic"` with `occurrence.deterministic.index_criteria`
  - Probabilistic (return periods) → `analysis_type: "probabilistic"` with `occurrence.probabilistic.return_period`
  - Empirical (observations) → `analysis_type: "empirical"` with `occurrence.empirical.temporal`
- `occurrence_range`: only for probabilistic event_sets (schema guidance)
- `event_count`: only when `events` array is NOT populated

### Entity Contact Rule
- Entity requires `name` + at least one of `email` or `url` (schema `anyOf`)
- Do not invent email addresses — use only confirmed contacts

### Details vs Description
- `description`: what the dataset is (content, structure, coverage)
- `details`: additional context from report/data review NOT already in description (methodology, partnerships, key findings, data gaps)

## Metadata Editor (gfdrr.github.io) Extra Validation

The GFDRR Metadata Editor applies additional business rules beyond the JSON Schema:

1. **Resources must have `download_url` or `access_url`** — not required by schema, but editor enforces it. Use `https://datacatalog.worldbank.org` as placeholder until DDH URLs are known.
2. **Attributions must include `publisher`, `creator`, `contact_point` roles** — v1.0 schema has these as separate top-level fields, but the editor also wants them as attribution entries. Add them to `attributions[]` array mirroring the top-level entities.
3. **hazard `process` must be specified** at event_set or event level — schema says optional, editor requires it. For `strong_wind` with no fitting process, use `extratropical_cyclone` as the most general option.

Note: `publisher`, `creator`, `contact_point` are NOT in `roles.csv` codelist — they're custom values on an open codelist. Validator will warn but they're correct per editor requirements.

## Codelist Locations

- Local clone: `C:\Users\benny\OneDrive\Documents\Github\rdl-standard\schema\codelists`
  - `closed/` — 18 CSV files (strict enum)
  - `open/` — 34 CSV files (extensible, custom values allowed)
- User will frequently fetch to keep up-to-date
- **`imt_sea_level_rise.csv` does NOT exist** — no per-type IMT file for sea_level_rise; use IMT.csv universal entries
- **earthquake process**: closed codelist has `rupture` (singular). Schema conditional has `primary_rupture`/`secondary_rupture` — this is a schema bug. CSV is authoritative; use `rupture`.

## Validation

### Running v1.0 Validator
```bash
python scripts/validate_records.py <metadata.json>
```
- Auto-detects schema: prefers `../rdl-standard/schema/rdls_schema.json` (official, regularly synced) → falls back to `schema/rdls_schema_v1.0.json` (local snapshot)
- Auto-detects codelists at `../rdl-standard/schema/codelists`
- Handles both wrapped `{"datasets":[...]}` and unwrapped formats
- Target: **0 errors, 0 warnings**

### Three Layers
1. **JSON Schema** — structure, types, required fields
2. **Codelist CSV** — 33 checks against closed/open CSV files
3. **Semantic** — 8 cross-field rules (type→process, type→IMT, quantity→unit, scale→countries, analysis→occurrence, entity contact, risk_data_type→sections, scenario→baseline)

### Interpreting Results
- **Errors**: Must fix — schema violations or closed codelist mismatches
- **Warnings**: Should verify — open codelist custom values or semantic advisories

## GCA-Specific Notes

### File: `rdls_hzd-drc_gca_moyiclimate.json`
- 6 folder-level resources matching DDH upload structure (1 zip per folder)
- 6 event_sets: 2 for extreme_temp (deterministic + probabilistic), 1 extreme_precip, 1 humidity/precip, 1 wind, 1 lightning
- Solar radiation resource has no hazard metadata (contextual variable, not a hazard)
- Lightning resource uses `empirical` analysis_type with `observed` calculation_method
- drh80 (humidity days) mapped as `extreme_temperature`/`extreme_heat` compound indicator with `HI:-` IMT

## GeoNode Adapter Lessons

- **Loss-dominance suppression**: PDNA/RiskScape output datasets (title: "damaged buildings estimated", "regional impact estimated") classify as hazard+loss without it — add title patterns to `classification.yaml` loss_dominance section
- **GeoNode multi-country tagging**: PCRAFI datasets tag ALL project countries per dataset — narrow using `regions[].code` (3-letter ISO3) + title 2-letter prefix (CK→COK, FJ→FJI, etc.)
- **`_NON_ISO3_REGION_CODES`**: GeoNode uses 3-letter region codes (PAC, GLO, ASI, EAS, SEA, AFR, NAF, WAF, EAF, CAF, SAF, EUR, CAM, SAM, NAM, CAR, MDE) that look like ISO3 but aren't — always filter them
- **`_slug_title`**: When humanizing titles, the original technical code (e.g., `CK_EQ_HazardMap_03_100_MRP`) must be preserved as `_slug_title` for unique ID generation; the humanized title is only for display. Strip `_slug_title` in `integrate.py` before final output — verify it never leaks to JSON files
- **`distribute_records()` doesn't clean stale files** — always clear `07_validated/` before re-run; stale reclassified files (e.g., old `rdls_hl-*` after fix to `rdls_lss-*`) must be deleted manually
- **`volcanic_eruption` not valid in v0.3** — use `ashfall` for general Pacific volcanic hazard
- **`process_type` in v0.3 schema**: `ash_fall` (not `ashfall`), `lava_flow` (not `lava`) — check exact values

### Hazard Type Mapping Decisions
| Variable | hazard.type | hazard.process | IMT | Rationale |
|----------|-------------|----------------|-----|-----------|
| txx, stntx | extreme_temperature | extreme_heat | AirTemp:C | Direct fit |
| dtx35 | extreme_temperature | extreme_heat | HD:- | Hot days count |
| rx1day | flood | pluvial_flood | pptn24:mm | Precipitation as flood trigger |
| raccm, dr1 | drought | meteorological_drought | PNP:% | Precipitation trends |
| drh80 | extreme_temperature | extreme_heat | HI:- | Compound heat-humidity |
| wgxx | strong_wind | (none) | PGWS:m/s | No matching process — general wind gust |
| rsds | (none) | (none) | (none) | Not a hazard — contextual variable |
| lightning | convective_storm | lightning | Ltng:count/km/y | v1.0 perfect fit |
