# Validate and enrich

After any conversion or new run, records go through two steps: **validate** (the [5-layer audit](audit-layers.md)) and **enrich** (apply the safe, general fixes that bring records into compliance). Both are general transforms - each applies to *every* record matching a condition, never to a named dataset.

## The CLI

`scripts/validate_records.py` is the single entry point.

```bash
# Validate: run the audit over one file (reports errors + warnings)
python scripts/validate_records.py path/to/record.json

# Enrich: apply safe post-conversion fixes across a glob/dir, then report what still needs a human
python scripts/validate_records.py --enrich "output/<collection>/**/*.json"
```

Run `--enrich` on every newly converted record before considering the conversion done.

## Automatic enrichment (`src/enrich.py`)

These are mechanical, lossless fixes applied to any record that needs them:

- `measurement.unit = "count"` where `quantity_kind = count`
- repair GED4ALL `uri` values and restore the `scheme` where an asset type maps to a taxonomy code
- drop an invalid `scheme = "Custom"`
- remove disallowed (e.g. Commercial) licenses
- set a default `intensity_measure` for flood loss hazards where the source omits it

Anything `enrich` cannot decide safely is reported for manual review rather than guessed.

## The fixers (general transforms)

Each fixer in `scripts/` encodes one **general rule** and applies it to all records matching its condition. They were distilled from validation findings and downstream-consumer feedback, then generalized - so they fix the whole class of records, not the one that surfaced the issue. Every fixer maps to an audit layer/rule it satisfies.

| Fixer | General rule it applies | Audit layer/rule |
|-------|-------------------------|------------------|
| `fix_loss_impact_structure.py` | nest loss impact under `impact_and_losses.impact.{type,modelling,metric,measurement}`; migrate obsolete `impact_metric` | Layer 3 - rules 9, 10 |
| `fix_quantity_kind_codelist.py` | migrate obsolete `quantity_kind`/`unit` vocabulary to the current codelist | Layer 2, Layer 3 - rule 3 |
| `fix_ged4all_asset_codes.py`, `fix_ged4all_buildings_code.py` | set `asset_type.id` to the GED4ALL code for its exposure category when scheme is GED4ALL | Layer 2 (GED4ALL) |
| `fix_resource_media_type_consistency.py`, `fix_ogc_resource_media_type.py` | align resource `media_type`/`format` to what the URL actually serves | Layer 4 |
| `fix_eventset_hazards_coverage.py`, `fix_combined_flood_eventset.py` | reconcile a combined-hazard event_set's declared processes with its events | Layer 3 - rules 1, 11 |
| `fix_hazard_ids.py` | give every hazard object a stable `id` (consumer ingestion reads it unconditionally) | Layer 5 |
| `fix_title_tag_codes.py` | strip trailing `[code]` tags from human-facing titles | quality |
| `fix_resource_descriptions.py`, `wrap_resource_descriptions.py` | turn a bare code/slug resource description into `"{title} ({label})"` | quality / Metadata Editor |
| `fix_measurement_and_junk.py` | normalize measurement structure and remove pipeline-internal junk fields | Layer 1/2 |
| `fix_broken_stats_descriptions.py` | repair malformed statistics blocks | Layer 1 |

(Fixer filenames sometimes carry the source/record where the issue first appeared; the transform itself is general and safe to run corpus-wide.)

## Anti-recurrence checkers

Run after every schema resync so the template and codelists never drift from the standard:

- `scripts/refresh_published_schema.py` - re-bake the published v1.0 schema the audit validates against
- `scripts/check_template_against_schema.py` - every schema field represented in the template (0 missing / 0 stale)
- `scripts/check_codelist_coverage.py` - every codelist CSV on disk represented in the template
- `scripts/generate_template_from_schema.py`, `scripts/merge_template.py` - regenerate/merge the annotated template from the schema + codelists

## Principle

Completeness is driven from the **actual files** - the schema and the codelist CSVs - not from memory or a list of known cases. A new failure becomes a new general check in the audit and a new general fixer here, so it is fixed for every current and future record at once.
