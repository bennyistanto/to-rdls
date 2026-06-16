# The 5-layer audit

Every RDLS record this toolkit produces - regardless of source - must pass **all five validation layers** before it is considered done. The audit is the toolkit's general correctness guarantee: it is written against the schema and codelist files themselves, so it applies uniformly to any record rather than to specific datasets.

Implementation: `src/audit.py` - `validate(record, schema, registry)` runs the layers in order and returns a `ValidationResult` (errors + warnings). The layers are deliberately separated because each catches a class of problem the previous one structurally cannot.

| Layer | Checks | Why the previous layer can't |
|------:|--------|------------------------------|
| 1 | **JSON Schema** (Draft 2020-12) - shape, required fields, types, enums | - |
| 2 | **Codelist membership** - field values against the codelist CSV files | the schema treats many codelists as open strings |
| 3 | **Semantic / cross-field rules** (11 rules, below) | a value can be individually valid but wrong in combination |
| 4 | **Resource media-type vs URL** - declared type vs what the link actually serves | the schema cannot fetch a URL or compare two fields |
| 5 | **Consumer business rules** (Metadata Editor, DDH, JKAN) | downstream tools require things the schema marks optional |

## Layer 1 - JSON Schema
Standard `jsonschema` validation against the **published, baked** `schema/rdls_schema_v1.0.json` (refresh it with `scripts/refresh_published_schema.py`). Always validate a single **unwrapped** record - never wrap it in `{"datasets": [...]}` first.

## Layer 2 - Codelist membership
Validates each coded field against the authoritative CSV files in `rdl-standard/schema/codelists/{closed,open}`. Closed codelists must match exactly; open codelists accept typed custom values but reject obsolete/renamed vocabulary. Includes the GED4ALL taxonomy check: when an `asset_type.scheme` is GED4ALL, its `id` must be a real taxonomy code.

## Layer 3 - Semantic / cross-field rules
Eleven rules that the schema cannot express on its own:

| Rule | Enforces |
|-----:|----------|
| 1 | a hazard's `process` is valid for its `type` |
| 2 | a hazard's `intensity_measure` is valid for its `type` |
| 3 | a measurement's `unit` is valid for its `quantity_kind` |
| 4 | `spatial.scale` matches the presence/absence of `countries` |
| 5 | `analysis_type` matches the expected `occurrence` key (probabilistic/deterministic/empirical) |
| 6 | every entity has a `name` plus at least one of `email`/`url` |
| 7 | `risk_data_type` matches which HEVL sections are present |
| 8 | a `climate.scenario` is accompanied by a `baseline_period` |
| 9 | loss impact is **nested** (`impact_and_losses.impact.{...}`), not the flat v0.3 shape |
| 10 | `impact_metric` uses the current codelist (obsolete values are migrated, not accepted) |
| 11 | an `event_set.hazards[]` declaring several processes is consistent with its `events[]` (warns, to flag combined-vs-separate maps) |

## Layer 4 - Resource media-type vs URL
The URL is ground truth. If a `download_url` ends in `.zip`, a single-file media type (`text/csv`, `image/tiff`, ...) is a contradiction - the container is `application/zip`. If it ends in a specific extension (`.xlsx`, `.tif`, ...), the declared `media_type`/`format` must agree. Landing pages and extensionless endpoints make no assertion (the layer never guesses).

## Layer 5 - Consumer business rules
Downstream consumers require fields the JSON Schema marks optional. Examples (general, not source-specific): the Metadata Editor requires the nested loss-impact fields and vulnerability-function impact fields; JKAN's ingestion reads `hazard.id` on every hazard object. Layer 5 mirrors these so a record that passes the schema still fails here if a consumer would reject it. The rule of thumb: **a consumer that reads a field unconditionally makes it a de-facto requirement** - but confirm against the consumer's current code before enforcing as an error, and keep recommendations as warnings.

## Running the audit
```bash
# single record
python scripts/validate_records.py path/to/record.json
# every non-HDX record, exhaustively, with a per-pattern summary
python scripts/audit_nonhdx.py [--root output]
```
Both run the same `src/audit.py` layers. The fixers that bring records into compliance are documented in [validate-and-enrich.md](validate-and-enrich.md).

## Principle
The audit is driven from the **actual schema and codelist files**, never from memory or a list of known-bad records. When a new failure class appears, the fix is a new general check here (and a general fixer), so it cannot recur on any record - not a one-off patch for the record that surfaced it.
