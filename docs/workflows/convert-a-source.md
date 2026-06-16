# Workflow: convert a source to RDLS v1.0

End-to-end steps for turning a catalogue's metadata into validated RDLS v1.0 records. The same flow handles a brand-new source and a v0.3 -> v1.0 conversion of an existing catalogue - only the front of the pipeline (how fields are obtained) differs.

## 1. Get the fields (adapter)

Write or reuse a [source adapter](../pipelines/sources.md): `normalize_record()` + `extract_fields()` returning the common field dictionary, plus a `configs/sources/<source>.yaml`. For a v0.3 -> v1.0 conversion, the "adapter" is instead a reader for the existing v0.3 records.

## 2. Classify + build the base record

- v1.0 (canonical): a single LLM call (`src/llm_classify.py`) classifies the RDLS component types and extracts the HEVL fields in one pass.
- The base record (id, title, entities, spatial, license, resources, field ordering) is built by `src/translate.py`; ids follow the [naming](../reference/naming.md) standard `rdls_{types}-{iso3}_{org}_{slug}`.

## 3. Build and integrate the HEVL blocks

`src/extract.py` builds the hazard/exposure/vulnerability/loss blocks from the classification, and `src/integrate.py` merges them and reconciles `risk_data_type` with the sections actually present.

## 4. Validate (all 5 layers)

Run the [audit](../validation/audit-layers.md). A record is only "done" when it passes Layer 1 (schema) through Layer 5 (consumer rules) with zero errors.

```bash
python scripts/validate_records.py path/to/record.json
```

## 5. Enrich + fix

Apply the safe post-conversion fixes and re-check:

```bash
python scripts/validate_records.py --enrich "output/<collection>/**/*.json"
```

Then apply any general [fixers](../validation/validate-and-enrich.md) the audit flags (e.g. nest loss impact, migrate obsolete codelist values, align resource media types, add GED4ALL codes). Fixers are general transforms - run them corpus-wide, not per record.

## 6. Place the output

One record per file, named by its RDLS id, written under `output/<collection>/.../high/` once it passes the audit. Records that cannot be made valid without information the source does not contain are held aside for human follow-up rather than forced through.

## Conversion-specific notes (v0.3 -> v1.0)

Recurring structural changes when converting older records (all handled by general fixers):

- `processes: [x]` (array) -> `process: x` (string) on hazards; infer a hazard's `type` from its `process` where only the process is given.
- flat loss impact -> nested `impact_and_losses.impact.{...}`; migrate obsolete `impact_metric`.
- bare `exposure` object -> array; move `quantity_kind` into a `measurement`.
- flat vulnerability -> `functions.{vulnerability|fragility}[]` with the required `relationship`.
- derive `publisher`/`creator`/`contact_point` from `attributions` by role; add missing entity URLs.
- migrate v0.2 resource fields (`data_format`, `access_modality`) to `media_type`/`format`.

Where the schema requires a field the source genuinely omits (e.g. a vulnerability function's `relationship`), assign the most defensible value **and flag it for review** - never fabricate silently.
