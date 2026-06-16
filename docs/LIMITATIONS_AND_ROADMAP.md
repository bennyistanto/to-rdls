# Limitations and Roadmap

What the toolkit does not do yet, and what is planned next.

## Current limitations

### No automated test suite
The pipeline is validated through the [5-layer audit](validation/audit-layers.md) and spot-checks rather than unit tests. A small set of tests covers the review module; the core modules (classify, translate, extract, integrate, audit, enrich) have no automated tests. The audit is the de-facto regression guard - any record that regresses fails it - but module-level unit tests are still wanted.

### `occurrence` on event-less hazards
The schema requires an `occurrence` on every `Event`, but some sources (e.g. climate-index hazards) describe a hazard with no discrete, return-period events. The toolkit's faithful handling is to keep the `event_set` (`hazards[]` + `analysis_type` + `temporal`) and omit the empty events rather than invent return periods. Whether the schema should relax this for index-type hazards is an open question for the standard.

### Records that need source data
Some legacy records cannot be made valid without information the source does not contain (a missing resource URL, an empty vulnerability block, etc.). These are held aside for human follow-up rather than forced through - see the JKAN conversion notes. They are a data-availability limit, not a pipeline bug.

### LLM cache invalidation
The LLM pipeline caches results keyed by prompt hash; changing a prompt invalidates the cache and requires a re-run. Make prompt changes deliberately.

### Windows / OneDrive
- `PYTHONPATH` must include the repo root to import `src/`.
- A git repository inside a cloud-sync folder (OneDrive) suffers `.git` lock failures during git's auto-gc; mitigate with `git config gc.auto 0` and an antivirus exclusion, or keep the repo outside the synced folder. See [jkan-upload](workflows/jkan-upload.md).

### Country-code edge cases
Kosovo (`XKX`) is not in ISO 3166-1 alpha-3; it is handled via `configs/spatial.yaml`, but raw `pycountry` lookups return `None`.

## Resolved (kept for context)

- **Content-blind classification** - the v0.3 regex pipeline could not tell "data about a hazard" from "data of a hazard". The v1.0 LLM-first pipeline classifies from content and is source-agnostic, so this is no longer a structural ceiling.
- **GeoNode / multi-source support** - GeoNode, STAC, and CoCliCo now have full v1.0 translators and have been run at scale; the toolkit is no longer HDX-only.
- **Double-wrapping bug** - `validate_record()` validates a single unwrapped record.

## Roadmap

**Near-term**
1. Resolve the `occurrence` schema tension with the standard (relax for index-type hazards, or formalize the events-omitted representation).
2. Clear the held-aside records once their missing source data or convention decisions land.

**Medium-term**
3. Automated unit tests for the core modules.
4. A DELTA adapter (`src/sources/delta.py`) when example DELTA data is available.

**Long-term**
5. Move the toolkit to its own standalone repository.

## Related exploration (archived)

Comparison work with UNDRR's DELTA standard:
- [System-level comparison](archive/delta_vs_rdls_system_comparison.md)
- [Schema-level comparison](archive/delta_vs_rdls_schema_comparison.md)

Older issue drafts are kept under [docs/archive/issues/](archive/issues/).
