# Limitations and Roadmap

This document describes what to-rdls cannot do yet, what work is pending, and the near-term roadmap.

---

## Current Limitations

### Content-Blind Classification (Partially Solved)

The regex extraction pipeline classifies HEVL components based on metadata text only. It cannot distinguish between "data ABOUT earthquakes" (loss assessment) and "data CONTAINING earthquake measurements" (hazard data). This affected 2,313 records (82% of all hazard-classified records) in the HDX pipeline.

**Mitigation:** The LLM review pipeline (Phase 3) addresses this for HDX data by combining metadata with actual column headers and sending to Claude Haiku for semantic classification. This reduced fabricated hazard blocks from 2,313 to near zero and correctly reclassified 3,443 records (27.3%).

**Remaining gap:** The LLM pipeline is currently implemented only for HDX sources (which have CKAN API column headers). Non-HDX sources would need their own column enrichment strategy.

For full details, see the [Problem 7 analysis](../temp/github_issue_problem7.md).

### `occurrence: {}` Schema Gap

The RDLS v0.3 JSON Schema requires `occurrence` objects to have at least one property (`minProperties: 1`), but the pipeline produces empty `occurrence: {}` blocks for datasets that lack event-specific information. This causes 2,690 otherwise-valid records to fail schema validation.

**Impact:** Of 8,822 RDLS-relevant records after LLM review, only 3,998 pass schema validation (45%). The remaining 4,493 are blocked primarily by this constraint. With the schema fix, projected validity rate rises to approximately 99.8%.

**Status:** Waiting on team decision about schema revision (relax `minProperties` or make `occurrence` optional).

### GeoNode Adapter Not Implemented

`src/sources/geonode.py` exists as a stub with the interface defined but no implementation. The common field dictionary interface is ready, and the pattern from `sources/hdx.py` can be followed.

### No Automated Test Suite

The pipeline is validated manually through notebook runs and spot-checks. Only two test files exist (`tests/test_review_basic.py`, `tests/test_review_robustness.py`) covering the review module. Core pipeline modules (classify, translate, extract, integrate, validate) lack automated tests.

### LLM Cache Invalidation

The LLM review pipeline caches classifications keyed by `prompt_hash`. Changing the system prompt or user prompt template invalidates all cached results, requiring a full re-run (~$22 for 12,594 records). Prompt changes should be made deliberately.

### Windows-Specific Issues

- **PYTHONPATH:** Must be set explicitly to import `src/` modules (`set PYTHONPATH=C:\path\to\to-rdls`)
- **OneDrive file locks:** OneDrive may lock files during sync, causing write failures in output directories
- **Path separators:** Some scripts assume Unix paths; `pathlib.Path` is used consistently in `src/` but not all notebooks

### Country Code Edge Cases

- **XKX (Kosovo):** Not in ISO 3166-1 alpha-3 standard. The pipeline includes it via `spatial.yaml` country name fixes, but `pycountry` lookups will fail for Kosovo. Requires explicit handling.

---

## Pending Work

### Notebooks 01-05 Migration

The HDX-specific pipeline (hdx-metadata-crawler notebooks 01-05) has not been ported to modular `src/` code:

| Notebook | Function | Migration Status |
|----------|----------|-----------------|
| 01 HDX Crawler | CKAN API crawling, metadata download | `src/sources/hdx.py` has `HDXClient` (partial) |
| 02 OSM Policy Exclusion | OpenStreetMap dataset detection and exclusion | Not ported |
| 03 Define Mapping | Tag/keyword/org signal mapping setup | `src/classify.py` handles scoring (partial) |
| 04 Classify Candidates | Integer-based HEVL scoring and candidate selection | `src/classify.py` (partial) |
| 05 Review Overrides | Manual classification corrections, component dependency enforcement | `src/classify.py` has `apply_overrides()` and `enforce_component_deps()` (partial) |

Notebooks 06-13 (translate, validate, extract HEVL, integrate) are fully ported to `src/` modules.

### DesInventar and NISMOD Output Re-Run

Both notebook scripts need re-execution to regenerate output files with the corrected ID format (after naming convention updates). Current output files have stale ID patterns.

### Data Inventory Notebook (Draft)

`notebooks/rdls_data_inventory_contents.ipynb` proposes using MCP + LLM for automated metadata writing from bulk data deliveries. The approach:
1. Inventory folder contents via `src/inventory.py`
2. Inspect files via `src/review.py`
3. Use `inspect_folder_for_llm` MCP tool for Claude to classify and draft metadata

**Status:** Draft/untested. Core `src/` functions work, but the notebook workflow has not been validated end-to-end.

---

## Exploration

### DELTA Resilience

UNDRR's DELTA system is an operational disaster tracking database with 40+ tables covering disaggregated human effects, damages, losses, and disruption. Detailed comparison work has been completed:

- [System-level comparison](delta_vs_rdls_system_comparison.md) — Architectural differences between DELTA (operational database) and RDLS (metadata catalog)
- [Schema-level comparison](delta_vs_rdls_schema_comparison.md) — Field-by-field mapping (106 fields assessed: 10% good fit, 35% need adjustment, 55% no equivalent)
- [Issue #19 revision notes](github_issue_19_revision.md) — Specific corrections for GFDRR/rdl-datapipeline mapping proposal

**Status:** Waiting for example DELTA data export. When available, the team will develop `src/sources/delta.py` adapter and a notebook script for DELTA-to-RDLS transformation.

---

## Roadmap

### Near-Term

1. **Port notebooks 01-05** to modular `src/` code, completing the HDX pipeline migration
2. **Resolve `occurrence: {}` schema constraint** — either relax `minProperties` in the RDLS schema or make `occurrence` optional. This unblocks ~4,493 records.
3. **Re-run DesInventar and NISMOD scripts** with corrected naming convention
4. **Fix `schema.py` double-wrapping bug** — `validate_record()` wraps records in `{"datasets": [...]}` when they're already wrapped

### Medium-Term

5. **GeoNode adapter implementation** — Follow the `sources/hdx.py` pattern
6. **Automated test suite** — Unit tests for classify, translate, extract, integrate, validate modules
7. **DELTA adapter** — When example data becomes available
8. **Data inventory workflow validation** — Test the MCP + LLM metadata writing approach end-to-end

### Long-Term

9. **Dedicated repository** — Move to-rdls to its own standalone repository
10. **JKAN catalog enhancements** — Collapsible loss record display for datasets with many entries (see [jkan_issue_loss_display.md](jkan_issue_loss_display.md))
11. **Column enrichment for non-HDX sources** — Extend the LLM review column enrichment strategy beyond CKAN API
