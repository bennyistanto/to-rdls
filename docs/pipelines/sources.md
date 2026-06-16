# Source adapters

The toolkit is source-independent: a thin **adapter** normalizes a catalogue's native JSON into a common field dictionary, and the shared pipeline (classify → translate → HEVL extract → integrate → validate → enrich) runs identically regardless of where the metadata came from.

## The adapter contract

A source adapter does three things:

1. **`normalize_record(raw)`** - unwrap the source's native envelope into a flat dataset object.
2. **`extract_fields(record)`** - return the **common field dictionary** the rest of the pipeline expects (the same keys `extract_hdx_fields()` returns: title, description, organization, resources, tags, spatial hints, dates, license, etc.).
3. Read its settings from `configs/sources/<source>.yaml` - endpoints, format overrides, field-path mappings - so no source quirks leak into the shared modules.

Everything downstream is generic. Adding a source means writing one adapter + one config; see [Convert a source](../workflows/convert-a-source.md).

## Adapters in `src/sources/`

| Module | Role |
|--------|------|
| `hdx.py` | HDX / CKAN API client, field extraction, OSM detection (reference implementation) |
| `geonode.py` | GeoNode adapter (resource-type filtering, link typing) |
| `stac.py` | STAC catalogue adapter (Item / Collection traversal) |
| `ckan_columns.py` | CKAN column-header fetcher with a disk-backed cache |
| `hdx_review.py` | HDX second-pass HEVL review with column detection |
| `hdx_llm_review.py` | HDX v0.3 four-phase LLM classification pipeline |

v1.0 record construction for the non-HDX catalogues lives in dedicated translators: `src/translate_geonode_v10.py`, `src/translate_stac_v10.py`, `src/translate_stac_coclico_v10.py`.

## Catalogues processed to date

These are the sources run so far; the pipeline applies the same to any new one.

| Source | Generation | Notes |
|--------|-----------|-------|
| HDX | v1.0 (LLM-first) | the largest corpus (~8,300 records) |
| GeoNode (multiple portals) | v1.0 | per-portal crawl; records land in the validated/high tier |
| STAC catalogues | v1.0 | Item-per-record |
| CoCliCo | v1.0 | Collection-per-record |
| DesInventar | v0.3/v1.0 | national disaster-loss inventories |
| NISMOD (ICRA, SDK) | v1.0 | program datasets |
| MDG, WBG-UFRA, Tomorrow Cities | v1.0 | program datasets |
| JKAN legacy catalogue | v0.3 -> v1.0 conversion | see [Convert a source](../workflows/convert-a-source.md) |

## Where records land

Validated v1.0 records are written under `output/<collection>/.../<tier>/`, where the tier reflects confidence (e.g. `high`). Only records that pass the [5-layer audit](../validation/audit-layers.md) are placed in the high-confidence tier. Each record is one file, named by its RDLS id (see [naming](../reference/naming.md)).
