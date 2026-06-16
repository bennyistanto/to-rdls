# Codelists

RDLS coded fields are validated against the **authoritative CSV files** shipped with the standard, never against a hardcoded list in this repo. This keeps the toolkit correct as the standard evolves: when a code is added or renamed upstream, re-syncing the CSVs is all that's needed.

- **Location:** `rdl-standard/schema/codelists/{closed,open}/*.csv`
- **Loader:** `src/codelists.py` (the single source of truth in code) - `load_codelists_v10()`, `normalise_unit()`, `normalise_source_type()`, and the `VALID_*` sets.
- **Enforced by:** [Layer 2 of the audit](../validation/audit-layers.md).

## Closed vs open

- **Closed** codelists must match a CSV value exactly (e.g. `risk_data_type`, `hazard_type`, `exposure_category`, `analysis_type`, `loss_type`, `impact_type`, `loss_statistic`). An unknown value is an error.
- **Open** codelists (e.g. `quantity_kind`, `unit`) accept a typed custom value when no listed code fits - the Metadata Editor allows this. What is *not* allowed is obsolete or renamed vocabulary (see migrations below).

## Per-type process and triplet rules

Some codelists are conditional on another field. The audit enforces these as semantic rules (Layer 3):

- a hazard's `process` must be valid for its `type` (e.g. a `flood` type takes `fluvial_flood`, `pluvial_flood`, `coastal_flood`, ...).
- a hazard's `intensity_measure` must be valid for its `type`.
- an exposure metric's `(dimension, quantity_kind)` must be a valid pair for its `category`.

The authoritative per-type tables live in [constraints](constraints.md) and `src/audit.py`.

## GED4ALL asset taxonomy

`taxonomy_ged4all.csv` is a closed codelist used for `asset_type`. When `asset_type.scheme` is `GED4ALL`, the `id` must be a real taxonomy code, chosen by exposure category - for example population maps to a population code, roads/power to the relevant infrastructure codes, croplands to an agriculture code. Do not assume a code is missing: read the CSV - the taxonomy is broad. Where no clean code exists for a category, omit `asset_type` rather than invent one.

## Obsolete-vocabulary migrations

Earlier schema versions used terms that the current codelists renamed. The audit rejects the old terms and the fixers migrate them (it never silently accepts the obsolete value):

- `quantity_kind`: `monetary` -> `currency`, `weight` -> `mass`, `percent` -> `dimensionless_ratio` (with `unit: percent`)
- `impact_metric`: `economic_loss_value`/`asset_loss` -> `loss`, `fatality_count` -> `death`, `displaced_count` -> `displaced`, `damage_ratio` -> `damage`, `exposure_to_hazard` -> `exposure`, and similar

The canonical migration maps are `IMPACT_METRIC_OBSOLETE` and `QUANTITY_KIND_OBSOLETE` in `src/audit.py`; the fixers are `fix_quantity_kind_codelist.py` and `fix_loss_impact_structure.py` (see [validate-and-enrich](../validation/validate-and-enrich.md)).

## Keeping in sync

After any upstream schema/codelist resync, run `scripts/check_codelist_coverage.py` (every CSV on disk represented in the template) and `scripts/refresh_published_schema.py` (re-bake the schema the audit validates against). Drive everything from the actual CSV files, not from memory.
