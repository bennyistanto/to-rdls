# RDLS Naming Convention Reference

All naming rules from `to-rdls/configs/naming.yaml`.

## ID format

```
rdls_{type}-{iso3}{org}_{titleslug}
```

Example: `rdls_hzd-uryucra_floodhazardmap`

With collision suffix: `rdls_hzd-uryucra_floodhazardmap__a1b2c3d4`

## Component codes

### Single-type (3-letter)
| component | code |
|---|---|
| hazard | hzd |
| exposure | exp |
| vulnerability | vln |
| loss | lss |

### Multi-type (1-letter, concatenated in HEVL order)
| component | letter |
|---|---|
| hazard | h |
| exposure | e |
| vulnerability | v |
| loss | l |

Examples: `he` (hazard+exposure), `hevl` (all four), `hel` (hazard+exposure+loss)

Order is always: hazard → exposure → vulnerability → loss

## Hazard item codes (2-letter)

| hazard_type | code |
|---|---|
| flood | fl |
| earthquake | eq |
| tsunami | ts |
| strong_wind | tc |
| volcanic | vo |
| landslide | ls |
| convective_storm | cs |
| wildfire | wf |
| drought | dr |
| extreme_temperature | et |
| coastal_flood | cf |

## Exposure item codes (2-letter)

| exposure_category | code |
|---|---|
| buildings | bd |
| population | pp |
| infrastructure | if |
| agriculture | ag |
| natural_environment | en |
| economic_indicator | ec |
| development_index | dv |

## Title slug rules

- **max_length**: 25 characters
- **stop_words** (removed from slug): for, of, the, and, in, a, an, to, at, by, on, with, from
- Country names and org names are stripped from slug (already in other segments)
- Uses `slugify_title()` from utils

## Country segment rules

- **max_countries**: 5 (if more, uses region code or truncates)
- ISO3 codes concatenated without separator: `uryken` (Uruguay + Kenya)
- Single country: `ury` (3 chars)

## Org shortname rules

- **max_length**: 15 characters
- Resolved via `org_abbreviations` lookup in naming.yaml (265 entries)
- If not in lookup: auto-truncated from org_slug
- Examples: `unicef`, `wfp`, `fao`, `worldbank`, `gfdrr`

## Collision avoidance

- Suffix format: `__{uuid8}` (double underscore + 8-char UUID)
- Applied only when duplicate IDs detected
- `build_rdls_id_with_collision()` handles this automatically

## Classification thresholds (from classification.yaml)

| level | min score |
|---|---|
| high confidence | ≥ 7 |
| medium confidence | ≥ 4 |
| RDLS candidate | ≥ 5 |
| keyword_hit_weight | 2 (points per keyword match) |
