# Revision notes for GFDRR/rdl-datapipeline#19

The issue content is mostly still valid. Below are the specific changes needed, organized as find→replace blocks you can apply as an edit to the issue.

---

## Change 1: Fix RDLS impact_metric values in the entity mapping table

The original table used incorrect metric names. Replace the table:

### BEFORE:
```
| DELTA Entity | RDLS Mapping | Notes |
|---|---|---|
| **Event** (hazard type via HIP, date, location) | `loss.losses[].hazard_type` + `hazard_process` | HIP hazard taxonomy needs mapping to RDLS 11 hazard_type + 28 process_type enums |
| **Losses** (deaths, injured, houses damaged/destroyed, economic losses) | `loss.losses[].impact_and_losses` | Maps directly to impact_metric: `fatalities`, `injuries`, `displaced_people`, `damage_ratio`, `economic_loss` etc. |
| **Affected** (people affected, relocated, evacuated) | Same `impact_and_losses` block | `affected_people`, `displaced_people` metrics |
| **Geographic** (admin levels 1-2, location) | `spatial` (countries, bbox, gazetteer) | DELTA has richer sub-national geography than DesInventar |
| **Sendai indicators** (A-D targets) | `description` / `purpose` | RDLS doesn't have a dedicated Sendai field, but purpose/details can reference it |
| **Validation status** (draft, validated) | Not in RDLS | Could go in `details` or `lineage` |
```

### AFTER:
```
| DELTA Entity | RDLS Mapping | Notes |
|---|---|---|
| **Hazardous Event** (hazard type via HIP, date, magnitude, spatial footprint) | `loss.losses[].hazard_type` + `hazard_process` | HIP 3-level taxonomy (type→cluster→hazard, e.g. MH0004) needs mapping to RDLS 2-level (11 hazard_type + 30 process_type enums). |
| **Deaths** (per record, disaggregated by sex/age/disability/poverty) | `impact_metric: casualty_count`, `quantity_kind: count` | RDLS cannot distinguish deaths from injuries (same metric). Disaggregation dimensions are lost entirely. |
| **Injured** (per record, disaggregated) | `impact_metric: casualty_count`, `quantity_kind: count` | Same metric as deaths — would need separate loss entry or note in description. |
| **Missing** (per record, disaggregated) | *(no RDLS equivalent)* | RDLS has no missing persons metric. Could note in description. |
| **Affected** (direct + indirect, disaggregated) | `impact_metric: exposure_to_hazard` + `impact_type: direct/indirect` | |
| **Displaced** (disaggregated + assisted/timing/duration enums) | `impact_metric: displaced_count`, `quantity_kind: count` | Loses DELTA's rich enums: assisted/not_assisted, pre-emptive/reactive, duration (short/medium/long/permanent). |
| **Damages** (per sector, per asset, partially damaged vs totally destroyed) | `asset_category` + `impact_metric: economic_loss_value` | Loses pd/td distinction, per-asset detail, repair/replacement/recovery costs. |
| **Losses** (per sector, public/private split) | `asset_category` + `quantity_kind: monetary` + `currency` | Loses public/private split. |
| **Geographic** (divisionTable hierarchy + GeoJSON footprints per record) | `spatial` (countries, bbox, gazetteer) | DELTA has much richer sub-national geography than DesInventar — hierarchical divisions + per-record GeoJSON, aggregated to per-dataset bbox. |
| **Sendai indicators** (A-D targets, implied by structure) | `purpose` / `details` | RDLS doesn't have a dedicated Sendai field. Targets A-D align with impact_metric groups. |
| **Validation status** (draft/waiting/revision/validated/published) | Not in RDLS | Could go in `details`. |
```

---

## Change 2: Fix process_type count

### BEFORE:
> HIP hazard taxonomy needs mapping to RDLS 11 hazard_type + **28** process_type enums

### AFTER:
> HIP hazard taxonomy needs mapping to RDLS 11 hazard_type + **30** process_type enums

---

## Change 3: Update "Key differences from DesInventar" section

### ADD after existing point 4 (Admin-level geography):

```
5. **Disaggregated human effects** - DELTA's biggest advancement: every human effect (deaths, injured, missing, affected, displaced) can be broken down by sex, age, disability, globalPovertyLine, nationalPovertyLine, plus custom dimensions stored as JSON. DesInventar had flat counts only. RDLS has zero disaggregation support — this granularity is lost in any DELTA→RDLS transformation.

6. **Damages vs Losses distinction** - DELTA has separate tables: `damagesTable` (physical: partially damaged / totally destroyed, with repair/replacement/recovery costs per asset) and `lossesTable` (economic: public/private split, per sector). DesInventar combined these more loosely.

7. **Displaced population richness** - DELTA tracks: assisted/not_assisted, pre-emptive/reactive timing, duration (short/medium_short/medium_long/long/permanent), as-of date. Far richer than DesInventar's simple count.

8. **Cascading events** - DELTA's `eventRelationshipTable` links events via "caused_by" relationships (parent→child), enabling compound disaster tracking. No RDLS equivalent. DesInventar did not model this.

9. **Non-economic losses** - DELTA has a dedicated `nonecoLossesTable` with categories. New vs DesInventar.
```

---

## Change 4: Update "What an RDLS record would look like" section

### ADD after the existing bullet points:

```
**Key data losses in DELTA→RDLS transformation:**
- All demographic disaggregation (sex, age, disability, poverty) is dropped
- Deaths and injured are collapsed into a single `casualty_count` metric
- Missing persons have no RDLS equivalent
- Partially damaged vs totally destroyed distinction is lost
- Public/private economic loss split is merged
- Displacement detail enums (assisted, timing, duration) are lost
- Per-record GeoJSON footprints are aggregated to a bounding box
```

---

## Change 5: Update recommended next steps

### REPLACE step 2:
```
2. **Map the HIP taxonomy** - we'll need a `delta_mapping.yaml` with HIP hazard codes mapped to RDLS hazard_type/process_type (3-level HIP hierarchy → 2-level RDLS). The mapping is more complex than DesInventar's free-text event types, but also more precise since HIP codes are standardized.
```

---

## Summary of what's valid vs needs revision

| Section | Status |
|---------|--------|
| "RDLS for whole catalogue = not useful" | ✅ Still valid |
| "RDLS for individual records = very useful" | ✅ Still valid |
| Entity mapping table | ⚠️ **Needs revision** — wrong metric names, missing entities |
| "Key differences from DesInventar" (points 1-4) | ✅ Still valid, but **add points 5-9** |
| "What an RDLS record would look like" | ✅ Still valid, **add data loss notes** |
| Data access section | ✅ Still valid |
| Recommended next steps | ⚠️ Step 2 needs minor update (3-level→2-level note) |
