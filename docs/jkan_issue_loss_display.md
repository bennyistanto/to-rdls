# Enhancement: Collapsible loss record display for datasets with many entries

## Background

With the recent integration of DesInventar national loss inventories (16 datasets, 809 loss entries total), the catalog now includes loss datasets with significantly more entries per dataset than we had before — Albania has 111 loss records across 9 hazard types, Colombia has 136.

The current mapper was originally designed for datasets with a handful of loss entries (2-4), and it works well for those. The DesInventar integration gives us a good opportunity to improve how loss information is presented when there are many entries, making it easier for users to browse and understand the data.

## Current Behavior

Today, `make_loss()` in `mappers.py` summarizes all loss entries into a single flat row per field. For the `description` field, this means all individual descriptions are joined into one comma-separated string. With 111 entries, this produces a very long text block in the Description cell that's hard to scan.

The other loss fields (hazard_type, impact_metric, etc.) handle this fine because they contain short codelist values. It's really only the free-text `description` that becomes unwieldy at scale.

## Idea: Group loss records by hazard type with collapsible sections

Instead of showing all descriptions as a single block, we could group them by hazard type and present each group as a collapsible section. This way users get a quick overview (which hazard types, how many records each) and can expand any group to see the detail.

### What it would look like

For Albania (111 loss records, 9 hazard types):

```
Description        | 111 loss records across 9 hazard types
Total Loss Records | 111
Loss Records Detail|
                   | ▶ Coastal Flood [2]
                   | ▶ Convective Storm [8]
                   | ▼ Flood [18]                    ← expanded
                   |   • Deaths (flash flood) — 44 events out of 949
                   |   • Persons injured — 20 events
                   |   • Homes destroyed — 139 events
                   |   • Economic losses (local currency) — 282 events
                   |   • ...
                   | ▶ Earthquake [14]
                   | ▶ Landslide [16]
                   | ▶ Strong Wind [14]
                   | ▶ Wildfire [14]
```

For small datasets with 2-3 loss entries, everything stays exactly as it is today.

### Interactive mockup

I put together an HTML mockup using the actual Albania data so you can see and click around:

👉 **[mockup_loss_groups_proposed_a.html](https://github.com/GFDRR/rdl-jkan/files/mockup_loss_groups_proposed_a.html)** *(see attached file)*

It uses Bootstrap 3.3.6 to match the catalog's styling. The collapsible sections use HTML5 `<details>/<summary>` — no JavaScript needed.

### How it could work

This would involve four small changes:

**1. `python/mappers.py`** — Add grouping logic to `make_loss()`:

```python
# Group descriptions by hazard type
groups = {}
for l in loss.get("losses", []):
    ht = l.get("hazard_type", "unknown")
    desc = l.get("description", "")
    if desc:
        groups.setdefault(ht, []).append(desc)

loss_groups = [
    {"hazard_type": ht, "count": len(descs), "descriptions": descs}
    for ht, descs in sorted(groups.items())
]
```

Add `loss_groups` and `loss_count` to the return dict. For datasets with >5 entries, replace the concatenated description with a short summary like *"111 loss records across 9 hazard types"*.

**2. New template `_includes/display/loss_groups.html`**:

```html
<tr>
  <th>{{ include.field.label }}</th>
  <td>
    {% for group in include.value %}
      {% capture ht_code %}{{ group.hazard_type }}{% endcapture %}
      {% assign ht_label = site.data.rdl-hazard_type[ht_code] | default: ht_code %}
      <details class="loss-group">
        <summary class="loss-group-summary">
          {{ ht_label }}
          <span class="badge">{{ group.count }}</span>
        </summary>
        <ul class="loss-group-descriptions">
          {% for desc in group.descriptions %}
            <li>{{ desc }}</li>
          {% endfor %}
        </ul>
      </details>
    {% endfor %}
  </td>
</tr>
```

**3. Schema files** (`_data/schemas/rdl-02.yml` and `rdl-03.yml`) — Add two fields after `description`:

```yaml
  - field_name: loss_count
    label: Total Loss Records
  - field_name: loss_groups
    label: Loss Records Detail
    display_template: display/loss_groups.html
```

**4. `css/main.css`** — A few lines for the collapsible styling:

```css
.loss-group { margin-bottom: 4px; border: 1px solid #e0e0e0; border-radius: 3px; }
.loss-group-summary { padding: 6px 10px; cursor: pointer; font-weight: 600;
                      background-color: #f9f9f9; list-style: revert; }
.loss-group-summary:hover { background-color: #f0f0f0; }
.loss-group-summary .badge { margin-left: 6px; font-weight: normal; }
.loss-group-descriptions { margin: 0; padding: 6px 10px 6px 30px; font-size: 90%; }
.loss-group-descriptions li { margin-bottom: 3px; }
```

### Why `<details>/<summary>`

- No JavaScript needed — native HTML5 element
- Accessible out of the box (keyboard, screen readers)
- Degrades gracefully — shows expanded in older browsers
- Multiple groups can be open at the same time for comparison
- ~15 lines of Liquid, lightweight

### Backward compatibility

Datasets that don't have `loss_groups` in their frontmatter simply skip the new field — no visual change at all. The existing `description` field continues to work as before for small datasets.

## Open to other ideas

This is just one approach — happy to discuss alternatives or adjustments. The DesInventar data is a great test case for thinking about how the catalog handles larger datasets in general.
