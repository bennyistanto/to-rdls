---
marp: true
theme: default
paginate: true
style: |
  section {
    font-family: 'Segoe UI', sans-serif;
    font-size: 18px;
    color: #1a1a2e;
    background: #ffffff;
  }
  h1 {
    color: #003366;
    font-size: 2.0em;
    border-bottom: 3px solid #0066cc;
    padding-bottom: 0.2em;
  }
  h2 {
    color: #003366;
    font-size: 1.5em;
  }
  h3 {
    color: #0066cc;
    font-size: 1.1em;
  }
  strong { color: #003366; }
  em { color: #cc4400; font-style: normal; font-weight: bold; }
  code {
    background: #f0f4f8;
    color: #1a1a2e;
    font-size: 0.85em;
    border-radius: 3px;
    padding: 1px 4px;
  }
  pre {
    background: #0d1117;
    color: #c9d1d9;
    font-size: 0.72em;
    border-radius: 6px;
    padding: 14px;
    border-left: 4px solid #0066cc;
  }
  pre code { background: none; color: inherit; padding: 0; }
  table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.85em;
  }
  th {
    background: #003366;
    color: white;
    padding: 6px 10px;
    text-align: left;
  }
  td { padding: 5px 10px; border-bottom: 1px solid #dee2e6; }
  tr:nth-child(even) { background: #f8f9fa; }
  .columns {
    display: grid;
    grid-template-columns: repeat(2, minmax(0, 1fr));
    gap: 1.5rem;
  }
  .columns3 {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 1rem;
  }
  .box {
    background: #f0f4f8;
    border-left: 4px solid #0066cc;
    padding: 10px 14px;
    border-radius: 4px;
    margin: 6px 0;
  }
  .box-green {
    background: #f0fff4;
    border-left: 4px solid #22a848;
    padding: 10px 14px;
    border-radius: 4px;
    margin: 6px 0;
  }
  .box-orange {
    background: #fff8f0;
    border-left: 4px solid #e07800;
    padding: 10px 14px;
    border-radius: 4px;
    margin: 6px 0;
  }
  .box-red {
    background: #fff0f0;
    border-left: 4px solid #cc2222;
    padding: 10px 14px;
    border-radius: 4px;
    margin: 6px 0;
  }
  .tag {
    display: inline-block;
    background: #003366;
    color: white;
    border-radius: 12px;
    padding: 2px 10px;
    font-size: 0.8em;
    margin: 2px;
  }
  .tag-green { background: #22a848; }
  .tag-orange { background: #e07800; }
  .tag-red { background: #cc2222; }
  section.title {
    background: linear-gradient(135deg, #003366 0%, #0066cc 100%);
    color: white;
    display: flex;
    flex-direction: column;
    justify-content: center;
    padding: 60px;
  }
  section.title h1 {
    color: white;
    border-color: rgba(255,255,255,0.4);
    font-size: 2.4em;
  }
  section.title h2 { color: rgba(255,255,255,0.85); font-size: 1.3em; }
  section.title p { color: rgba(255,255,255,0.75); }
  section.act {
    background: linear-gradient(135deg, #1a1a2e 0%, #003366 100%);
    color: white;
    display: flex;
    flex-direction: column;
    justify-content: center;
    padding: 60px;
  }
  section.act h1 { color: white; border-color: rgba(255,255,255,0.3); font-size: 2.6em; }
  section.act h2 { color: rgba(255,255,255,0.85); }
  section.act p { color: rgba(255,255,255,0.75); font-size: 1.1em; }
  footer { font-size: 0.75em; color: #888; }
---

<!-- _class: title -->

# From Keywords to Understanding
## How we catalogued 26,246 HDX risk datasets using regex, LLM, and finally LLM-first

**Global Facility for Disaster Reduction and Recovery (GFDRR), World Bank**
Digital Earth Team - May 2026

---

# The Mission

**Transform 26,246 datasets** on the Humanitarian Data Exchange (HDX) into the Risk Data Library Standard (RDLS) - a structured schema for cataloguing hazard, exposure, vulnerability, and loss data.

<div class="columns">
<div>

### The target: RDLS HEVL classification

```
H  Hazard       flood depth grids, seismic
                intensity maps, drought indices

E  Exposure     building inventories,
                population grids, facility data

V  Vulnerability damage/fragility functions,
                vulnerability curves

L  Loss         casualties, displacement,
                economic damage, facility status
```

</div>
<div>

### The scale

| Dataset Source | Count |
|---------------|------:|
| Total HDX datasets | 26,246 |
| Potential RDLS candidates | 12,594 |
| Confirmed RDLS-relevant | ~7,000+ |
| Schema-valid records | 3,312+ |

**Manual review rate**: ~5-10 min/dataset
**Manual cost if paid**: ~$2M+

**We needed automation.**

</div>
</div>

---

<!-- _class: act -->

# Act 1
## The Regex Pipeline
### Deterministic, fast - and content-blind

---

# Act 1: Pattern Matching at Scale

A curated dictionary of **400+ regex patterns** mapped to RDLS codelists. Every metadata field - title, tags, description, resource filenames - was scanned and scored.

```
signal_dictionary.yaml  (excerpt)
─────────────────────────────────────────────────────────────────────
hazard:
  - pattern: "flood.?(depth|inundation|extent|map)"
    type: flood
    process: fluvial_flood
    confidence: high
  - pattern: "earthquake|seismic|ground.motion"
    type: earthquake
    confidence: high
exposure:
  - pattern: "building.?(stock|footprint|inventory|count)"
    category: buildings
    confidence: high
  - pattern: "population.?(grid|density|distribution)"
    category: population
    confidence: high
loss:
  - pattern: "casualt|mortalit|death.toll|killed"
    confidence: medium
  - pattern: "displacement|displaced.person|IDP"
    confidence: medium
```

**Result**: 12,594 candidates classified in **under 60 seconds**.

---

# Act 1: How Signal Scoring Works

Each pattern hit adds to a weighted score. Components above the threshold are included in the record.

<div class="columns">
<div>

### Example: "Global Flood Hazard Map"

```
Title scan:
  "flood"      -> +0.8  hazard/flood
  "hazard"     -> +0.6  hazard (any)
  "map"        -> +0.3  geospatial

Tag scan:
  "floodplain" -> +0.5  hazard/flood
  "GIS"        -> +0.2  format

Description scan:
  "inundation depth" -> +0.7  hazard/flood
  "return period"    -> +0.8  probabilistic
─────────────────────────────────────────────
Hazard score: 3.7  >>  threshold 0.5
Other scores: 0.0  <<  threshold
```

**Output**: Hazard only. Correct.

</div>
<div>

### What the record looks like

```json
{
  "risk_data_type": ["hazard"],
  "hazard": {
    "event_sets": [{
      "hazards": [{
        "type": "flood",
        "process": "fluvial_flood",
        "intensity_measure": "wd:m"
      }],
      "analysis_type": "probabilistic"
    }]
  }
}
```

<div class="box-green">

**This works perfectly** for clear-cut metadata that accurately describes data content.

</div>

</div>
</div>

---

# Act 1: When Pattern Matching Hits Its Ceiling

**The problem**: metadata describes the *context* of a dataset - the disaster that motivated its collection - not the actual *data content*.

<div class="box-red">

**Content-blind over-classification**: a keyword in the title generates a HEVL component whether or not that component's data exists in the file.

</div>

### The trigger mechanism

```
Dataset: "Kenya Drought-Related Key Figures"

Title keywords matched:
  "drought"      -> Hazard (drought, SPI:-)       [FABRICATED]
  "Kenya"        -> country = KEN
  "Key Figures"  -> (neutral)

Description keywords matched:
  "population"   -> Exposure (population, count)  [FABRICATED]
  "affected"     -> Vulnerability                  [FABRICATED]
  "mortality"    -> Loss                           [CORRECT]

Result: H + E + V + L  (all four components)
```

**Reality**: the files contain only displacement counts and mortality figures. Pure loss data. Three components invented.

---

# Act 1: The Kenya Drought Case

The diagnostic example that revealed the systematic ceiling.

| Attribute | Regex output | Reality |
|-----------|-------------|---------|
| Classification | Hazard + Exposure + Vulnerability + *Loss* | **Loss only** |
| Hazard block | "drought hazard data for Kenya", intensity: `SPI:-` | No drought index data in any file |
| Exposure block | Template built from "population" keyword | No asset or population inventory |
| Vulnerability block | Template built from "affected" keyword | No fragility or damage functions |
| Loss block | Displacement counts, mortality figures | Correct |

<div class="columns">
<div>

### What was generated

```json
"hazard": {
  "event_sets": [{
    "hazards": [{"type": "drought",
                 "intensity_measure": "SPI:-"}]
  }]
},
"exposure": [{"category": "population"}],
"vulnerability": {"functions": {...}}
```

</div>
<div>

### What should have been generated

```json
"risk_data_type": ["loss"],
"loss": {
  "losses": [{
    "hazard": {"type": "drought"},
    "asset_category": "population",
    "impact_metric": "displaced_count"
  }]
}
```

</div>
</div>

---

# Act 1: Results and Verdict

<div class="columns">
<div>

### What worked well

- 12,594 records processed in **< 60 seconds**
- Deterministic and fully reproducible
- Zero API cost
- Correctly handled unambiguous cases
- ~81.6% of records were correct

### The gap

- **2,313 records** (18.4%) misclassified
- Phantom components with fabricated data
- No way to distinguish data content from topic
- Single hazard event, no return periods
- Single exposure category, no combinations

</div>
<div>

### By the numbers

| Metric | Count |
|--------|------:|
| Total candidates | 12,594 |
| Correctly classified | ~10,281 |
| Over-classified | 2,313 |
| Fabricated hazard blocks | ~2,100 |
| Fabricated exposure blocks | ~1,800 |

<br>

<div class="box-red">

**Verdict**: pattern matching provides a fast deterministic foundation but cannot cross the semantic boundary between *topic* and *content*.

</div>

</div>
</div>

---

<!-- _class: act -->

# Act 2
## The Hybrid Pipeline (v0.3)
### Adding LLM semantic review where it matters

---

# Act 2: The Four-Phase Design

A hybrid architecture that uses regex for clear-cut cases and routes ambiguous records to LLM review.

```
12,594 candidates
       |
       v
  Phase 1: Signal Triage
  ┌─────────────────────────────────────────────────────────────┐
  │  High confidence (regex score >= 0.8)   -> 4,088 records   │  48% bypasses LLM
  │  Borderline / ambiguous                 -> 7,836 records   │
  │  No signal at all                       ->   455 records   │
  │  Random validation sample (5%)          ->   215 records   │
  └─────────────────────────────────────────────────────────────┘
       |
       v  (8,506 records proceed to LLM)
  Phase 2: Column Enrichment
       |    Inject actual column headers from CKAN API cache
       v
  Phase 3: LLM Classification
       |    Claude Haiku 4.5 - structured prompt with RDLS definitions
       v
  Phase 4: Reconciliation
            Merge LLM result with regex output
            Remove phantom components
            Rebuild record IDs and filenames
```

---

# Act 2: The Column Enrichment Breakthrough

The single most impactful design decision. Column headers from the actual data files give the LLM **direct evidence of data content** - not just metadata.

<div class="columns">
<div>

### Before enrichment (title + tags only)

```
Dataset: "Somalia Flood Impact Assessment"

Title: "Somalia Flood Impact Assessment"
Tags: flood, Somalia, displacement,
      humanitarian, affected population
Description: "Assessment of flood impacts
  on affected communities..."
```

**LLM sees**: flood + population + affected = H+E+L?

</div>
<div>

### After enrichment (+ column headers)

```
Dataset: "Somalia Flood Impact Assessment"
...same metadata...

Column headers from resources:
  affected_population.csv:
    Region, District, Flood_Date,
    People_Affected, People_Displaced,
    Households_Affected, Deaths,
    Injuries, Houses_Damaged_Total,
    Houses_Damaged_Severe
```

**LLM sees**: casualty/displacement columns = Loss only. No flood depth, no building count.

</div>
</div>

<div class="box-green">

**One-time crawl**: 88,327 resources across HDX queried via CKAN API (~48 hours). Column headers cached to disk. Available for 48,719 of 26,246 datasets. Zero API cost on every subsequent run.

</div>

---

# Act 2: The LLM Prompt Definitions

The key engineering was not prompt length but **precision of definitions**. The LLM was explicitly taught the RDLS semantic boundary.

```
RDLS Component Definitions
───────────────────────────────────────────────────────────────────────

Hazard: The dataset CONTAINS hazard measurements or models.
        A dataset that merely REFERENCES a hazard event does NOT contain
        hazard data. Required: measurement values, model outputs,
        intensity grids, probability maps.

Exposure: The dataset CONTAINS spatial inventories of assets at risk.
          A dataset that mentions affected populations does NOT contain
          exposure data unless it provides an asset inventory.
          Required: building locations/counts, population grids, facility lists.

Vulnerability: The dataset CONTAINS damage or fragility functions.
               Required: curves, functions, or matrices relating hazard
               intensity to expected damage or loss rate.

Loss: The dataset CONTAINS post-event impact data.
      Casualties, displacement, damage status, economic losses,
      operational status - measured after an event occurred.
```

<div class="box">

**Finding**: with precise definitions, even a compact model (Haiku, the smallest in Anthropic's family) achieved **94% average confidence** with effectively zero classification errors. **Prompt engineering beats model scale** for structured tasks.

</div>

---

# Act 2: The Kenya Drought Resolution

With column headers, the LLM reached the correct answer with high confidence.

```
Prompt sent to LLM:

Dataset: Kenya Drought-Related Key Figures
Tags: drought, Kenya, displacement, mortality, population
Description: "Drought-related key figures for Kenya..."

Column headers from resources:
  kenya_drought_key_figures.csv:
    Region, Month, Year, People_Affected,
    People_Displaced, Acute_Food_Insecurity_IPC3plus,
    Deaths_Reported, Children_Under5_Acutely_Malnourished,
    Livestock_Deaths, Pasture_Condition

LLM response:
  is_rdls: true
  components: ["loss"]
  reasoning: "All columns are post-event impact metrics: displacement counts,
    mortality, malnutrition, food insecurity rates. No drought index values,
    no SPI or SPEI grids, no asset inventory. This is loss data exclusively."
  confidence: 0.95
```

<div class="box-green">

**Result**: Hazard, Exposure, and Vulnerability blocks removed. Loss block corrected. Record ID and filename rebuilt. Classified correctly.

</div>

---

# Act 2: Results

<div class="columns">
<div>

### Pipeline output

| Metric | Count |
|--------|------:|
| RDLS candidates (input) | 12,594 |
| Records sent to LLM | 8,506 |
| **Reclassified by LLM** | **3,511 (27.9%)** |
| Non-risk datasets separated | 5,448 |
| **Final RDLS-relevant records** | **7,146** |
| Schema-valid records | 3,312 |

### What LLM changed

| Change type | Count |
|-------------|------:|
| Phantom components removed | 2,313 |
| Multi-component -> single | 1,204 |
| Loss-only corrected | 891 |
| Not-RDLS separated | 4,794 |

</div>
<div>

### Cost and performance

| Metric | Value |
|--------|-------|
| LLM cost (initial run) | **~USD 22** |
| Re-run cost (cached) | **USD 0** |
| Processing time (cached) | ~22 min |
| Column header crawl (one-time) | ~48 hours |
| Cost per record | ~$0.003 |

### Non-risk categories separated

| Domain | Count |
|--------|------:|
| General (statistics, trade) | 2,657 |
| Health surveillance | 941 |
| Reference / admin boundaries | 894 |
| Humanitarian operations | 263 |
| Climate (non-risk) | 39 |

</div>
</div>

---

# Act 2: What We Still Could Not Do

Despite the improvements, the hybrid pipeline had a hard ceiling imposed by its v0.3 architecture.

<div class="columns">
<div>

### Limitation 1: Extraction quality ceiling

Even when classification was correct, the regex extraction produced **minimal HEVL blocks**:

```json
"hazard": {
  "event_sets": [{
    "hazards": [{"type": "flood",
                 "intensity_measure": "wd:m"}],
    "analysis_type": "probabilistic",
    "events": []           <- no return periods
  }]
},
"exposure": [
  {"category": "buildings"}  <- no dimension/metric
]
```

A dataset with 6 return periods (10, 25, 50, 100, 200, 500 years) produced one empty event set.

</div>
<div>

### Limitation 2: v1.0 fields needed

RDLS v1.0 introduced provenance fields that regex cannot populate:

```
publisher:        who published
creator:          who created the data
contact_point:    who to contact
lineage.sources:  contributing datasets/models
                  with per-source provenance
attributions:     contributing organisations
                  with their specific role
```

These require **reading and understanding** free-text methodology descriptions - not pattern matching.

<div class="box-red">

**Verdict**: the hybrid approach was the right tool for v0.3. For v1.0, we needed to rethink from the ground up.

</div>

</div>
</div>

---

<!-- _class: act -->

# Act 3
## The LLM-First Pipeline (v1.0)
### One call. Full richness. Zero regex.

---

# Act 3: The Motivation to Remove Regex Entirely

Two forces converged to make a fresh start the right choice.

<div class="columns">
<div>

### v1.0 schema requirements

The new schema demanded fields only an LLM can populate:

- `publisher` / `creator` / `contact_point` entities
- `lineage.sources` - per contributing dataset/model provenance
- Multi-event `event_sets` with explicit return periods
- Multiple `Exposure_item` entries per dataset
- Multiple `Losses` entries with `impact_modelling` and `loss_approach` aligned to analysis type

**Regex cannot read methodology descriptions and identify that OpenStreetMap provided the exposure layer and LISFLOOD-FP generated the inundation depths.**

</div>
<div>

### Economics changed

| Approach | Cost | Coverage |
|----------|------|---------|
| Act 1: Regex only | $0 | 100% |
| Act 2: Regex + LLM | $22 | 68% LLM |
| Act 3: LLM all 26k | **~$123** | **100% LLM** |

$123 for the entire 26,246-dataset corpus - comparable to **less than one day of contractor time**.

The cost of sending everything through the LLM is no longer the limiting factor.

<div class="box-green">

**Decision**: remove the regex gate. Send every dataset to the LLM. Simplify the architecture to a single code path.

</div>

</div>
</div>

---

# Act 3: The New Architecture

```
HDX JSON metadata (raw)
         |
         v
  build_prompt_v10()
  ┌─────────────────────────────────────────────────────────┐
  │  Title + description + tags + organization              │
  │  Methodology text (up to 300 chars)                     │
  │  Resource list: name + format (up to 20)                │
  │  Column headers from CKAN cache (up to 50 columns)      │
  └─────────────────────────────────────────────────────────┘
         |
         v
  LLM: single call  (Claude Haiku 4.5 or Sonnet 4.6)
  ┌─────────────────────────────────────────────────────────┐
  │  1. Is this RDLS? Which components? (H/E/V/L)           │
  │  2. Hazard: type, process, analysis, IMT, return periods│
  │  3. Exposure: list of categories with dimension/metric  │
  │  4. Loss: list of impact entries                        │
  │  5. Countries (ISO3), spatial scale                     │
  │  6. Contributing sources (for lineage)                  │
  │  7. Lineage description text                            │
  └─────────────────────────────────────────────────────────┘
         |
    is_rdls: false ──────> not_rdls/ (70% of HDX)
         |
    is_rdls: true
         |
         v
  build_base_record_v10()   +   integrate_hevl_v10()
         |
         v
  validate_record()
  ┌──────────────────┬────────────────┬──────────────┐
  │ high confidence  │ medium conf.   │ invalid      │
  │ dist/high/       │ dist/medium/   │ dist/invalid/│
  └──────────────────┴────────────────┴──────────────┘
```

---

# Act 3: What the LLM Returns (Full Response)

```json
{
  "is_rdls": true,
  "components": ["hazard", "exposure"],
  "hazard": {
    "type": "flood",
    "process": "fluvial_flood",
    "analysis_type": "probabilistic",
    "imt": "wd:m",
    "calculation_method": "simulated",
    "return_periods": [10, 25, 50, 100, 200, 500],
    "description": "Probabilistic fluvial flood hazard at 6 return periods"
  },
  "exposure": [
    {
      "category": "buildings",
      "dimension": "structure",
      "quantity_kind": "count",
      "description": "Residential and commercial building stock by construction type"
    },
    {
      "category": "population",
      "dimension": "population",
      "quantity_kind": "count",
      "description": "Gridded population at 100m resolution"
    }
  ],
  "loss": null,
  "countries": ["BGD"],
  "spatial_scale": "national",
  "contributing_sources": [
    {"name": "OpenStreetMap", "used_in": "exposure", "type": "dataset"},
    {"name": "LISFLOOD-FP",  "used_in": "hazard",   "type": "model"}
  ],
  "lineage_description": "Flood hazard layers derived from hydrodynamic modelling...",
  "confidence": 0.92
}
```

---

# Act 3: Multi-Return-Period Events

**One event per return period** - equivalent to a hand-authored expert record.

<div class="columns">
<div>

### Act 2 output (regex extraction)

```json
"hazard": {
  "event_sets": [{
    "id": "event_set_1",
    "hazards": [{"type": "flood",
                 "intensity_measure": "wd:m"}],
    "analysis_type": "probabilistic",
    "events": []
  }]
}
```

**One empty event set.**
Return periods: unknown.

</div>
<div>

### Act 3 output (LLM extraction)

```json
"hazard": {
  "event_sets": [{
    "id": "event_set_1",
    "hazards": [{"type": "flood",
                 "process": "fluvial_flood",
                 "intensity_measure": "wd:m"}],
    "analysis_type": "probabilistic",
    "calculation_method": "simulated",
    "events": [
      {"id": "event_10yr",
       "occurrence": {"probabilistic":
         {"return_period": 10, "event_rate": 0.1}}},
      {"id": "event_25yr",  "occurrence": ...},
      {"id": "event_50yr",  "occurrence": ...},
      {"id": "event_100yr", "occurrence": ...},
      {"id": "event_200yr", "occurrence": ...},
      {"id": "event_500yr", "occurrence": ...}
    ]
  }]
}
```

**Six structured events.** Full provenance.

</div>
</div>

---

# Act 3: Multiple Exposure Categories

**One `Exposure_item` per asset type** - each with dimension and measurement.

<div class="columns">
<div>

### Act 2 output

```json
"exposure": [
  {
    "id": "exposure_1",
    "category": "buildings"
  }
]
```

**One category. No dimension. No metric.**

</div>
<div>

### Act 3 output

```json
"exposure": [
  {
    "id": "exposure_1",
    "category": "buildings",
    "asset_type": {
      "id": "buildings",
      "description": "Residential and commercial
        building stock by construction type"
    },
    "metrics": [{
      "id": "metric_1",
      "dimension": "structure",
      "measurement": {"quantity_kind": "count"}
    }]
  },
  {
    "id": "exposure_2",
    "category": "population",
    "asset_type": {"id": "population",
      "description": "Gridded population at 100m"},
    "metrics": [{"id": "metric_1",
      "dimension": "population",
      "measurement": {"quantity_kind": "count"}}]
  }
]
```

</div>
</div>

---

# Act 3: Multiple Loss Entries

**One `Losses` entry per impact type/asset combination** - with semantically correct modelling fields.

```json
"loss": {
  "losses": [
    {
      "id": "loss_1",
      "hazard":           {"type": "flood", "intensity_measure": "wd:m"},
      "asset_category":   "buildings",
      "asset_dimension":  "structure",
      "impact_and_losses": {
        "impact_type":       "direct",
        "impact_modelling":  "simulated",      <- probabilistic model -> simulated
        "impact_metric":     "buildings_damaged_count",
        "loss_approach":     "analytical",     <- model-based -> analytical
        "loss_frequency_type": "probabilistic"
      }
    },
    {
      "id": "loss_2",
      "hazard":           {"type": "flood", "intensity_measure": "wd:m"},
      "asset_category":   "population",
      "asset_dimension":  "population",
      "impact_and_losses": {
        "impact_type":       "indirect",
        "impact_modelling":  "simulated",
        "impact_metric":     "displaced_count",
        "loss_approach":     "analytical",
        "loss_frequency_type": "probabilistic"
      }
    }
  ]
}
```

**Two loss entries** covering building damage AND population displacement - each semantically correct.

---

# Act 3: Impact Modelling Logic (Fixed Bug)

Two fields that were hardcoded wrong - now derived from analysis type.

| Analysis type | `impact_modelling` | `loss_approach` | Rationale |
|--------------|-------------------|----------------|-----------|
| `empirical` | `observed` | `empirical` | Field-collected, directly measured impact |
| `probabilistic` | `simulated` | `analytical` | Model output at defined return periods |
| `deterministic` | `inferred` | `analytical` | Single scenario, analytically derived |

<div class="columns">
<div>

### Before (hardcoded)

```python
impact_modelling = "inferred"  # always
loss_approach = "empirical"    # always
```

A probabilistic flood model would produce:
- `impact_modelling: "inferred"` - wrong
- `loss_approach: "empirical"` - wrong

</div>
<div>

### After (analysis-type-aware)

```python
impact_modelling = (
    "observed"  if analysis_type == "empirical"
    else "simulated" if analysis_type == "probabilistic"
    else "inferred"
)
loss_approach = (
    "empirical"  if analysis_type == "empirical"
    else "analytical"
)
```

Semantically correct for all three analysis types.

</div>
</div>

---

# Act 3: Schema Compliance - 7 Bugs Fixed

Iterative testing against the RDLS v1.0 JSON Schema caught and fixed every failure mode before production.

| # | Error observed | Root cause | Fix |
|---|---------------|------------|-----|
| 1 | `exposure.dimension: 'count' is not one of [...]` | LLM returns non-enum values | `_VALID_DIMENSIONS` check with category default |
| 2 | `spatial: {scale:'global', countries:[...]}` | groups lookup + LLM countries combined | Strip `countries` when `scale='global'` |
| 3 | `loss.hazard.type: 'multiple' is not one of [...]` | LLM uses "multiple" for multi-hazard | Reject in parser; fall back to primary type |
| 4 | `resources.0: not valid under any of the given schemas` | Empty URL fails `Resource.anyOf` | Skip resources with neither URL |
| 5 | `spatial.countries: ['XKX']` fails codelist | LLM generates Kosovo code not in 249-entry list | Filter all country lists against frozenset |
| 6 | `impact_modelling: 'inferred'` for probabilistic | Hardcoded default | Three-way conditional (see previous slide) |
| 7 | `loss_approach: 'empirical'` for model-based | Hardcoded default | Model-type-aware conditional |

<div class="box-green">

**All 7 bugs resolved before the production run. 100-dataset test produced zero invalid records.**

</div>

---

# Act 3: Test Results

A 100-dataset test with Claude Haiku 4.5 after all fixes:

<div class="columns">
<div>

### Classification accuracy

| Outcome | Count |
|---------|------:|
| Total datasets processed | 100 |
| RDLS-relevant | 30 |
| Not-RDLS | 70 |
| Failed (pipeline errors) | **0** |

### Schema validation

| Result | Count |
|--------|------:|
| Schema-valid records | **30 (100%)** |
| Invalid records | **0** |

</div>
<div>

### Extraction richness (the key improvement)

| Metric | Count |
|--------|------:|
| Multi-return-period event sets | 12 |
| Multi-exposure records | 18 |
| Multi-loss records | 24 |

### Cost and tokens

| Metric | Value |
|--------|-------|
| Total cost (100 datasets) | **$0.47** |
| Input tokens / dataset | 3,185 avg |
| Output tokens / dataset | 297 avg |
| Processing time | 6m 36s |
| Estimated full run | ~$123, ~24hrs |

</div>
</div>

---

# Act 3: Model Trade-Off

The pipeline supports any Anthropic Claude model. We evaluated two candidates.

| Attribute | Claude Haiku 4.5 | Claude Sonnet 4.6 |
|-----------|-----------------|------------------|
| Input cost | $1.00 / MTok | $3.00 / MTok |
| Output cost | $5.00 / MTok | $15.00 / MTok |
| Estimated full run cost | **~$123** | ~$370 |
| Measured call latency | ~4s | ~11s |
| Estimated full run time | **~24 hours** | ~80 hours |
| Classification accuracy | Very high | Marginally higher |
| Extraction richness | Rich | Marginally richer |

<div class="box-orange">

**Finding**: for a bulk corpus run, Sonnet's marginal quality gain does not justify 3x the cost and 3.5x the time. Haiku is the correct choice for production.

</div>

---

# Act 3: The Recommended Production Strategy

A two-run hybrid that gets near-Sonnet quality at a fraction of full-Sonnet cost.

```
Step 1: Haiku for all 26,246 datasets
─────────────────────────────────────────────────────────────
  Model: claude-haiku-4-5-20251001
  Config: max_cost_usd = 150   (buffer over ~$123 estimate)

  Output:
    dist/high/     confidence >= 0.7   <- ready for publication
    dist/medium/   confidence 0.4-0.7  <- borderline cases
    not_rdls/      is_rdls: false

  Estimated cost: ~$123  |  Estimated time: ~24 hours

Step 2: Sonnet for medium-confidence tier only
─────────────────────────────────────────────────────────────
  Input: records from dist/medium/ (~5-10% of RDLS records)
  Model: claude-sonnet-4-6
  Estimated records: 1,500 - 2,500

  Process:
    - Remove medium-tier IDs from progress.jsonl
    - Switch config to Sonnet
    - Re-run pipeline (processes only the removed IDs)
    - Haiku high-tier records untouched

  Estimated cost: ~$35-60  |  Estimated time: ~8 hours

Total: ~$160-180  |  Total time: ~32 hours over 2 runs
```

---

# The Journey in Numbers

A side-by-side comparison of all three approaches.

| Metric | Act 1: Regex | Act 2: Hybrid | Act 3: LLM-First |
|--------|-------------|--------------|-----------------|
| Pipeline complexity | 1 phase | 4 phases | 1 phase |
| Records processed | 12,594 | 12,594 | 26,246 |
| LLM coverage | 0% | 68% | **100%** |
| Phantom components | 2,313 | 0 | 0 |
| RDLS-valid records | ~10,281 | 3,312 | TBD (0 invalid in test) |
| Return periods captured | 0 | 0 | Full (LLM-extracted) |
| Multi-exposure records | No | No | **Yes** |
| Multi-loss records | No | No | **Yes** |
| Provenance fields (v1.0) | No | No | **Yes** |
| Total LLM cost | $0 | ~$22 | ~$123 |
| Re-run cost | $0 | $0 | **$0** (cached) |
| Schema target | v0.3 | v0.3 | **v1.0** |
| Classification accuracy | ~82% | ~100% | ~100% |

---

# Key Lessons

<div class="columns3">
<div>

### Lesson 1
**Pattern matching hits a semantic ceiling**

Regex is fast and deterministic. It works for ~82% of cases where metadata accurately describes content. But it cannot cross the boundary from *topic* to *content*. The 18% that crosses that boundary cannot be fixed by adding more patterns.

</div>
<div>

### Lesson 2
**Column headers are the decisive signal**

The single most impactful enhancement in Act 2 was not the LLM model choice - it was injecting actual data column headers. `People_Displaced` in a CSV column header is unambiguous in a way that "displaced populations" in a dataset title is not.

</div>
<div>

### Lesson 3
**Prompt precision beats model scale**

A compact, low-cost model with precise RDLS component definitions achieved 94% confidence. The key was encoding the content-vs-topic distinction in the prompt. For classification tasks, domain-specific definitions outweigh model scale.

</div>
</div>

<div class="columns3">
<div>

### Lesson 4
**Caching eliminates iteration cost**

Responses cached by prompt hash. Once classification is done, code fixes, schema updates, and output format changes re-run at USD 0 LLM cost. Iterative refinement is economically free after the first run.

</div>
<div>

### Lesson 5
**Economics drove architecture**

The shift from hybrid to LLM-first was enabled by cost, not capability. Haiku pricing made sending all 26,246 records to the LLM cheaper than most data collection tasks. When the tool becomes affordable, the architectural compromise becomes unnecessary.

</div>
<div>

### Lesson 6
**Test against the schema, not assumptions**

7 schema bugs were found through iterative testing against the v1.0 validator. Every one was a case where we had a plausible-sounding value that turned out to be wrong (`"inferred"` for probabilistic outputs, `"multiple"` for hazard types, Kosovo's `XKX` code).

</div>
</div>

---

# Next Steps

<div class="columns">
<div>

### Immediate (production run)

1. **Run Haiku for all 26,246 datasets**
   - Pipeline is ready. Config set to Haiku.
   - Estimated 24 hours, ~$123.
   - Resumes automatically if interrupted.

2. **Run Sonnet on medium-confidence tier**
   - ~1,500-2,500 records (5-10% of RDLS records)
   - Estimated 8 hours, ~$35-60.
   - Replaces borderline Haiku outputs.

3. **Quality review of output**
   - Sample from high/medium tiers
   - Spot-check multi-return-period events
   - Verify contributing source extraction

</div>
<div>

### Downstream

4. **Semantic validation (Layer 2)**
   - Run `scripts/validate_records_v03.py` across all records
   - Catch open-codelist and cross-field errors
   - Fix any systematic patterns found

5. **Publication pipeline**
   - Integrate into RDLS public catalogue
   - Generate search indexes
   - Link back to HDX source URLs

6. **Other data portals**
   - Same pipeline architecture applies to GeoNode, World Bank Data Catalog, CKAN instances
   - Only the source adapter needs changing

</div>
</div>

---

<!-- _class: title -->

# Thank You

**From 60 seconds of regex to 7,146 structured RDLS records**
**From template metadata to full HEVL richness at scale**

<br>

**Resources**

- Source code: `github.com/bennyistanto/to-rdls`
- v0.3 hybrid pipeline: `docs/llm_assisted_metadata_classification.md`
- v1.0 LLM-first pipeline: `docs/llm_pipeline_architecture.md`
- RDLS standard: `docs.riskdatalibrary.org`

<br>

*Global Facility for Disaster Reduction and Recovery (GFDRR)*
*Digital Earth Team - World Bank*
*May 2026*
