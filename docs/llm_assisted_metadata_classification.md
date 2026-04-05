# From Keywords to Understanding: LLM-Assisted Classification for Risk Data Cataloguing

> **Global Facility for Disaster Reduction and Recovery (GFDRR), World Bank**
> March 2026

---

## Summary

Cataloguing risk datasets requires classifying each dataset by the type of risk information it contains: hazard measurements, exposure inventories, vulnerability models, or loss assessments. We developed an automated pipeline to transform 12,594 candidate datasets -- identified from 26,246 total datasets on the Humanitarian Data Exchange (HDX) as of February 2026 -- into the Risk Data Library Standard (RDLS) schema. An initial approach using keyword and pattern matching correctly classified the majority of records but systematically misclassified 18% of datasets where metadata described the *context* rather than the data *content*. To address this, we introduced a hybrid pipeline that combines deterministic pattern matching with Large Language Model (LLM) semantic review, enriched by actual data column headers. The LLM-assisted pipeline reclassified 3,511 records (27.9%), separated 5,448 non-risk datasets, and produced 7,146 RDLS-relevant records at a total cost of approximately USD 22. This report describes the approach, results, and lessons learned for applying LLM-assisted classification to structured metadata transformation at scale.

---

## 1. Introduction

### 1.1 Background

The [Risk Data Library Standard (RDLS)](https://docs.riskdatalibrary.org/) is an open schema for cataloguing risk data. It classifies datasets into four component types, collectively referred to as HEVL:

- **Hazard (H)** -- measurements or models of natural hazards (e.g., flood depth grids, seismic intensity maps, drought indices)
- **Exposure (E)** -- spatial inventories of assets or populations at risk (e.g., building footprints, population grids, facility registries)
- **Vulnerability (V)** -- damage or fragility functions relating hazard intensity to expected damage
- **Loss (L)** -- post-event impact data (e.g., casualties, displacement, economic damage, operational status)

Accurate HEVL classification is essential for the catalogue's usability: researchers searching for hazard data should not find loss assessments mislabelled as hazard records.

### 1.2 Objective

The [Humanitarian Data Exchange (HDX)](https://data.humdata.org/), managed by the United Nations Office for the Coordination of Humanitarian Affairs (OCHA), hosts 26,246 datasets as of February 2026. Of these, 12,594 were identified as potential RDLS candidates -- datasets that may contain one or more HEVL components based on initial screening. The objective was to transform these 12,594 candidates into RDLS-compliant JSON records with correct HEVL classification.

---

## 2. Methods

### 2.1 Phase 1: Signal-Based Classification (Pattern Matching)

The initial pipeline used regular expression (regex) pattern matching against metadata fields -- titles, tags, descriptions, and resource filenames -- to assign HEVL components through weighted signal scoring. Keywords such as *"earthquake"*, *"flood depth"*, or *"population density"* triggered component assignments based on a curated dictionary of 400+ patterns mapped to RDLS codelists.

This approach was deterministic, reproducible, and processed all 12,594 records in under 60 seconds. It correctly handled cases where metadata accurately described data content (e.g., a dataset titled *"Global Flood Hazard Map"* containing flood depth grids).

### 2.2 The Classification Gap

A systematic limitation emerged: **pattern matching could not distinguish between what a dataset is *about* and what it actually *contains***. This is referred to as *content-blind over-classification*.

For example, a dataset titled *"Kenya Drought-Related Key Figures"* received all four HEVL components because:
- *"drought"* triggered **Hazard** (with a fabricated intensity measure `SPI:-`)
- *"population"* triggered **Exposure** (with a fabricated asset category)
- *"affected"* triggered **Vulnerability**
- *"mortality"* triggered **Loss**

In reality, the dataset contained exclusively post-event impact statistics (displacement counts, mortality figures) -- a **Loss-only** dataset. The pipeline had fabricated three components based on topical keywords rather than actual data content.

This pattern affected **2,313 records** (18.4% of total), predominantly disaster impact assessments, satellite damage analyses, and displacement monitoring datasets. These records contained fabricated hazard blocks with template descriptions (e.g., *"earthquake hazard data for Papua New Guinea"*) and invented measurement parameters that did not exist in the underlying datasets.

### 2.3 Phase 2: LLM-Assisted Review Pipeline

To resolve the classification gap, we introduced a 4-phase review pipeline using a Large Language Model (LLM) -- specifically Anthropic's Claude Haiku 4.5, a compact model optimised for classification tasks.

**Phase 1 -- Signal Triage.** The regex signal scores from the initial pipeline were used to sort records into confidence buckets. High-confidence records (4,088) were retained without LLM review. Borderline (7,836), no-signal (455), and a 5% validation sample (215) were sent to the LLM -- totalling 8,506 records.

**Phase 2 -- Column Enrichment.** For each record, we injected the actual column headers of its underlying data files. These headers were obtained through a one-time crawl of the HDX CKAN Application Programming Interface (API), covering 88,327 resources across all datasets. Column headers such as *"Status Post Earthquake (Open/Closed)"* and *"Health Facility Building Damaged (Severe/Moderate/Minor)"* provided the LLM with direct evidence of data content.

**Phase 3 -- LLM Classification.** Each record was classified by the LLM using a structured prompt containing: (a) the dataset's metadata (title, description, tags, publisher), (b) the data column headers from Phase 2, and (c) strict RDLS component definitions.

The critical element was the prompt's definitions, which explicitly distinguished data content from topic:

> *"Hazard: The dataset CONTAINS hazard measurements or models. A dataset that merely REFERENCES a hazard event does NOT contain hazard data."*
>
> *"Loss: The dataset CONTAINS post-event impact data -- casualties, displacement, damage status, affected populations, economic losses."*

**Phase 4 -- Reconciliation.** LLM classifications were merged with the original signal-based assessments. The pipeline verified that each component in `risk_data_type` corresponded to an actual data block in the JSON record (preventing phantom components). Record identifiers and filenames were rebuilt to reflect the corrected classification.

---

## 3. Results

### 3.1 Overall Pipeline Output

| Metric | Count | Percentage |
|--------|------:|----------:|
| HDX datasets (total) | 26,246 | -- |
| RDLS candidates (input) | 12,594 | 100% |
| Reclassified by LLM | 3,511 | 27.9% |
| Non-risk datasets separated | 5,448 | 43.3% |
| **RDLS-relevant records (final)** | **7,146** | **56.7%** |
| Schema-valid (production-ready) | 3,312 | 46.3% of relevant |
| Schema-invalid (known data quality issues) | 3,834 | 53.7% of relevant |
| Record identifiers renamed | 2,425 | 19.3% |

### 3.2 Non-Risk Dataset Separation

The LLM identified 4,794 records as not containing risk data. An additional 654 records had no HEVL data blocks after phantom component reconciliation. These 5,448 records were separated into the following domain categories:

| Domain | Count | Description |
|--------|------:|-------------|
| General (other) | 2,657 | Education statistics, price indices, trade data, gender indicators |
| Health | 941 | Disease surveillance, nutrition surveys, health facility assessments |
| Reference | 894 | Administrative boundaries, code lists, gazetteers |
| Humanitarian operations | 263 | Aid delivery tracking, camp management, 3W/4W matrices |
| Climate (non-risk) | 39 | General climate records without risk modelling context |
| No HEVL blocks | 654 | Records where no component data existed in the file |

### 3.3 Classification Accuracy

| Measure | Value |
|---------|-------|
| Average LLM confidence score | 0.94 (scale 0--1) |
| High confidence (>=0.9) | 88.2% of classifications |
| Medium confidence (0.7--0.9) | 11.7% |
| Low confidence (<0.7) | 0.1% (11 records) |
| Validation sample disagreements | 132 of 215 (LLM overrode regex in 61% of cross-checked records) |

### 3.4 Cost and Performance

| Metric | Value |
|--------|-------|
| LLM model | Claude Haiku 4.5 (Anthropic) |
| Total LLM API cost (initial run) | ~USD 22 |
| Re-run cost (cached) | USD 0 |
| Processing time (with cache) | ~22 minutes |
| Column header crawl (one-time) | ~48 hours |

---

## 4. Illustrative Case

At first glance, a dataset titled *"Kenya Drought-Related Key Figures"* appears straightforward: the title signals drought (hazard), the description mentions populations (exposure) and affected communities (vulnerability). Every keyword points to a multi-component risk dataset. Yet when the actual data resources are opened, they contain only displacement counts, mortality figures, and people in need -- purely loss metrics, with no hazard measurements, no asset inventories, and no fragility curves. With 12,594 datasets, manual inspection of every resource is not feasible. This is precisely where the LLM adds value: it evaluates the evidence holistically and reaches the correct classification that keyword matching alone cannot.

**Dataset**: *Kenya Drought-Related Key Figures* ([HDX](https://data.humdata.org/dataset/kenya-drought-related-key-figures))

| Attribute | Before (pattern matching) | After (LLM-assisted) |
|-----------|--------------------------|---------------------|
| Classification | Hazard + Exposure + Vulnerability + Loss | **Loss only** |
| Hazard block | *"drought hazard data for Kenya"*, intensity: `SPI:-` | Removed -- no drought index data in file |
| Exposure block | Template from *"population"* keyword | Removed -- no asset or population inventory |
| Vulnerability block | Template from *"affected"* keyword | Removed -- no fragility functions |
| LLM reasoning | -- | *"Dataset contains displacement figures, mortality counts, and people in need -- post-event impact metrics, not hazard measurements or exposure inventories"* |
| Confidence | -- | 0.95 |

---

## 5. Discussion and Lessons Learned

**5.1 Pattern matching reaches a ceiling at the semantic boundary.** Regex-based classification performs well when metadata accurately describes data content (e.g., *"Global Flood Hazard Map"*). It fails systematically when metadata describes the context rather than the data type. For the HDX collection, this boundary affected approximately 1 in 5 records.

**5.2 Data column headers provide critical evidence.** Enriching the LLM prompt with actual column names from the underlying data files was the single most impactful design decision. Column headers such as *"Building Damaged (Severe/Moderate/Minor)"* provide unambiguous evidence that the data contains loss observations, not vulnerability functions -- a distinction that no amount of title or tag analysis can resolve.

**5.3 Precise definitions outweigh model complexity.** The LLM prompt explicitly encoded the RDLS distinction between data content and topic for each of the four component types. With these definitions, even a compact model (Claude Haiku, the smallest in Anthropic's model family) achieved 94% average confidence with effectively zero classification errors. This suggests that for structured classification tasks, **prompt engineering with domain-specific definitions is more important than model scale**.

**5.4 Hybrid architectures balance cost and precision.** The regex pipeline processes 4,088 clear-cut cases at zero marginal cost. The LLM handles 8,506 semantically ambiguous cases at approximately USD 0.003 per record. Neither approach alone achieves both the coverage and precision required. The hybrid design reduces LLM costs by 48% while maintaining full coverage.

**5.5 Response caching eliminates recurring costs.** All LLM responses are cached by a hash of the input prompt. The initial classification run costs approximately USD 22. Every subsequent pipeline execution -- including after code corrections, schema updates, or output format changes -- reuses cached responses at zero additional cost. This makes iterative refinement economically viable.

---

## 6. Applicability and Replicability

The pipeline architecture generalises to any metadata transformation task where:

1. **Source metadata describes context rather than content** -- a common characteristic of humanitarian, development, and scientific data portals.
2. **The target schema requires semantic precision** about data types, not just topical tags.
3. **Volume exceeds manual review capacity** but involves nuances that pure pattern matching cannot resolve.

The four-phase design (triage, enrichment, LLM classification, reconciliation) is reusable. Adapting it to a different target schema requires replacing the RDLS component definitions in the prompt with the target schema's type definitions. The column enrichment strategy applies to any data portal built on [CKAN](https://ckan.org/) or similar platforms that expose resource-level metadata through an API.

---

## 7. Conclusion

Pattern-based metadata classification provides a fast and deterministic foundation for large-scale data cataloguing. However, when source metadata describes the *context* of data rather than its *content*, pattern matching alone produces systematic misclassification. Introducing LLM-assisted semantic review -- enriched with actual data column headers and guided by precise schema definitions -- resolved this gap for the RDLS catalogue, correctly reclassifying 3,511 records and separating 5,448 non-risk datasets at a total cost of approximately USD 22.

The resulting catalogue of 7,146 RDLS-relevant records, with 3,312 schema-valid and production-ready, represents the largest automated transformation of HDX metadata into the Risk Data Library Standard to date.

---

*Source code: https://github.com/bennyistanto/to-rdls*</br>
*Detailed output analysis: https://github.com/bennyistanto/to-rdls/blob/main/docs/llm_review_output.md*</br>
*LLM Review Guide: https://github.com/bennyistanto/to-rdls/blob/main/docs/llm_review_guide.md*</br>
