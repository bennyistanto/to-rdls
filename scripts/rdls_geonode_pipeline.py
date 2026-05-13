r"""
rdls_geonode_pipeline.py
=========================
End-to-end pipeline: crawl GeoNode portals -> classify -> translate ->
extract HEVL -> integrate -> validate -> distribute RDLS records.

Follows the same pipeline as HDX but uses GeoNode REST API v2 as the
metadata source.  GeoNode provides ISO 19115-aligned metadata which is
richer than HDX/CKAN -- the adapter extracts both the common dict (for
the standard pipeline) and GeoNode-native fields (for enrichment).

Output structure per portal:
    output/geonode/{portal_name}/
        01_raw/          Raw GeoNode API responses (one JSON per dataset)
        02_records/      Extracted common dict fields
        03_classified/   Classification results (RDLS candidates only)
        not_rdls/        Datasets rejected by classifier
        04_translated/   RDLS base records
        05_extracted/    HEVL extraction results
        06_integrated/   Merged HEVL blocks into RDLS records
        07_validated/    Final validated + tiered distribution
            high/        Composite confidence >= 0.8
            medium/      Composite confidence >= 0.5
            low/         Composite confidence < 0.5
            invalid/     Schema validation failures
        reports/         Summary CSVs and logs

Usage:
    cd C:\Users\benny\OneDrive\Documents\Github\to-rdls
    conda activate to-rdls
    set PYTHONPATH=C:\Users\benny\OneDrive\Documents\Github\to-rdls
    python notebooks\rdls_geonode_pipeline.py

Resumable: each step checks for existing output and skips already-processed
datasets.  Delete the output folder to force a full re-run.
"""

import csv
import json
import sys
import time
from collections import Counter
from pathlib import Path

# --- Paths ---
PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR))

CONFIGS_DIR = PROJECT_DIR / "configs"
OUTPUT_DIR = PROJECT_DIR / "output" / "geonode"
SCHEMA_PATH = PROJECT_DIR / "schema" / "rdls_schema_v0.3.json"

# --- Stage folder names ---
STAGE_RAW = "01_raw"
STAGE_RECORDS = "02_records"
STAGE_CLASSIFIED = "03_classified"
STAGE_NOT_RDLS = "not_rdls"
STAGE_TRANSLATED = "04_translated"
STAGE_EXTRACTED = "05_extracted"
STAGE_INTEGRATED = "06_integrated"
STAGE_VALIDATED = "07_validated"
STAGE_REPORTS = "reports"

# --- Imports ---
from src.utils import load_json, load_yaml, write_json
from src.sources.geonode import (
    GeoNodeConfig,
    GeoNodeClient,
    iter_datasets,
    normalize_geonode_record,
    extract_geonode_fields,
)
from src.classify import classify_dataset
from src.translate_v03 import build_rdls_record, load_format_config, load_license_config
from src.spatial import (
    load_spatial_config, build_iso3_table_from_naming, country_name_to_iso3,
)
from src.extract_hazard import HazardExtractor, build_hazard_block
from src.extract_exposure import ExposureExtractor, build_exposure_block
from src.extract_vulnloss import (
    VulnerabilityExtractor, LossExtractor,
    build_vulnerability_block, build_loss_block,
)
from src.integrate import integrate_record
from src.validate import validate_and_score, distribute_records


def portal_dir(portal_name: str, stage: str) -> Path:
    """Build and create output directory for a portal + stage."""
    d = OUTPUT_DIR / portal_name / stage
    d.mkdir(parents=True, exist_ok=True)
    return d


# ================================================================
# STEP 0: Configuration
# ================================================================
print("=" * 60)
print("  STEP 0: LOAD CONFIGURATION")
print("=" * 60)

geonode_config = GeoNodeConfig.from_yaml(
    str(CONFIGS_DIR / "sources" / "geonode.yaml")
)
classification_config = load_yaml(str(CONFIGS_DIR / "classification.yaml"))
signal_config = load_yaml(str(CONFIGS_DIR / "signal_dictionary.yaml"))
defaults_config = load_yaml(str(CONFIGS_DIR / "rdls_defaults.yaml"))
format_config = load_format_config(str(CONFIGS_DIR / "format_mapping.yaml"))
license_config = load_license_config(str(CONFIGS_DIR / "license_mapping.yaml"))
spatial_config = load_spatial_config(str(CONFIGS_DIR / "spatial.yaml"))
naming_config = load_yaml(str(CONFIGS_DIR / "naming.yaml"))
# Build name→ISO3 reverse table from naming config (pycountry fallback)
spatial_config["iso3_table"] = build_iso3_table_from_naming(naming_config)
schema_config = load_yaml(str(CONFIGS_DIR / "rdls_schema.yaml"))
pipeline_config = load_yaml(str(CONFIGS_DIR / "pipeline.yaml"))

if not geonode_config.portals:
    print("ERROR: No portals configured in geonode.yaml.")
    print("Add at least one portal entry with name and base_url.")
    sys.exit(1)

enabled = [p for p in geonode_config.portals if p.enabled]
print(f"Portals configured: {len(geonode_config.portals)} "
      f"({len(enabled)} enabled)")
for p in enabled:
    print(f"  - {p.name}: {p.base_url} "
          f"(ssl={'on' if p.verify_ssl else 'off'})")

schema = load_json(str(SCHEMA_PATH)) if SCHEMA_PATH.exists() else None
if schema is None:
    print(f"WARNING: Schema not found at {SCHEMA_PATH}, "
          "validation will be skipped")

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
t0 = time.time()


# ================================================================
# STEP 1: Crawl portal metadata
# ================================================================
print("\n" + "=" * 60)
print("  STEP 1: CRAWL GEONODE PORTALS")
print("=" * 60)

crawl_counts: Counter = Counter()

for portal in enabled:
    raw_dir = portal_dir(portal.name, STAGE_RAW)
    existing = set(f.stem for f in raw_dir.glob("*.json"))
    print(f"\nPortal: {portal.name} ({portal.base_url})")
    print(f"  Existing raw files: {len(existing)}")

    client = GeoNodeClient(portal)
    if not client.probe_api():
        print(f"  WARNING: API v2 not available, skipping")
        continue

    new_count = 0
    for ds in iter_datasets(
        client, portal, geonode_config.max_datasets_per_portal,
        hevl_keywords=geonode_config.hevl_keywords,
        hevl_categories=geonode_config.hevl_categories,
    ):
        ds_id = str(ds.get("pk") or ds.get("uuid") or "")
        if not ds_id:
            continue
        if ds_id in existing:
            continue

        write_json(str(raw_dir / f"{ds_id}.json"), ds)
        new_count += 1
        crawl_counts[portal.name] += 1

        if new_count % 50 == 0:
            print(f"  Crawled {new_count} new datasets...")

    total = len(list(raw_dir.glob("*.json")))
    print(f"  Done: {new_count} new, {total} total")

crawl_time = time.time() - t0
print(f"\nCrawl complete: {sum(crawl_counts.values())} new datasets "
      f"in {crawl_time:.1f}s")


# ================================================================
# STEP 2: Normalize & extract fields
# ================================================================
print("\n" + "=" * 60)
print("  STEP 2: NORMALIZE & EXTRACT FIELDS")
print("=" * 60)

extract_counts: Counter = Counter()

for portal in enabled:
    raw_dir = portal_dir(portal.name, STAGE_RAW)
    records_dir = portal_dir(portal.name, STAGE_RECORDS)
    existing = set(f.stem for f in records_dir.glob("*.json"))
    raw_files = sorted(raw_dir.glob("*.json"))
    print(f"\nPortal: {portal.name} - {len(raw_files)} raw, "
          f"{len(existing)} already extracted")

    new_count = 0
    for raw_path in raw_files:
        ds_id = raw_path.stem
        if ds_id in existing:
            continue
        raw = load_json(str(raw_path))
        ds = normalize_geonode_record(raw)
        fields = extract_geonode_fields(
            ds,
            portal_name=portal.name,
            portal_base_url=portal.base_url,
            category_tag_map=geonode_config.category_tag_map,
            skip_link_types=geonode_config.skip_link_types,
            link_modality_map=geonode_config.link_modality_map,
            mime_format_map=geonode_config.mime_format_map,
            title_humanize_config=geonode_config.title_humanize_config,
        )
        write_json(str(records_dir / f"{ds_id}.json"), fields)
        new_count += 1
        extract_counts[portal.name] += 1

    total = len(list(records_dir.glob("*.json")))
    print(f"  Extracted: {new_count} new, {total} total")


# ================================================================
# STEP 3: Classify
# ================================================================
print("\n" + "=" * 60)
print("  STEP 3: CLASSIFY DATASETS")
print("=" * 60)

classify_counts: Counter = Counter()
skip_counts: Counter = Counter()

for portal in enabled:
    records_dir = portal_dir(portal.name, STAGE_RECORDS)
    classified_dir = portal_dir(portal.name, STAGE_CLASSIFIED)
    not_rdls_dir = portal_dir(portal.name, STAGE_NOT_RDLS)
    existing_cls = set(f.stem for f in classified_dir.glob("*.json"))
    existing_nr = set(f.stem for f in not_rdls_dir.glob("*.json"))
    existing = existing_cls | existing_nr
    record_files = sorted(records_dir.glob("*.json"))
    print(f"\nPortal: {portal.name} - {len(record_files)} records, "
          f"{len(existing)} already processed")

    for rec_path in record_files:
        ds_id = rec_path.stem
        if ds_id in existing:
            continue

        fields = load_json(str(rec_path))
        result = classify_dataset(fields, classification_config)

        if not result.rdls_candidate:
            # Save to not_rdls with reason
            out = {
                "fields": fields,
                "classification": {
                    "scores": result.scores,
                    "components": result.components,
                    "confidence": result.confidence,
                    "rdls_candidate": False,
                },
            }
            write_json(str(not_rdls_dir / f"{ds_id}.json"), out)
            skip_counts[portal.name] += 1
            continue

        out = {
            "fields": fields,
            "classification": {
                "scores": result.scores,
                "components": result.components,
                "confidence": result.confidence,
                "top_signals": result.top_signals,
            },
        }
        write_json(str(classified_dir / f"{ds_id}.json"), out)
        classify_counts[portal.name] += 1

    print(f"  Candidates: {classify_counts[portal.name]}, "
          f"Not RDLS: {skip_counts[portal.name]}")

total_candidates = sum(classify_counts.values())
total_skipped = sum(skip_counts.values())
print(f"\nClassification: {total_candidates} candidates, "
      f"{total_skipped} not RDLS")


# ================================================================
# STEP 4: Translate to RDLS
# ================================================================
print("\n" + "=" * 60)
print("  STEP 4: TRANSLATE TO RDLS RECORDS")
print("=" * 60)

translate_counts: Counter = Counter()
translate_skip: Counter = Counter()

for portal in enabled:
    classified_dir = portal_dir(portal.name, STAGE_CLASSIFIED)
    translated_dir = portal_dir(portal.name, STAGE_TRANSLATED)
    existing = set(f.stem for f in translated_dir.glob("*.json"))
    classified_files = sorted(classified_dir.glob("*.json"))
    print(f"\nPortal: {portal.name} - {len(classified_files)} classified")

    for cls_path in classified_files:
        ds_id = cls_path.stem
        data = load_json(str(cls_path))
        fields = data["fields"]
        components = data["classification"]["components"]

        # Set org_slug to portal name for better ID generation
        # (e.g., rdls_exp-bwa_rcmrd_settlements instead of patrick_kabatha)
        fields["org_slug"] = fields.get("_source_portal", portal.name)

        record = build_rdls_record(
            fields, components,
            spatial_config=spatial_config,
            format_config=format_config,
            license_config=license_config,
            naming_config=naming_config,
        )
        if record is None:
            translate_skip[portal.name] += 1
            continue

        # --- GeoNode-specific spatial enrichment ---
        # Use authoritative ISO3 codes from GeoNode regions when available.
        # When the title identifies a specific country (via 2-letter prefix),
        # narrow multi-region records to that single country.
        iso3_table = spatial_config.get("iso3_table", {})
        country_fixes = spatial_config.get("country_name_fixes", {})
        region_iso3 = fields.get("_region_iso3_codes", [])
        if region_iso3:
            import re as _re
            slug_title = fields.get("_slug_title", "")
            title_cc_match = _re.match(r'^([A-Z]{2})_', slug_title)
            title_iso3 = None
            if title_cc_match:
                _CC_TO_ISO3 = {
                    "CK": "COK", "FJ": "FJI", "FM": "FSM", "KI": "KIR",
                    "MH": "MHL", "NR": "NRU", "NU": "NIU", "PG": "PNG",
                    "PW": "PLW", "SB": "SLB", "TL": "TLS", "TO": "TON",
                    "TV": "TUV", "VU": "VUT", "WS": "WSM",
                }
                title_iso3 = _CC_TO_ISO3.get(title_cc_match.group(1))
            if title_iso3 and title_iso3 in region_iso3:
                record["spatial"] = {"scale": "national", "countries": [title_iso3]}
            elif len(region_iso3) == 1:
                record["spatial"] = {"scale": "national", "countries": region_iso3}
            elif len(region_iso3) <= 5:
                record["spatial"] = {"scale": "regional", "countries": sorted(region_iso3)}
            else:
                for candidate in record.get("title", "").split():
                    iso3 = country_name_to_iso3(
                        candidate, fixes=country_fixes, iso3_table=iso3_table,
                    )
                    if iso3 and iso3 in region_iso3:
                        record["spatial"] = {"scale": "national", "countries": [iso3]}
                        break
        elif record["spatial"].get("scale") == "global":
            candidates = list(fields.get("tags", []))
            candidates.extend(record.get("title", "").split())
            for candidate in candidates:
                iso3 = country_name_to_iso3(
                    candidate, fixes=country_fixes, iso3_table=iso3_table,
                )
                if iso3:
                    record["spatial"] = {"scale": "national", "countries": [iso3]}
                    break

        # Add bbox from GeoNode spatial metadata
        gn_spatial = fields.get("_geonode_spatial", {})
        bbox_poly = gn_spatial.get("bbox")
        if bbox_poly and isinstance(bbox_poly, dict):
            coords = bbox_poly.get("coordinates", [[]])
            if coords and coords[0]:
                ring = coords[0]
                lons = [p[0] for p in ring]
                lats = [p[1] for p in ring]
                record["spatial"]["bbox"] = [
                    min(lons), min(lats), max(lons), max(lats)
                ]
        # Add coordinate system from SRID
        srid = gn_spatial.get("srid", "")
        if srid:
            for res in record.get("resources", []):
                if "coordinate_system" not in res:
                    res["coordinate_system"] = srid

        rdls_id = record.get("id", ds_id)
        # Save record + original fields (extractors need common dict fields)
        write_json(str(translated_dir / f"{rdls_id}.json"), {
            "record": record,
            "fields": fields,
        })
        translate_counts[portal.name] += 1

    print(f"  Translated: {translate_counts[portal.name]}, "
          f"Skipped: {translate_skip[portal.name]}")


# ================================================================
# STEP 5: HEVL extraction
# ================================================================
print("\n" + "=" * 60)
print("  STEP 5: HEVL EXTRACTION")
print("=" * 60)

hevl_counts: Counter = Counter()

hazard_extractor = HazardExtractor(signal_config, defaults_config)
exposure_extractor = ExposureExtractor(signal_config, defaults_config)
vulnerability_extractor = VulnerabilityExtractor(signal_config, defaults_config)
loss_extractor = LossExtractor(signal_config, defaults_config)

for portal in enabled:
    translated_dir = portal_dir(portal.name, STAGE_TRANSLATED)
    extracted_dir = portal_dir(portal.name, STAGE_EXTRACTED)
    existing = set(f.stem for f in extracted_dir.glob("*.json"))
    translated_files = sorted(translated_dir.glob("*.json"))
    print(f"\nPortal: {portal.name} - {len(translated_files)} translated")

    for tr_path in translated_files:
        rdls_id = tr_path.stem
        if rdls_id in existing:
            continue

        data = load_json(str(tr_path))
        record = data.get("record", data)  # support both old/new format
        fields = data.get("fields", record)  # use original fields for extraction
        rdt = record.get("risk_data_type", [])

        h_block = None
        e_block = None
        v_block = None
        l_block = None

        # HEVL extractors use common dict fields (name, notes, tags, methodology)
        # NOT the translated RDLS record (title, description, details)
        if "hazard" in rdt:
            h_extraction = hazard_extractor.extract(fields)
            h_block = build_hazard_block(h_extraction)
        if "exposure" in rdt:
            e_extraction = exposure_extractor.extract(fields)
            e_block = build_exposure_block(e_extraction)
        if "vulnerability" in rdt:
            v_extraction = vulnerability_extractor.extract(fields)
            v_block = build_vulnerability_block(v_extraction)
        if "loss" in rdt:
            l_extraction = loss_extractor.extract(fields)
            l_block = build_loss_block(l_extraction)

        out = {
            "record": record,
            "blocks": {
                "hazard": h_block,
                "exposure": e_block,
                "vulnerability": v_block,
                "loss": l_block,
            },
        }
        write_json(str(extracted_dir / f"{rdls_id}.json"), out)
        hevl_counts[portal.name] += 1

    print(f"  Extracted: {hevl_counts[portal.name]}")


# ================================================================
# STEP 6: Integrate
# ================================================================
print("\n" + "=" * 60)
print("  STEP 6: INTEGRATE HEVL BLOCKS")
print("=" * 60)

integrate_counts: Counter = Counter()

for portal in enabled:
    extracted_dir = portal_dir(portal.name, STAGE_EXTRACTED)
    integrated_dir = portal_dir(portal.name, STAGE_INTEGRATED)
    existing = set(f.stem for f in integrated_dir.glob("*.json"))
    extracted_files = sorted(extracted_dir.glob("*.json"))
    print(f"\nPortal: {portal.name} - {len(extracted_files)} extracted")

    for ext_path in extracted_files:
        rdls_id = ext_path.stem
        if rdls_id in existing:
            continue

        data = load_json(str(ext_path))
        record = data["record"]
        blocks = data["blocks"]

        # Build provenance note for GeoNode source
        source_url = record.get("resources", [{}])[0].get("access_url", "")
        prov_note = (
            "[Source: This metadata record was automatically extracted from "
            f"GeoNode portal {portal.name}]"
        )

        merged = integrate_record(
            record,
            hazard_block=blocks.get("hazard"),
            exposure_block=blocks.get("exposure"),
            vulnerability_block=blocks.get("vulnerability"),
            loss_block=blocks.get("loss"),
            naming_config=naming_config,
            provenance_note=prov_note,
            org_slug=portal.name,
        )
        if merged is None:
            continue

        # Use new ID from integration (rebuilt with naming config)
        merged_id = merged.get("id", rdls_id)
        write_json(str(integrated_dir / f"{merged_id}.json"), merged)
        integrate_counts[portal.name] += 1

    print(f"  Integrated: {integrate_counts[portal.name]}")


# ================================================================
# STEP 7: Validate & distribute
# ================================================================
print("\n" + "=" * 60)
print("  STEP 7: VALIDATE & DISTRIBUTE")
print("=" * 60)

tier_counts: Counter = Counter()

for portal in enabled:
    integrated_dir = portal_dir(portal.name, STAGE_INTEGRATED)
    dist_dir = portal_dir(portal.name, STAGE_VALIDATED)
    reports_dir = portal_dir(portal.name, STAGE_REPORTS)

    integrated_files = sorted(integrated_dir.glob("*.json"))
    print(f"\nPortal: {portal.name} - {len(integrated_files)} integrated")

    if not integrated_files:
        continue

    records = []
    for int_path in integrated_files:
        records.append(load_json(str(int_path)))

    # Validate each record
    validated = []  # list of (record, validation_result) tuples
    for record in records:
        result = validate_and_score(record, schema)
        validated.append((record, result))
        tier = result.get("tier", "unknown")
        tier_counts[tier] += 1

    # Distribute to tiered folders
    tier_dist = distribute_records(validated, str(dist_dir))

    # Write per-portal summary CSV
    csv_path = reports_dir / "validation_summary.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "rdls_id", "tier", "is_valid", "error_count",
            "confidence"
        ])
        for record, result in validated:
            writer.writerow([
                record.get("id", ""),
                result.get("tier", ""),
                result.get("is_valid", ""),
                len(result.get("errors", [])),
                result.get("confidence", ""),
            ])

    for tier, count in sorted(tier_dist.items()):
        print(f"  {tier}: {count}")


# ================================================================
# STEP 8: Summary report
# ================================================================
print("\n" + "=" * 60)
print("  SUMMARY")
print("=" * 60)

total_time = time.time() - t0

for portal in enabled:
    counts = {}
    for stage_name, stage_dir_name in [
        ("Raw datasets", STAGE_RAW),
        ("Extracted records", STAGE_RECORDS),
        ("RDLS candidates", STAGE_CLASSIFIED),
        ("Not RDLS", STAGE_NOT_RDLS),
        ("Translated", STAGE_TRANSLATED),
        ("HEVL extracted", STAGE_EXTRACTED),
        ("Integrated", STAGE_INTEGRATED),
    ]:
        d = OUTPUT_DIR / portal.name / stage_dir_name
        counts[stage_name] = len(list(d.glob("*.json"))) if d.exists() else 0

    print(f"\n  {portal.name} ({portal.base_url}):")
    for label, count in counts.items():
        print(f"    {label:20s}: {count}")

    # Tier breakdown from validated
    val_dir = OUTPUT_DIR / portal.name / STAGE_VALIDATED
    if val_dir.exists():
        for tier_name in sorted(val_dir.iterdir()):
            if tier_name.is_dir():
                n = len(list(tier_name.glob("*.json")))
                if n:
                    print(f"    {'Tier ' + tier_name.name:20s}: {n}")

print(f"\nTotal time: {total_time:.1f}s")
print("Done.")
