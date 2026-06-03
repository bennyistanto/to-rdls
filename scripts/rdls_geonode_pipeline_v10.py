r"""
rdls_geonode_pipeline_v10.py
============================

RDLS v1.0 GeoNode pipeline. Reuses ONLY the existing 01_raw crawled payloads
(no re-download — pacificdata is Cloudflare-blocked from this machine).
Everything from normalize onward is rebuilt fresh under the v1.0 subtree so
the data lineage is clean and self-contained.

Output structure per portal:
    output/geonode/{portal}/
      01_raw/                    REUSED  - existing crawled GeoNode payloads
      v1.0/
        02_records/              normalised common dict (+ _geonode_* fields)
        03_classified/           passes classifier
        not_rdls/                rejects (with reason)
        04_translated/           v1.0 base record from translate_geonode_v10
        05_extracted/            HEVL extraction blobs
        06_integrated/           HEVL blocks merged into base record
        07_validated/            3-layer audit, tiered into high/medium/low/invalid
        reports/                 per-portal validation_summary.csv

Validation tiers (v1.0):
    high     -> 0 errors, 0 warnings
    medium   -> 0 errors, 1-3 warnings
    low      -> 0 errors, 4+ warnings
    invalid  -> >=1 error

Each stage is resumable. Delete the v1.0/{stage} dir to force re-run.

Usage:
    set PYTHONPATH=C:\Users\benny\OneDrive\Documents\Github\to-rdls
    C:/Users/benny/miniforge3/envs/to-rdls/python.exe scripts/rdls_geonode_pipeline_v10.py
"""

from __future__ import annotations

import csv
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional

# --- Paths / sys.path ---
PROJECT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_DIR))

CONFIGS_DIR = PROJECT_DIR / "configs"
OUTPUT_DIR = PROJECT_DIR / "output" / "geonode"
SCHEMA_PATH = PROJECT_DIR.parent / "rdl-standard" / "schema" / "rdls_schema.json"
CODELISTS_DIR = PROJECT_DIR.parent / "rdl-standard" / "schema" / "codelists"

STAGE_RAW = "01_raw"   # reused
V10 = "v1.0"
STAGE_RECORDS = f"{V10}/02_records"
STAGE_CLASSIFIED = f"{V10}/03_classified"
STAGE_NOT_RDLS = f"{V10}/not_rdls"
STAGE_TRANSLATED = f"{V10}/04_translated"
STAGE_EXTRACTED = f"{V10}/05_extracted"
STAGE_INTEGRATED = f"{V10}/06_integrated"
STAGE_VALIDATED = f"{V10}/07_validated"
STAGE_REPORTS = f"{V10}/reports"

# --- Imports ---
from src.utils import load_json, load_yaml, write_json
from src.sources.geonode import (
    GeoNodeConfig,
    normalize_geonode_record,
    extract_geonode_fields,
)
from src.classify import classify_dataset
from src.spatial import load_spatial_config, build_iso3_table_from_naming
from src.naming import load_naming_config

from src.translate_geonode_v10 import build_base_record_geonode_v10
from src.extract import integrate_hevl_v10
from src.extract_hazard import HazardExtractor, HazardExtraction
from src.extract_exposure import ExposureExtractor, ExposureExtraction
from src.extract_vulnloss import (
    VulnerabilityExtractor, LossExtractor,
    VulnerabilityExtraction, LossExtraction,
)
from src.audit import (
    CodelistRegistry, ValidationResult,
    validate_layer1_schema, validate_layer2_codelists, validate_layer3_semantic,
)


def portal_dir(portal_name: str, stage: str) -> Path:
    d = OUTPUT_DIR / portal_name / stage
    d.mkdir(parents=True, exist_ok=True)
    return d


def quiet_validate(data: dict, schema: dict, registry: CodelistRegistry) -> ValidationResult:
    """Run all three audit layers without the standalone print-per-record noise."""
    result = ValidationResult()
    validate_layer1_schema(data, schema, result)
    validate_layer2_codelists(data, registry, result)
    validate_layer3_semantic(data, registry, result)
    return result


# ---------------------------------------------------------------------------
# HEVL adapter: dataclass extractions -> v1.0 build_*_block parameter dicts
# ---------------------------------------------------------------------------

# Reconciliation: v1.0 enforces strict (hazard_type, process) pairing. The
# legacy signal dictionary sometimes mis-pairs convective_storm with
# tropical_cyclone (TC inputs from the Pacific portals). Map process →
# canonical type for the cases where the process picks the type unambiguously.
_PROCESS_TO_TYPE = {
    "tropical_cyclone": "strong_wind",
    "extratropical_cyclone": "strong_wind",
    "storm_surge": "coastal_flood",
    "fluvial_flood": "flood",
    "pluvial_flood": "flood",
    "groundwater_flood": "flood",
    "ground_motion": "earthquake",
    "rupture": "earthquake",
    "liquefaction": "earthquake",
}

# Type → valid processes (from v1.0 audit.TYPE_TO_PROCESS). Used to drop a
# bad process and let build_hazard_block fall back to a type-appropriate
# default rather than emit an invalid pair.
_TYPE_PROCESSES = {
    "coastal_flood": {"coastal_flood", "storm_surge"},
    "convective_storm": {"tornado", "lightning", "thunderstorm", "hail"},
    "drought": {"agricultural_drought", "hydrological_drought",
                "meteorological_drought", "socioeconomic_drought"},
    "dust_sand_storm": {"dust_sand_storm"},
    "earthquake": {"rupture", "ground_motion", "liquefaction", "subsidence_uplift"},
    "erosion": {"coastal_erosion", "soil_erosion"},
    "extreme_temperature": {"extreme_cold", "extreme_heat"},
    "flood": {"fluvial_flood", "pluvial_flood", "groundwater_flood",
              "coastal_flood", "glacial_lake_outburst"},
    "landslide": {"snow_avalanche", "landslide_general", "landslide_rockslide",
                  "landslide_mudflow", "landslide_rockfall"},
    "pest_infestation": {"pest"},
    "sea_level_rise": {"sea_level_rise"},
    "strong_wind": {"extratropical_cyclone", "tropical_cyclone", "tornado"},
    "tsunami": {"tsunami"},
    "volcanic": {"ashfall", "volcano_ballistics", "lahar", "lava",
                 "pyroclastic_flow", "volcano_gas_aerosols"},
    "wildfire": {"wildfire", "wildfire_smoke"},
}


def reconcile_hazard(hazard_type: str, process: Optional[str]) -> tuple[str, Optional[str]]:
    """Make (type, process) v1.0-valid.

    1. If process points at exactly one type (e.g. tropical_cyclone -> strong_wind),
       override the type.
    2. Else if process is invalid for the type, drop it (None) so build_hazard_block
       picks a per-type default.
    """
    if process and process in _PROCESS_TO_TYPE:
        return _PROCESS_TO_TYPE[process], process
    if process and hazard_type in _TYPE_PROCESSES:
        if process not in _TYPE_PROCESSES[hazard_type]:
            return hazard_type, None
    return hazard_type, process


def hazard_to_dict(ex: HazardExtraction) -> Optional[Dict[str, Any]]:
    if not ex.has_hazard:
        return None
    raw_type = ex.hazard_types[0].value
    raw_process = ex.process_types[0].value if ex.process_types else None
    fixed_type, fixed_process = reconcile_hazard(raw_type, raw_process)
    return {
        "type": fixed_type,
        "process": fixed_process,
        "analysis_type": ex.analysis_type.value if ex.analysis_type else "probabilistic",
        "imt": ex.intensity_measures[0] if ex.intensity_measures else None,
        "return_periods": ex.return_periods or None,
        "calculation_method": ex.calculation_method,
        "description": ex.description,
    }


# v0.3 → v1.0 quantity_kind translation. The legacy extractors (v0.3-flavoured)
# use "monetary" but v1.0's quantity_kind codelist uses "currency". v1.0 also
# requires an ISO 4217 unit when quantity_kind == "currency".
def _v10_qk(raw_qk: Optional[str], currency_hint: Optional[str] = None) -> tuple[Optional[str], Optional[str]]:
    """Return (quantity_kind, unit) using v1.0 codelist values.

    Maps v0.3 'monetary' -> v1.0 'currency' (+ USD fallback unit). Other
    values pass through; when quantity_kind == 'count', sets unit='count'
    to mirror the convention used in the Mombasa/Accra hand-authored records.
    """
    if not raw_qk:
        return None, None
    qk = raw_qk.strip().lower()
    unit: Optional[str] = None
    if qk == "monetary":
        qk = "currency"
        unit = (currency_hint or "USD").upper().strip()
    elif qk == "currency":
        unit = (currency_hint or "USD").upper().strip()
    elif qk == "count":
        unit = "count"
    return qk, unit


def exposure_to_list(ex: ExposureExtraction) -> Optional[List[Dict[str, Any]]]:
    if not ex.has_exposure:
        return None
    out: List[Dict[str, Any]] = []
    for cat_match in ex.categories:
        cat = cat_match.value
        m_list = ex.metrics.get(cat, [])
        m = m_list[0] if m_list else None
        qk, unit = _v10_qk(
            m.quantity_kind if m else None,
            currency_hint=getattr(ex, "currency", None),
        )
        item: Dict[str, Any] = {
            "category": cat,
            "dimension": m.dimension if m else None,
            "quantity_kind": qk,
            "description": None,
        }
        if unit:
            item["unit"] = unit
        out.append(item)
    return out


def vulnerability_to_dict(
    ex: VulnerabilityExtraction,
    hazard_info: Optional[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not ex.has_vulnerability:
        return None
    f = ex.functions[0] if ex.functions else None
    return {
        "hazard_type": (hazard_info or {}).get("type"),
        "imt": (hazard_info or {}).get("imt"),
        "category": getattr(f, "category", None) if f else None,
    }


def loss_to_list(ex: LossExtraction) -> Optional[List[Dict[str, Any]]]:
    if not ex.has_loss:
        return None
    out: List[Dict[str, Any]] = []
    for le in ex.losses:
        qk, unit = _v10_qk(le.quantity_kind, currency_hint=le.currency)
        entry: Dict[str, Any] = {
            "hazard_type": le.hazard_type,
            "hazard_process": le.hazard_process,
            "asset_category": le.asset_category,
            "impact_metric": le.impact_metric,
            "impact_type": le.impact_type,
            "imt": None,
        }
        if qk:
            entry["quantity_kind"] = qk
        if unit:
            entry["unit"] = unit
        out.append(entry)
    return out


# ---------------------------------------------------------------------------
# Tier from ValidationResult
# ---------------------------------------------------------------------------

def tier_of(result: ValidationResult) -> str:
    if result.errors:
        return "invalid"
    w = len(result.warnings)
    if w == 0:
        return "high"
    if w <= 3:
        return "medium"
    return "low"


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def main() -> int:
    print("=" * 60)
    print("  STEP 0: LOAD CONFIGURATION")
    print("=" * 60)

    geonode_config = GeoNodeConfig.from_yaml(str(CONFIGS_DIR / "sources" / "geonode.yaml"))
    classification_config = load_yaml(str(CONFIGS_DIR / "classification.yaml"))
    signal_config = load_yaml(str(CONFIGS_DIR / "signal_dictionary.yaml"))
    defaults_config = load_yaml(str(CONFIGS_DIR / "rdls_defaults.yaml"))
    spatial_config = load_spatial_config(str(CONFIGS_DIR / "spatial.yaml"))
    naming_config = load_naming_config(str(CONFIGS_DIR / "naming.yaml"))
    spatial_config["iso3_table"] = build_iso3_table_from_naming(naming_config)

    if not SCHEMA_PATH.exists():
        print(f"ERROR: v1.0 schema not found at {SCHEMA_PATH}", file=sys.stderr)
        return 1
    schema = load_json(str(SCHEMA_PATH))
    registry = CodelistRegistry(CODELISTS_DIR)

    # Work from existing 01_raw — no crawl. Any portal with 01_raw/*.json
    # gets processed end-to-end into the v1.0 subtree.
    portals_to_run = []
    for p in geonode_config.portals:
        raw_dir = OUTPUT_DIR / p.name / STAGE_RAW
        if raw_dir.exists() and any(raw_dir.glob("*.json")):
            portals_to_run.append(p)
    print(f"Portals with existing 01_raw: {len(portals_to_run)}")
    for p in portals_to_run:
        n = len(list((OUTPUT_DIR / p.name / STAGE_RAW).glob("*.json")))
        print(f"  - {p.name}: {n} raw datasets (reusing)")

    hazard_x = HazardExtractor(signal_config, defaults_config)
    exposure_x = ExposureExtractor(signal_config, defaults_config)
    vulnerability_x = VulnerabilityExtractor(signal_config, defaults_config)
    loss_x = LossExtractor(signal_config, defaults_config)

    t0 = time.time()
    total_counters: Dict[str, Counter] = {
        "records": Counter(),
        "candidates": Counter(),
        "not_rdls": Counter(),
        "translated": Counter(),
        "translate_skip": Counter(),
        "extracted": Counter(),
        "integrated": Counter(),
        "tier": Counter(),
    }

    for portal in portals_to_run:
        print("\n" + "=" * 60)
        print(f"  PORTAL: {portal.name}")
        print("=" * 60)

        raw_dir = OUTPUT_DIR / portal.name / STAGE_RAW
        records_dir = portal_dir(portal.name, STAGE_RECORDS)
        classified_dir = portal_dir(portal.name, STAGE_CLASSIFIED)
        not_rdls_dir = portal_dir(portal.name, STAGE_NOT_RDLS)
        translated_dir = portal_dir(portal.name, STAGE_TRANSLATED)
        extracted_dir = portal_dir(portal.name, STAGE_EXTRACTED)
        integrated_dir = portal_dir(portal.name, STAGE_INTEGRATED)
        validated_dir = portal_dir(portal.name, STAGE_VALIDATED)
        reports_dir = portal_dir(portal.name, STAGE_REPORTS)

        # ---- Stage 2: normalize / extract common dict from 01_raw ----
        print("\n[2] normalize raw -> common-dict records")
        existing = {p.stem for p in records_dir.glob("*.json")}
        raw_files = sorted(raw_dir.glob("*.json"))
        for raw_path in raw_files:
            ds_id = raw_path.stem
            if ds_id in existing:
                continue
            try:
                raw = load_json(str(raw_path))
            except Exception as e:
                print(f"    SKIP {ds_id}: cannot parse raw json: {e}")
                continue
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
            total_counters["records"][portal.name] += 1
        print(f"    records: {total_counters['records'][portal.name]} new "
              f"(total {len(list(records_dir.glob('*.json')))})")

        # ---- Stage 3: classify ----
        print("\n[3] classify -> RDLS candidates vs not_rdls")
        existing_cls = {p.stem for p in classified_dir.glob("*.json")}
        existing_nr = {p.stem for p in not_rdls_dir.glob("*.json")}
        for rec_path in sorted(records_dir.glob("*.json")):
            ds_id = rec_path.stem
            if ds_id in existing_cls or ds_id in existing_nr:
                continue
            fields = load_json(str(rec_path))
            result = classify_dataset(fields, classification_config)

            if not result.rdls_candidate:
                write_json(str(not_rdls_dir / f"{ds_id}.json"), {
                    "fields": fields,
                    "classification": {
                        "scores": result.scores,
                        "components": result.components,
                        "confidence": result.confidence,
                        "rdls_candidate": False,
                    },
                })
                total_counters["not_rdls"][portal.name] += 1
                continue
            write_json(str(classified_dir / f"{ds_id}.json"), {
                "fields": fields,
                "classification": {
                    "scores": result.scores,
                    "components": result.components,
                    "confidence": result.confidence,
                    "top_signals": result.top_signals,
                },
            })
            total_counters["candidates"][portal.name] += 1
        print(f"    candidates: {total_counters['candidates'][portal.name]}  "
              f"not_rdls: {total_counters['not_rdls'][portal.name]}")

        # ---- Stage 4: translate base record ----
        print("\n[4] translate -> v1.0 base records")
        existing_tr = {p.stem for p in translated_dir.glob("*.json")}
        for cls_path in sorted(classified_dir.glob("*.json")):
            data = load_json(str(cls_path))
            fields = data.get("fields") or {}
            components = data.get("classification", {}).get("components") or []
            record = build_base_record_geonode_v10(
                fields,
                components,
                portal_name=portal.name,
                portal_base_url=portal.base_url,
                spatial_config=spatial_config,
                naming_config=naming_config,
            )
            if record is None:
                total_counters["translate_skip"][portal.name] += 1
                continue
            out_id = record["id"]
            if out_id in existing_tr:
                continue
            write_json(str(translated_dir / f"{out_id}.json"),
                       {"record": record, "fields": fields})
            total_counters["translated"][portal.name] += 1
        print(f"    translated: {total_counters['translated'][portal.name]}  "
              f"skipped: {total_counters['translate_skip'][portal.name]}")

        # ---- Stage 5: HEVL extract ----
        print("\n[5] HEVL extract (dataclasses)")
        for tr_path in sorted(translated_dir.glob("*.json")):
            out_path = extracted_dir / tr_path.name
            if out_path.exists():
                continue
            data = load_json(str(tr_path))
            record = data["record"]
            fields = data["fields"]
            rdt = record.get("risk_data_type", [])

            haz = exp = vul = lss = None
            if "hazard" in rdt:
                haz = hazard_to_dict(hazard_x.extract(fields))
            if "exposure" in rdt:
                exp = exposure_to_list(exposure_x.extract(fields))
            if "vulnerability" in rdt:
                vul = vulnerability_to_dict(vulnerability_x.extract(fields), haz)
            if "loss" in rdt:
                lss = loss_to_list(loss_x.extract(fields))

            write_json(str(out_path), {
                "record": record,
                "components": rdt,
                "hazard_info": haz,
                "exposure_info": exp,
                "vulnerability_info": vul,
                "loss_info": lss,
            })
            total_counters["extracted"][portal.name] += 1
        print(f"    extracted: {total_counters['extracted'][portal.name]}")

        # ---- Stage 6: integrate ----
        print("\n[6] integrate -> final v1.0 record")
        for ex_path in sorted(extracted_dir.glob("*.json")):
            out_path = integrated_dir / ex_path.name
            if out_path.exists():
                continue
            data = load_json(str(ex_path))
            record = integrate_hevl_v10(
                base_record=data["record"],
                components=data["components"],
                hazard_info=data.get("hazard_info"),
                exposure_info=data.get("exposure_info"),
                vulnerability_info=data.get("vulnerability_info"),
                loss_info=data.get("loss_info"),
            )
            write_json(str(out_path), record)
            total_counters["integrated"][portal.name] += 1
        print(f"    integrated: {total_counters['integrated'][portal.name]}")

        # ---- Stage 7: validate + tier ----
        print("\n[7] validate (3-layer audit) + tier")
        for sub in ("high", "medium", "low", "invalid"):
            (validated_dir / sub).mkdir(parents=True, exist_ok=True)

        portal_tier: Counter = Counter()
        rows: list[list[Any]] = []
        for int_path in sorted(integrated_dir.glob("*.json")):
            record = load_json(str(int_path))
            # audit.validate_layer*() expect the unwrapped record (datasets[0]),
            # NOT the envelope. The envelope is added when writing the final file.
            result = quiet_validate(record, schema, registry)
            tier = tier_of(result)
            portal_tier[tier] += 1
            total_counters["tier"][tier] += 1

            wrapped = {"datasets": [record]}
            out_path = validated_dir / tier / int_path.name
            write_json(str(out_path), wrapped)
            rows.append([
                record.get("id", ""),
                tier,
                "true" if result.is_valid else "false",
                len(result.errors),
                len(result.warnings),
            ])

        csv_path = reports_dir / "validation_summary.csv"
        with csv_path.open("w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["rdls_id", "tier", "is_valid", "error_count", "warning_count"])
            for row in rows:
                w.writerow(row)
        print(f"    tiers: {dict(portal_tier)}")
        print(f"    report: {csv_path}")

    print("\n" + "=" * 60)
    print("  SUMMARY")
    print("=" * 60)
    elapsed = time.time() - t0
    for portal in portals_to_run:
        print(f"\n  {portal.name}:")
        for k in ("records", "candidates", "not_rdls", "translated",
                  "translate_skip", "extracted", "integrated"):
            print(f"    {k:14s} = {total_counters[k][portal.name]}")
    print(f"\n  Tier totals (combined): {dict(total_counters['tier'])}")
    print(f"\nTotal time: {elapsed:.1f}s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
