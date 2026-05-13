#!/usr/bin/env python3
"""
rdls_desinventar_01_generate_records.py

Generate RDLS v0.3 loss records from DesInventar (UNDRR) disaster loss databases.
Each DesInventar dataset is a national multi-hazard disaster loss inventory with
event-level records (deaths, injuries, houses destroyed, economic losses, etc.).

This script:
  1. Iterates over all DesInventar datasets registered in the YAML config
  2. Downloads XLS data from HDX (with local caching) for each dataset
  3. Reads HDX metadata JSON for dataset-level fields (title, license, resources)
  4. Maps DesInventar event types → RDLS hazard_type/process_type
  5. Generates one RDLS JSON per dataset with loss entries per hazard × loss column
  6. Validates output against RDLS v0.3 schema

Usage:
    python rdls_desinventar_01_generate_records.py

Inputs:
    - to-rdls/configs/desinventar_mapping.yaml   (event & loss column mapping)
    - to-rdls/configs/country_bbox.yaml           (country bounding boxes)
    - to-rdls/configs/geonames_country_ids.yaml   (GeoNames lookups)
    - XLS data file (local path)
    - HDX metadata JSON (from dataset_metadata/)

Output:
    - to-rdls/output/desinventar/metadata/rdls_lss-{iso3}_undrr_desinventar.json
"""

import json
import os
import sys
import re
import time
import urllib.request
from pathlib import Path
from collections import defaultdict

import yaml
import pandas as pd

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent          # to-rdls/
REPO_ROOT = PROJECT_ROOT.parent           # hdx-metadata-crawler/

CONFIG_DIR = PROJECT_ROOT / "configs"
SCHEMA_PATH = PROJECT_ROOT / "schema" / "rdls_schema_v0.3.json"
OUTPUT_DIR = PROJECT_ROOT / "output" / "desinventar" / "metadata"
HDX_METADATA_DIR = REPO_ROOT / "hdx_dataset_metadata_dump" / "dataset_metadata"
XLS_CACHE_DIR = REPO_ROOT / "data" / "desinventar"

# ---------------------------------------------------------------------------
# Load configs
# ---------------------------------------------------------------------------
def load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_configs():
    mapping = load_yaml(CONFIG_DIR / "desinventar_mapping.yaml")
    bbox_data = load_yaml(CONFIG_DIR / "country_bbox.yaml")
    geonames_data = load_yaml(CONFIG_DIR / "geonames_country_ids.yaml")
    return mapping, bbox_data, geonames_data


# ---------------------------------------------------------------------------
# Read DesInventar XLS data
# ---------------------------------------------------------------------------
def read_desinventar_xls(xls_path: str) -> pd.DataFrame:
    """Read DesInventar XLS/XLSX file. Returns DataFrame with all event records.

    Some DesInventar files on HDX are actually tab-separated text files
    with .xls extension, so we fall back to TSV parsing if Excel fails.
    """
    try:
        df = pd.read_excel(xls_path)
    except Exception:
        # Fallback: try reading as tab-separated text
        print(f"  Note: {Path(xls_path).name} is not a real Excel file, reading as TSV")
        df = pd.read_csv(xls_path, sep="\t", encoding="utf-8", low_memory=False)

    print(f"  Read {len(df)} event records from {Path(xls_path).name}")
    print(f"  Columns: {list(df.columns)}")
    return df


def analyse_events(df: pd.DataFrame, event_type_mapping: dict) -> dict:
    """
    Analyse DesInventar events and aggregate loss data by hazard type.

    Returns dict:
        {
            rdls_hazard_key: {
                "hazard_type": str,
                "process_type": str,
                "event_count": int,
                "date_range": (earliest, latest),
                "di_event_types": [str, ...],    # original DI event names
                "loss_totals": { col_name: total_sum, ... }
            }
        }
    """
    # Normalize event column name
    event_col = None
    for candidate in ["Event", "EVENT", "event", "Event type", "event_type"]:
        if candidate in df.columns:
            event_col = candidate
            break
    if event_col is None:
        print("  WARNING: No 'Event' column found. Available:", list(df.columns))
        return {}

    # Date column
    date_col = None
    for candidate in ["Date (YMD)", "Date", "DATE", "date"]:
        if candidate in df.columns:
            date_col = candidate
            break

    # Loss columns present in this dataset
    all_loss_cols = [
        "Deaths", "Injured", "Missing", "Affected", "Victims",
        "Evacuated", "Relocated", "Houses Destroyed", "Houses Damaged",
        "Losses $USD", "Losses $Local", "Damages in crops Ha.",
        "Lost Cattle", "Education centers", "Hospitals", "Damages in roads Mts"
    ]
    present_loss_cols = [c for c in all_loss_cols if c in df.columns]

    # Group events by RDLS hazard type
    results = {}
    skipped_events = defaultdict(int)

    for event_name, group in df.groupby(event_col):
        event_upper = str(event_name).strip().upper()
        mapping = event_type_mapping.get(event_upper)

        if mapping is None:
            skipped_events[event_upper] = len(group)
            continue

        hazard_type = mapping["hazard_type"]
        process_type = mapping["process_type"]
        key = f"{hazard_type}__{process_type}"

        if key not in results:
            results[key] = {
                "hazard_type": hazard_type,
                "process_type": process_type,
                "event_count": 0,
                "date_min": None,
                "date_max": None,
                "di_event_types": [],
                "loss_totals": defaultdict(float),
                "loss_event_counts": defaultdict(int),  # events with non-zero values
            }

        entry = results[key]
        entry["event_count"] += len(group)
        entry["di_event_types"].append(str(event_name).strip())

        # Date range
        if date_col:
            dates = group[date_col].dropna().astype(str)
            if len(dates) > 0:
                # Parse DI dates (YYYY/M/D or YYYY-MM-DD)
                for d in [dates.iloc[0], dates.iloc[-1]]:
                    year_match = re.match(r"(\d{4})", str(d))
                    if year_match:
                        year = int(year_match.group(1))
                        if entry["date_min"] is None or year < entry["date_min"]:
                            entry["date_min"] = year
                        if entry["date_max"] is None or year > entry["date_max"]:
                            entry["date_max"] = year

        # Sum loss columns
        for col in present_loss_cols:
            col_data = pd.to_numeric(group[col], errors="coerce")
            non_zero = col_data.dropna()
            non_zero = non_zero[non_zero > 0]
            if len(non_zero) > 0:
                entry["loss_totals"][col] += non_zero.sum()
                entry["loss_event_counts"][col] += len(non_zero)

    # Report
    print(f"\n  Mapped to {len(results)} RDLS hazard types:")
    for key, data in sorted(results.items(), key=lambda x: -x[1]["event_count"]):
        print(f"    {data['hazard_type']:25s} ({data['process_type']:25s}): "
              f"{data['event_count']:5d} events  "
              f"[{', '.join(data['di_event_types'])}]")

    if skipped_events:
        total_skipped = sum(skipped_events.values())
        print(f"\n  Skipped {total_skipped} events (non-natural hazard):")
        for evt, count in sorted(skipped_events.items(), key=lambda x: -x[1]):
            print(f"    {evt}: {count}")

    return results


# ---------------------------------------------------------------------------
# Read HDX metadata JSON
# ---------------------------------------------------------------------------
def find_hdx_metadata(dataset_id: str) -> dict | None:
    """Find and read HDX metadata JSON for a dataset ID."""
    import glob
    pattern = str(HDX_METADATA_DIR / f"{dataset_id}__*.json")
    matches = glob.glob(pattern)
    if not matches:
        # Try direct file
        direct = HDX_METADATA_DIR / f"{dataset_id}.json"
        if direct.exists():
            matches = [str(direct)]
    if not matches:
        print(f"  WARNING: No HDX metadata found for {dataset_id}")
        return None

    with open(matches[0], "r", encoding="utf-8") as f:
        return json.load(f)


def find_xls_download_url(dataset_id: str) -> tuple | None:
    """Find XLS/XLSX download URL from HDX metadata JSON resources.

    Returns (download_url, filename) or None if not found.
    """
    hdx_meta = find_hdx_metadata(dataset_id)
    if hdx_meta is None:
        return None

    for res in hdx_meta.get("resources", []):
        fmt = res.get("format", "").upper()
        if fmt in ("XLS", "XLSX"):
            url = res.get("download_url", "")
            if url:
                filename = url.rsplit("/", 1)[-1]
                return (url, filename)
    return None


def download_xls(url: str, filename: str, cache_dir: Path) -> Path:
    """Download XLS file from HDX with local caching.

    Returns path to cached file. Skips download if file already exists.
    """
    cache_dir.mkdir(parents=True, exist_ok=True)
    local_path = cache_dir / filename

    if local_path.exists():
        print(f"  Using cached: {local_path.name}")
        return local_path

    print(f"  Downloading: {filename}")
    urllib.request.urlretrieve(url, str(local_path))
    size_kb = local_path.stat().st_size / 1024
    print(f"  Downloaded: {size_kb:.1f} KB -> {local_path.name}")
    return local_path


def resolve_xls_path(dataset_id: str, cache_dir: Path) -> Path | None:
    """Resolve XLS path: check cache first, then download from HDX.

    Returns Path to the local XLS/XLSX file, or None if unavailable.
    """
    # Find download URL from HDX metadata
    result = find_xls_download_url(dataset_id)
    if result is None:
        print(f"  WARNING: No XLS/XLSX resource found in HDX metadata")
        return None

    url, filename = result

    # Check cache
    local_path = cache_dir / filename
    if local_path.exists():
        print(f"  Using cached: {local_path.name}")
        return local_path

    # Download
    try:
        return download_xls(url, filename, cache_dir)
    except Exception as e:
        print(f"  ERROR downloading {filename}: {e}")
        return None


# ---------------------------------------------------------------------------
# Hazard type display names (RDLS enum → human-readable)
# ---------------------------------------------------------------------------
HAZARD_DISPLAY_NAMES = {
    "coastal_flood": "Coastal Flood",
    "convective_storm": "Convective Storm",
    "drought": "Drought",
    "earthquake": "Earthquake",
    "extreme_temperature": "Extreme Temperature",
    "flood": "Flood",
    "landslide": "Landslide",
    "strong_wind": "Strong Wind",
    "tsunami": "Tsunami",
    "volcanic": "Volcanic",
    "wildfire": "Wildfire",
}


# ---------------------------------------------------------------------------
# Build RDLS loss record
# ---------------------------------------------------------------------------
def build_rdls_record(
    dataset_id: str,
    dataset_info: dict,
    hdx_meta: dict,
    hazard_data: dict,
    loss_col_mapping: dict,
    bbox_data: dict,
    geonames_data: dict,
    country_currencies: dict,
) -> dict:
    """Build a complete RDLS v0.3 JSON record for one DesInventar dataset."""

    iso3 = dataset_info["iso3"]
    country_name = dataset_info["country_name"]
    contact_point_name = dataset_info["contact_point"]

    # Spatial
    bbox = bbox_data.get("country_bbox", {}).get(iso3, [0, 0, 0, 0])
    geonames = geonames_data.get("geonames_country_ids", {}).get(iso3, {})
    geoname_id = geonames.get("geoname_id", "")
    geoname_name = geonames.get("name", country_name)

    # License from HDX
    license_title = hdx_meta.get("license_title", "")
    if "CC BY" in license_title.upper():
        rdls_license = "CC-BY-4.0"
    elif "CC0" in license_title.upper():
        rdls_license = "CC0-1.0"
    elif "ODC" in license_title.upper() or "PDDL" in license_title.upper():
        rdls_license = "ODC-PDDL-1.0"
    else:
        rdls_license = "CC-BY-4.0"  # Default for UNDRR data

    # Date range across all hazards
    all_min = min((d["date_min"] for d in hazard_data.values() if d["date_min"]), default=None)
    all_max = max((d["date_max"] for d in hazard_data.values() if d["date_max"]), default=None)
    total_events = sum(d["event_count"] for d in hazard_data.values())

    # Hazard type summary for description (human-readable names)
    hazard_summary = ", ".join(sorted(set(
        HAZARD_DISPLAY_NAMES.get(d["hazard_type"], d["hazard_type"].replace("_", " ").title())
        for d in hazard_data.values()
    )))
    date_range_str = f"{all_min}-{all_max}" if all_min and all_max else "historical"

    # Description
    description = (
        f"National disaster loss inventory for {country_name} from the DesInventar "
        f"Sendai Framework Monitor database, compiled by {contact_point_name} and "
        f"published by the United Nations Office for Disaster Risk Reduction (UNDRR). "
        f"Contains {total_events:,} event-level loss records covering {date_range_str}, "
        f"with observed impacts from {hazard_summary} events. "
        f"Loss data includes human casualties, displacement, building damage, "
        f"economic losses, agricultural damage, and infrastructure impacts. "
        f"[Source: This metadata record was automatically extracted from the "
        f"Humanitarian Data Exchange (HDX) at https://data.humdata.org] "
        f"[Original dataset: https://data.humdata.org/dataset/{dataset_id}]"
    )

    # Resources from HDX
    resources = []
    for i, res in enumerate(hdx_meta.get("resources", []), start=1):
        fmt = res.get("format", "").upper()
        if fmt == "SHP":
            data_format = "Shapefile (shp)"
        elif fmt in ("XLS", "XLSX"):
            data_format = "Excel (xlsx)"
        elif fmt == "CSV":
            data_format = "CSV (csv)"
        elif fmt in ("GEOJSON",):
            data_format = "GeoJSON (geojson)"
        elif fmt == "JSON":
            data_format = "JSON (json)"
        elif fmt == "XML":
            data_format = "XML (xml)"
        elif fmt == "PDF":
            data_format = "PDF (pdf)"
        elif fmt in ("GEOTIFF", "TIF", "TIFF"):
            data_format = "GeoTIFF (tif)"
        elif fmt == "DOCX":
            continue  # Skip data dictionary resource
        else:
            data_format = fmt

        res_title = (res.get("description") or res.get("name") or "").strip()
        if not res_title:
            res_title = f"DesInventar {data_format.split(' ')[0]} data for {country_name}"
        res_desc = (res.get("description") or "").strip()
        if not res_desc:
            res_desc = f"DesInventar disaster loss data for {country_name}"

        resource_entry = {
            "id": f"resource_{i:03d}",
            "title": res_title,
            "description": res_desc,
            "data_format": data_format,
            "access_modality": "file_download",
            "download_url": res.get("download_url", ""),
            "access_url": f"https://data.humdata.org/dataset/{dataset_id}",
        }
        resources.append(resource_entry)

    # Ensure at least one resource
    if not resources:
        resources.append({
            "id": "resource_001",
            "title": f"DesInventar disaster data for {country_name}",
            "description": f"Tabular disaster loss data from DesInventar for {country_name}",
            "data_format": "Excel (xlsx)",
            "access_modality": "download_page",
            "access_url": f"https://data.humdata.org/dataset/{dataset_id}",
        })

    # Build loss entries
    losses = []
    loss_idx = 0

    for hazard_key, hdata in sorted(hazard_data.items()):
        hazard_type = hdata["hazard_type"]
        process_type = hdata["process_type"]
        event_count = hdata["event_count"]
        di_types = ", ".join(sorted(set(hdata["di_event_types"])))

        for col_name, total_value in sorted(hdata["loss_totals"].items()):
            if total_value <= 0:
                continue

            col_cfg = loss_col_mapping.get(col_name)
            if col_cfg is None:
                continue

            loss_idx += 1
            col_events = hdata["loss_event_counts"].get(col_name, 0)

            # Build ID slug
            hazard_slug = hazard_type.replace("_", "")
            metric_slug = col_name.lower().replace(" ", "_").replace("$", "").replace(".", "")
            loss_id = f"loss_{loss_idx:03d}_{hazard_slug}_{metric_slug}"

            # Description (concise)
            suffix = col_cfg.get("description_suffix", col_name)
            hazard_display = HAZARD_DISPLAY_NAMES.get(hazard_type, hazard_type.replace("_", " ").title())
            date_suffix = f" ({hdata['date_min']}-{hdata['date_max']})" if hdata["date_min"] and hdata["date_max"] else ""
            desc = f"Observed {suffix} from {hazard_display} events in {country_name}{date_suffix}."

            # Impact and losses
            impact_and_losses = {
                "impact_type": "direct",
                "impact_modelling": "observed",
                "impact_metric": col_cfg["impact_metric"],
                "quantity_kind": col_cfg["quantity_kind"],
                "loss_type": col_cfg.get("loss_type", "ground_up"),
                "loss_approach": "empirical",
                "loss_frequency_type": "empirical",
            }

            # Add currency if applicable
            if col_cfg.get("quantity_kind") == "currency":
                if col_name == "Losses $USD":
                    impact_and_losses["currency"] = "USD"
                elif col_name == "Losses $Local":
                    local_currency = country_currencies.get(iso3)
                    if local_currency:
                        impact_and_losses["currency"] = local_currency

            loss_entry = {
                "id": loss_id,
                "hazard_type": hazard_type,
                "hazard_process": process_type,
                "asset_category": col_cfg["asset_category"],
                "asset_dimension": col_cfg["asset_dimension"],
                "impact_and_losses": impact_and_losses,
                "description": desc,
            }
            losses.append(loss_entry)

    # Assemble full record
    record = {
        "datasets": [
            {
                "id": f"rdls_lss-{iso3.lower()}_undrr_desinventar",
                "title": f"DesInventar Disaster Loss and Damage Dataset for {country_name}",
                "description": description,
                "risk_data_type": ["loss"],
                "version": "1",
                "purpose": (
                    f"To document historical disaster losses in {country_name} "
                    f"from the DesInventar national disaster loss inventory, "
                    f"supporting disaster risk reduction monitoring under the "
                    f"Sendai Framework."
                ),
                "project": {
                    "name": "DesInventar Sendai - Disaster Information Management System",
                    "url": "https://www.desinventar.net/"
                },
                "details": (
                    f"Event-level disaster loss records from the DesInventar database "
                    f"for {country_name}, maintained by {contact_point_name}. "
                    f"Covers {total_events:,} observed disaster events from "
                    f"{date_range_str}. Data collected through direct observational "
                    f"reporting and includes human impacts (deaths, injuries, missing, "
                    f"affected, evacuated, relocated), physical impacts (houses "
                    f"destroyed/damaged, education centres, hospitals, roads), "
                    f"economic losses (local currency and USD), and agricultural "
                    f"impacts (crop damage in hectares, livestock losses). "
                    f"Methodology: Direct Observational Data / Anecdotal Data."
                ),
                "spatial": {
                    "scale": "national",
                    "countries": [iso3],
                    "bbox": bbox,
                    "gazetteer_entries": [
                        {
                            "scheme": "GEONAMES",
                            "description": geoname_name,
                            "uri": f"https://www.geonames.org/{geoname_id}/{geoname_name.lower().replace(' ', '-')}.html",
                            "id": "gazetteer_1"
                        }
                    ] if geoname_id else []
                },
                "license": rdls_license,
                "attributions": [
                    {
                        "id": "attribution_publisher",
                        "role": "publisher",
                        "entity": {
                            "name": "United Nations Office for Disaster Risk Reduction (UNDRR)",
                            "email": "isdr@un.org",
                            "url": "https://www.undrr.org/"
                        }
                    },
                    {
                        "id": "attribution_creator",
                        "role": "creator",
                        "entity": {
                            "name": "United Nations Office for Disaster Risk Reduction (UNDRR)",
                            "email": "isdr@un.org",
                            "url": "https://www.desinventar.net/"
                        }
                    },
                    {
                        "id": "attribution_contact",
                        "role": "contact_point",
                        "entity": {
                            "name": contact_point_name,
                            "url": ((hdx_meta.get("caveats") or "").strip()
                                    or f"https://data.humdata.org/dataset/{dataset_id}")
                        }
                    }
                ],
                "sources": [
                    {
                        "id": "source_desinventar",
                        "name": "DesInventar Sendai",
                        "description": (
                            f"National disaster loss database for {country_name} "
                            f"maintained using the DesInventar methodology."
                        ),
                        "url": "https://www.desinventar.net/",
                        "type": "dataset",
                        "component": "loss"
                    },
                    {
                        "id": "source_hdx",
                        "name": "Humanitarian Data Exchange (HDX)",
                        "description": (
                            f"Dataset published on HDX: {hdx_meta.get('title', '')}"
                        ),
                        "url": f"https://data.humdata.org/dataset/{dataset_id}",
                        "type": "dataset",
                        "component": "loss"
                    }
                ],
                "resources": resources,
                "loss": {
                    "losses": losses
                },
                "links": [
                    {
                        "href": "https://docs.riskdatalibrary.org/en/0__3__0/rdls_schema.json",
                        "rel": "describedby"
                    },
                    {
                        "href": f"https://data.humdata.org/dataset/{dataset_id}",
                        "rel": "source"
                    }
                ]
            }
        ]
    }

    return record


# ---------------------------------------------------------------------------
# Validate against RDLS v0.3 schema
# ---------------------------------------------------------------------------
def validate_record(record: dict, schema_path: Path) -> list:
    """Validate RDLS record against schema. Returns list of error strings.

    The record uses the {"datasets": [...]} wrapper convention.
    The schema validates the inner dataset object (root level = one dataset).
    """
    try:
        from jsonschema import Draft202012Validator
    except ImportError:
        try:
            from jsonschema import Draft7Validator as Draft202012Validator
        except ImportError:
            print("  WARNING: jsonschema not installed, skipping validation")
            return []

    with open(schema_path, "r", encoding="utf-8") as f:
        schema = json.load(f)

    # Validate the inner dataset object, not the wrapper
    dataset_obj = record.get("datasets", [record])[0]

    validator = Draft202012Validator(schema)
    errors = []
    for error in sorted(validator.iter_errors(dataset_obj), key=lambda e: list(e.path)):
        path = ".".join(str(p) for p in error.absolute_path)
        errors.append(f"  [{path}] {error.message}")
    return errors


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def generate_record(
    dataset_id: str,
    xls_path: str,
    mapping: dict,
    bbox_data: dict,
    geonames_data: dict,
) -> dict | None:
    """Generate one RDLS record for a DesInventar dataset."""

    dataset_info = mapping["datasets"].get(dataset_id)
    if dataset_info is None:
        print(f"  ERROR: Dataset {dataset_id} not found in config")
        return None

    iso3 = dataset_info["iso3"]
    print(f"\n{'='*70}")
    print(f"Processing: {dataset_info['country_name']} ({iso3})")
    print(f"  Dataset ID: {dataset_id}")
    print(f"  XLS path: {xls_path}")
    print(f"{'='*70}")

    # Read HDX metadata
    hdx_meta = find_hdx_metadata(dataset_id)
    if hdx_meta is None:
        hdx_meta = {
            "title": f"Disaster loss and damage dataset for {dataset_info['country_name']}",
            "organization": "United Nations Office for Disaster Risk Reduction (UNDRR)",
            "license_title": "Creative Commons Attribution International (CC BY)",
            "resources": [],
        }

    # Read XLS data
    df = read_desinventar_xls(xls_path)

    # Analyse events
    hazard_data = analyse_events(df, mapping["event_type_mapping"])

    if not hazard_data:
        print("  WARNING: No mappable hazard events found. Skipping.")
        return None

    # Build record
    record = build_rdls_record(
        dataset_id=dataset_id,
        dataset_info=dataset_info,
        hdx_meta=hdx_meta,
        hazard_data=hazard_data,
        loss_col_mapping=mapping["loss_column_mapping"],
        bbox_data=bbox_data,
        geonames_data=geonames_data,
        country_currencies=mapping.get("country_currencies", {}),
    )

    loss_count = len(record["datasets"][0]["loss"]["losses"])
    print(f"\n  Generated {loss_count} loss entries")

    # Validate
    errors = validate_record(record, SCHEMA_PATH)
    if errors:
        print(f"\n  VALIDATION FAILED: {len(errors)} errors")
        for err in errors[:20]:
            print(f"    {err}")
        if len(errors) > 20:
            print(f"    ... and {len(errors) - 20} more")
    else:
        print(f"\n  VALIDATION PASSED (0 errors)")

    return record


def main():
    # Load configs
    mapping, bbox_data, geonames_data = load_configs()

    # Ensure output directories exist
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    XLS_CACHE_DIR.mkdir(parents=True, exist_ok=True)

    datasets = mapping["datasets"]
    total = len(datasets)
    results = {"success": [], "failed": [], "skipped": []}

    print(f"Processing {total} DesInventar datasets...")
    print(f"XLS cache: {XLS_CACHE_DIR}")
    print(f"Output:    {OUTPUT_DIR}")

    for idx, (dataset_id, dataset_info) in enumerate(datasets.items(), start=1):
        iso3 = dataset_info["iso3"]
        country_name = dataset_info["country_name"]
        series = dataset_info.get("series")

        label = f"{country_name} ({iso3})"
        if series:
            label += f" series {series}"

        print(f"\n{'='*70}")
        print(f"[{idx}/{total}] {label}")
        print(f"  Dataset ID: {dataset_id}")
        print(f"{'='*70}")

        try:
            # Step 1: Resolve XLS path (cache or download)
            xls_path = resolve_xls_path(dataset_id, XLS_CACHE_DIR)
            if xls_path is None:
                print(f"  SKIP: No XLS file available for {country_name}")
                results["skipped"].append((dataset_id, label, "no XLS"))
                continue

            # Step 2: Generate RDLS record
            record = generate_record(
                dataset_id=dataset_id,
                xls_path=str(xls_path),
                mapping=mapping,
                bbox_data=bbox_data,
                geonames_data=geonames_data,
            )

            if record is None:
                results["failed"].append((dataset_id, label, "generate_record returned None"))
                continue

            # Step 3: Construct slug (with series suffix for duplicate countries)
            base_slug = f"rdls_lss-{iso3.lower()}_undrr_desinventar"
            slug = f"{base_slug}_{series}" if series else base_slug

            # Patch record ID for series disambiguation
            record["datasets"][0]["id"] = slug

            # Step 4: Write output
            output_path = OUTPUT_DIR / f"{slug}.json"
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(record, f, indent=2, ensure_ascii=False)

            loss_count = len(record["datasets"][0]["loss"]["losses"])
            print(f"\n  Written: {output_path.name}  ({loss_count} loss entries)")
            results["success"].append((dataset_id, label, slug))

            # Brief pause between downloads to be polite to HDX
            time.sleep(0.5)

        except Exception as e:
            print(f"\n  ERROR: {e}")
            results["failed"].append((dataset_id, label, str(e)))
            continue

    # ── Summary ───────────────────────────────────────────────────────
    print(f"\n{'='*70}")
    print(f"SUMMARY: {len(results['success'])} success, "
          f"{len(results['failed'])} failed, "
          f"{len(results['skipped'])} skipped  (of {total} datasets)")
    print(f"{'='*70}")

    if results["success"]:
        print(f"\n  Success ({len(results['success'])}):")
        for _, label, slug in results["success"]:
            print(f"    [OK]   {label:40s} -> {slug}.json")

    if results["failed"]:
        print(f"\n  Failed ({len(results['failed'])}):")
        for _, label, reason in results["failed"]:
            print(f"    [FAIL] {label:40s} -- {reason}")

    if results["skipped"]:
        print(f"\n  Skipped ({len(results['skipped'])}):")
        for _, label, reason in results["skipped"]:
            print(f"    [SKIP] {label:40s} -- {reason}")

    print(f"\nDone.")


if __name__ == "__main__":
    main()
