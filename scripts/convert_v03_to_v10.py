#!/usr/bin/env python3
"""Convert RDLS v0.3 metadata JSON to v1.0 format.

Usage:
    python schema/convert_v03_to_v10.py <input_v03.json> [output_v10.json]

If output is not specified, writes to <input>_v1.0.json.
Handles both wrapped {"datasets": [...]} and unwrapped single-dataset formats.

Conversion reference (from rdls_template_v0.3.json _v03_vs_v10_key_differences):
  - sources[] -> lineage.sources[] (component->used_in, lineage field->removed)
  - data_format + access_modality -> media_type + format
  - temporal.temporal_resolution -> resource.temporal_resolution (moved up)
  - attributions -> extract publisher/creator/contact_point as top-level fields
  - hazard_process -> process (in hazard objects)
  - vulnerability flat fields -> nested hazard_primary object
  - loss flat fields -> nested hazard + measurement objects
  - license_url -> removed
  - links href -> updated to v1.0 URL
"""

import json
import sys
import copy
from pathlib import Path

# ---------------------------------------------------------------------------
# Mapping tables
# ---------------------------------------------------------------------------

# v0.3 data_format (closed enum) -> v1.0 media_type + format
DATA_FORMAT_MAP = {
    "GeoTIFF (tif)":                {"media_type": "image/tiff", "format": "GeoTIFF"},
    "Cloud Optimized GeoTIFF (cog)": {"media_type": "image/tiff", "format": "Cloud Optimized GeoTIFF"},
    "GRID (grd)":                   {"format": "GRID"},
    "NetCDF (nc)":                  {"media_type": "application/x-netcdf", "format": "NetCDF"},
    "GRIB (grib)":                  {"format": "GRIB"},
    "HDF5 (hdf5)":                  {"media_type": "application/x-hdf5", "format": "HDF5"},
    "Zarr (zarr)":                  {"format": "Zarr"},
    "GeoPackage (gpkg)":            {"media_type": "application/geopackage+sqlite3", "format": "GeoPackage"},
    "GeoJSON (geojson)":            {"media_type": "application/geo+json", "format": "GeoJSON"},
    "FlatGeobuf (fgb)":             {"media_type": "application/flatgeobuf", "format": "FlatGeobuf"},
    "Shapefile (shp)":              {"media_type": "application/x-shapefile", "format": "Shapefile"},
    "File Geodatabase (gdb)":       {"format": "File Geodatabase"},
    "KML (kml)":                    {"media_type": "application/vnd.google-earth.kml+xml", "format": "KML"},
    "CSV (csv)":                    {"media_type": "text/csv", "format": "CSV"},
    "Parquet (parquet)":            {"media_type": "application/x-parquet", "format": "Parquet"},
    "Excel (xlsx)":                 {"media_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "format": "Excel"},
    "JSON (json)":                  {"media_type": "application/json", "format": "JSON"},
    "XML (xml)":                    {"media_type": "application/xml", "format": "XML"},
    "PDF (pdf)":                    {"media_type": "application/pdf", "format": "PDF"},
    "LAS (las)":                    {"format": "LAS"},
    "COPC (copc)":                  {"format": "COPC"},
}

V03_LINK = "https://docs.riskdatalibrary.org/en/0__3__0/rdls_schema.json"
V10_LINK = "https://docs.riskdatalibrary.org/en/1__0__0/rdls_schema.json"


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def pop_if(d: dict, key: str, default=None):
    """Pop a key from dict if it exists, return value."""
    return d.pop(key, default)


def is_empty(val) -> bool:
    """Check if a value is empty/blank (empty string, None, empty list/dict)."""
    if val is None:
        return True
    if isinstance(val, str) and val.strip() == "":
        return True
    if isinstance(val, (list, dict)) and len(val) == 0:
        return True
    return False


def clean_empty(d):
    """Recursively remove empty strings, None values, and empty containers."""
    if isinstance(d, dict):
        return {k: clean_empty(v) for k, v in d.items() if not is_empty(v)}
    if isinstance(d, list):
        cleaned = [clean_empty(item) for item in d if not is_empty(item)]
        return cleaned
    return d


# ---------------------------------------------------------------------------
# Conversion functions for each section
# ---------------------------------------------------------------------------

def convert_sources_to_lineage(dataset: dict) -> None:
    """Move dataset.sources[] -> dataset.lineage.sources[] with field renames."""
    sources = pop_if(dataset, "sources")
    if not sources:
        return

    converted = []
    for src in sources:
        new_src = {"id": src.get("id", "")}

        # v0.3 'name' stays 'name' in v1.0
        if "name" in src and not is_empty(src["name"]):
            new_src["name"] = src["name"]

        if "url" in src and not is_empty(src["url"]):
            new_src["url"] = src["url"]

        if "type" in src and not is_empty(src["type"]):
            new_src["type"] = src["type"]

        # v0.3 'component' -> v1.0 'used_in'
        component = src.get("component")
        if component and not is_empty(component):
            new_src["used_in"] = component

        if "license" in src and not is_empty(src["license"]):
            new_src["license"] = src["license"]

        # v0.3 'description' stays
        if "description" in src and not is_empty(src["description"]):
            new_src["description"] = src["description"]

        # v0.3 'lineage' (processing steps string) -> absorbed into description or dropped
        lineage_text = src.get("lineage")
        if lineage_text and not is_empty(lineage_text):
            # Append to description if exists, otherwise store as description
            if "description" in new_src:
                new_src["description"] += f" Lineage: {lineage_text}"
            else:
                new_src["description"] = f"Lineage: {lineage_text}"

        converted.append(new_src)

    dataset["lineage"] = {"sources": converted}


def convert_resources(dataset: dict) -> None:
    """Convert resource fields: data_format->media_type+format, temporal restructure."""
    resources = dataset.get("resources", [])
    for res in resources:
        # data_format -> media_type + format
        data_format = pop_if(res, "data_format")
        if data_format and not is_empty(data_format):
            mapped = DATA_FORMAT_MAP.get(data_format, {})
            if "media_type" in mapped:
                res["media_type"] = mapped["media_type"]
            if "format" in mapped:
                res["format"] = mapped["format"]
            if not mapped:
                # Unknown format -- keep as format string
                res["format"] = data_format

        # access_modality -> removed in v1.0
        pop_if(res, "access_modality")

        # temporal.temporal_resolution -> resource.temporal_resolution (move up)
        temporal = res.get("temporal")
        if temporal and isinstance(temporal, dict):
            temp_res = pop_if(temporal, "temporal_resolution")
            if temp_res and not is_empty(temp_res):
                res["temporal_resolution"] = temp_res
            # Clean up empty temporal
            if not temporal or all(is_empty(v) for v in temporal.values()):
                pop_if(res, "temporal")


def extract_top_level_entities(dataset: dict) -> None:
    """Extract publisher/creator/contact_point from attributions into top-level fields."""
    attributions = dataset.get("attributions", [])

    role_map = {}
    for attr in attributions:
        role = attr.get("role")
        entity = attr.get("entity")
        if role and entity:
            role_map.setdefault(role, entity)

    for role in ("publisher", "creator", "contact_point"):
        if role in role_map and role not in dataset:
            dataset[role] = copy.deepcopy(role_map[role])


def convert_hazard_object(hazard: dict) -> dict:
    """Convert a v0.3 Hazard object: hazard_process -> process."""
    new_haz = {}
    for key, val in hazard.items():
        if key == "hazard_process":
            new_haz["process"] = val
        elif key == "trigger":
            new_haz["trigger"] = convert_trigger(val)
        else:
            new_haz[key] = val
    return new_haz


def convert_trigger(trigger: dict) -> dict:
    """Convert a v0.3 Trigger: hazard_process -> process."""
    new_trig = {}
    for key, val in trigger.items():
        if key == "hazard_process":
            new_trig["process"] = val
        elif key == "processes":
            # Old template had 'processes' (singular mapping)
            new_trig["process"] = val
        else:
            new_trig[key] = val
    return new_trig


def convert_hazard_section(dataset: dict) -> None:
    """Convert hazard section: rename hazard_process->process in all hazard objects."""
    hazard = dataset.get("hazard")
    if not hazard:
        return

    for event_set in hazard.get("event_sets", []):
        # Convert hazards array
        if "hazards" in event_set:
            event_set["hazards"] = [
                convert_hazard_object(h) for h in event_set["hazards"]
            ]

        # Convert events
        for event in event_set.get("events", []):
            if "hazard" in event and isinstance(event["hazard"], dict):
                event["hazard"] = convert_hazard_object(event["hazard"])

            # Convert occurrence.empirical.temporal -- move temporal_resolution out
            occ = event.get("occurrence", {})
            emp = occ.get("empirical", {})
            if isinstance(emp, dict):
                temp = emp.get("temporal")
                if isinstance(temp, dict):
                    pop_if(temp, "temporal_resolution")  # not in v1.0 empirical.temporal


def convert_exposure(dataset: dict) -> None:
    """Convert exposure: flat quantity_kind -> nested measurement {quantity_kind, unit}."""
    exposure = dataset.get("exposure")
    if not exposure:
        return

    for exp_item in exposure:
        for metric in exp_item.get("metrics", []):
            qk = pop_if(metric, "quantity_kind")
            currency = pop_if(metric, "currency")
            if qk and not is_empty(qk):
                measurement = {"quantity_kind": qk}
                if currency and not is_empty(currency):
                    measurement["unit"] = currency
                metric["measurement"] = measurement


def convert_vulnerability_function(func: dict) -> dict:
    """Convert a v0.3 vulnerability/fragility function to v1.0 structure.

    v0.3 flat fields:
      hazard_primary, hazard_secondary, hazard_process_primary,
      hazard_process_secondary, intensity_measure
    -> v1.0 nested:
      hazard_primary: {type, process, intensity_measure}
      hazard_secondary: {type, process}
    """
    new_func = {}

    hp_type = pop_if(func, "hazard_primary")
    hp_proc = pop_if(func, "hazard_process_primary")
    hs_type = pop_if(func, "hazard_secondary")
    hs_proc = pop_if(func, "hazard_process_secondary")
    imt = pop_if(func, "intensity_measure")

    # Build nested hazard_primary
    if hp_type and not is_empty(hp_type):
        hp = {"type": hp_type}
        if hp_proc and not is_empty(hp_proc):
            hp["process"] = hp_proc
        if imt and not is_empty(imt):
            hp["intensity_measure"] = imt
        new_func["hazard_primary"] = hp

    # Build nested hazard_secondary
    if hs_type and not is_empty(hs_type):
        hs = {"type": hs_type}
        if hs_proc and not is_empty(hs_proc):
            hs["process"] = hs_proc
        new_func["hazard_secondary"] = hs

    # v0.3 flat quantity_kind -> v1.0 impact_measurement {quantity_kind, unit}
    qk = pop_if(func, "quantity_kind")
    if qk and not is_empty(qk):
        new_func["impact_measurement"] = {"quantity_kind": qk}

    # Copy remaining fields
    for key, val in func.items():
        if key not in new_func:
            new_func[key] = val

    return new_func


def convert_vulnerability(dataset: dict) -> None:
    """Convert vulnerability section: flat fields -> nested objects."""
    vuln = dataset.get("vulnerability")
    if not vuln:
        return

    functions = vuln.get("functions", {})
    for func_type in ("vulnerability", "fragility", "damage_to_loss", "engineering_demand"):
        func_list = functions.get(func_type)
        if func_list:
            functions[func_type] = [
                convert_vulnerability_function(f) for f in func_list
            ]


def convert_loss(dataset: dict) -> None:
    """Convert loss section: flat hazard/quantity -> nested objects, remove lineage."""
    loss = dataset.get("loss")
    if not loss:
        return

    for loss_item in loss.get("losses", []):
        # v0.3 flat hazard_type + hazard_process -> v1.0 nested hazard object
        h_type = pop_if(loss_item, "hazard_type")
        h_proc = pop_if(loss_item, "hazard_process")
        if h_type and not is_empty(h_type):
            hazard_obj = {"type": h_type}
            if h_proc and not is_empty(h_proc):
                hazard_obj["process"] = h_proc
            loss_item["hazard"] = hazard_obj

        # v0.3 impact_and_losses.quantity_kind + currency -> measurement {quantity_kind, unit}
        ial = loss_item.get("impact_and_losses", {})
        qk = pop_if(ial, "quantity_kind")
        currency = pop_if(ial, "currency")
        if qk and not is_empty(qk):
            measurement = {"quantity_kind": qk}
            if currency and not is_empty(currency):
                measurement["unit"] = currency
            ial["measurement"] = measurement

        # v0.3 loss.lineage -> removed in v1.0
        pop_if(loss_item, "lineage")


def convert_links(dataset: dict) -> None:
    """Update links href from v0.3 to v1.0 schema URL."""
    links = dataset.get("links", [])
    for link in links:
        if link.get("href") == V03_LINK:
            link["href"] = V10_LINK


def remove_license_url(dataset: dict) -> None:
    """Remove license_url field (v0.3 only)."""
    pop_if(dataset, "license_url")


def remove_template_annotations(obj):
    """Remove _comment, _legend, _cross_field_rules, _req, _note, _item etc."""
    if isinstance(obj, dict):
        return {
            k: remove_template_annotations(v)
            for k, v in obj.items()
            if not k.startswith("_")
        }
    if isinstance(obj, list):
        return [remove_template_annotations(item) for item in obj]
    return obj


# ---------------------------------------------------------------------------
# Main conversion
# ---------------------------------------------------------------------------

def convert_dataset(dataset: dict) -> dict:
    """Apply all v0.3 -> v1.0 conversions to a single dataset."""
    ds = copy.deepcopy(dataset)

    # Remove any template annotations
    ds = remove_template_annotations(ds)

    # 1. Sources -> lineage.sources
    convert_sources_to_lineage(ds)

    # 2. Resources: data_format, temporal_resolution
    convert_resources(ds)

    # 3. Extract publisher/creator/contact_point from attributions
    extract_top_level_entities(ds)

    # 4. Hazard: hazard_process -> process
    convert_hazard_section(ds)

    # 5. Exposure: flat quantity_kind -> measurement
    convert_exposure(ds)

    # 6. Vulnerability: flat fields -> nested
    convert_vulnerability(ds)

    # 7. Loss: flat fields -> nested, remove lineage
    convert_loss(ds)

    # 8. Links: update schema URL
    convert_links(ds)

    # 9. Remove license_url
    remove_license_url(ds)

    # 10. Clean up empty values
    ds = clean_empty(ds)

    return ds


def convert_file(input_path: str, output_path: str = None) -> str:
    """Convert a v0.3 JSON file to v1.0 format.

    Returns the output file path.
    """
    inp = Path(input_path)
    if output_path is None:
        output_path = str(inp.with_stem(inp.stem + "_v1.0"))
    out = Path(output_path)

    with open(inp, "r", encoding="utf-8") as f:
        data = json.load(f)

    # Handle both wrapped and unwrapped formats
    if "datasets" in data and isinstance(data["datasets"], list):
        # Wrapped format
        converted_datasets = [convert_dataset(ds) for ds in data["datasets"]]
        result = {"datasets": converted_datasets}
    else:
        # Unwrapped -- single dataset object
        converted = convert_dataset(data)
        result = {"datasets": [converted]}

    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    return str(out)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: python convert_v03_to_v10.py <input_v03.json> [output_v10.json]")
        print()
        print("Converts RDLS v0.3 metadata JSON to v1.0 format.")
        print("If output is not specified, writes to <input>_v1.0.json.")
        print()
        print("Key conversions:")
        print("  - sources[] -> lineage.sources[] (component->used_in)")
        print("  - data_format -> media_type + format")
        print("  - temporal.temporal_resolution -> resource.temporal_resolution")
        print("  - attributions -> extracts publisher/creator/contact_point")
        print("  - hazard_process -> process")
        print("  - vulnerability flat fields -> nested hazard objects")
        print("  - loss flat fields -> nested hazard + measurement")
        print("  - links href -> v1.0 schema URL")
        print()
        print("Fields that need MANUAL review after conversion:")
        print("  - resource.climate (model, scenario, percentile) -- new in v1.0")
        print("  - resource.baseline_period -- new in v1.0")
        print("  - resource.spatial_aggregation -- new in v1.0")
        print("  - resource.spatial -- new in v1.0 (resource-level spatial coverage)")
        print("  - period.central_year -- new in v1.0")
        print("  - New hazard types: dust_sand_storm, pest_infestation, erosion, sea_level_rise")
        print("  - New process types: lightning, thunderstorm, hail, wildfire_smoke, etc.")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    if not Path(input_file).exists():
        print(f"Error: Input file not found: {input_file}")
        sys.exit(1)

    result_path = convert_file(input_file, output_file)

    # Report what was done
    with open(result_path, "r", encoding="utf-8") as f:
        result = json.load(f)

    n_datasets = len(result.get("datasets", []))
    print(f"Converted {n_datasets} dataset(s): {input_file} -> {result_path}")
    print()
    print("Automatic conversions applied:")
    print("  [x] sources[] -> lineage.sources[] (component->used_in)")
    print("  [x] data_format -> media_type + format")
    print("  [x] temporal.temporal_resolution -> resource.temporal_resolution")
    print("  [x] publisher/creator/contact_point extracted from attributions")
    print("  [x] hazard_process -> process")
    print("  [x] vulnerability flat fields -> nested hazard_primary object")
    print("  [x] loss flat fields -> nested hazard + measurement")
    print("  [x] links href -> v1.0 schema URL")
    print("  [x] license_url removed")
    print("  [x] Empty fields cleaned up")
    print()
    print("MANUAL review needed for v1.0-only fields:")
    print("  [ ] resource.climate (model, scenario, percentile)")
    print("  [ ] resource.baseline_period")
    print("  [ ] resource.spatial_aggregation")
    print("  [ ] resource.spatial (resource-level coverage)")
    print("  [ ] period.central_year")


if __name__ == "__main__":
    main()
