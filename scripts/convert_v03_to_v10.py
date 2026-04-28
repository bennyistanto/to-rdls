#!/usr/bin/env python3
"""Convert RDLS v0.3 metadata JSON to v1.0 format.

Usage:
    python scripts/convert_v03_to_v10.py <input_v03.json> [output_v10.json]

If output is not specified, writes to <input>_v1.0.json.
Handles both wrapped {"datasets": [...]} and unwrapped single-dataset formats.

Automatic conversions:
  - sources[] -> lineage.sources[] (component -> used_in)
  - data_format -> media_type (IANA) or format (free text when IANA unknown)
  - access_modality -> removed
  - temporal.temporal_resolution -> resource.temporal_resolution (moved up)
  - attributions -> publisher/creator/contact_point extracted as top-level Entity fields
  - hazard.hazard_process -> process
  - vulnerability flat fields (hazard_primary string, intensity_measure, impact_type/modelling/metric)
    -> nested hazard_primary object + nested impact object
  - quantity_kind "fraction"/"ratio" -> "dimensionless_ratio"
  - loss flat hazard_type/process -> nested hazard object
  - exposure.taxonomy (flat string) -> asset_type {id, scheme} object
  - license short code -> full URL
  - date_published YYYY or YYYY-MM -> YYYY-MM-DD
  - links href -> updated to v1.0 URL
  - license_url -> removed

Fields marked [TODO] in output (require human review):
  - license codes not in the known mapping table
  - data_format values containing multiple comma-separated formats
  - data_format values not in the IANA lookup table
  - ZIP resources (packaging format -- inner format needed)
"""

import json
import re
import sys
import copy
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

V03_LINK = "https://docs.riskdatalibrary.org/en/0__3__0/rdls_schema.json"
V10_LINK = "https://docs.riskdatalibrary.org/en/1__0__0/rdls_schema.json"

# ---------------------------------------------------------------------------
# Mapping: data_format (v0.3 closed enum label) -> IANA media_type
#
# Rule:  if IANA code is in media_type.csv -> use media_type ONLY
#        if no IANA code in codelist -> use format free text ONLY
#        NEVER both together.
#
# Source: rdl-standard/schema/codelists/open/media_type.csv
# ---------------------------------------------------------------------------

DATA_FORMAT_TO_MEDIA_TYPE = {
    # GeoTIFF
    "GeoTIFF (tif)":                 "image/tiff;application=geotiff",
    "GeoTIFF":                       "image/tiff;application=geotiff",
    "tif":                           "image/tiff;application=geotiff",
    "tiff":                          "image/tiff;application=geotiff",
    # Cloud Optimized GeoTIFF
    "Cloud Optimized GeoTIFF (cog)": "image/tiff;application=geotiff;profile=cloud-optimized",
    "COG":                           "image/tiff;application=geotiff;profile=cloud-optimized",
    # NetCDF
    "NetCDF (nc)":                   "application/netcdf",
    "NetCDF":                        "application/netcdf",
    "nc":                            "application/netcdf",
    # HDF5
    "HDF5 (hdf5)":                   "application/x-hdf5",
    "HDF5":                          "application/x-hdf5",
    # Zarr
    "Zarr (zarr)":                   "application/vnd.zarr",
    "Zarr":                          "application/vnd.zarr",
    # GeoPackage
    "GeoPackage (gpkg)":             "application/geopackage+sqlite3",
    "GeoPackage":                    "application/geopackage+sqlite3",
    "gpkg":                          "application/geopackage+sqlite3",
    # GeoJSON
    "GeoJSON (geojson)":             "application/geo+json",
    "GeoJSON":                       "application/geo+json",
    "geojson":                       "application/geo+json",
    # FlatGeobuf
    "FlatGeobuf (fgb)":              "application/vnd.flatgeobuf",
    "FlatGeobuf":                    "application/vnd.flatgeobuf",
    # Shapefile
    "Shapefile (shp)":               "application/vnd.shp",
    "Shapefile":                     "application/vnd.shp",
    "shp":                           "application/vnd.shp",
    # File Geodatabase -- no IANA code in codelist, keep as format free text
    # KML
    "KML (kml)":                     "application/vnd.google-earth.kml+xml",
    "KML":                           "application/vnd.google-earth.kml+xml",
    # CSV
    "CSV (csv)":                     "text/csv",
    "CSV":                           "text/csv",
    "csv":                           "text/csv",
    # Parquet
    "Parquet (parquet)":             "application/vnd.apache.parquet",
    "Parquet":                       "application/vnd.apache.parquet",
    # Excel
    "Excel (xlsx)":                  "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "Excel (.xlsx)":                 "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "Excel":                         "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "xlsx":                          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    # JSON
    "JSON (json)":                   "application/json",
    "JSON":                          "application/json",
    "json":                          "application/json",
    # XML
    "XML (xml)":                     "application/xml",
    "XML":                           "application/xml",
    "xml":                           "application/xml",
    # PDF
    "PDF (pdf)":                     "application/pdf",
    "PDF":                           "application/pdf",
    # LAS
    "LAS (las)":                     "application/vnd.las",
    "LAS":                           "application/vnd.las",
    # COPC
    "COPC (copc)":                   "application/vnd.laszip+copc",
    "COPC":                          "application/vnd.laszip+copc",
    # Text
    "Text (txt)":                    "text/plain",
    "TXT":                           "text/plain",
    "txt":                           "text/plain",
    # HDF
    "HDF (hdf)":                     "application/x-hdf",
    "HDF":                           "application/x-hdf",
    # 3D Tiles
    "3D Tiles":                      "application/3dtiles+json",
    # PMTiles
    "PMTiles":                       "application/vnd.pmtiles",
}

# Formats that have no IANA code in media_type.csv -- keep as format free text
NO_IANA_FORMATS = {
    "GRID", "GRIB", "File Geodatabase", "File Geodatabase (gdb)",
    "Database", "Python", "Python (.py)", "R", "MATLAB",
    "API", "WMS", "WFS", "WCS", "OGC API",
}

# License code -> full URL
LICENSE_URL_MAP = {
    "CC0":             "https://creativecommons.org/publicdomain/zero/1.0/",
    "CC0-1.0":         "https://creativecommons.org/publicdomain/zero/1.0/",
    "CC-BY-4.0":       "https://creativecommons.org/licenses/by/4.0/",
    "CC-BY-SA-4.0":    "https://creativecommons.org/licenses/by-sa/4.0/",
    "CC-BY-NC-4.0":    "https://creativecommons.org/licenses/by-nc/4.0/",
    "CC-BY-ND-4.0":    "https://creativecommons.org/licenses/by-nd/4.0/",
    "CC-BY-NC-SA-4.0": "https://creativecommons.org/licenses/by-nc-sa/4.0/",
    "CC-BY-NC-ND-4.0": "https://creativecommons.org/licenses/by-nc-nd/4.0/",
    "ODbL":            "https://opendatacommons.org/licenses/odbl/1-0/",
    "ODbL-1.0":        "https://opendatacommons.org/licenses/odbl/1-0/",
    "PDDL":            "https://opendatacommons.org/licenses/pddl/1-0/",
    # "open" is a placeholder -- treated as CC-BY-4.0 (academic default)
    "open":            "https://creativecommons.org/licenses/by/4.0/",
}

# quantity_kind normalisation -- v0.3 used non-standard values
QUANTITY_KIND_NORM = {
    "fraction":     "dimensionless_ratio",
    "ratio":        "dimensionless_ratio",
    "percentage":   "dimensionless_ratio",
    "dimensionless":"dimensionless_ratio",
}


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def pop_if(d: dict, key: str, default=None):
    return d.pop(key, default)


def is_empty(val) -> bool:
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
        return [clean_empty(item) for item in d if not is_empty(item)]
    return d


def normalise_quantity_kind(qk: str) -> str:
    return QUANTITY_KIND_NORM.get(qk.lower() if qk else qk, qk)


def fix_date_published(val) -> str:
    """Convert year-only 'YYYY' or partial 'YYYY-MM' to 'YYYY-MM-DD'."""
    if not isinstance(val, str):
        return val
    if re.fullmatch(r"\d{4}", val):
        return val + "-01-01"
    if re.fullmatch(r"\d{4}-\d{2}", val):
        return val + "-01"
    return val


# ---------------------------------------------------------------------------
# Conversion: sources -> lineage.sources
# ---------------------------------------------------------------------------

def convert_sources_to_lineage(dataset: dict) -> None:
    """Move dataset.sources[] -> dataset.lineage.sources[] with field renames."""
    sources = pop_if(dataset, "sources")
    if not sources:
        return

    converted = []
    for src in sources:
        new_src = {"id": src.get("id", "")}
        for field in ("name", "url", "type"):
            if field in src and not is_empty(src[field]):
                new_src[field] = src[field]
        # license on sources: convert code to URL same as dataset-level license
        src_lic = src.get("license")
        if src_lic and not is_empty(src_lic):
            if src_lic.startswith("http"):
                new_src["license"] = src_lic
            else:
                mapped = LICENSE_URL_MAP.get(src_lic)
                if mapped:
                    new_src["license"] = mapped
                elif src_lic.lower() in ("unknown", "unspecified", "n/a"):
                    pass  # omit unknown licenses rather than flagging
                else:
                    new_src["license"] = f"[TODO: replace '{src_lic}' with full license URL]"
        # v0.3 'component' -> v1.0 'used_in'
        component = src.get("component")
        if component and not is_empty(component):
            new_src["used_in"] = component
        # v0.3 'lineage' processing-steps string -> append to description
        lineage_text = src.get("lineage")
        description = src.get("description", "")
        if lineage_text and not is_empty(lineage_text):
            description = (description + " Lineage: " + lineage_text).strip()
        if description:
            new_src["description"] = description
        converted.append(new_src)

    # Merge into existing lineage or create new
    if "lineage" in dataset and isinstance(dataset["lineage"], dict):
        dataset["lineage"]["sources"] = converted
    else:
        dataset["lineage"] = {"sources": converted}


# ---------------------------------------------------------------------------
# Conversion: resources
# ---------------------------------------------------------------------------

def _resolve_data_format(df: str) -> dict:
    """Return {"media_type": ...} or {"format": ...} for a data_format string.

    Rules:
      - IANA known (in media_type.csv) -> media_type only
      - IANA unknown -> format free text only
      - Multiple comma-separated -> [TODO] marker
      - ZIP -> [TODO] marker (packaging, not format)
    """
    if not df or is_empty(df):
        return {}

    # Multiple formats
    parts = [p.strip() for p in df.split(",")]
    if len(parts) > 1:
        return {"format": f"[TODO: multiple formats - split into separate resources with media_type per format. Formats were: {df}]"}

    # Strip trailing parenthetical extension like "(csv)" or "(.shp)" for lookup
    clean = re.sub(r"\s*\(\.?\w+\)\s*$", "", df).strip()

    # ZIP is packaging -- inner format needed
    if clean.lower() in ("zip", "zip archive"):
        return {"format": f"[TODO: ZIP is packaging - use media_type of the inner data format instead (e.g. image/tiff;application=geotiff). Was: {df}]"}

    # Known no-IANA formats
    if df in NO_IANA_FORMATS or clean in NO_IANA_FORMATS:
        return {"format": df}

    # Try IANA lookup -- exact match first, then cleaned key
    iana = DATA_FORMAT_TO_MEDIA_TYPE.get(df) or DATA_FORMAT_TO_MEDIA_TYPE.get(clean)
    if iana:
        return {"media_type": iana}

    # Try case-insensitive lookup
    df_lower = df.lower()
    clean_lower = clean.lower()
    for key, val in DATA_FORMAT_TO_MEDIA_TYPE.items():
        if key.lower() in (df_lower, clean_lower):
            return {"media_type": val}

    # Unknown -- keep as free text format
    return {"format": df}


def convert_resources(dataset: dict) -> None:
    """Convert resource fields: data_format, access_modality, temporal."""
    for res in dataset.get("resources", []):
        # data_format -> media_type or format (never both)
        data_format = pop_if(res, "data_format")
        if data_format and not is_empty(data_format):
            if "media_type" not in res and "format" not in res:
                res.update(_resolve_data_format(data_format))

        # access_modality -> removed in v1.0
        pop_if(res, "access_modality")

        # temporal.temporal_resolution -> resource.temporal_resolution
        temporal = res.get("temporal")
        if temporal and isinstance(temporal, dict):
            temp_res = pop_if(temporal, "temporal_resolution")
            if temp_res and not is_empty(temp_res):
                res["temporal_resolution"] = temp_res
            if not temporal or all(is_empty(v) for v in temporal.values()):
                pop_if(res, "temporal")


# ---------------------------------------------------------------------------
# Conversion: attributions -> top-level entities
# ---------------------------------------------------------------------------

def extract_top_level_entities(dataset: dict) -> None:
    """Extract publisher/creator/contact_point from attributions to top-level fields.
    Removes those entries from the attributions array (they are not valid role codes).
    """
    attributions = dataset.get("attributions", [])
    role_map: dict[str, dict] = {}
    remaining = []

    for attr in attributions:
        role = attr.get("role", "")
        entity = attr.get("entity", {})
        if role in ("publisher", "creator", "contact_point") and entity:
            role_map.setdefault(role, copy.deepcopy(entity))
        else:
            remaining.append(attr)

    for role in ("publisher", "creator", "contact_point"):
        if role in role_map and role not in dataset:
            dataset[role] = role_map[role]

    if remaining:
        dataset["attributions"] = remaining
    elif "attributions" in dataset:
        del dataset["attributions"]


# ---------------------------------------------------------------------------
# Conversion: hazard section
# ---------------------------------------------------------------------------

def _convert_hazard_obj(h: dict) -> dict:
    """Rename hazard_process -> process; recursively handle trigger."""
    new_h = {}
    for key, val in h.items():
        if key == "hazard_process":
            new_h["process"] = val
        elif key == "trigger" and isinstance(val, dict):
            new_h["trigger"] = _convert_hazard_obj(val)
        else:
            new_h[key] = val
    return new_h


def convert_hazard_section(dataset: dict) -> None:
    hazard = dataset.get("hazard")
    if not hazard:
        return
    for es in hazard.get("event_sets", []):
        if "hazards" in es:
            es["hazards"] = [_convert_hazard_obj(h) for h in es["hazards"]]
        for evt in es.get("events", []):
            if isinstance(evt.get("hazard"), dict):
                evt["hazard"] = _convert_hazard_obj(evt["hazard"])
            # Remove temporal_resolution from occurrence.empirical.temporal (not in v1.0)
            occ = evt.get("occurrence", {})
            emp = occ.get("empirical", {})
            if isinstance(emp, dict) and isinstance(emp.get("temporal"), dict):
                pop_if(emp["temporal"], "temporal_resolution")


# ---------------------------------------------------------------------------
# Conversion: exposure
# ---------------------------------------------------------------------------

def convert_exposure(dataset: dict) -> None:
    """Convert exposure items: quantity_kind -> measurement; taxonomy -> asset_type."""
    for exp_item in dataset.get("exposure") or []:
        # taxonomy (v0.3 flat string) -> asset_type.scheme (v1.0 object)
        # v0.3: taxonomy: "GED4ALL"
        # v1.0: asset_type: {id: "GED4ALL", scheme: "GED4ALL"}  (open: classification_scheme.csv)
        taxonomy = pop_if(exp_item, "taxonomy")
        if taxonomy and not is_empty(taxonomy) and "asset_type" not in exp_item:
            exp_item["asset_type"] = {"id": taxonomy, "scheme": taxonomy}

        # metrics: flat quantity_kind -> nested measurement {quantity_kind, unit}
        for metric in exp_item.get("metrics", []):
            qk = pop_if(metric, "quantity_kind")
            currency = pop_if(metric, "currency")
            if qk and not is_empty(qk):
                measurement = {"quantity_kind": normalise_quantity_kind(qk)}
                if currency and not is_empty(currency):
                    measurement["unit"] = currency
                metric["measurement"] = measurement


# ---------------------------------------------------------------------------
# Conversion: vulnerability functions
# ---------------------------------------------------------------------------

def _convert_vuln_function(func: dict) -> dict:
    """Convert a single v0.3 vulnerability/fragility/damage_to_loss/engineering_demand function.

    v0.3 flat fields:
      hazard_primary (string or object)
      hazard_process_primary
      hazard_secondary (string or object)
      hazard_process_secondary
      intensity_measure
      impact_type, impact_modelling, impact_metric, quantity_kind

    v1.0 nested:
      hazard_primary: {type, process, intensity_measure}
      hazard_secondary: {type, process}
      impact: {type, modelling, metric, measurement: {quantity_kind}}
    """
    fn = copy.deepcopy(func)

    # --- hazard_primary ---
    hp_raw  = pop_if(fn, "hazard_primary")
    hp_proc = pop_if(fn, "hazard_process_primary")
    imt     = pop_if(fn, "intensity_measure")

    if hp_raw is not None and not is_empty(hp_raw):
        if isinstance(hp_raw, str):
            # v0.3: plain hazard type string
            hp = {"type": hp_raw}
            if hp_proc and not is_empty(hp_proc):
                hp["process"] = hp_proc
            if imt and not is_empty(imt):
                hp["intensity_measure"] = imt
        elif isinstance(hp_raw, dict):
            # Already an object (partially converted)
            hp = hp_raw
            if hp_proc and not is_empty(hp_proc) and "process" not in hp:
                hp["process"] = hp_proc
            if imt and not is_empty(imt) and "intensity_measure" not in hp:
                hp["intensity_measure"] = imt
        else:
            hp = {"type": str(hp_raw)}
        fn["hazard_primary"] = hp

    # --- hazard_secondary ---
    hs_raw  = pop_if(fn, "hazard_secondary")
    hs_proc = pop_if(fn, "hazard_process_secondary")

    if hs_raw is not None and not is_empty(hs_raw):
        if isinstance(hs_raw, str):
            hs = {"type": hs_raw}
            if hs_proc and not is_empty(hs_proc):
                hs["process"] = hs_proc
        elif isinstance(hs_raw, dict):
            hs = hs_raw
            if hs_proc and not is_empty(hs_proc) and "process" not in hs:
                hs["process"] = hs_proc
        else:
            hs = {"type": str(hs_raw)}
        fn["hazard_secondary"] = hs

    # --- impact: nest flat impact_type/modelling/metric + quantity_kind ---
    existing_impact = pop_if(fn, "impact") or {}
    it     = pop_if(fn, "impact_type")     or existing_impact.get("type")
    im     = pop_if(fn, "impact_modelling") or existing_impact.get("modelling")
    metric = pop_if(fn, "impact_metric")   or existing_impact.get("metric")
    qk     = pop_if(fn, "quantity_kind")

    # Remove legacy flat key if converter put it there in a previous pass
    pop_if(fn, "impact_measurement")

    impact: dict = {}
    if it and not is_empty(it):
        impact["type"] = it
    if im and not is_empty(im):
        impact["modelling"] = im
    if metric and not is_empty(metric):
        impact["metric"] = metric

    existing_meas = existing_impact.get("measurement") or {}
    if qk and not is_empty(qk):
        existing_meas["quantity_kind"] = normalise_quantity_kind(qk)
    if existing_meas:
        impact["measurement"] = existing_meas

    if impact:
        fn["impact"] = impact

    return fn


def convert_vulnerability(dataset: dict) -> None:
    vuln = dataset.get("vulnerability")
    if not vuln:
        return
    functions = vuln.get("functions", {})
    if not isinstance(functions, dict):
        return
    for ftype in ("vulnerability", "fragility", "damage_to_loss", "engineering_demand"):
        func_list = functions.get(ftype)
        if func_list and isinstance(func_list, list):
            functions[ftype] = [_convert_vuln_function(f) for f in func_list]


# ---------------------------------------------------------------------------
# Conversion: loss
# ---------------------------------------------------------------------------

def convert_loss(dataset: dict) -> None:
    loss = dataset.get("loss")
    if not loss:
        return
    for loss_item in loss.get("losses", []):
        # flat hazard_type + hazard_process -> nested hazard object
        h_type = pop_if(loss_item, "hazard_type")
        h_proc = pop_if(loss_item, "hazard_process")
        # intensity_measure is REQUIRED on loss.hazard in v1.0 schema (via Hazard.$defs)
        # v0.3 loss records rarely carried it -- flag for human review if absent
        existing_hazard = loss_item.get("hazard", {})
        h_imt = pop_if(loss_item, "intensity_measure")  # may have been on loss_item directly
        if not h_imt:
            h_imt = existing_hazard.get("intensity_measure")  # or already nested

        if h_type and not is_empty(h_type):
            hazard_obj = {"type": h_type}
            if h_proc and not is_empty(h_proc):
                hazard_obj["process"] = h_proc
            if h_imt and not is_empty(h_imt):
                hazard_obj["intensity_measure"] = h_imt
            else:
                hazard_obj["intensity_measure"] = (
                    f"[TODO: add intensity_measure for hazard type '{h_type}' "
                    f"(e.g. PGA:g for earthquake, wd:m for flood) - required by v1.0 schema]"
                )
            loss_item["hazard"] = hazard_obj
        elif existing_hazard and "intensity_measure" not in existing_hazard:
            existing_hazard["intensity_measure"] = (
                "[TODO: add intensity_measure - required by v1.0 schema]"
            )

        # flat quantity_kind/currency in impact_and_losses -> measurement
        ial = loss_item.get("impact_and_losses", {})
        qk = pop_if(ial, "quantity_kind")
        currency = pop_if(ial, "currency")
        if qk and not is_empty(qk):
            measurement = {"quantity_kind": normalise_quantity_kind(qk)}
            if currency and not is_empty(currency):
                measurement["unit"] = currency
            ial["measurement"] = measurement

        pop_if(loss_item, "lineage")


# ---------------------------------------------------------------------------
# Conversion: license
# ---------------------------------------------------------------------------

def convert_license(dataset: dict) -> None:
    """Convert license short code to full URL. Mark unknown codes with [TODO]."""
    lic = dataset.get("license")
    if not lic or not isinstance(lic, str):
        return
    if lic.startswith("http"):
        return  # Already a URL
    url = LICENSE_URL_MAP.get(lic)
    if url:
        dataset["license"] = url
    else:
        dataset["license"] = f"[TODO: replace '{lic}' with full license URL (e.g. https://creativecommons.org/licenses/by/4.0/)]"


# ---------------------------------------------------------------------------
# Conversion: referenced_by date_published
# ---------------------------------------------------------------------------

def convert_referenced_by(dataset: dict) -> None:
    for ref in dataset.get("referenced_by", []):
        if "date_published" in ref:
            ref["date_published"] = fix_date_published(ref["date_published"])


# ---------------------------------------------------------------------------
# Conversion: links
# ---------------------------------------------------------------------------

def convert_links(dataset: dict) -> None:
    for link in dataset.get("links", []):
        if link.get("href") == V03_LINK:
            link["href"] = V10_LINK
    # Add v1.0 link if no links array exists
    if "links" not in dataset:
        dataset["links"] = [{"href": V10_LINK, "rel": "describedby"}]


# ---------------------------------------------------------------------------
# Field ordering — canonical v1.0 template order
# ---------------------------------------------------------------------------

# Top-level field order from rdls_template_v1.0.json.
# Fields not in this list are appended after "loss" and before "links".
V10_FIELD_ORDER = [
    "id", "title", "description", "risk_data_type",
    "publisher", "version", "purpose", "project", "details",
    "contact_point", "creator",
    "spatial", "spatial_resolution", "temporal", "temporal_resolution",
    "license", "attributions", "lineage", "referenced_by", "resources",
    "hazard", "exposure", "vulnerability", "loss",
    "links",
]


def reorder_fields(ds: dict) -> dict:
    """Return a new dict with keys in V10_FIELD_ORDER.

    Keys in ds but not in V10_FIELD_ORDER are inserted between 'loss' and 'links'
    so that 'links' is always last and nothing is silently dropped.
    """
    known = set(V10_FIELD_ORDER)
    extra = [k for k in ds if k not in known]

    ordered: dict = {}
    for key in V10_FIELD_ORDER:
        if key == "links":
            # Insert any unknown keys just before links
            for ek in extra:
                if ek in ds:
                    ordered[ek] = ds[ek]
        if key in ds:
            ordered[key] = ds[key]

    return ordered


# ---------------------------------------------------------------------------
# Main conversion pipeline
# ---------------------------------------------------------------------------

def convert_dataset(dataset: dict) -> dict:
    """Apply all v0.3 -> v1.0 conversions to a single dataset record."""
    ds = copy.deepcopy(dataset)

    # Remove template annotation keys (_comment, _req, etc.)
    ds = {k: v for k, v in ds.items() if not k.startswith("_")}

    convert_sources_to_lineage(ds)          # 1. sources -> lineage.sources
    convert_resources(ds)                   # 2. resources: data_format, access_modality
    extract_top_level_entities(ds)          # 3. attributions -> publisher/creator/contact_point
    convert_hazard_section(ds)              # 4. hazard_process -> process
    convert_exposure(ds)                    # 5. exposure: quantity_kind -> measurement; taxonomy -> asset_type
    convert_vulnerability(ds)              # 6. vulnerability: flat -> nested objects
    convert_loss(ds)                        # 7. loss: flat -> nested objects
    convert_license(ds)                     # 8. license code -> URL
    convert_referenced_by(ds)               # 9. date_published YYYY/YYYY-MM -> YYYY-MM-DD
    convert_links(ds)                       # 10. links href -> v1.0 URL
    pop_if(ds, "license_url")               # 11. remove v0.3-only field

    ds = clean_empty(ds)
    ds = reorder_fields(ds)                 # 12. enforce canonical v1.0 field order
    return ds


def convert_file(input_path: str, output_path: str = None) -> str:
    inp = Path(input_path)
    if output_path is None:
        output_path = str(inp.with_stem(inp.stem + "_v1.0"))
    out = Path(output_path)

    with open(inp, encoding="utf-8") as f:
        data = json.load(f)

    if "datasets" in data and isinstance(data["datasets"], list):
        converted = [convert_dataset(ds) for ds in data["datasets"]]
        result = {"datasets": converted}
    else:
        result = {"datasets": [convert_dataset(data)]}

    with open(out, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    return str(out)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/convert_v03_to_v10.py <input_v03.json> [output_v10.json]")
        print()
        print("Automatic conversions:")
        print("  [x] sources[] -> lineage.sources[] (component -> used_in)")
        print("  [x] data_format -> media_type (IANA) or format (free text when unknown)")
        print("  [x] access_modality removed")
        print("  [x] publisher/creator/contact_point extracted from attributions")
        print("  [x] hazard_process -> process")
        print("  [x] vulnerability flat fields -> nested hazard_primary + impact objects")
        print("  [x] quantity_kind 'fraction'/'ratio' -> 'dimensionless_ratio'")
        print("  [x] loss flat hazard_type/process -> nested hazard object")
        print("  [x] exposure.taxonomy (flat string) -> asset_type {id, scheme}")
        print("  [x] license code -> full URL")
        print("  [x] date_published YYYY/YYYY-MM -> YYYY-MM-DD")
        print("  [x] links href updated to v1.0 schema URL")
        print("  [x] license_url removed")
        print()
        print("Fields marked [TODO] in output (require human review):")
        print("  [ ] unknown license codes")
        print("  [ ] multi-format data_format values (split into separate resources)")
        print("  [ ] ZIP resources (use inner format media_type)")
        print("  [ ] unrecognised data_format values")
        print("  [ ] loss.hazard.intensity_measure missing (required by v1.0 schema)")
        print()
        print("v1.0-only fields not auto-populated (add manually if applicable):")
        print("  [ ] resource.climate (model, scenario, percentile)")
        print("  [ ] resource.baseline_period")
        print("  [ ] resource.spatial_aggregation")
        print("  [ ] resource.spatial (resource-level spatial coverage)")
        print("  [ ] period.central_year")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2] if len(sys.argv) > 2 else None

    if not Path(input_file).exists():
        print(f"Error: Input file not found: {input_file}")
        sys.exit(1)

    result_path = convert_file(input_file, output_file)

    with open(result_path, encoding="utf-8") as f:
        result = json.load(f)

    n = len(result.get("datasets", []))
    print(f"Converted {n} dataset(s): {input_file} -> {result_path}")

    # Report [TODO] items
    raw = json.dumps(result)
    todos = sorted(set(
        line.strip()
        for line in raw.replace(",", "\n").splitlines()
        if "[TODO" in line
    ))
    if todos:
        print(f"\nManual review required ({len(todos)} item(s)):")
        for t in todos:
            print(f"  {t[:120]}")
    else:
        print("\nNo [TODO] items - output may be ready for validation.")

    print(f"\nNext step: python scripts/validate_v1.0.py {result_path}")


if __name__ == "__main__":
    main()
