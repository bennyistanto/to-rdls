r"""
rdls_hdx_sanitize_validate.py
=================================
Post-LLM-review pipeline: rebuild, sanitize, and validate RDLS records.

Rebuilds output/llm/revised/ from:
- output/hdx/revised/ (source records, 12,594 files)
- output/llm/reports/review_report.csv (LLM decisions: rename, changes, not-RDLS)
- output/llm/reports/llm_classifications.jsonl (REMOVE operations for changed records)

Schema-driven sanitization:
- Strips empty optional fields (author_names, doi, etc.)
- Removes structurally invalid HEVL blocks (empty arrays, missing required sub-fields)
- Filters invalid country codes against schema codelist
- Reconciles risk_data_type with actual blocks present
- Reorders fields to match schema property order
- Rebuilds IDs/filenames to match updated risk_data_type

Fully offline, no API calls. ~2 min runtime.

Usage:
    cd C:\Users\benny\OneDrive\Documents\Github\to-rdls
    conda activate to-rdls
    set PYTHONPATH=C:\Users\benny\OneDrive\Documents\Github\to-rdls
    python notebooks\rdls_hdx_sanitize_validate.py
"""

import copy
import csv
import json
import re
import shutil
import sys
import time
from collections import Counter
from pathlib import Path

# --- Paths ---
PROJECT_DIR = Path(__file__).resolve().parent.parent
HDX_CRAWLER_DIR = PROJECT_DIR.parent / "hdx-metadata-crawler"

INPUT_DIR = PROJECT_DIR / "output" / "hdx" / "revised"  # original source
OUTPUT_DIR = PROJECT_DIR / "output" / "llm"
REVISED_DIR = OUTPUT_DIR / "revised"
NOT_RDLS_DIR = OUTPUT_DIR / "not_rdls"
DIST_FINAL_DIR = OUTPUT_DIR / "dist"
REPORTS_DIR = OUTPUT_DIR / "reports"
SCHEMA_PATH = (
    HDX_CRAWLER_DIR
    / "hdx_dataset_metadata_dump"
    / "rdls"
    / "schema"
    / "rdls_schema_v0.3.json"
)
CLASSIFICATIONS_PATH = REPORTS_DIR / "llm_classifications.jsonl"

# Verify paths
for name, path in [
    ("INPUT_DIR", INPUT_DIR),
    ("SCHEMA_PATH", SCHEMA_PATH),
    ("REPORTS_DIR", REPORTS_DIR),
]:
    if not path.exists():
        print(f"ERROR: {name} not found: {path}")
        sys.exit(1)

sys.path.insert(0, str(PROJECT_DIR))
from src.utils import load_json, write_json

# ================================================================
# STEP 1: Load review report (LLM decisions)
# ================================================================
print("=" * 60)
print("  STEP 1: LOAD REVIEW REPORT")
print("=" * 60)

report_path = REPORTS_DIR / "review_report.csv"
if not report_path.exists():
    print(f"ERROR: {report_path} not found. Run the full pipeline first.")
    sys.exit(1)

# Build lookup: rdls_id -> {new_id, changes, has_change, final_rdt, ...}
report_rows = {}
not_rdls_ids = set()
rename_map = {}
with open(report_path, "r", encoding="utf-8") as f:
    for row in csv.DictReader(f):
        rdls_id = row["rdls_id"]
        report_rows[rdls_id] = row
        new_id = row.get("new_id", "") or rdls_id
        if new_id != rdls_id:
            rename_map[rdls_id] = new_id
        if "not RDLS relevant" in (row.get("changes") or ""):
            not_rdls_ids.add(new_id)

print(f"  Report rows:    {len(report_rows)}")
print(f"  Renames:        {len(rename_map)}")
print(f"  Not-RDLS:       {len(not_rdls_ids)}")

# ================================================================
# STEP 2: Load LLM classifications for changed records
# ================================================================
print(f"\n{'=' * 60}")
print("  STEP 2: LOAD LLM CLASSIFICATIONS")
print("=" * 60)

llm_decisions = {}  # rdls_id -> {components dict}
if CLASSIFICATIONS_PATH.exists():
    with open(CLASSIFICATIONS_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            rid = obj.get("rdls_id", "")
            if rid:
                llm_decisions[rid] = obj
    print(f"  LLM classifications loaded: {len(llm_decisions)}")
else:
    print("  WARNING: No llm_classifications.jsonl found")

# ================================================================
# STEP 3: Index source files
# ================================================================
print(f"\n{'=' * 60}")
print("  STEP 3: INDEX SOURCE FILES")
print("=" * 60)

# Index all input files (output/hdx/revised/**/*.json)
source_files = {}
for fp in INPUT_DIR.rglob("*.json"):
    source_files[fp.stem] = fp

print(f"  Source files (hdx/revised): {len(source_files)}")

# --- Helper functions (used in Steps 4 and 5) ---
single_map = {"hazard": "hzd", "exposure": "exp", "vulnerability": "vln", "loss": "lss"}
letter_map = {"hazard": "h", "exposure": "e", "vulnerability": "v", "loss": "l"}
order = ["hazard", "exposure", "vulnerability", "loss"]


def expected_code(rdt):
    """Return the HEVL code string for a given risk_data_type list."""
    present = [c for c in order if c in rdt]
    if not present:
        return None
    if len(present) == 1:
        return single_map[present[0]]
    return "".join(letter_map[c] for c in present)


def rebuild_id(old_id, new_code):
    """Replace the HEVL code in an ID: rdls_hevl-xxx -> rdls_lss-xxx"""
    m = re.match(r"(rdls_)[a-z]+(-.*)", old_id)
    if m:
        return f"{m.group(1)}{new_code}{m.group(2)}"
    return old_id


# ================================================================
# Schema-driven sanitizer
# ================================================================
# Principles:
#   1. Never fabricate data - only strip/remove
#   2. Follow schema hierarchy: root → HEVL blocks → sub-structures
#   3. Optional arrays with minItems: remove if empty (can't satisfy)
#   4. Required arrays with minItems: remove parent if can't satisfy
#   5. Empty optional fields ("",[],None): strip to avoid minLength/minItems
#   6. occurrence:{} kept as-is (team will revise schema)
#   7. Reconcile risk_data_type with actual blocks
#   8. Reorder fields to match schema property order

_sanitize_stats = Counter()

# Schema property order (from rdls_schema_v0.3.json)
SCHEMA_ORDER = [
    "id", "title", "description", "risk_data_type", "version",
    "purpose", "project", "details", "spatial", "license",
    "license_url", "attributions", "sources", "referenced_by",
    "resources", "hazard", "exposure", "vulnerability", "loss",
    "links",
]


def _strip_empty_optionals(obj, optional_keys):
    """Remove keys from obj whose values are empty ("", [], None).
    Only for keys that are NOT required by schema."""
    for key in optional_keys:
        val = obj.get(key)
        if val is None or val == "" or val == []:
            obj.pop(key, None)


def _clean_referenced_by(rec):
    """referenced_by[]: items require only 'id'.
    Optional: name, author_names (minItems=1), date_published, url, doi (minLength=1).
    Strip empty optionals; drop items missing 'id'; remove array if empty."""
    refs = rec.get("referenced_by")
    if not isinstance(refs, list):
        return
    cleaned = []
    for ref in refs:
        _strip_empty_optionals(ref, ["author_names", "doi", "date_published", "name", "url"])
        if ref.get("id"):
            cleaned.append(ref)
    if cleaned:
        rec["referenced_by"] = cleaned
        _sanitize_stats["referenced_by_cleaned"] += 1
    else:
        rec.pop("referenced_by", None)
        _sanitize_stats["referenced_by_removed"] += 1


def _clean_sources(rec):
    """sources[]: items require only 'id'.
    Optional: name, description, lineage, url, type, component, license.
    Strip empty optionals; drop items missing 'id'; remove array if empty."""
    sources = rec.get("sources")
    if not isinstance(sources, list):
        return
    cleaned = []
    for src in sources:
        _strip_empty_optionals(src, ["name", "description", "lineage", "url",
                                      "type", "component", "license"])
        if src.get("id"):
            cleaned.append(src)
    if cleaned:
        rec["sources"] = cleaned
    else:
        rec.pop("sources", None)
        _sanitize_stats["sources_removed"] += 1


def _clean_resources(rec):
    """resources[]: minItems=1 (REQUIRED at root).
    Items require: id, title, description, data_format + anyOf(download_url, access_url).
    Optional: access_modality, spatial_resolution, coordinate_system, temporal.
    Strip empty optionals in each resource; drop resources missing required fields."""
    res = rec.get("resources")
    if not isinstance(res, list):
        return
    cleaned = []
    for r in res:
        _strip_empty_optionals(r, ["access_modality", "spatial_resolution",
                                    "coordinate_system", "access_url",
                                    "download_url", "temporal"])
        # Also clean temporal sub-object
        temp = r.get("temporal")
        if isinstance(temp, dict):
            _strip_empty_optionals(temp, ["start", "end", "duration", "temporal_resolution"])
            if not temp:  # all fields were empty
                r.pop("temporal", None)
        # Resource requires: id, title, description, data_format + url
        has_required = (r.get("id") and r.get("title") and r.get("description")
                        and r.get("data_format"))
        has_url = r.get("download_url") or r.get("access_url")
        if has_required and has_url:
            cleaned.append(r)
        else:
            _sanitize_stats["resource_dropped_incomplete"] += 1
    if cleaned:
        rec["resources"] = cleaned
    else:
        # Can't fabricate resources - flag for not_rdls
        rec["resources"] = []
        _sanitize_stats["empty_resources"] += 1


def _clean_hazard(rec):
    """hazard.event_sets[]: minItems=1 (required when hazard present).
    Event_set requires: id, hazards (minItems=1), analysis_type.
    Event_set.events[] is OPTIONAL - remove if empty.
    Event requires: id, calculation_method, hazard, occurrence (minProperties=1).
    Hazard requires: id, type, hazard_process.
    NOTE: occurrence:{} is kept as-is (team will revise schema)."""
    haz = rec.get("hazard")
    if not isinstance(haz, dict):
        return

    event_sets = haz.get("event_sets")
    if not isinstance(event_sets, list) or len(event_sets) == 0:
        rec.pop("hazard", None)
        _sanitize_stats["hazard_removed_no_es"] += 1
        return

    valid_es = []
    for es in event_sets:
        # --- Clean hazards array (required, minItems=1) ---
        # Hazard requires: id, type, hazard_process
        # Hazard optional: intensity_measure, trigger
        hazards = es.get("hazards")
        if isinstance(hazards, list):
            valid_hazards = []
            for h in hazards:
                _strip_empty_optionals(h, ["intensity_measure"])
                # Clean trigger sub-object (optional, all fields optional inside)
                trigger = h.get("trigger")
                if isinstance(trigger, dict):
                    _strip_empty_optionals(trigger, ["type", "hazard_process"])
                    if not trigger:
                        h.pop("trigger", None)
                if h.get("id") and h.get("type") and h.get("hazard_process"):
                    valid_hazards.append(h)
            if valid_hazards:
                es["hazards"] = valid_hazards
            else:
                _sanitize_stats["es_dropped_no_hazards"] += 1
                continue  # Drop this event_set - can't satisfy required hazards

        # --- Clean events array (optional, minItems=1 if present) ---
        events = es.get("events")
        if isinstance(events, list):
            if len(events) == 0:
                es.pop("events", None)
                _sanitize_stats["empty_events_removed"] += 1
            else:
                # Event requires: id, calculation_method, hazard, occurrence
                # Event optional: disaster_identifiers (minItems=1), description
                valid_events = []
                for ev in events:
                    _strip_empty_optionals(ev, ["description"])
                    # disaster_identifiers: optional array minItems=1
                    di = ev.get("disaster_identifiers")
                    if isinstance(di, list) and len(di) == 0:
                        ev.pop("disaster_identifiers", None)
                    # Clean event.hazard sub-object (optional fields)
                    eh = ev.get("hazard")
                    if isinstance(eh, dict):
                        _strip_empty_optionals(eh, ["intensity_measure"])
                        trigger = eh.get("trigger")
                        if isinstance(trigger, dict):
                            _strip_empty_optionals(trigger, ["type", "hazard_process"])
                            if not trigger:
                                eh.pop("trigger", None)
                    # Note: occurrence:{} kept as-is (team will revise schema)
                    has_req = (ev.get("id") and ev.get("calculation_method")
                              and ev.get("hazard") and ev.get("occurrence") is not None)
                    if has_req:
                        valid_events.append(ev)
                    else:
                        _sanitize_stats["event_dropped_incomplete"] += 1
                if valid_events:
                    es["events"] = valid_events
                else:
                    es.pop("events", None)
                    _sanitize_stats["empty_events_removed"] += 1

        # --- Check event_set has required fields ---
        if es.get("id") and es.get("hazards") and es.get("analysis_type"):
            _strip_empty_optionals(es, ["frequency_distribution", "seasonality",
                                        "calculation_method", "event_count",
                                        "occurrence_range"])
            valid_es.append(es)
        else:
            _sanitize_stats["es_dropped_incomplete"] += 1

    if valid_es:
        haz["event_sets"] = valid_es
    else:
        rec.pop("hazard", None)
        _sanitize_stats["hazard_removed_no_es"] += 1


def _clean_exposure(rec):
    """exposure[]: minItems=1 (required when exposure present).
    Exposure_item requires: category, metrics (minItems=1).
    Metric requires: id, dimension, quantity_kind.
    Optional per metric: currency."""
    exp = rec.get("exposure")
    if not isinstance(exp, list):
        return
    valid_items = []
    for item in exp:
        # Clean metrics array
        # Metric requires: id, dimension, quantity_kind
        # Metric optional: currency
        metrics = item.get("metrics")
        if isinstance(metrics, list):
            valid_metrics = []
            for m in metrics:
                _strip_empty_optionals(m, ["currency"])
                if m.get("id") and m.get("dimension") and m.get("quantity_kind"):
                    valid_metrics.append(m)
            if valid_metrics:
                item["metrics"] = valid_metrics
            else:
                _sanitize_stats["exposure_item_no_metrics"] += 1
                continue  # Drop this exposure item
        else:
            _sanitize_stats["exposure_item_no_metrics"] += 1
            continue

        # Exposure_item requires: category, metrics
        # Exposure_item optional: taxonomy
        if item.get("category"):
            _strip_empty_optionals(item, ["taxonomy"])
            valid_items.append(item)
        else:
            _sanitize_stats["exposure_item_no_category"] += 1
    if valid_items:
        rec["exposure"] = valid_items
    else:
        rec.pop("exposure", None)
        _sanitize_stats["exposure_removed"] += 1


def _clean_vuln_function(func_entry):
    """Clean a single vulnerability/fragility/damage_to_loss/engineering_demand entry.
    Required (10): approach, relationship, hazard_primary, hazard_analysis_type,
                   intensity_measure, category, impact_type, impact_modelling,
                   impact_metric, quantity_kind.
    Optional: hazard_secondary, hazard_process_primary, hazard_process_secondary,
              taxonomy, analysis_details, damage_scale_name, damage_states_names, parameter.
    Strip empty optionals. Return True if entry has all required fields."""
    _strip_empty_optionals(func_entry, [
        "hazard_secondary", "hazard_process_primary", "hazard_process_secondary",
        "taxonomy", "analysis_details", "damage_scale_name", "parameter",
    ])
    # damage_states_names: array minItems=1 - remove if empty
    dsn = func_entry.get("damage_states_names")
    if isinstance(dsn, list) and len(dsn) == 0:
        func_entry.pop("damage_states_names", None)
    FUNC_REQUIRED = ["approach", "relationship", "hazard_primary", "hazard_analysis_type",
                     "intensity_measure", "category", "impact_type", "impact_modelling",
                     "impact_metric", "quantity_kind"]
    return all(func_entry.get(k) for k in FUNC_REQUIRED)


def _clean_vulnerability(rec):
    """vulnerability: anyOf(functions, socio_economic) - at least one required.
    functions: object minProperties=1, contains vulnerability/fragility/
               damage_to_loss/engineering_demand arrays (each minItems=1).
    socio_economic[]: minItems=1.
      Item requires: indicator_name, indicator_code, description, reference_year.
      Item optional: scheme, threshold, uri, analysis_details."""
    vuln = rec.get("vulnerability")
    if not isinstance(vuln, dict):
        return

    # --- Clean functions sub-object ---
    funcs = vuln.get("functions")
    if isinstance(funcs, dict):
        for func_type in ["vulnerability", "fragility", "damage_to_loss", "engineering_demand"]:
            arr = funcs.get(func_type)
            if isinstance(arr, list):
                if len(arr) == 0:
                    funcs.pop(func_type, None)
                else:
                    valid = [f for f in arr if _clean_vuln_function(f)]
                    if valid:
                        funcs[func_type] = valid
                    else:
                        funcs.pop(func_type, None)
                        _sanitize_stats["vuln_func_arr_dropped"] += 1
        if not funcs:  # All function arrays removed
            vuln.pop("functions", None)

    # --- Clean socio_economic array ---
    se = vuln.get("socio_economic")
    if isinstance(se, list):
        if len(se) == 0:
            vuln.pop("socio_economic", None)
            _sanitize_stats["vuln_empty_se_removed"] += 1
        else:
            valid_se = []
            for item in se:
                _strip_empty_optionals(item, ["scheme", "threshold", "uri", "analysis_details"])
                has_req = (item.get("indicator_name") and item.get("indicator_code")
                           and item.get("description"))
                has_year = item.get("reference_year") is not None
                if has_req and has_year:
                    valid_se.append(item)
            if valid_se:
                vuln["socio_economic"] = valid_se
            else:
                vuln.pop("socio_economic", None)
                _sanitize_stats["vuln_se_items_dropped"] += 1

    # --- Remove vulnerability if neither functions nor socio_economic ---
    has_funcs = bool(vuln.get("functions"))
    has_socio = bool(vuln.get("socio_economic"))
    if not has_funcs and not has_socio:
        rec.pop("vulnerability", None)
        _sanitize_stats["vuln_removed"] += 1


def _clean_loss(rec):
    """loss.losses[]: minItems=1.
    Losses item requires: id, hazard_type, asset_category, asset_dimension, impact_and_losses.
    impact_and_losses requires: impact_type, impact_modelling, impact_metric,
                                quantity_kind, loss_type, loss_approach, loss_frequency_type.
    Optional per loss: hazard_process, lineage, description."""
    loss = rec.get("loss")
    if not isinstance(loss, dict):
        return

    losses = loss.get("losses")
    if not isinstance(losses, list) or len(losses) == 0:
        rec.pop("loss", None)
        _sanitize_stats["loss_removed_empty"] += 1
        return

    valid_losses = []
    for entry in losses:
        # Must have impact_and_losses with required sub-fields
        # impact_and_losses requires (7): impact_type, impact_modelling, impact_metric,
        #   quantity_kind, loss_type, loss_approach, loss_frequency_type
        # impact_and_losses optional: currency
        ial = entry.get("impact_and_losses")
        if not isinstance(ial, dict) or not ial:
            _sanitize_stats["loss_entry_no_ial"] += 1
            continue

        _strip_empty_optionals(ial, ["currency"])
        ial_required = ["impact_type", "impact_modelling", "impact_metric",
                        "quantity_kind", "loss_type", "loss_approach", "loss_frequency_type"]
        ial_ok = all(ial.get(k) for k in ial_required)
        if not ial_ok:
            _sanitize_stats["loss_entry_ial_incomplete"] += 1
            continue

        # Losses item requires (5): id, hazard_type, asset_category, asset_dimension,
        #   impact_and_losses
        # Losses item optional: hazard_process, lineage (all sub-fields optional),
        #   description
        has_required = (entry.get("id") and entry.get("hazard_type")
                        and entry.get("asset_category") and entry.get("asset_dimension"))
        if has_required:
            _strip_empty_optionals(entry, ["hazard_process", "description"])
            # Clean lineage sub-object (all fields optional, minProperties=1)
            lineage = entry.get("lineage")
            if isinstance(lineage, dict):
                _strip_empty_optionals(lineage, ["hazard_dataset", "exposure_dataset",
                                                  "vulnerability_dataset"])
                if not lineage:
                    entry.pop("lineage", None)
            valid_losses.append(entry)
        else:
            _sanitize_stats["loss_entry_incomplete"] += 1

    if valid_losses:
        loss["losses"] = valid_losses
    else:
        rec.pop("loss", None)
        _sanitize_stats["loss_removed_all_invalid"] += 1


def _clean_spatial(rec, valid_country_codes=None):
    """spatial: required at root. Contains: scale, countries, bbox, centroid,
    gazetteer_entries, geometry. All optional sub-fields but minProperties=1.
    countries[]: closed codelist (ISO 3166-1 alpha-3), minItems=1 if present."""
    spatial = rec.get("spatial")
    if not isinstance(spatial, dict):
        return
    _strip_empty_optionals(spatial, ["scale", "bbox", "centroid"])

    # Filter invalid country codes against schema codelist
    countries = spatial.get("countries")
    if isinstance(countries, list) and valid_country_codes:
        filtered = [c for c in countries if c in valid_country_codes]
        if filtered:
            if len(filtered) != len(countries):
                removed = [c for c in countries if c not in valid_country_codes]
                _sanitize_stats["country_codes_removed"] += len(removed)
            spatial["countries"] = filtered
        else:
            # All country codes invalid - remove the array
            spatial.pop("countries", None)
            _sanitize_stats["countries_arr_removed"] += 1

    # Clean gazetteer_entries (optional, minItems=1 if present)
    # Gazetteer_entry requires: id. Optional: scheme, description, uri.
    gz = spatial.get("gazetteer_entries")
    if isinstance(gz, list):
        if len(gz) == 0:
            spatial.pop("gazetteer_entries", None)
        else:
            valid = []
            for g in gz:
                _strip_empty_optionals(g, ["scheme", "description", "uri"])
                if g.get("id"):
                    valid.append(g)
            if valid:
                spatial["gazetteer_entries"] = valid
            else:
                spatial.pop("gazetteer_entries", None)


def _clean_attributions(rec):
    """attributions[]: minItems=3 (required). Items require: id, entity, role.
    Entity requires: name + anyOf(email, url)."""
    attrs = rec.get("attributions")
    if not isinstance(attrs, list):
        return
    valid = []
    for a in attrs:
        entity = a.get("entity", {})
        if isinstance(entity, dict):
            _strip_empty_optionals(entity, ["email", "url"])
        if a.get("id") and a.get("role") and isinstance(entity, dict) and entity.get("name"):
            valid.append(a)
    if valid:
        rec["attributions"] = valid


def _reconcile_rdt(rec):
    """Reconcile risk_data_type with actual HEVL blocks present."""
    rdt = rec.get("risk_data_type", [])
    actual = [c for c in order if c in rec and rec[c]]
    if sorted(actual) != sorted(rdt):
        if actual:
            rec["risk_data_type"] = sorted(actual)
            _sanitize_stats["rdt_reconciled"] += 1
        else:
            _sanitize_stats["rdt_empty"] += 1


def _reorder_fields(rec):
    """Reorder record fields to match schema property order.
    Links is always last."""
    ordered = {}
    for key in SCHEMA_ORDER:
        if key in rec:
            ordered[key] = rec[key]
    for key in rec:
        if key not in ordered:
            ordered[key] = rec[key]
    rec.clear()
    rec.update(ordered)


# Load valid country codes from schema (closed codelist)
_schema_data = load_json(SCHEMA_PATH)
_VALID_COUNTRY_CODES = set(
    _schema_data.get("$defs", {}).get("Location", {})
    .get("properties", {}).get("countries", {})
    .get("items", {}).get("enum", [])
)
print(f"  Schema country codes loaded: {len(_VALID_COUNTRY_CODES)}")


def sanitize_record(rec):
    """Schema-driven sanitization of RDLS record.

    Walks each section following the schema hierarchy.
    Never fabricates data - only strips empty optionals and
    removes structurally invalid blocks."""

    # 1. Clean all optional array/object sections
    _clean_referenced_by(rec)
    _clean_sources(rec)
    _clean_resources(rec)
    _clean_spatial(rec, valid_country_codes=_VALID_COUNTRY_CODES)
    _clean_attributions(rec)

    # 2. Clean HEVL blocks (each may be removed if structurally invalid)
    _clean_hazard(rec)
    _clean_exposure(rec)
    _clean_vulnerability(rec)
    _clean_loss(rec)

    # 3. Strip empty optional root fields
    _strip_empty_optionals(rec, ["version", "purpose", "details", "license_url"])
    project = rec.get("project")
    if isinstance(project, dict):
        _strip_empty_optionals(project, ["url"])
        if not project.get("name"):
            rec.pop("project", None)

    # 4. Reconcile risk_data_type with actual blocks
    _reconcile_rdt(rec)

    # 5. Reorder fields to match schema
    _reorder_fields(rec)

    return rec


# ================================================================
# STEP 4: Rebuild revised/ and not_rdls/
# ================================================================
print(f"\n{'=' * 60}")
print("  STEP 4: REBUILD REVISED/ AND NOT_RDLS/")
print("=" * 60)

# Clean target directories
if REVISED_DIR.exists():
    shutil.rmtree(REVISED_DIR, ignore_errors=True)
REVISED_DIR.mkdir(parents=True, exist_ok=True)

if NOT_RDLS_DIR.exists():
    shutil.rmtree(NOT_RDLS_DIR, ignore_errors=True)
NOT_RDLS_DIR.mkdir(parents=True, exist_ok=True)

# Component name mapping for HEVL block removal
COMP_BLOCKS = {"hazard": "hazard", "exposure": "exposure",
               "vulnerability": "vulnerability", "loss": "loss"}

written_revised = 0
written_not_rdls = 0
renamed = 0
hevl_changed = 0
skipped = 0

for rdls_id, row in report_rows.items():
    # Find source file
    src_path = source_files.get(rdls_id)
    if not src_path:
        skipped += 1
        continue

    # Load source record
    data = load_json(src_path)
    if "datasets" in data and isinstance(data["datasets"], list):
        record = data["datasets"][0]
    else:
        record = data

    # Determine final ID
    new_id = row.get("new_id", "") or rdls_id

    # Apply HEVL changes if the record was changed
    if row.get("has_change", "") == "True":
        changes_str = row.get("changes", "")
        if changes_str:
            changes = [c.strip() for c in changes_str.split(";") if c.strip()]
            for change in changes:
                if change.startswith("REMOVE "):
                    comp = change.split(" ", 1)[1]
                    if comp in record:
                        del record[comp]
                # ADD operations would need extractors - keep existing blocks
                # The LLM only REMOVES fabricated components; ADDs are rare

            # Update risk_data_type based on which blocks remain
            final_rdt_str = row.get("final_rdt", "")
            if final_rdt_str:
                record["risk_data_type"] = sorted(final_rdt_str.split("|"))
            else:
                # Recompute from remaining blocks
                remaining = []
                for comp in ("hazard", "exposure", "vulnerability", "loss"):
                    if comp in record and record[comp]:
                        remaining.append(comp)
                record["risk_data_type"] = sorted(remaining)

            hevl_changed += 1

    # Sanitize: fix schema violations
    record = sanitize_record(record)

    # Rebuild ID if HEVL code changed (even if report didn't set new_id)
    rdt = record.get("risk_data_type", [])
    exp_code = expected_code(rdt)

    # If sanitization removed all blocks, send to not_rdls
    if not exp_code:
        not_rdls_ids.add(new_id)

    # If no resources, send to not_rdls (can't pass schema validation)
    res = record.get("resources")
    if res is None or (isinstance(res, list) and len(res) == 0):
        not_rdls_ids.add(new_id)

    if exp_code:
        m = re.match(r"rdls_([a-z]+)-", new_id)
        if m and m.group(1) != exp_code:
            new_id = rebuild_id(new_id, exp_code)

    # Update record ID
    if new_id != rdls_id:
        record["id"] = new_id
        renamed += 1

    # Wrap in datasets envelope
    out_data = {"datasets": [record]}

    # Write to appropriate directory
    if new_id in not_rdls_ids:
        write_json(NOT_RDLS_DIR / f"{new_id}.json", out_data)
        written_not_rdls += 1
    else:
        # Put in tier subdir (use "high" as default)
        tier_dir = REVISED_DIR / "high"
        tier_dir.mkdir(parents=True, exist_ok=True)
        write_json(tier_dir / f"{new_id}.json", out_data)
        written_revised += 1

    if (written_revised + written_not_rdls) % 2000 == 0:
        print(f"  [{written_revised + written_not_rdls}/{len(report_rows)}]")

print(f"\n  Written to revised/:  {written_revised}")
print(f"  Written to not_rdls/: {written_not_rdls}")
print(f"  Renamed:              {renamed}")
print(f"  HEVL changed:         {hevl_changed}")
print(f"  Skipped (no source):  {skipped}")

if _sanitize_stats:
    print(f"\n  Sanitization fixes:")
    for key, count in sorted(_sanitize_stats.items()):
        print(f"    {count:5d}  {key}")

# ================================================================
# STEP 5: Verify filename vs record.id
# ================================================================
print(f"\n{'=' * 60}")
print("  STEP 5: VERIFY FILENAME vs RECORD.ID")
print("=" * 60)

mismatch = 0
id_mismatch = 0
total_files = 0
for fp in sorted(REVISED_DIR.rglob("*.json")):
    total_files += 1
    stem = fp.stem
    m = re.match(r"rdls_([a-z]+)-", stem)
    if not m:
        continue
    file_code = m.group(1)

    with open(fp, "r", encoding="utf-8") as f:
        rec_data = json.load(f)
    rec = rec_data["datasets"][0] if "datasets" in rec_data else rec_data
    rdt = rec.get("risk_data_type", [])
    rec_id = rec.get("id", "")

    if rec_id != stem:
        id_mismatch += 1
        if id_mismatch <= 3:
            print(f"  ID mismatch: file={stem} record.id={rec_id}")

    exp = expected_code(rdt)
    if exp and file_code != exp:
        mismatch += 1
        if mismatch <= 3:
            print(f"  Type mismatch: file={stem} code={file_code} rdt={rdt} expected={exp}")

print(f"\n  Total files:           {total_files}")
print(f"  Filename/ID mismatch:  {id_mismatch}")
print(f"  Filename/RDT mismatch: {mismatch}")

# ================================================================
# STEP 6: Validate & distribute
# ================================================================
print(f"\n{'=' * 60}")
print("  STEP 6: VALIDATE & DISTRIBUTE")
print("=" * 60)

try:
    from jsonschema import Draft202012Validator
except ImportError:
    from jsonschema import Draft7Validator as Draft202012Validator

schema = load_json(SCHEMA_PATH)
validator = Draft202012Validator(schema)

json_files = sorted(REVISED_DIR.rglob("*.json"))
print(f"  Records to validate: {len(json_files)}")

# Clean previous distribution
if DIST_FINAL_DIR.exists():
    shutil.rmtree(DIST_FINAL_DIR, ignore_errors=True)

t0 = time.time()
valid_count = 0
invalid_count = 0
error_counter = Counter()

for i, fp in enumerate(json_files):
    data = load_json(fp)
    if "datasets" in data and isinstance(data["datasets"], list):
        record = data["datasets"][0]
    else:
        record = data

    errors = sorted(validator.iter_errors(record), key=lambda e: list(e.path))

    if not errors:
        valid_count += 1
        tier = "high"
    else:
        invalid_count += 1
        tier = "invalid"
        for err in errors:
            path = ".".join(str(p) for p in err.absolute_path) or "(root)"
            path = re.sub(r"\.\d+\.", ".*.", path)
            error_counter[f"{path}: {err.validator}"] += 1

    tier_dir = DIST_FINAL_DIR / tier
    tier_dir.mkdir(parents=True, exist_ok=True)
    write_json(tier_dir / fp.name, data)

    if (i + 1) % 2000 == 0:
        print(f"  [{i+1}/{len(json_files)}]")

t1 = time.time()
total = valid_count + invalid_count

print(f"\n  Validation time: {t1-t0:.1f}s")
print(f"\n  Total:   {total}")
print(f"  Valid:   {valid_count} ({100*valid_count/total:.1f}%)")
print(f"  Invalid: {invalid_count} ({100*invalid_count/total:.1f}%)")

print(f"\n  Distribution:")
for d in sorted(DIST_FINAL_DIR.iterdir()):
    count = len(list(d.glob("*.json")))
    print(f"    {d.name}/: {count}")

if error_counter:
    print(f"\n  Top 15 validation errors:")
    for err, count in error_counter.most_common(15):
        print(f"    {count:5d}  {err}")

# Save validation report
val_report = {
    "total": total,
    "valid": valid_count,
    "invalid": invalid_count,
    "valid_pct": round(100 * valid_count / total, 1),
    "top_errors": dict(error_counter.most_common(20)),
    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
}
write_json(REPORTS_DIR / "validation_report.json", val_report)

print(f"\n{'=' * 60}")
print("  DONE")
print("=" * 60)
print(f"\n  revised/:  {written_revised} RDLS records")
print(f"  not_rdls/: {written_not_rdls} non-disaster records")
print(f"  dist/:     {valid_count} valid + {invalid_count} invalid")
