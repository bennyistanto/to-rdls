"""
MCP Server for the to-rdls toolkit.

Exposes geospatial data review and RDLS metadata tools to Claude Code.
Runs in the to-rdls conda environment (GDAL, rasterio, fiona, geopandas, etc.).

Start command:
    conda run --no-banner -n to-rdls python mcp_server.py

Or via Claude Code MCP config:
    "command": "C:\\Users\\benny\\miniforge3\\Scripts\\conda.exe",
    "args": ["run", "--no-banner", "-n", "to-rdls", "python", "<path>/mcp_server.py"]
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import Optional

# Ensure src/ is importable
sys.path.insert(0, str(Path(__file__).parent))

from mcp.server.fastmcp import FastMCP

# Redirect logging to stderr (stdout is reserved for MCP protocol)
logging.basicConfig(stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger("to-rdls-server")

mcp = FastMCP("to-rdls")


@mcp.tool()
def inventory_folder(
    path: str,
    formats: str = "json",
    include_hash: bool = False,
    inspect_zips: bool = True,
) -> str:
    """Inventory a data delivery folder or ZIP file.

    Scans the target path and lists all files with sizes, formats, MIME types,
    and modification dates. ZIP contents are listed without extraction.
    Returns a JSON object with stats and a markdown summary.

    Args:
        path: Folder or ZIP file path to inventory
        formats: Output formats to write (json, md, csv) - only json returned to caller
        include_hash: Compute SHA256 checksums for each file (slower)
        inspect_zips: Peek inside ZIP files without extracting
    """
    from src.inventory import inventory_folder as _inventory_folder

    md, rows, stats = _inventory_folder(
        target=path,
        formats=formats,
        include_hash=include_hash,
        inspect_zips=inspect_zips,
        verbose=False,
    )
    return json.dumps(
        {"stats": stats, "row_count": len(rows), "markdown": md},
        indent=2,
        default=str,
    )


@mcp.tool()
def review_folder(
    path: str,
    max_inspect: int = 30,
) -> str:
    """Full automated data review for RDLS metadata creation.

    Inspects files (including inside ZIPs), classifies into HEVL components
    (Hazard, Exposure, Vulnerability, Loss), analyzes gaps against RDLS schema,
    extracts naming patterns (scenarios, return periods), and suggests dataset structure.
    Writes review JSON + markdown to a _rdls_review/ subfolder.

    Args:
        path: Folder to review
        max_inspect: Maximum number of files to individually inspect (default 30)
    """
    from src.review import (
        review_folder as _review_folder,
        render_review_markdown,
    )
    from dataclasses import asdict

    result = _review_folder(target=path, max_inspect=max_inspect, verbose=False)
    md = render_review_markdown(result)

    # Serialize to JSON-safe dict
    result_dict = {
        "target": result.target,
        "generated_utc": result.generated_utc,
        "stats": result.stats,
        "group_count": len(result.file_groups),
        "inspection_count": len(result.inspections),
        "dataset_count": len(result.suggested_datasets),
        "quality_issues": result.quality_issues,
        "project_metadata": result.project_metadata,
    }
    return json.dumps(
        {"markdown": md, "structured": result_dict},
        indent=2,
        default=str,
    )


@mcp.tool()
def inspect_file(
    path: str,
) -> str:
    """Inspect a single file's metadata and content.

    Works with GeoTIFF, Shapefile, GeoJSON, GeoPackage, XLSX, CSV, JSON,
    PDF, DOCX, NetCDF, and text files. For files inside ZIP archives,
    use the 'archive.zip::inner/path/file.ext' path format.

    Args:
        path: File path (or ZIP::member path) to inspect
    """
    from src.review import inspect_file as _inspect_file
    from src.zipaccess import resolve_and_open

    if "::" in path:
        ctx = resolve_and_open(path, Path("."))
        with ctx as temp_path:
            result = _inspect_file(temp_path)
            if result:
                result.path = path
    else:
        result = _inspect_file(Path(path))

    if result is None:
        return json.dumps({"error": f"Unsupported format: {Path(path).suffix}"})

    return json.dumps(
        {
            "path": result.path,
            "format": result.format,
            "inspection": result.inspection,
        },
        indent=2,
        default=str,
    )


@mcp.tool()
def validate_record(
    record_path: str,
) -> str:
    """Validate an RDLS JSON record against the v0.3 schema.

    Reads a JSON file containing one or more RDLS dataset records and
    validates each against the RDLS v0.3 JSON Schema. Returns validation
    status and any errors found.

    Args:
        record_path: Path to a JSON file containing RDLS dataset record(s)
    """
    from src.schema import validate_record as _validate_record, load_rdls_schema
    from src.utils import load_json

    schema_path = Path(__file__).parent / "schema" / "rdls_schema_v0.3.json"
    if not schema_path.exists():
        return json.dumps({"error": f"Schema not found: {schema_path}"})

    schema = load_rdls_schema(schema_path)
    record = load_json(record_path)

    # Handle both single record and wrapped {"datasets": [...]}
    if "datasets" in record and isinstance(record["datasets"], list):
        results = []
        for i, ds in enumerate(record["datasets"]):
            is_valid, errors = _validate_record(ds, schema)
            results.append({
                "index": i,
                "id": ds.get("id", "?"),
                "valid": is_valid,
                "errors": errors[:20],
            })
        return json.dumps(
            {"record_count": len(results), "results": results},
            indent=2,
        )
    else:
        is_valid, errors = _validate_record(record, schema)
        return json.dumps(
            {"valid": is_valid, "errors": errors[:20]},
            indent=2,
        )


@mcp.tool()
def inspect_folder_for_llm(
    path: str,
    max_inspect: int = 30,
) -> str:
    """Inspect a data folder and return structured metadata for LLM classification.

    Unlike review_folder (which does automated HEVL classification), this tool
    returns RAW inspection data WITHOUT classification. Use this when you want
    Claude to do the semantic classification using its domain knowledge about
    RDLS, geospatial standards, and risk data.

    The LLM should use the returned data to:
    1. Classify files into RDLS HEVL components (Hazard, Exposure, Vulnerability, Loss)
    2. Identify hazard types, exposure categories, process types
    3. Suggest dataset structure and naming
    4. Draft metadata descriptions

    Returns structured JSON with:
    - folder_summary: file counts, formats, intermediate exclusions
    - file_groups: grouped files with sample filenames and naming patterns
    - file_inspections: detailed metadata per inspected file (CRS, bounds, columns, stats)
    - readme_extractions: project metadata from README/text files
    - rdls_context: RDLS schema requirements for reference

    Args:
        path: Folder or ZIP file path to inspect
        max_inspect: Maximum files to individually inspect (default 30)
    """
    from src.review import (
        _inspect_pipeline,
        analyze_naming_patterns,
        extract_readme_metadata,
        _get_config,
    )
    from src.inventory import human_size, iso_time
    import time as _time

    target = Path(path).resolve()
    cfg = _get_config()

    # Steps 1-3: shared pipeline
    pipe = _inspect_pipeline(target, max_inspect=max_inspect, verbose=False)

    # Naming analysis per group (NO classification)
    for group in pipe.groups:
        naming = analyze_naming_patterns(group.files)
        group._naming = naming  # type: ignore[attr-defined]

    # README extraction
    proj_meta = extract_readme_metadata(pipe.inspections)

    # Format distribution
    format_dist: dict = {}
    for row in pipe.rows:
        ext = row.get("ext", "").lower().lstrip(".")
        if ext:
            format_dist[ext] = format_dist.get(ext, 0) + 1

    # Build LLM-optimised output
    file_groups = []
    for g in pipe.groups:
        naming = getattr(g, "_naming", {})
        sample = g.files[:5] if len(g.files) <= 5 else (
            g.files[:2] + [f"... ({len(g.files) - 4} more) ..."] + g.files[-2:]
        )
        fg = {
            "name": g.name,
            "file_count": len(g.files),
            "formats": g.formats,
            "total_size_human": human_size(g.total_size_bytes),
            "sample_filenames": [Path(f.split("::")[-1] if "::" in f else f).name for f in sample],
        }
        # Add naming patterns if any matched
        if naming.get("matched_count", 0) > 0:
            fg["filename_patterns"] = {
                k: v for k, v in naming.items()
                if k not in ("matched_count", "total_count") and v
            }
            fg["filename_patterns"]["matched_files"] = f"{naming['matched_count']}/{naming['total_count']}"
        file_groups.append(fg)

    # File inspections (serialised for LLM consumption)
    file_inspections = []
    for insp in pipe.inspections:
        fi = {
            "path": insp.path,
            "format": insp.format,
            "metadata": insp.inspection,
        }
        file_inspections.append(fi)

    # RDLS context from config
    rdls = cfg.get("rdls_schema", {})

    result = {
        "target": str(target),
        "generated_utc": iso_time(_time.time()),
        "folder_summary": {
            "total_files": pipe.stats.get("files", 0),
            "total_size_human": pipe.stats.get("total_human", "?"),
            "format_distribution": format_dist,
            "intermediate_files_excluded": pipe.intermediate_summary.get("total_excluded", 0),
            "exclusion_reasons": [
                f"{info['reasons'][0]} ({info['excluded_count']} files)"
                for info in pipe.intermediate_summary.get("groups", {}).values()
                if info.get("reasons")
            ][:10],
        },
        "file_groups": file_groups,
        "file_inspections": file_inspections,
        "readme_extractions": proj_meta,
        "rdls_context": {
            "required_fields": rdls.get("required", []),
            "recommended_fields": rdls.get("recommended", []),
            "valid_risk_data_types": ["hazard", "exposure", "vulnerability", "loss"],
            "valid_hazard_types": [
                "flood", "earthquake", "tsunami", "landslide", "volcanic",
                "strong_wind", "drought", "wildfire", "extreme_temperature",
                "coastal_flood", "convective_storm",
            ],
            "valid_exposure_categories": [
                "buildings", "population", "infrastructure", "agriculture",
                "natural_environment", "economic_indicator", "development_index",
            ],
        },
    }

    return json.dumps(result, indent=2, default=str)


if __name__ == "__main__":
    logger.info("Starting to-rdls MCP server (stdio transport)")
    mcp.run(transport="stdio")
