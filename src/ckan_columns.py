"""CKAN column header fetcher with disk-backed caching.

Fetches actual column headers from HDX resources via the CKAN resource_show API.
Parses fs_check_info (CSV/XLSX) and shape_info (GeoJSON/SHP) to extract column
names without downloading any data files.

Usage:
    python -m src.ckan_columns \\
        --metadata-dir path/to/dataset_metadata \\
        --cache-dir output/column_cache \\
        [--max-datasets 100] [--api-key KEY] [--stats-only]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class ColumnInfo:
    """Column header information for a single resource (or sheet)."""

    resource_id: str
    resource_name: str
    format: str
    columns: List[str]                       # Column/field names
    column_types: Optional[List[str]] = None  # Data types if available
    hxl_tags: Optional[List[str]] = None     # HXL tags if is_hxlated
    sheet_name: Optional[str] = None         # For multi-sheet XLSX
    n_rows: Optional[int] = None
    n_cols: Optional[int] = None
    source: str = "fs_check_info"            # "fs_check_info" or "shape_info"


@dataclass
class FetchStats:
    """Statistics for a batch column fetch run."""

    total_datasets: int = 0
    total_resources: int = 0
    cached: int = 0
    fetched: int = 0
    with_columns: int = 0        # Resources that returned column headers
    without_columns: int = 0     # Resources without column data (API, broken)
    errors: int = 0
    skipped_formats: int = 0     # Formats we don't fetch (PDF, etc.)
    elapsed_seconds: float = 0.0


# ---------------------------------------------------------------------------
# Cache
# ---------------------------------------------------------------------------

class ColumnCache:
    """Disk-backed cache for column headers, keyed by resource ID.

    Cache files:
        {cache_dir}/{resource_id}.json   — column info (list of ColumnInfo dicts)
        {cache_dir}/{resource_id}.none   — sentinel: resource has no columns
    """

    def __init__(self, cache_dir: Path):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def _data_path(self, resource_id: str) -> Path:
        return self.cache_dir / f"{resource_id}.json"

    def _none_path(self, resource_id: str) -> Path:
        return self.cache_dir / f"{resource_id}.none"

    def has(self, resource_id: str) -> bool:
        """Check if resource is cached (either with data or as 'no columns')."""
        return self._data_path(resource_id).exists() or self._none_path(resource_id).exists()

    def get(self, resource_id: str) -> Optional[List[ColumnInfo]]:
        """Get cached column info. Returns None if not cached or cached as 'no columns'."""
        dp = self._data_path(resource_id)
        if dp.exists():
            try:
                raw = json.loads(dp.read_text(encoding="utf-8"))
                return [ColumnInfo(**item) for item in raw]
            except (json.JSONDecodeError, TypeError):
                return None
        return None

    def is_none_cached(self, resource_id: str) -> bool:
        """Check if resource is cached as 'no columns available'."""
        return self._none_path(resource_id).exists()

    def put(self, resource_id: str, infos: List[ColumnInfo]) -> None:
        """Cache column info for a resource."""
        dp = self._data_path(resource_id)
        data = [asdict(info) for info in infos]
        dp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    def put_none(self, resource_id: str) -> None:
        """Cache that a resource has no column data available."""
        self._none_path(resource_id).write_text("", encoding="utf-8")

    def count(self) -> Tuple[int, int]:
        """Return (with_columns, without_columns) counts from cache."""
        data_count = len(list(self.cache_dir.glob("*.json")))
        none_count = len(list(self.cache_dir.glob("*.none")))
        return data_count, none_count


# ---------------------------------------------------------------------------
# API interaction
# ---------------------------------------------------------------------------

# Formats worth fetching columns for (tabular or geospatial)
_FETCHABLE_FORMATS = {
    "csv", "xlsx", "xls", "tsv",
    "geojson", "shp", "shapefile", "geodatabase", "gdb",
    "gpkg", "geopackage", "kml", "kmz",
}

# Formats we skip (documents, images, etc.)
_SKIP_FORMATS = {
    "pdf", "doc", "docx", "ppt", "pptx",
    "jpg", "jpeg", "png", "gif", "tif", "tiff",
    "zip", "gz", "tar", "7z", "rar",
    "json", "xml", "html", "htm",
    "api", "web app",
}


def fetch_resource_columns(
    resource_id: str,
    base_url: str = "https://data.humdata.org/api/3/action",
    api_key: Optional[str] = None,
    timeout: float = 15.0,
) -> Optional[List[ColumnInfo]]:
    """Fetch column headers for a single resource via CKAN resource_show API.

    Returns list of ColumnInfo (one per sheet for XLSX, one for CSV/geo),
    or None if no column data is available.
    """
    url = f"{base_url}/resource_show"
    params = {"id": resource_id}
    headers = {}
    if api_key:
        headers["Authorization"] = api_key

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=timeout)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
    except requests.RequestException:
        return None

    result = resp.json().get("result", {})
    resource_name = result.get("name", "") or result.get("title", "") or ""
    fmt = (result.get("format", "") or "").upper()

    # Try fs_check_info first (CSV, XLSX, XLS, TSV)
    fs_raw = result.get("fs_check_info")
    if fs_raw:
        infos = _parse_fs_check_info(fs_raw, resource_id, resource_name, fmt)
        if infos:
            return infos

    # Try shape_info (GeoJSON, SHP, Geodatabase)
    shape_raw = result.get("shape_info")
    if shape_raw:
        infos = _parse_shape_info(shape_raw, resource_id, resource_name, fmt)
        if infos:
            return infos

    return None


def _parse_fs_check_info(
    raw: str,
    resource_id: str,
    resource_name: str,
    fmt: str,
) -> List[ColumnInfo]:
    """Parse fs_check_info JSON string into ColumnInfo list.

    fs_check_info is a JSON-encoded array of processing events.
    We find the latest entry with state='success' and hxl_proxy_response.
    """
    try:
        entries = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return []

    if not isinstance(entries, list):
        return []

    # Find latest successful entry with hxl_proxy_response
    hxl_response = None
    for entry in reversed(entries):  # Latest entries at the end
        if not isinstance(entry, dict):
            continue
        if entry.get("state") != "success":
            continue
        hxl_resp = entry.get("hxl_proxy_response")
        if hxl_resp and isinstance(hxl_resp, dict):
            hxl_response = hxl_resp
            break

    if not hxl_response:
        return []

    results: List[ColumnInfo] = []

    # Check for sheets array (used for both CSV and XLSX)
    sheets = hxl_response.get("sheets", [])
    if sheets:
        for sheet in sheets:
            if not isinstance(sheet, dict):
                continue
            headers = sheet.get("headers")
            if not headers or not isinstance(headers, list):
                continue

            sheet_name = sheet.get("name")
            # CSV uses "__DEFAULT__" as sheet name
            if sheet_name == "__DEFAULT__":
                sheet_name = None

            hxl_headers = sheet.get("hxl_headers")
            if isinstance(hxl_headers, list) and all(h is None for h in hxl_headers):
                hxl_headers = None

            results.append(ColumnInfo(
                resource_id=resource_id,
                resource_name=resource_name,
                format=fmt,
                columns=headers,
                column_types=None,  # fs_check_info doesn't provide types
                hxl_tags=hxl_headers,
                sheet_name=sheet_name,
                n_rows=sheet.get("nrows"),
                n_cols=sheet.get("ncols"),
                source="fs_check_info",
            ))
    else:
        # Fallback: headers directly on hxl_proxy_response (older format)
        headers = hxl_response.get("headers")
        if headers and isinstance(headers, list):
            hxl_headers = hxl_response.get("hxl_headers")
            if isinstance(hxl_headers, list) and all(h is None for h in hxl_headers):
                hxl_headers = None

            results.append(ColumnInfo(
                resource_id=resource_id,
                resource_name=resource_name,
                format=fmt,
                columns=headers,
                column_types=None,
                hxl_tags=hxl_headers,
                sheet_name=None,
                n_rows=hxl_response.get("nrows"),
                n_cols=hxl_response.get("ncols"),
                source="fs_check_info",
            ))

    return results


def _parse_shape_info(
    raw: Any,
    resource_id: str,
    resource_name: str,
    fmt: str,
) -> List[ColumnInfo]:
    """Parse shape_info into ColumnInfo list.

    shape_info is a JSON string containing an array of processing events.
    We find the latest successful entry with layer_fields.
    """
    try:
        entries = json.loads(raw) if isinstance(raw, str) else raw
    except (json.JSONDecodeError, TypeError):
        return []

    if not isinstance(entries, list):
        return []

    # Find latest successful entry with layer_fields
    for entry in reversed(entries):
        if not isinstance(entry, dict):
            continue
        if entry.get("state") != "success":
            continue
        layer_fields = entry.get("layer_fields")
        if not layer_fields or not isinstance(layer_fields, list):
            continue

        columns = []
        col_types = []
        for field_def in layer_fields:
            if not isinstance(field_def, dict):
                continue
            fname = field_def.get("field_name", "")
            ftype = field_def.get("data_type", "")
            # Skip internal geometry columns
            if ftype == "USER-DEFINED" or fname == "wkb_geometry":
                continue
            if fname:
                columns.append(fname)
                col_types.append(ftype)

        if columns:
            return [ColumnInfo(
                resource_id=resource_id,
                resource_name=resource_name,
                format=fmt,
                columns=columns,
                column_types=col_types,
                hxl_tags=None,
                sheet_name=None,
                n_rows=None,
                n_cols=len(columns),
                source="shape_info",
            )]

    return []


# ---------------------------------------------------------------------------
# Dataset-level enrichment
# ---------------------------------------------------------------------------

def _should_fetch_resource(resource: Dict[str, Any]) -> bool:
    """Check if a resource format is worth fetching columns for."""
    fmt = (resource.get("format", "") or "").lower().strip()
    # Direct format match
    if fmt in _FETCHABLE_FORMATS:
        return True
    # Check URL type — skip API resources
    url_type = (resource.get("url_type", "") or "").lower()
    if url_type == "api":
        return False
    # Skip known non-tabular formats
    if fmt in _SKIP_FORMATS:
        return False
    # For unknown formats, skip (conservative)
    return False


def enrich_dataset(
    hdx_meta: Dict[str, Any],
    cache: ColumnCache,
    base_url: str = "https://data.humdata.org/api/3/action",
    api_key: Optional[str] = None,
    delay: float = 0.5,
    max_resources: int = 10,
) -> Tuple[List[ColumnInfo], FetchStats]:
    """Fetch column headers for all fetchable resources in one HDX dataset.

    Returns (column_infos, stats).
    """
    stats = FetchStats()
    all_infos: List[ColumnInfo] = []

    resources = hdx_meta.get("resources", [])
    stats.total_resources = len(resources)

    fetched_count = 0
    for res in resources:
        res_id = res.get("id", "")
        if not res_id:
            continue

        # Check format
        if not _should_fetch_resource(res):
            stats.skipped_formats += 1
            continue

        # Check cache
        if cache.has(res_id):
            cached_infos = cache.get(res_id)
            if cached_infos:
                all_infos.extend(cached_infos)
                stats.with_columns += 1
            else:
                stats.without_columns += 1
            stats.cached += 1
            continue

        # Rate limit
        if fetched_count >= max_resources:
            break
        if fetched_count > 0:
            time.sleep(delay)

        # Fetch from API
        try:
            infos = fetch_resource_columns(res_id, base_url, api_key)
            if infos:
                cache.put(res_id, infos)
                all_infos.extend(infos)
                stats.with_columns += 1
            else:
                cache.put_none(res_id)
                stats.without_columns += 1
            stats.fetched += 1
            fetched_count += 1
        except Exception:
            stats.errors += 1

    return all_infos, stats


# ---------------------------------------------------------------------------
# Batch enrichment
# ---------------------------------------------------------------------------

def _scan_metadata_dir(metadata_dir: Path) -> List[Path]:
    """Scan dataset_metadata directory for JSON files."""
    return sorted(metadata_dir.glob("*.json"))


def enrich_batch(
    metadata_dir: Path,
    cache: ColumnCache,
    base_url: str = "https://data.humdata.org/api/3/action",
    api_key: Optional[str] = None,
    delay: float = 0.5,
    max_datasets: Optional[int] = None,
    max_resources_per_dataset: int = 10,
    verbose: bool = True,
) -> FetchStats:
    """Batch fetch columns for all datasets in metadata_dir.

    Returns aggregate stats.
    """
    files = _scan_metadata_dir(metadata_dir)
    if max_datasets:
        files = files[:max_datasets]

    total = len(files)
    agg_stats = FetchStats(total_datasets=total)

    if verbose:
        print(f"[ckan_columns] Scanning {total} datasets...")

    t0 = time.time()
    for idx, fpath in enumerate(files):
        # Progress
        if verbose and idx > 0 and idx % 100 == 0:
            elapsed = time.time() - t0
            rate = idx / elapsed if elapsed > 0 else 0
            eta = (total - idx) / rate if rate > 0 else 0
            print(
                f"  [{idx}/{total}] "
                f"cached={agg_stats.cached} fetched={agg_stats.fetched} "
                f"with_cols={agg_stats.with_columns} errors={agg_stats.errors} "
                f"({rate:.1f} ds/s, ETA {eta:.0f}s)"
            )

        # Load HDX metadata
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                hdx_meta = json.load(f)
        except (json.JSONDecodeError, OSError):
            agg_stats.errors += 1
            continue

        # Enrich
        _, ds_stats = enrich_dataset(
            hdx_meta, cache, base_url, api_key, delay, max_resources_per_dataset,
        )

        # Aggregate
        agg_stats.total_resources += ds_stats.total_resources
        agg_stats.cached += ds_stats.cached
        agg_stats.fetched += ds_stats.fetched
        agg_stats.with_columns += ds_stats.with_columns
        agg_stats.without_columns += ds_stats.without_columns
        agg_stats.errors += ds_stats.errors
        agg_stats.skipped_formats += ds_stats.skipped_formats

    agg_stats.elapsed_seconds = time.time() - t0
    return agg_stats


def print_stats(stats: FetchStats, cache: ColumnCache) -> None:
    """Print a summary of fetch statistics."""
    cache_data, cache_none = cache.count()
    total_cached = cache_data + cache_none

    print(f"\n{'='*60}")
    print(f"  COLUMN ENRICHMENT SUMMARY")
    print(f"{'='*60}")
    print(f"  Datasets scanned:  {stats.total_datasets}")
    print(f"  Total resources:   {stats.total_resources}")
    print(f"  Skipped (format):  {stats.skipped_formats}")
    print(f"  Cache hits:        {stats.cached}")
    print(f"  API fetched:       {stats.fetched}")
    print(f"  With columns:      {stats.with_columns}")
    print(f"  Without columns:   {stats.without_columns}")
    print(f"  Errors:            {stats.errors}")
    print(f"  Time:              {stats.elapsed_seconds:.1f}s")
    print(f"")
    print(f"  Cache total:       {total_cached} resources")
    print(f"    with columns:    {cache_data}")
    print(f"    no columns:      {cache_none}")
    if stats.with_columns + stats.without_columns > 0:
        pct = stats.with_columns / (stats.with_columns + stats.without_columns) * 100
        print(f"  Column coverage:   {pct:.1f}%")
    print(f"{'='*60}")


# ---------------------------------------------------------------------------
# Utility: load columns for a specific HDX UUID
# ---------------------------------------------------------------------------

def load_columns_for_uuid(
    hdx_uuid: str,
    hdx_meta: Dict[str, Any],
    cache: ColumnCache,
) -> List[ColumnInfo]:
    """Load cached column info for all resources in a dataset.

    Does NOT make API calls — only reads from cache.
    Use enrich_dataset() or enrich_batch() to populate the cache first.
    """
    all_infos: List[ColumnInfo] = []
    for res in hdx_meta.get("resources", []):
        res_id = res.get("id", "")
        if not res_id:
            continue
        infos = cache.get(res_id)
        if infos:
            all_infos.extend(infos)
    return all_infos


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Fetch column headers from HDX CKAN API and cache them.",
    )
    parser.add_argument(
        "--metadata-dir", required=True,
        help="Path to HDX dataset_metadata directory",
    )
    parser.add_argument(
        "--cache-dir", default="output/column_cache",
        help="Path to column cache directory (default: output/column_cache)",
    )
    parser.add_argument(
        "--max-datasets", type=int, default=None,
        help="Max datasets to process (for testing)",
    )
    parser.add_argument(
        "--api-key", default=None,
        help="HDX CKAN API key for higher rate limits",
    )
    parser.add_argument(
        "--delay", type=float, default=None,
        help="Delay between API calls in seconds (default: 0.5 without key, 0.1 with key)",
    )
    parser.add_argument(
        "--stats-only", action="store_true",
        help="Only print cache stats, don't fetch anything",
    )
    parser.add_argument(
        "--max-resources", type=int, default=10,
        help="Max resources to fetch per dataset (default: 10)",
    )

    args = parser.parse_args()

    metadata_dir = Path(args.metadata_dir)
    if not metadata_dir.is_dir():
        print(f"Error: metadata directory not found: {metadata_dir}")
        sys.exit(1)

    cache = ColumnCache(Path(args.cache_dir))

    # Resolve API key from arg or env
    api_key = args.api_key or os.environ.get("HDX_API_KEY")

    # Resolve delay
    if args.delay is not None:
        delay = args.delay
    elif api_key:
        delay = 0.1   # With API key: 300 req/min
    else:
        delay = 0.5   # Without key: 60 req/min

    if args.stats_only:
        # Just print cache stats
        files = _scan_metadata_dir(metadata_dir)
        stats = FetchStats(total_datasets=len(files))
        # Count total resources
        for fpath in files:
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    meta = json.load(f)
                stats.total_resources += len(meta.get("resources", []))
            except Exception:
                pass
        print_stats(stats, cache)
        return

    print(f"[ckan_columns] Metadata dir: {metadata_dir}")
    print(f"[ckan_columns] Cache dir:    {args.cache_dir}")
    print(f"[ckan_columns] API key:      {'yes' if api_key else 'no (public API)'}")
    print(f"[ckan_columns] Delay:        {delay}s between API calls")
    if args.max_datasets:
        print(f"[ckan_columns] Max datasets: {args.max_datasets}")
    print()

    stats = enrich_batch(
        metadata_dir=metadata_dir,
        cache=cache,
        api_key=api_key,
        delay=delay,
        max_datasets=args.max_datasets,
        max_resources_per_dataset=args.max_resources,
    )

    print_stats(stats, cache)


if __name__ == "__main__":
    main()
