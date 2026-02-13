"""
HDX (Humanitarian Data Exchange) source adapter.

Provides HDX-specific crawling, normalization, field extraction,
and OSM policy detection. Uses CKAN API.
"""

import random
import time
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from ..utils import load_yaml, norm_str


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class HDXCrawlerConfig:
    """Configuration for HDX metadata crawler."""
    base_url: str = "https://data.humdata.org"
    rows_per_page: int = 500
    requests_per_second: float = 2.0
    max_retries: int = 6
    timeout: int = 60
    max_datasets: Optional[int] = None
    add_slug_to_filename: bool = True
    slug_max_length: int = 80

    @property
    def ckan_api_url(self) -> str:
        return f"{self.base_url}/api/3/action"

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "HDXCrawlerConfig":
        """Load config from HDX source YAML file."""
        cfg = load_yaml(yaml_path)
        api = cfg.get("api", {})
        crawler = cfg.get("crawler", {})
        return cls(
            base_url=api.get("base_url", cls.base_url),
            rows_per_page=api.get("rows_per_page", cls.rows_per_page),
            requests_per_second=api.get("rate_limit", cls.requests_per_second),
            max_retries=api.get("max_retries", cls.max_retries),
            timeout=api.get("timeout", cls.timeout),
            add_slug_to_filename=crawler.get("add_slug_to_filename", cls.add_slug_to_filename),
            slug_max_length=crawler.get("slug_max_length", cls.slug_max_length),
        )


# ---------------------------------------------------------------------------
# HTTP Client
# ---------------------------------------------------------------------------

class HDXClient:
    """HTTP client for HDX API with rate limiting, retries, bot-check detection."""

    def __init__(self, config: HDXCrawlerConfig):
        self.config = config
        self._last_request_time = 0.0
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "hdx-metadata-crawler/1.0 (RDLS pipeline)",
            "Accept": "application/json,text/plain,*/*",
        })

    def _looks_like_bot_check(self, text: str) -> bool:
        t = text.lower()
        return ("verify that you're not a robot" in t) or ("javascript is disabled" in t)

    def _rate_limit(self) -> None:
        if self.config.requests_per_second <= 0:
            return
        min_interval = 1.0 / self.config.requests_per_second
        elapsed = time.time() - self._last_request_time
        if elapsed < min_interval:
            time.sleep(min_interval - elapsed)
        self._last_request_time = time.time()

    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """GET JSON with retries and error handling."""
        for attempt in range(self.config.max_retries):
            self._rate_limit()
            try:
                response = self.session.get(url, params=params, timeout=self.config.timeout)
            except requests.RequestException:
                wait = min(60, (2 ** attempt) + random.random())
                time.sleep(wait)
                continue

            if response.status_code in (429, 500, 502, 503, 504):
                retry_after = response.headers.get("Retry-After")
                if retry_after and retry_after.isdigit():
                    time.sleep(int(retry_after))
                else:
                    time.sleep(min(60, (2 ** attempt) + random.random()))
                continue

            if response.status_code >= 400:
                raise RuntimeError(f"HTTP {response.status_code} for {response.url}")

            content_type = (response.headers.get("Content-Type") or "").lower()
            if "json" not in content_type:
                if self._looks_like_bot_check(response.text[:5000]):
                    raise RuntimeError(f"Bot-check page returned for {response.url}")

            try:
                return response.json()
            except Exception:
                raise RuntimeError(f"Non-JSON response for {response.url}: {response.text[:200]}")

        raise RuntimeError(f"Failed after {self.config.max_retries} retries: {url}")

    def ckan_action(self, action: str, **params: Any) -> Dict[str, Any]:
        """Call CKAN Action API."""
        url = f"{self.config.ckan_api_url}/{action}"
        response = self.get_json(url, params=params)
        if not response.get("success", False):
            raise RuntimeError(
                f"CKAN action failed: {action} params={params} error={response.get('error')}"
            )
        return response["result"]


# ---------------------------------------------------------------------------
# Dataset iteration & download
# ---------------------------------------------------------------------------

def iter_datasets(client: HDXClient, config: HDXCrawlerConfig,
                  query: str = "*:*") -> Iterable[Dict[str, Any]]:
    """Iterate over all HDX datasets using CKAN package_search pagination."""
    start = 0
    yielded = 0
    while True:
        result = client.ckan_action(
            "package_search",
            q=query,
            rows=config.rows_per_page,
            start=start,
            sort="id asc",
            facet="false",
        )
        count = result.get("count", 0)
        datasets = result.get("results", [])
        if not datasets:
            break
        for ds in datasets:
            yield ds
            yielded += 1
            if config.max_datasets is not None and yielded >= config.max_datasets:
                return
        start += config.rows_per_page
        if start >= count:
            break


def download_dataset_metadata(client: HDXClient, config: HDXCrawlerConfig,
                              dataset_id: str) -> Tuple[Dict[str, Any], str]:
    """Download dataset metadata with CKAN fallback."""
    export_url = f"{config.base_url}/dataset/{dataset_id}/download_metadata"
    try:
        meta = client.get_json(export_url, params={"format": "json"})
        return meta, "download_metadata"
    except Exception as e:
        pkg = client.ckan_action("package_show", id=dataset_id)
        return {
            "_fallback_reason": str(e),
            "_note": "Fallback used: CKAN package_show.",
            "dataset": pkg,
        }, "ckan_package_show_fallback"


# ---------------------------------------------------------------------------
# Record normalization & field extraction
# ---------------------------------------------------------------------------

def normalize_dataset_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize HDX record shape (handles CKAN export wrapper)."""
    if isinstance(raw, dict) and "id" in raw:
        return raw
    if isinstance(raw, dict) and "dataset" in raw and isinstance(raw["dataset"], dict):
        return raw["dataset"]
    return raw


def get_org_title(ds: Dict[str, Any]) -> str:
    """Extract organization title/name."""
    org = ds.get("organization")
    if isinstance(org, dict):
        return (org.get("title") or org.get("name") or "").strip()
    return (org or "").strip()


def get_tags(ds: Dict[str, Any]) -> List[str]:
    """Extract tags as lowercase strings."""
    tags = ds.get("tags") or []
    out: List[str] = []
    if isinstance(tags, list):
        for t in tags:
            if isinstance(t, dict):
                name = t.get("name") or ""
                if name:
                    out.append(name.strip().lower())
            elif isinstance(t, str):
                out.append(t.strip().lower())
    return out


def get_resources(ds: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Extract resources list."""
    res = ds.get("resources") or []
    return res if isinstance(res, list) else []


def get_license_title(ds: Dict[str, Any]) -> str:
    """Extract normalized license string."""
    lt = ds.get("license_title") or ds.get("license_id") or ""
    return (lt or "").strip()


def extract_hdx_fields(ds: Dict[str, Any]) -> Dict[str, Any]:
    """Extract a common field structure from HDX metadata.

    Returns a dict with standardized keys usable by the rest of the pipeline.
    """
    return {
        "id": ds.get("id", ""),
        "name": ds.get("name", ""),
        "title": ds.get("title", ""),
        "notes": ds.get("notes", ""),
        "methodology": ds.get("methodology", ""),
        "organization": get_org_title(ds),
        "org_name": ds.get("organization", {}).get("name", "") if isinstance(ds.get("organization"), dict) else "",
        "org_description": ds.get("organization", {}).get("description", "") if isinstance(ds.get("organization"), dict) else "",
        "license_title": get_license_title(ds),
        "license_url": ds.get("license_url", ""),
        "groups": [
            (g.get("title") or g.get("name", "")) if isinstance(g, dict) else str(g)
            for g in (ds.get("groups") or [])
        ],
        "tags": get_tags(ds),
        "resources": get_resources(ds),
        "dataset_date": ds.get("dataset_date", ""),
        "dataset_source": ds.get("dataset_source", ""),
        "maintainer": ds.get("maintainer", ""),
        "url": ds.get("url", ""),
    }


# ---------------------------------------------------------------------------
# OSM Policy Detection
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class OSMDetectionResult:
    """Result of OSM detection for a dataset."""
    is_osm: bool
    reasons: Tuple[str, ...]
    signals: Dict[str, Any]


# Default markers (can be overridden from HDX source config YAML)
DEFAULT_OSM_MARKERS = {
    "fast_markers": (
        "openstreetmap contributors",
        '"dataset_source":',
        '"license_title": "odbl"',
        '"license_title":"odbl"',
        "open database license",
        "hotosm",
        "export.hotosm.org",
        "openstreetmap.org",
        "openstreetmap",
    ),
    "url_markers": (
        "openstreetmap.org",
        "hotosm.org",
        "export.hotosm.org",
        "exports-stage.hotosm.org",
        "production-raw-data-api",
    ),
    "org_markers": (
        "humanitarian openstreetmap",
        "hotosm",
        "openstreetmap",
    ),
    "title_markers": (
        "openstreetmap export",
        "(openstreetmap export)",
        "openstreetmap",
    ),
    "notes_markers": (
        "openstreetmap",
        "wiki.openstreetmap.org",
        "osm",
    ),
}


def load_osm_markers(hdx_config: Dict[str, Any]) -> Dict[str, tuple]:
    """Load OSM markers from HDX source config."""
    osm_cfg = hdx_config.get("osm_detection", {})
    markers = {}
    for key in DEFAULT_OSM_MARKERS:
        values = osm_cfg.get(key, DEFAULT_OSM_MARKERS[key])
        markers[key] = tuple(values) if isinstance(values, list) else values
    return markers


def prefilter_maybe_osm(text: str, fast_markers: tuple = None) -> bool:
    """Quick text scan for OSM indicators."""
    if fast_markers is None:
        fast_markers = DEFAULT_OSM_MARKERS["fast_markers"]
    t = text.lower()
    return any(m in t for m in fast_markers)


def detect_osm(
    ds: Dict[str, Any],
    markers: Optional[Dict[str, tuple]] = None,
    threshold: int = 2,
) -> OSMDetectionResult:
    """Detect whether a dataset is derived from OpenStreetMap.

    Uses multiple signals with policy-based scoring.

    Args:
        ds: Dataset metadata dict.
        markers: OSM detection markers dict. Defaults to built-in markers.
        threshold: Minimum supporting evidence signals for borderline cases.
    """
    if markers is None:
        markers = DEFAULT_OSM_MARKERS

    url_markers = markers.get("url_markers", ())
    org_markers = markers.get("org_markers", ())
    title_markers = markers.get("title_markers", ())
    notes_markers = markers.get("notes_markers", ())

    title_l = norm_str(ds.get("title", ""))
    notes_l = norm_str(ds.get("notes", ""))
    dataset_source_l = norm_str(ds.get("dataset_source", ""))
    org_l = norm_str(get_org_title(ds))
    license_l = norm_str(get_license_title(ds))
    tags = get_tags(ds)
    resources = get_resources(ds)

    reasons: List[str] = []

    # Rule 1: dataset_source references OpenStreetMap
    if "openstreetmap" in dataset_source_l:
        reasons.append("dataset_source_mentions_openstreetmap")

    # Rule 2: ODbL license with OSM cues
    if "odbl" in license_l or "open database license" in license_l:
        if any("openstreetmap" in x for x in [title_l, notes_l, dataset_source_l]):
            reasons.append("odbl_license_plus_osm_cue")

    # Rule 3: Resource URLs point to HOT/OSM
    for r in resources:
        url = norm_str(r.get("download_url") or r.get("url") or "")
        if url and any(m in url for m in url_markers):
            reasons.append("resource_url_osm_domain")
            break

    # Rule 4: Organization mentions OSM/HOT
    if any(m in org_l for m in org_markers):
        reasons.append("organization_mentions_osm_or_hot")

    # Rule 5: Title mentions OSM
    if any(m in title_l for m in title_markers):
        reasons.append("title_mentions_osm_export")

    # Rule 6: Tags
    if "openstreetmap" in tags:
        reasons.append("tag_openstreetmap_present")

    # Rule 7: Notes
    supporting = sum(1 for m in notes_markers if m in notes_l)
    if supporting >= threshold:
        reasons.append("notes_multiple_osm_references")

    is_osm = len(reasons) > 0

    return OSMDetectionResult(
        is_osm=is_osm,
        reasons=tuple(reasons),
        signals={
            "title": title_l[:100],
            "org": org_l[:100],
            "license": license_l,
            "dataset_source": dataset_source_l[:100],
        },
    )
