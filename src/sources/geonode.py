"""
GeoNode source adapter.

Provides multi-portal GeoNode crawling via REST API v2, normalization,
and field extraction.  Returns the same common dict as hdx.extract_hdx_fields()
so the downstream pipeline (classify -> translate -> extract -> integrate ->
validate) works identically.
"""

import random
import re
import time
import urllib3
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Tuple

import requests

from ..utils import load_yaml

# Suppress warnings for portals with self-signed/invalid SSL certificates
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

@dataclass
class GeoNodePortalConfig:
    """Configuration for a single GeoNode portal instance.

    Filter fields use None to mean "use global defaults" and [] to mean
    "no filters (crawl everything)".
    """
    name: str = ""
    base_url: str = ""
    enabled: bool = True
    rate_limit: float = 1.0
    max_retries: int = 3
    timeout: int = 60
    rows_per_page: int = 100
    verify_ssl: bool = True
    keyword_filters: Optional[List[str]] = None
    category_filters: Optional[List[str]] = None


@dataclass
class GeoNodeConfig:
    """Top-level configuration for multi-portal GeoNode crawling."""
    portals: List[GeoNodePortalConfig] = field(default_factory=list)
    rows_per_page: int = 100
    timeout: int = 60
    max_retries: int = 3
    rate_limit: float = 1.0
    max_datasets_per_portal: Optional[int] = None
    # Loaded from YAML for use by extract_geonode_fields
    category_tag_map: Dict[str, str] = field(default_factory=dict)
    skip_link_types: List[str] = field(default_factory=list)
    link_modality_map: Dict[str, str] = field(default_factory=dict)
    mime_format_map: Dict[str, str] = field(default_factory=dict)
    hevl_keywords: List[str] = field(default_factory=list)
    hevl_categories: List[str] = field(default_factory=list)
    title_humanize_config: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "GeoNodeConfig":
        """Load config from GeoNode source YAML file."""
        cfg = load_yaml(yaml_path)
        api = cfg.get("api", {})

        # Top-level defaults
        defaults = {
            "rows_per_page": api.get("rows_per_page", 100),
            "timeout": api.get("timeout", 60),
            "max_retries": api.get("max_retries", 3),
            "rate_limit": api.get("rate_limit", 1.0),
        }
        max_ds = api.get("max_datasets_per_portal")

        # Build portal configs, inheriting defaults
        portals = []
        for p in (cfg.get("portals") or []):
            if not isinstance(p, dict):
                continue
            # keyword_filters / category_filters:
            #   absent or null in YAML -> None (use global defaults)
            #   empty list []          -> [] (no filters, crawl all)
            #   non-empty list         -> use those filters
            kw_raw = p.get("keyword_filters")
            cat_raw = p.get("category_filters")
            portals.append(GeoNodePortalConfig(
                name=p.get("name", ""),
                base_url=p.get("base_url", "").rstrip("/"),
                enabled=p.get("enabled", True),
                rate_limit=p.get("rate_limit", defaults["rate_limit"]),
                max_retries=p.get("max_retries", defaults["max_retries"]),
                timeout=p.get("timeout", defaults["timeout"]),
                rows_per_page=p.get("rows_per_page", defaults["rows_per_page"]),
                verify_ssl=p.get("verify_ssl", True),
                keyword_filters=kw_raw if isinstance(kw_raw, list) else None,
                category_filters=cat_raw if isinstance(cat_raw, list) else None,
            ))

        return cls(
            portals=portals,
            max_datasets_per_portal=max_ds,
            category_tag_map=cfg.get("category_tag_map", {}),
            skip_link_types=cfg.get("skip_link_types", []),
            link_modality_map=cfg.get("link_modality_map", {}),
            mime_format_map=cfg.get("mime_format_map", {}),
            hevl_keywords=cfg.get("hevl_keywords", []),
            hevl_categories=cfg.get("hevl_categories", []),
            title_humanize_config=cfg.get("title_humanize", {}),
            **defaults,
        )


# ---------------------------------------------------------------------------
# HTTP Client
# ---------------------------------------------------------------------------

class GeoNodeClient:
    """HTTP client for GeoNode REST API v2 with rate limiting and retries."""

    def __init__(self, portal: GeoNodePortalConfig):
        self.portal = portal
        self._last_request_time = 0.0
        self.session = requests.Session()
        self.session.verify = portal.verify_ssl
        self.session.headers.update({
            "User-Agent": "geonode-metadata-crawler/1.0 (RDLS pipeline)",
            "Accept": "application/json",
        })
        self._api_available: Optional[bool] = None

    def _rate_limit(self) -> None:
        """Enforce minimum interval between requests."""
        if self.portal.rate_limit <= 0:
            return
        elapsed = time.time() - self._last_request_time
        if elapsed < self.portal.rate_limit:
            time.sleep(self.portal.rate_limit - elapsed)
        self._last_request_time = time.time()

    def get_json(self, url: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """GET JSON with retries and error handling."""
        for attempt in range(self.portal.max_retries):
            self._rate_limit()
            try:
                response = self.session.get(
                    url, params=params, timeout=self.portal.timeout
                )
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
                raise RuntimeError(
                    f"HTTP {response.status_code} for {response.url}"
                )

            try:
                return response.json()
            except Exception:
                raise RuntimeError(
                    f"Non-JSON response for {response.url}: "
                    f"{response.text[:200]}"
                )

        raise RuntimeError(
            f"Failed after {self.portal.max_retries} retries: {url}"
        )

    def api_url(self, endpoint: str) -> str:
        """Build full API v2 URL."""
        base = self.portal.base_url.rstrip("/")
        endpoint = endpoint.lstrip("/")
        return f"{base}/api/v2/{endpoint}"

    def probe_api(self) -> bool:
        """Check if GeoNode API v2 is available.

        Returns True if the endpoint responds, False otherwise.
        Caches result for the lifetime of this client instance.
        """
        if self._api_available is not None:
            return self._api_available
        try:
            self._rate_limit()
            resp = self.session.get(
                self.api_url(""),
                timeout=min(self.portal.timeout, 15),
            )
            self._api_available = resp.status_code < 400
        except Exception:
            self._api_available = False
        return self._api_available


# ---------------------------------------------------------------------------
# Dataset iteration
# ---------------------------------------------------------------------------

def iter_datasets(
    client: GeoNodeClient,
    portal: GeoNodePortalConfig,
    max_datasets: Optional[int] = None,
    hevl_keywords: Optional[List[str]] = None,
    hevl_categories: Optional[List[str]] = None,
) -> Iterable[Dict[str, Any]]:
    """Paginate through GeoNode datasets API v2.

    Uses PageNumberPagination: ?page=N&page_size=M.
    Applies keyword/category filters from portal config or fallback defaults.
    """
    page = 1
    yielded = 0
    params: Dict[str, Any] = {"page_size": portal.rows_per_page}

    # Keyword filters: None = use global defaults, [] = no filters (crawl all)
    if portal.keyword_filters is None:
        kw = hevl_keywords or []
    else:
        kw = portal.keyword_filters
    if kw:
        params["filter{keywords.slug.in}"] = ",".join(kw)

    if portal.category_filters is None:
        cat = hevl_categories or []
    else:
        cat = portal.category_filters
    if cat:
        params["filter{category.identifier.in}"] = ",".join(cat)

    while True:
        params["page"] = page
        try:
            response = client.get_json(client.api_url("datasets"), params=params)
        except RuntimeError as e:
            # Some GeoNode versions return 404 on empty pages
            if "404" in str(e):
                break
            raise

        # Handle different response shapes across GeoNode versions
        datasets = response.get("datasets", [])
        if not datasets:
            datasets = response.get("results", [])
        if not datasets:
            # Might be a flat list at root level
            if isinstance(response, list):
                datasets = response
            else:
                break

        for ds in datasets:
            yield ds
            yielded += 1
            if max_datasets and yielded >= max_datasets:
                return

        # Check for next page
        links = response.get("links", {})
        next_url = None
        if isinstance(links, dict):
            next_url = links.get("next")
        if not next_url:
            next_url = response.get("next")
        if not next_url:
            # Check if we got a full page (more data likely)
            total = response.get("total") or response.get("count")
            if total is not None and yielded >= total:
                break
            if len(datasets) < portal.rows_per_page:
                break
        page += 1


def iter_all_portals(
    config: GeoNodeConfig,
) -> Iterable[Tuple[str, Dict[str, Any]]]:
    """Iterate datasets across all enabled portals.

    Yields (portal_name, raw_dataset_dict) tuples.
    """
    for portal in config.portals:
        if not portal.enabled:
            continue
        client = GeoNodeClient(portal)
        if not client.probe_api():
            print(f"WARNING: {portal.name} ({portal.base_url}) "
                  f"API v2 not available, skipping")
            continue
        print(f"Crawling portal: {portal.name} ({portal.base_url})")
        count = 0
        for ds in iter_datasets(
            client, portal, config.max_datasets_per_portal,
            hevl_keywords=config.hevl_keywords,
            hevl_categories=config.hevl_categories,
        ):
            yield portal.name, ds
            count += 1
        print(f"  -> {count} datasets from {portal.name}")


# ---------------------------------------------------------------------------
# Record normalization
# ---------------------------------------------------------------------------

def normalize_geonode_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize GeoNode API response shape.

    Handles GeoNode 4.x wrapping ({"dataset": {...}}) and pass-through
    for standard v2 responses.
    """
    if "dataset" in raw and isinstance(raw["dataset"], dict):
        return raw["dataset"]
    return raw


# ---------------------------------------------------------------------------
# Field extraction
# ---------------------------------------------------------------------------

def _build_ogc_url(
    base_url: str,
    service: str,
    layer_name: str,
) -> str:
    """Build a layer-specific OGC service URL.

    Args:
        base_url: GeoServer OWS endpoint (e.g. https://portal/geoserver/ows).
        service: OGC service type (WFS, WMS, WCS).
        layer_name: GeoNode layer name (e.g. "botswana_settlements").

    Returns:
        Full OGC URL with service/version/request/typeName params.
    """
    base = base_url.split("?")[0]  # strip any existing params
    workspace = "geonode"
    if service.upper() == "WFS":
        return (
            f"{base}?service=WFS&version=1.0.0&request=GetFeature"
            f"&typeName={workspace}:{layer_name}"
            f"&outputFormat=application%2Fjson"
        )
    if service.upper() == "WMS":
        return (
            f"{base}?service=WMS&version=1.1.1&request=GetMap"
            f"&layers={workspace}:{layer_name}&format=image%2Fpng"
        )
    if service.upper() == "WCS":
        return (
            f"{base}?service=WCS&version=2.0.1&request=DescribeCoverage"
            f"&CoverageId={workspace}__{layer_name}"
        )
    return base_url


def _build_layer_page_url(portal_base_url: str, layer_name: str) -> str:
    """Build the GeoNode dataset layer page URL.

    This is the user-facing page where the layer can be previewed and
    downloaded.  Pattern: {base}/datasets/geonode:{layer_name}
    """
    base = portal_base_url.rstrip("/")
    return f"{base}/datasets/geonode:{layer_name}"


def _map_geonode_links(
    links: List[Any],
    ds: Dict[str, Any],
    portal_base_url: str = "",
    skip_types: List[str] = None,
    modality_map: Dict[str, str] = None,
    mime_map: Dict[str, str] = None,
) -> List[Dict[str, Any]]:
    """Map GeoNode links array to common resource dicts.

    Returns list of dicts with: id, name, description, format, url,
    _download_url matching what translate.build_rdls_record() expects.

    For OGC services:
    - ``url`` is the GeoNode layer page (user-friendly access point)
    - ``_download_url`` is the layer-specific OGC geoserver URL

    translate.build_resources() will map these to ``access_url`` and
    ``download_url`` respectively.
    """
    if skip_types is None:
        skip_types = ["metadata", "image", "html"]
    if modality_map is None:
        modality_map = {
            "original": "file_download",
            "data": "file_download",
            "OGC:WMS": "WMS",
            "OGC:WFS": "WFS",
            "OGC:WCS": "WCS",
        }
    if mime_map is None:
        mime_map = {}

    # Layer name for constructing OGC service URLs
    layer_name = (
        ds.get("name") or ds.get("alternate") or ""
    ).strip()

    resources: List[Dict[str, Any]] = []
    for link in links:
        if not isinstance(link, dict):
            continue

        link_type = (link.get("link_type") or "").strip()

        # Skip non-data link types
        if link_type.lower() in [s.lower() for s in skip_types]:
            continue

        url = link.get("url") or ""
        if not url:
            continue

        # Determine format from extension, mime, or link_type
        ext = (link.get("extension") or "").strip()
        mime = (link.get("mime") or "").strip()

        fmt = ext.upper() if ext else ""
        if not fmt and mime:
            fmt = mime_map.get(mime.lower(), "")

        # Determine access modality hint and build layer-specific URL
        modality_hint = ""
        for prefix, modality in modality_map.items():
            if link_type == prefix or link_type.startswith(prefix):
                modality_hint = modality
                break

        # Override format for OGC service links (GeoNode sets
        # extension=html for these, which is misleading).
        # Use alias keys that map_data_format() recognises.
        if "WMS" in link_type:
            fmt = "GeoTIFF"
        elif "WFS" in link_type:
            fmt = "GeoJSON"
        elif "WCS" in link_type:
            fmt = "GeoTIFF"

        # For OGC services: access_url = layer page, download_url = geoserver
        download_url = ""
        if layer_name and modality_hint in ("WFS", "WMS", "WCS"):
            download_url = _build_ogc_url(url, modality_hint, layer_name)
            if portal_base_url:
                url = _build_layer_page_url(portal_base_url, layer_name)

        resources.append({
            "id": str(link.get("pk", len(resources))),
            "name": link.get("name") or link_type or "",
            "description": "",
            "format": fmt,
            "url": url,
            "_download_url": download_url,
            "_link_type": link_type,
            "_modality_hint": modality_hint,
        })

    # Fallback: if no links, check for download_url on the dataset
    if not resources:
        dl_url = ds.get("download_url") or ""
        if dl_url:
            resources.append({
                "id": str(ds.get("pk", "")),
                "name": ds.get("title") or "Download",
                "description": "",
                "format": "",
                "url": dl_url,
            })

    return resources


def _humanize_title(
    title: str,
    config: Optional[Dict[str, Any]] = None,
) -> str:
    """Convert technical GeoNode titles into human-readable form.

    Handles patterns like:
      "CK_EQ_HazardMap_03_100_MRP" -> "Cook Islands Earthquake Hazard Map, 0.3s Spectral Acceleration, 100-year Return Period"
      "VU_TC_HazardMap Wind 500 MRP" -> "Vanuatu Tropical Cyclone Wind Hazard Map, 500-year Return Period"
      "FJ_Roads" -> "Fiji Roads"

    Config-driven: all mappings (country codes, spectral params, patterns)
    come from configs/sources/geonode.yaml title_humanize section.

    Returns:
        Humanized title, or original title if no pattern matches.
    """
    if not config or not title:
        return title

    country_codes = config.get("country_codes", {})
    hazard_codes = config.get("hazard_codes", {})
    spectral_codes = config.get("spectral_codes", {})
    feature_names = config.get("feature_names", {})
    patterns = config.get("patterns", [])

    for pat_entry in patterns:
        regex = pat_entry.get("regex", "")
        template = pat_entry.get("template", "")
        if not regex or not template:
            continue

        m = re.match(regex, title)
        if not m:
            continue

        groups = m.groups()
        # All patterns have country code as group 1
        cc = groups[0] if groups else ""
        country = country_codes.get(cc, cc)

        # EQ hazard map: groups = (cc, spectral, rp, suffix)
        if "spectral" in template and len(groups) >= 3:
            spectral_raw = groups[1]
            spectral = spectral_codes.get(spectral_raw, spectral_raw)
            rp = groups[2]
            suffix = groups[3].strip(" _") if len(groups) > 3 else ""
            if suffix:
                suffix = " " + suffix.replace("_", " ").strip()
            return template.format(
                country=country, spectral=spectral, rp=rp, suffix=suffix,
            )

        # TC wind hazard map: groups = (cc, rp, suffix)
        if "Tropical Cyclone" in template and len(groups) >= 2:
            rp = groups[1]
            suffix = groups[2].strip(" _") if len(groups) > 2 else ""
            if suffix:
                suffix = " " + suffix.replace("_", " ").strip()
            return template.format(country=country, rp=rp, suffix=suffix)

        # Simple XX_Feature pattern: groups = (cc, feature)
        if "feature" in template and len(groups) >= 2:
            feature_raw = groups[1]
            feature = feature_names.get(feature_raw, feature_raw)
            return template.format(country=country, feature=feature)

    return title


def extract_geonode_fields(
    ds: Dict[str, Any],
    portal_name: str = "",
    portal_base_url: str = "",
    category_tag_map: Optional[Dict[str, str]] = None,
    skip_link_types: Optional[List[str]] = None,
    link_modality_map: Optional[Dict[str, str]] = None,
    mime_format_map: Optional[Dict[str, str]] = None,
    title_humanize_config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Extract common field structure from GeoNode metadata.

    Returns a dict with the same keys as hdx.extract_hdx_fields() so the
    rest of the pipeline works identically.

    Args:
        ds: Normalized GeoNode dataset dict (from API v2).
        portal_name: Name of the source portal (for provenance).
        portal_base_url: Base URL of the portal (for constructing detail URLs).
        category_tag_map: ISO 19115 category -> synthetic tags mapping.
        skip_link_types: Link types to skip (thumbnails, metadata pages).
        link_modality_map: link_type -> access modality mapping.
        mime_format_map: MIME type -> format string mapping.
    """
    if category_tag_map is None:
        category_tag_map = {}

    # --- Owner / organization ---
    owner = ds.get("owner") or {}
    org = ""
    org_name = ""
    if isinstance(owner, dict):
        org = (owner.get("organization") or "").strip()
        org_name = (owner.get("username") or "").strip()
        if not org:
            first = (owner.get("first_name") or "").strip()
            last = (owner.get("last_name") or "").strip()
            org = f"{first} {last}".strip() or org_name

    # --- License ---
    license_obj = ds.get("license") or {}
    license_title = ""
    license_url = ""
    if isinstance(license_obj, dict):
        license_title = (
            license_obj.get("name")
            or license_obj.get("identifier")
            or ""
        ).strip()
        license_url = (license_obj.get("url") or "").strip()
    elif isinstance(license_obj, str):
        license_title = license_obj.strip()

    # --- Regions -> groups ---
    # GeoNode regions have both 'code' (ISO3) and 'name'.
    # We extract names for groups (used by infer_spatial), but also
    # collect the ISO3 codes directly — they're authoritative and avoid
    # name-resolution failures (e.g., "Cook Islands" not in lookup table).
    regions = ds.get("regions") or []
    groups: List[str] = []
    _region_iso3_codes: List[str] = []
    for r in regions:
        if isinstance(r, dict):
            name = (r.get("name") or "").strip()
            code = (r.get("code") or "").strip()
            if name:
                groups.append(name)
            elif code:
                groups.append(code)
            # Collect ISO3 codes (3-letter alpha codes, excluding
            # regional abbreviations that look like ISO3 but aren't)
            _NON_ISO3_REGION_CODES = {
                "GLO", "ASI", "EAS", "SAS", "SEA", "PAC",
                "AFR", "NAF", "WAF", "EAF", "CAF", "SAF", "CFR",
                "EUR", "CAM", "SAM", "NAM", "CAR", "MDE",
            }
            if (code and len(code) == 3 and code.isalpha()
                    and code.upper() not in _NON_ISO3_REGION_CODES):
                _region_iso3_codes.append(code.upper())
        elif isinstance(r, str) and r.strip():
            groups.append(r.strip())

    # --- Keywords -> tags (lowercase) ---
    keywords = ds.get("keywords") or []
    tags: List[str] = []
    for k in keywords:
        if isinstance(k, dict):
            name = (k.get("name") or k.get("slug") or "").strip()
            if name:
                tags.append(name.lower())
        elif isinstance(k, str) and k.strip():
            tags.append(k.strip().lower())

    # --- Inject category as synthetic tags for classification ---
    category = ds.get("category") or {}
    cat_id = ""
    if isinstance(category, dict):
        cat_id = (category.get("identifier") or "").strip()
        # Direct category label
        cat_label = (
            category.get("gn_description")
            or category.get("description")
            or ""
        ).strip()
        if cat_label:
            tags.append(cat_label.lower())
        # Mapped synthetic tags for classify_dataset() tag weights
        if cat_id and cat_id in category_tag_map:
            for synthetic in category_tag_map[cat_id].split():
                t = synthetic.strip().lower()
                if t and t not in tags:
                    tags.append(t)

    # --- Links -> resources ---
    resources = _map_geonode_links(
        ds.get("links") or [],
        ds,
        portal_base_url=portal_base_url,
        skip_types=skip_link_types,
        modality_map=link_modality_map,
        mime_map=mime_format_map,
    )

    # --- Detail URL ---
    detail_url = (ds.get("detail_url") or "").strip()
    if not detail_url and portal_base_url:
        pk = ds.get("pk") or ds.get("uuid") or ""
        if pk:
            detail_url = f"{portal_base_url.rstrip('/')}/datasets/{pk}"

    # --- Spatial extras (underscore prefix = not consumed by pipeline) ---
    srt = ds.get("spatial_representation_type")
    srt_id = ""
    if isinstance(srt, dict):
        srt_id = srt.get("identifier", "")
    elif isinstance(srt, str):
        srt_id = srt

    # --- Contacts / responsible parties ---
    # GeoNode has 11 distinct contact roles; extract the most relevant.
    # Different GeoNode versions return contacts as either a single dict
    # or a list of dicts -- normalize to list.
    contacts: Dict[str, List[Dict[str, Any]]] = {}
    for role in ("poc", "metadata_author", "publisher", "custodian",
                 "distributor", "originator", "principal_investigator"):
        val = ds.get(role)
        if not val:
            continue
        if isinstance(val, dict):
            contacts[role] = [val]
        elif isinstance(val, list) and val:
            contacts[role] = [v for v in val if isinstance(v, dict)]

    # --- Thesaurus keywords (structured, with URIs) ---
    tkeywords = ds.get("tkeywords") or []
    thesaurus_keywords: List[Dict[str, str]] = []
    for tk in tkeywords:
        if isinstance(tk, dict):
            thesaurus_keywords.append({
                "name": tk.get("name") or tk.get("alt_label") or "",
                "uri": tk.get("uri") or "",
                "thesaurus": (tk.get("thesaurus") or {}).get("name", "")
                    if isinstance(tk.get("thesaurus"), dict) else "",
            })

    # --- Attribute metadata (vector layers) ---
    attributes: List[Dict[str, str]] = []
    for attr in (ds.get("attribute_set") or []):
        if isinstance(attr, dict):
            attributes.append({
                "name": attr.get("attribute") or "",
                "type": attr.get("attribute_type") or "",
                "label": attr.get("attribute_label") or "",
                "description": attr.get("description") or "",
            })

    # --- Combine notes with purpose and data_quality_statement ---
    # Prefer raw_* fields (HTML-stripped) when available (Pacific Data Hub etc.)
    purpose = (ds.get("raw_purpose") or ds.get("purpose") or "").strip()
    data_quality = (
        ds.get("raw_data_quality_statement")
        or ds.get("data_quality_statement") or ""
    ).strip()
    supplemental = (
        ds.get("raw_supplemental_information")
        or ds.get("supplemental_information") or ""
    ).strip()
    # Merge into methodology: supplemental_information is the closest match,
    # but purpose and data_quality_statement carry valuable RDLS-relevant info
    methodology_parts = [p for p in [supplemental, purpose, data_quality] if p]
    methodology_combined = "\n\n".join(methodology_parts)

    # --- Restrictions ---
    restriction = ds.get("restriction_code_type") or {}
    restriction_id = ""
    if isinstance(restriction, dict):
        restriction_id = restriction.get("identifier", "")
    constraints_other = (ds.get("constraints_other") or "").strip()

    # --- Build contact_urls for attribution enrichment ---
    # Map GeoNode contact roles to RDLS attribution roles
    _contact_urls: Dict[str, str] = {}
    for gn_role, rdls_role in [
        ("publisher", "publisher"), ("originator", "creator"),
        ("poc", "contact_point"), ("metadata_author", "creator"),
    ]:
        if gn_role in contacts and contacts[gn_role]:
            c = contacts[gn_role][0]
            c_url = (c.get("link") or c.get("url") or "").strip()
            if c_url and rdls_role not in _contact_urls:
                _contact_urls[rdls_role] = c_url
    # Fallback: use detail_url for any role missing a URL
    if detail_url:
        for role in ("publisher", "creator", "contact_point"):
            if role not in _contact_urls:
                _contact_urls[role] = detail_url

    raw_title = (ds.get("title") or "").strip()
    humanized_title = _humanize_title(raw_title, config=title_humanize_config)
    # If title was humanized, keep original as slug_title for ID generation
    # (the original technical code is compact and unique; the humanized one
    # may truncate to identical slugs across many records).
    slug_title = raw_title if humanized_title != raw_title else ""

    return {
        # === 17 common keys matching extract_hdx_fields() ===
        "id": str(ds.get("pk") or ds.get("uuid") or ""),
        "name": ds.get("name") or ds.get("alternate") or "",
        "title": humanized_title,
        "notes": (ds.get("raw_abstract") or ds.get("abstract") or "").strip(),
        "methodology": methodology_combined,
        "organization": org,
        "org_name": org_name,
        "org_description": "",  # GeoNode org descriptions not in standard API
        "license_title": license_title,
        "license_url": license_url,
        "groups": groups,
        "tags": tags,
        "resources": resources,
        "dataset_date": ds.get("date") or ds.get("temporal_extent_start") or "",
        "dataset_source": (ds.get("attribution") or "").strip(),
        "maintainer": org_name,
        "url": detail_url,
        # === Extra keys for translate.py enrichment ===
        "_contact_urls": _contact_urls,
        # === GeoNode-native metadata (richer than HDX/CKAN) ===
        # These underscore-prefix keys are not consumed by the standard
        # pipeline but are available for GeoNode-specific enrichment.
        "_source_portal": portal_name,
        "_slug_title": slug_title,  # original technical title for unique ID slugs
        "_region_iso3_codes": _region_iso3_codes,  # authoritative ISO3 from GeoNode regions
        "_geonode_spatial": {
            "bbox": ds.get("bbox_polygon") or ds.get("ll_bbox_polygon"),
            "srid": ds.get("srid") or "EPSG:4326",
            "spatial_representation_type": srt_id,
            "extent": ds.get("extent"),
        },
        "_geonode_temporal": {
            "date": ds.get("date") or "",
            "date_type": ds.get("date_type") or "",
            "temporal_extent_start": ds.get("temporal_extent_start") or "",
            "temporal_extent_end": ds.get("temporal_extent_end") or "",
        },
        "_geonode_category": cat_id,
        "_geonode_contacts": contacts,
        "_geonode_quality": {
            "purpose": purpose,
            "data_quality_statement": data_quality,
            "supplemental_information": supplemental,
        },
        "_geonode_restrictions": {
            "restriction_code_type": restriction_id,
            "constraints_other": constraints_other,
        },
        "_geonode_language": ds.get("language") or "",
        "_geonode_maintenance_frequency": ds.get("maintenance_frequency") or "",
        "_geonode_subtype": ds.get("subtype") or "",
        "_geonode_thesaurus_keywords": thesaurus_keywords,
        "_geonode_attributes": attributes,
        "_geonode_edition": (ds.get("edition") or "").strip(),
        "_geonode_doi": (ds.get("doi") or "").strip(),
    }
