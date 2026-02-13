"""
GeoNode source adapter (stub for future implementation).

Provides the same interface as hdx.py for GeoNode metadata sources.
"""

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from ..utils import load_yaml


@dataclass
class GeoNodeConfig:
    """Configuration for GeoNode metadata crawler."""
    base_url: str = ""
    rows_per_page: int = 100
    timeout: int = 60
    max_retries: int = 3
    rate_limit: float = 5.0

    @classmethod
    def from_yaml(cls, yaml_path: str) -> "GeoNodeConfig":
        cfg = load_yaml(yaml_path)
        api = cfg.get("api", {})
        return cls(
            base_url=api.get("base_url", cls.base_url),
            rows_per_page=api.get("rows_per_page", cls.rows_per_page),
            timeout=api.get("timeout", cls.timeout),
            max_retries=api.get("max_retries", cls.max_retries),
            rate_limit=api.get("rate_limit", cls.rate_limit),
        )


class GeoNodeClient:
    """HTTP client for GeoNode REST API v2 (stub)."""

    def __init__(self, config: GeoNodeConfig):
        self.config = config
        # TODO: implement session, rate limiting, retries
        raise NotImplementedError("GeoNode client not yet implemented")


def normalize_geonode_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize GeoNode record shape (stub)."""
    # TODO: implement based on GeoNode API response structure
    return raw


def extract_geonode_fields(ds: Dict[str, Any]) -> Dict[str, Any]:
    """Extract common field structure from GeoNode metadata (stub).

    Should return the same keys as hdx.extract_hdx_fields() so the
    rest of the pipeline works identically.
    """
    # TODO: map GeoNode fields to common structure
    return {
        "id": ds.get("pk", ""),
        "name": ds.get("name", ""),
        "title": ds.get("title", ""),
        "notes": ds.get("abstract", ""),
        "methodology": ds.get("supplemental_information", ""),
        "organization": "",  # TODO: extract from owner
        "license_title": "",  # TODO: extract from license
        "groups": [],  # TODO: map regions
        "tags": [],  # TODO: map keywords
        "resources": [],  # TODO: map links
        "dataset_date": ds.get("date", ""),
        "url": ds.get("detail_url", ""),
    }
