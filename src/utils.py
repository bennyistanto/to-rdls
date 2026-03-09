"""
Generic utility functions for RDLS metadata transformation.

Includes text processing, file I/O (JSON/YAML/JSONL), slug generation,
and directory management. Source-independent.
"""

import json
import re
import shutil
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml


# ---------------------------------------------------------------------------
# Text processing
# ---------------------------------------------------------------------------

def sanitize_text(text: str) -> str:
    """Clean text for RDLS JSON: fix encoding, strip HTML, normalize characters.

    Handles: mojibake (double-encoded UTF-8 via CP1252), HTML tags/entities,
    smart quotes, em/en dashes, non-breaking spaces, zero-width spaces,
    control characters, and internal double quotes.
    """
    if not text:
        return text

    # 1. Fix mojibake (double-encoded UTF-8 via CP1252)
    try:
        clean = text.encode("cp1252").decode("utf-8")
    except (UnicodeEncodeError, UnicodeDecodeError):
        clean = text

    # 2. Strip HTML tags
    clean = re.sub(r"<[^>]+>", " ", clean)

    # 3. Decode HTML entities
    clean = clean.replace("&nbsp;", " ").replace("&amp;", "&")
    clean = clean.replace("&lt;", "<").replace("&gt;", ">")
    clean = clean.replace("&quot;", "'").replace("&#39;", "'")
    clean = re.sub(r"&#(\d+);", lambda m: chr(int(m.group(1))), clean)
    clean = re.sub(r"&#x([0-9a-fA-F]+);", lambda m: chr(int(m.group(1), 16)), clean)

    # 4. Normalize Unicode to ASCII-safe equivalents
    clean = clean.replace("\u2018", "'").replace("\u2019", "'")
    clean = clean.replace("\u201C", "'").replace("\u201D", "'")
    clean = clean.replace("\u2013", "-").replace("\u2014", "-")
    clean = clean.replace("\u2026", "...").replace("\u00A0", " ")
    clean = clean.replace("\u200B", "").replace("\u2022", "-")

    # 5. Remove/replace control characters
    clean = clean.replace("\t", " ").replace("\r", " ")

    # 6. Replace internal double quotes with single quotes
    clean = clean.replace('"', "'")

    # 7. Collapse whitespace to single space
    clean = re.sub(r"\s+", " ", clean)

    return clean.strip()


def slugify(s: str, max_len: int = 80) -> str:
    """Convert string to URL-safe slug."""
    s = (s or "").strip().lower()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return (s[:max_len].strip("-") or "unknown")


def slugify_token(s: str, max_len: int = 32) -> str:
    """Convert string to URL-safe slug token (underscore-separated)."""
    s = (s or "").strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    return (s[:max_len].strip("_") or "unknown")


def norm_str(x: Any) -> str:
    """Unicode NFKD normalization to lowercase, stripped."""
    s = str(x) if x is not None else ""
    return unicodedata.normalize("NFKD", s).strip().lower()


def short_text(s: str, max_len: int = 350) -> str:
    """Truncate text with ellipsis."""
    s = (s or "").strip()
    if len(s) <= max_len:
        return s
    return s[: max_len - 3].rsplit(" ", 1)[0] + "..."


def split_semicolon_list(s: Any) -> List[str]:
    """Split semicolon/comma separated string into list."""
    if s is None:
        return []
    if isinstance(s, float):
        # Handle NaN from pandas
        try:
            import math
            if math.isnan(s):
                return []
        except (ImportError, TypeError):
            pass
    if isinstance(s, list):
        return [str(x).strip() for x in s if str(x).strip()]
    s = str(s).strip()
    if not s:
        return []
    return [x.strip() for x in re.split(r"[;,]", s) if x.strip()]


def looks_like_url(s: str) -> bool:
    """Check if string looks like a URL."""
    return bool(re.match(r"^https?://", (s or "").strip(), flags=re.I))


def as_list(x: Any) -> list:
    """Ensure value is a list."""
    if x is None:
        return []
    if isinstance(x, list):
        return x
    if isinstance(x, (set, tuple)):
        return list(x)
    return [x]


def normalize_text(s: str) -> str:
    """Normalize text for pattern matching: lowercase, stripped, collapsed whitespace."""
    return re.sub(r"\s+", " ", (s or "").strip().lower())


# ---------------------------------------------------------------------------
# File I/O
# ---------------------------------------------------------------------------

def load_json(path: Union[str, Path]) -> Any:
    """Load JSON file with UTF-8 encoding."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: Union[str, Path], obj: Any, pretty: bool = True) -> None:
    """Write JSON file atomically with UTF-8 encoding."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2 if pretty else None)
            f.write("\n")
        tmp.replace(path)
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise


def append_jsonl(path: Union[str, Path], obj: Any) -> None:
    """Append a single JSON object as a line to a JSONL file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def load_yaml(path: Union[str, Path]) -> Any:
    """Load YAML file."""
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def write_yaml(path: Union[str, Path], obj: Any) -> None:
    """Write YAML file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.dump(obj, f, default_flow_style=False, allow_unicode=True, sort_keys=False)


def iter_json_files(folder: Union[str, Path]) -> List[Path]:
    """Return sorted list of JSON files in a directory."""
    folder = Path(folder)
    if not folder.is_dir():
        return []
    return sorted(folder.glob("*.json"))


# ---------------------------------------------------------------------------
# Directory management
# ---------------------------------------------------------------------------

def clean_directory(directory: Union[str, Path], label: str = "",
                    mode: str = "replace") -> None:
    """Clean a directory with controlled modes.

    Args:
        directory: Path to clean.
        label: Human-readable label for logging.
        mode: One of 'replace' (delete and recreate), 'skip' (leave as-is),
              'abort' (raise if exists).
    """
    directory = Path(directory)
    if not directory.exists():
        directory.mkdir(parents=True, exist_ok=True)
        return

    if mode == "skip":
        return
    elif mode == "abort":
        raise FileExistsError(f"Directory already exists: {directory} ({label})")
    elif mode == "replace":
        shutil.rmtree(directory)
        directory.mkdir(parents=True, exist_ok=True)
    else:
        raise ValueError(f"Unknown cleanup mode: {mode!r}")


# ---------------------------------------------------------------------------
# Nested dict/list navigation
# ---------------------------------------------------------------------------

def navigate_path(obj: Any, parts: List[str]):
    """Navigate to the parent of a nested field by dot-path parts.

    Args:
        obj: Root dict/list structure.
        parts: Path segments (e.g. ["exposure", "0", "metrics", "1", "currency"]).

    Returns:
        (parent, key) tuple where parent[key] is the target, or (None, None)
        if the path is invalid.
    """
    current = obj
    for part in parts[:-1]:
        if isinstance(current, dict):
            if part in current:
                current = current[part]
            else:
                return None, None
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return None, None
        else:
            return None, None
    last_key = parts[-1]
    if isinstance(current, list):
        try:
            last_key = int(last_key)
        except ValueError:
            return None, None
    return current, last_key


def remove_at_path(obj: Any, parts: List[str]) -> bool:
    """Remove a field at a nested dot-path. Returns True if removed."""
    parent, key = navigate_path(obj, parts)
    if parent is None:
        return False
    if isinstance(parent, dict) and key in parent:
        del parent[key]
        return True
    return False


def set_at_path(obj: Any, parts: List[str], value: Any) -> bool:
    """Set a value at a nested dot-path. Returns True if set."""
    parent, key = navigate_path(obj, parts)
    if parent is None:
        return False
    if isinstance(parent, dict):
        parent[key] = value
        return True
    elif isinstance(parent, list) and isinstance(key, int):
        parent[key] = value
        return True
    return False


# ---------------------------------------------------------------------------
# Temporal parsing
# ---------------------------------------------------------------------------

# Patterns for relative date expressions (from ai-config / MCP pipelines)
_RELATIVE_PATTERNS = [
    (re.compile(r'(\d+)\s*days?\s*ago', re.IGNORECASE), lambda m: f"P{m.group(1)}D"),
    (re.compile(r'(\d+)\s*weeks?\s*ago', re.IGNORECASE), lambda m: f"P{int(m.group(1)) * 7}D"),
    (re.compile(r'(\d+)\s*months?\s*ago', re.IGNORECASE), lambda m: f"P{m.group(1)}M"),
    (re.compile(r'(\d+)\s*years?\s*ago', re.IGNORECASE), lambda m: f"P{m.group(1)}Y"),
]


def parse_hdx_temporal(
    dataset_date: str,
    update_freq: str = "",
) -> Dict[str, Optional[str]]:
    """Parse HDX dataset_date to RDLS temporal fields.

    Handles:
    - Absolute ranges: ``[2017-05-23T00:00:00 TO 2017-05-23T23:59:59]``
    - Open-ended: ``[2021-12-16T00:00:00 TO *]``
    - Relative text: ``"90 days ago"``, ``"1 month ago"`` (from MCP pipelines)
    - Rolling-window detection: frequent updates + short date range -> duration

    Args:
        dataset_date: Raw HDX dataset_date string.
        update_freq: HDX data_update_frequency string (e.g. "Every week").

    Returns:
        Dict with keys ``start``, ``end``, ``duration`` (ISO 8601).
        Values are ``None`` when not applicable.
    """
    result: Dict[str, Optional[str]] = {"start": None, "end": None, "duration": None}

    if not dataset_date or not dataset_date.strip():
        return result

    raw = dataset_date.strip()

    # --- Handle relative text expressions (from MCP / ai-config) ---
    for pattern, dur_fn in _RELATIVE_PATTERNS:
        m = pattern.search(raw)
        if m:
            result["duration"] = dur_fn(m)
            return result  # Duration only, no start/end

    # --- Handle [start TO end] format ---
    m = re.match(r"\[(\S+)\s+TO\s+(\S+)\]", raw)
    if not m:
        return result

    raw_start, raw_end = m.group(1), m.group(2)

    # Parse start
    try:
        start_dt = datetime.fromisoformat(raw_start)
        result["start"] = start_dt.strftime("%Y-%m-%d")
    except ValueError:
        return result

    # Parse end
    if raw_end == "*":
        # Open-ended: start only, no end/duration
        return result

    try:
        end_dt = datetime.fromisoformat(raw_end)
        result["end"] = end_dt.strftime("%Y-%m-%d")

        # Rolling-window / live-service detection
        delta_days = (end_dt - start_dt).days
        freq_lower = (update_freq or "").lower()
        is_frequent = any(kw in freq_lower for kw in ("day", "week", "every"))

        if is_frequent and delta_days <= 1:
            # Live service: frequent updates + single-day or zero-day range
            # The dataset_date is meaningless (registration date); omit temporal
            return {"start": None, "end": None, "duration": None}
        elif is_frequent and 80 <= delta_days <= 100:
            # Rolling ~90 day window
            return {"start": None, "end": None, "duration": "P90D"}
        elif is_frequent and 25 <= delta_days <= 35:
            # Rolling ~1 month window
            return {"start": None, "end": None, "duration": "P1M"}
        else:
            # Static range — compute duration
            if delta_days >= 365:
                result["duration"] = f"P{delta_days // 365}Y"
            elif delta_days >= 30:
                result["duration"] = f"P{delta_days // 30}M"
            elif delta_days > 0:
                result["duration"] = f"P{delta_days}D"
    except ValueError:
        pass

    return result
