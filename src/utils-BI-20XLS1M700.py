"""
Generic utility functions for RDLS metadata transformation.

Includes text processing, file I/O (JSON/YAML/JSONL), slug generation,
and directory management. Source-independent.
"""

import json
import re
import shutil
import unicodedata
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
