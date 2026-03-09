"""ZIP member extraction for file inspection.

Provides context managers that extract individual files from ZIP archives
to temporary paths, allowing existing file inspectors to read them unchanged.
Handles nested ZIPs (ZIP-in-ZIP) with two-level temp extraction.

Uses only stdlib: zipfile, tempfile, pathlib, os.
"""
from __future__ import annotations

import os
import tempfile
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Tuple


def parse_zip_spec(fpath: str) -> Tuple[Path, str]:
    """Split a ``'archive.zip::inner/path/file.tif'`` path into components.

    Returns
    -------
    (zip_path, member_name)
        *zip_path* is the outer archive, *member_name* is the path inside it.
        If *member_name* itself contains ``::`` the caller should treat it as
        a nested-ZIP reference.
    """
    if "::" not in fpath:
        raise ValueError(f"Not a ZIP spec (no '::' separator): {fpath}")
    zip_part, member = fpath.split("::", 1)
    return Path(zip_part), member


@contextmanager
def open_zip_member(zip_path: Path, member_name: str) -> Iterator[Path]:
    """Extract a single member from a ZIP to a temp file and yield its path.

    Only the requested member is read — the rest of the archive is untouched,
    which is critical for multi-GB ZIPs.  The temp file is deleted on exit.

    Parameters
    ----------
    zip_path : Path
        Path to the ZIP archive on disk.
    member_name : str
        Path of the member inside the archive (as returned by ``parse_zip_spec``).

    Yields
    ------
    Path
        A temporary file containing the extracted member content.
    """
    suffix = Path(member_name).suffix or ".tmp"
    tmp_path: str | None = None
    try:
        with zipfile.ZipFile(zip_path, "r") as zf:
            data = zf.read(member_name)
        # Write to a named temp file so libraries that need a path can open it
        fd, tmp_path = tempfile.mkstemp(suffix=suffix, prefix="rdls_zip_")
        os.write(fd, data)
        os.close(fd)
        yield Path(tmp_path)
    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.unlink(tmp_path)
            except OSError:
                pass


@contextmanager
def open_nested_zip_member(
    outer_zip: Path,
    inner_zip_member: str,
    inner_file_member: str,
) -> Iterator[Path]:
    """Handle ZIP-in-ZIP: extract the inner archive, then extract the target file.

    For paths like ``03 AW3D DTM.zip::ORD4452_AW3D.zip::actual_file.tif``:
    *outer_zip* = ``03 AW3D DTM.zip``, *inner_zip_member* = ``ORD4452_AW3D.zip``,
    *inner_file_member* = ``actual_file.tif``.

    Two levels of temp files are created and both are cleaned up on exit.
    Max nesting depth is fixed at 2 to prevent pathological cases.
    """
    with open_zip_member(outer_zip, inner_zip_member) as inner_zip_path:
        with open_zip_member(inner_zip_path, inner_file_member) as final_path:
            yield final_path


def _find_zip(name: str, base: Path) -> Path:
    """Resolve a ZIP filename or relative path against *base*.

    First tries ``base / name`` directly.  If that doesn't exist and *name*
    looks like a bare filename (no directory separators), searches one level
    of subdirectories under *base*.  This handles legacy inventories that
    stored only the ZIP filename without the subdirectory prefix.
    """
    direct = base / name if not Path(name).is_absolute() else Path(name)
    if direct.exists():
        return direct

    # Fallback: search subdirectories for bare filenames
    if "/" not in name and "\\" not in name:
        for child in base.iterdir():
            if child.is_dir():
                candidate = child / name
                if candidate.exists():
                    return candidate

    # Give up — return the direct path and let the caller handle FileNotFoundError
    return direct


def resolve_and_open(fpath: str, base: Path):
    """Parse a ``::``-separated path and return the appropriate context manager.

    Parameters
    ----------
    fpath : str
        A path that may contain one or two ``::`` separators.
    base : Path
        Base directory to resolve relative ZIP paths against.

    Returns
    -------
    context manager
        Either ``open_zip_member`` or ``open_nested_zip_member``.
    """
    parts = fpath.split("::")
    if len(parts) < 2:
        raise ValueError(f"Not a ZIP spec: {fpath}")

    outer_zip = _find_zip(parts[0], base)

    if len(parts) == 2:
        return open_zip_member(outer_zip, parts[1])
    elif len(parts) == 3:
        return open_nested_zip_member(outer_zip, parts[1], parts[2])
    else:
        raise ValueError(f"ZIP nesting deeper than 2 levels not supported: {fpath}")
