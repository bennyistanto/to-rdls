# Inventory folders (and zip archives) into Markdown + optional CSV/JSON
# ---------------------------------------------------------------------
# Design goals:
# - Modular: Functions grouped by responsibility with rich docstrings and type hints.
# - Safe defaults: No symlink following; ZIP listing is capped; checksums off by default.
# - Portable: Standard library only; IPython display is optional.
# - Readable outputs: Clean Markdown report + machine-readable CSV/JSON.
#
# Extracted from rdls_data_inventory_contents.ipynb for CLI and automation use.
# Benny Istanto, GOST/DEC Data Group/The World Bank


from __future__ import annotations

import argparse
import csv
import datetime as dt
import hashlib
import json
import mimetypes
import os
import sys
import time
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple


# Optional notebook display (degrades gracefully outside IPython)
try:
    from IPython.display import Markdown, display
    _IPY_DISPLAY = True
except Exception:
    _IPY_DISPLAY = False


# ---------------------------------------------------------------------------
# Configuration model
# ---------------------------------------------------------------------------

@dataclass
class InventoryConfig:
    """
    User-facing configuration for a single inventory run.

    Attributes
    ----------
    target : Path
        Path to a *directory* or a single *file*. If a file and it's a .zip,
        it can be inspected without extraction.
    write_markdown_path : Optional[Path]
        Where to write the Markdown report (None = do not write).
    write_csv_path : Optional[Path]
        Where to write the CSV inventory (None = do not write).
    write_json_path : Optional[Path]
        Where to write the JSON inventory (None = do not write).
    include_hash : bool
        Compute SHA256 for regular files (slower).
    inspect_zips : bool
        List contents inside .zip archives without extracting.
    zip_max : int
        Maximum number of entries to list per zip file (defensive cap).
    excludes : List[str]
        Glob-style patterns, relative to 'target' root (e.g., "*/tmp/*", "*.bak").
    max_depth : Optional[int]
        Limit recursion depth (None = unlimited; 0 = top-level only).
    follow_symlinks : bool
        Follow symlinks when traversing directories (default False).
    verbose : bool
        Print extra runtime messages (console).
    """
    target: Path
    write_markdown_path: Optional[Path] = None
    write_csv_path: Optional[Path] = None
    write_json_path: Optional[Path] = None
    include_hash: bool = False
    inspect_zips: bool = True
    zip_max: int = 20_000
    excludes: List[str] = field(default_factory=list)
    max_depth: Optional[int] = None
    follow_symlinks: bool = False
    verbose: bool = False


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def human_size(n: int) -> str:
    """Convert bytes to a human-readable string (e.g., '12.3 MB')."""
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    size = float(n)
    for u in units:
        if size < 1024 or u == units[-1]:
            return f"{size:.1f} {u}"
        size /= 1024
    return f"{size:.1f} {units[-1]}"  # unreachable, satisfies type checker


def iso_time(ts: float) -> str:
    """Format a POSIX timestamp as UTC ISO-8601 with trailing Z."""
    return (
        dt.datetime.fromtimestamp(ts, dt.UTC)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def sha256_file(path: Path, *, bufsize: int = 1_048_576) -> str:
    """Compute streaming SHA256 for a file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(bufsize)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def mime_from_name(name: str) -> str:
    """Guess MIME type from a filename; fall back to octet-stream."""
    mt, _ = mimetypes.guess_type(name)
    return mt or "application/octet-stream"


def matches_any_glob(relpath: Path, patterns: List[str]) -> bool:
    """Check if a relative path matches any glob pattern."""
    if not patterns:
        return False
    s = str(relpath)
    return any(Path(s).match(p) for p in patterns)


# ---------------------------------------------------------------------------
# Scanning primitives
# ---------------------------------------------------------------------------

def iter_dir(
    root: Path,
    *,
    exclude: List[str],
    max_depth: Optional[int],
    follow_symlinks: bool,
    verbose: bool = False,
) -> Iterator[Tuple[Path, os.DirEntry]]:
    """
    Efficient recursive directory iterator using os.scandir.

    Yields (dir_path, entry) tuples. Respects glob-based excludes,
    max_depth, and symlink settings.
    """
    root = root.resolve()

    def _walk(d: Path, depth: int) -> Iterator[Tuple[Path, os.DirEntry]]:
        if max_depth is not None and depth > max_depth:
            return
        try:
            with os.scandir(d) as it:
                for entry in it:
                    p = Path(entry.path)
                    rel = p.relative_to(root) if p != root else Path(".")
                    # Exclusions
                    if matches_any_glob(rel, exclude):
                        if verbose:
                            print(f"[skip] exclude: {rel}")
                        continue
                    # Symlinks
                    if not follow_symlinks:
                        try:
                            if entry.is_symlink():
                                if verbose:
                                    print(f"[skip] symlink: {rel}")
                                continue
                        except OSError:
                            continue
                    yield d, entry
                    try:
                        if entry.is_dir(follow_symlinks=follow_symlinks):
                            yield from _walk(p, depth + 1)
                    except OSError:
                        continue
        except PermissionError:
            if verbose:
                print(f"[warn] permission denied: {d}")
        except FileNotFoundError:
            if verbose:
                print(f"[warn] directory vanished: {d}")

    yield from _walk(root, 0)


def list_zip_members(
    zpath: Path, *, zip_max: int, verbose: bool = False
) -> List[Dict]:
    """
    List (a capped number of) file entries inside a .zip archive — no extraction.
    """
    out: List[Dict] = []
    try:
        with zipfile.ZipFile(zpath, "r") as zf:
            count = 0
            for info in zf.infolist():
                if info.is_dir():
                    continue
                if count >= zip_max:
                    break
                zip_dt = dt.datetime(*info.date_time).replace(tzinfo=dt.UTC)
                mod_utc = (
                    zip_dt.replace(microsecond=0)
                    .isoformat()
                    .replace("+00:00", "Z")
                )
                out.append(
                    {
                        "container": zpath.name,
                        "path": f"{zpath.name}::{info.filename}",
                        "name": Path(info.filename).name,
                        "ext": Path(info.filename).suffix.lower(),
                        "mime": mime_from_name(info.filename),
                        "size_bytes": int(info.file_size),
                        "size_human": human_size(int(info.file_size)),
                        "modified_utc": mod_utc,
                        "is_in_zip": True,
                        "sha256": "",
                    }
                )
                count += 1
    except zipfile.BadZipFile:
        if verbose:
            print(f"[warn] bad zip: {zpath}")
    except Exception as e:
        if verbose:
            print(f"[warn] failed to read zip {zpath}: {e}")
    return out


def file_row(path: Path, *, include_hash: bool) -> Dict:
    """Create an inventory row for a single regular file on disk."""
    st = path.stat()
    return {
        "container": "",
        "path": str(path),
        "name": path.name,
        "ext": path.suffix.lower(),
        "mime": mime_from_name(path.name),
        "size_bytes": int(st.st_size),
        "size_human": human_size(int(st.st_size)),
        "modified_utc": iso_time(st.st_mtime),
        "is_in_zip": False,
        "sha256": sha256_file(path) if include_hash else "",
    }


def scan_target(cfg: InventoryConfig) -> Tuple[List[Dict], Dict]:
    """
    Scan a directory or a single file (including zip files).

    Returns (rows, stats) where rows are file inventory dicts and stats
    are summary counters.
    """
    rows: List[Dict] = []
    files = 0
    dirs = 0
    total_bytes = 0

    target = cfg.target.resolve()

    if not target.exists():
        raise FileNotFoundError(f"Target does not exist: {target}")

    if cfg.verbose:
        print(f"[info] scanning: {target}")

    # Case 1: single file input
    if target.is_file():
        if target.suffix.lower() == ".zip" and cfg.inspect_zips:
            rows.extend(
                list_zip_members(
                    target, zip_max=cfg.zip_max, verbose=cfg.verbose
                )
            )
        outer = file_row(target, include_hash=cfg.include_hash)
        rows.append(outer)
        files += 1
        total_bytes += outer["size_bytes"]

    # Case 2: directory
    else:
        root = target
        for _parent, entry in iter_dir(
            root,
            exclude=cfg.excludes,
            max_depth=cfg.max_depth,
            follow_symlinks=cfg.follow_symlinks,
            verbose=cfg.verbose,
        ):
            p = Path(entry.path)
            rel = p.relative_to(root)
            try:
                if entry.is_dir(follow_symlinks=cfg.follow_symlinks):
                    dirs += 1
                    continue
                if entry.is_file(follow_symlinks=cfg.follow_symlinks):
                    files += 1
                    if p.suffix.lower() == ".zip" and cfg.inspect_zips:
                        zip_rows = list_zip_members(
                            p, zip_max=cfg.zip_max, verbose=cfg.verbose
                        )
                        # Patch ZIP member paths to use relative path from root
                        # so that resolve_and_open can find the ZIP on disk.
                        rel_zip = str(rel)
                        for zr in zip_rows:
                            zr["container"] = rel_zip
                            zr["path"] = f"{rel_zip}::{zr['path'].split('::', 1)[1]}"
                        rows.extend(zip_rows)
                    r = file_row(p, include_hash=cfg.include_hash)
                    r["path"] = str(rel)
                    rows.append(r)
                    total_bytes += r["size_bytes"]
            except OSError:
                continue

    rows.sort(key=lambda r: (r["container"], r["path"]))

    stats = {
        "target": str(target),
        "files": files,
        "dirs": dirs,
        "total_bytes": total_bytes,
        "total_human": human_size(total_bytes),
        "generated_utc": iso_time(time.time()),
        "zip_entries": sum(1 for r in rows if r.get("is_in_zip")),
    }
    return rows, stats


# ---------------------------------------------------------------------------
# Rendering & output
# ---------------------------------------------------------------------------

def build_tree_lines(rows: List[Dict], base_label: str) -> List[str]:
    """Build an ASCII tree from inventory rows."""
    sep = os.sep
    zsep = "::"

    def explode(path_str: str) -> List[str]:
        if zsep in path_str:
            outer, inner = path_str.split(zsep, 1)
            parts = [outer + zsep] + inner.strip("/").split("/")
        else:
            parts = path_str.split(sep)
        return [p for p in parts if p not in ("", ".")]

    nodes: Dict[Tuple[str, ...], set] = {}
    for r in rows:
        parts = explode(r["path"])
        for i in range(1, len(parts) + 1):
            nodes.setdefault(tuple(parts[:i]), set())

    for r in rows:
        parts = explode(r["path"])
        for i in range(len(parts) - 1):
            parent = tuple(parts[: i + 1])
            child = parts[i + 1]
            nodes[parent].add(child)

    for k in list(nodes):
        nodes[k] = set(sorted(nodes[k], key=lambda s: s.lower()))

    lines = [f"{base_label}"]

    def _print(prefix: str, key: Tuple[str, ...]) -> None:
        children = sorted(nodes.get(key, []), key=lambda s: s.lower())
        for idx, child in enumerate(children):
            is_last = idx == len(children) - 1
            branch = "└── " if is_last else "├── "
            extender = "    " if is_last else "│   "
            lines.append(prefix + branch + child)
            _print(prefix + extender, key + (child,))

    _print("", tuple())
    return lines


def markdown_report(
    rows: List[Dict],
    stats: Dict,
    *,
    title: str,
    include_tree: bool = True,
) -> str:
    """Produce a human-friendly Markdown report."""
    md: List[str] = []
    md.append(f"# {title}")
    md.append("")
    md.append(f"- **Target**: `{stats['target']}`")
    md.append(f"- **Generated (UTC)**: `{stats['generated_utc']}`")
    md.append(
        f"- **Files**: `{stats['files']:,}`   |   "
        f"**Dirs**: `{stats['dirs']:,}`   |   "
        f"**Zip entries**: `{stats['zip_entries']:,}`"
    )
    md.append(f"- **Total size**: `{stats['total_human']}`")
    md.append("")

    if include_tree:
        md.append("## Contents (tree)")
        base_label = Path(stats["target"]).name or "."
        lines = build_tree_lines(rows, base_label=base_label)
        md.append("```text")
        md.extend(lines)
        md.append("```")
        md.append("")

    md.append("## File inventory")
    md.append("")
    md.append("| Path | Size | Type | Modified (UTC) | SHA256 |")
    md.append("|---|---:|---|---|---|")
    for r in rows:
        p = r["path"]
        size = r["size_human"]
        typ = r["mime"]
        mod = r["modified_utc"]
        sha = (r["sha256"][:12] + "…") if r.get("sha256") else ""
        md.append(f"| `{p}` | {size} | `{typ}` | `{mod}` | `{sha}` |")

    md.append("")
    md.append(
        "> Tip: The CSV/JSON (if exported) contains exact bytes, "
        "extensions, and optional full SHA256."
    )
    return "\n".join(md)


def write_csv(rows: List[Dict], path: Path) -> None:
    """Write rows to CSV."""
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "container", "path", "name", "ext", "mime",
        "size_bytes", "size_human", "modified_utc", "is_in_zip", "sha256",
    ]
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_json(rows: List[Dict], stats: Dict, path: Path) -> None:
    """Write rows + stats as JSON."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump({"stats": stats, "rows": rows}, f, indent=2)


def render_and_write(cfg: InventoryConfig) -> Tuple[str, List[Dict], Dict]:
    """
    Orchestrate scanning, rendering, and optional file writing.

    Returns (markdown_report, rows, stats) for downstream automation.
    """
    rows, stats = scan_target(cfg)
    title = f"Inventory: {Path(stats['target']).name or stats['target']}"
    md = markdown_report(rows, stats, title=title, include_tree=True)

    # Inline display (if in notebook)
    if _IPY_DISPLAY:
        display(Markdown(md))
    else:
        print(md)

    if cfg.write_markdown_path:
        cfg.write_markdown_path.parent.mkdir(parents=True, exist_ok=True)
        cfg.write_markdown_path.write_text(md, encoding="utf-8")
        if cfg.verbose:
            print(f"[info] wrote Markdown → {cfg.write_markdown_path}")

    if cfg.write_csv_path:
        write_csv(rows, cfg.write_csv_path)
        if cfg.verbose:
            print(f"[info] wrote CSV → {cfg.write_csv_path}")

    if cfg.write_json_path:
        write_json(rows, stats, cfg.write_json_path)
        if cfg.verbose:
            print(f"[info] wrote JSON → {cfg.write_json_path}")

    return md, rows, stats


# ---------------------------------------------------------------------------
# Convenience: build config from a path with sensible defaults
# ---------------------------------------------------------------------------

def inventory_folder(
    target: str | Path,
    *,
    output_dir: str | Path | None = None,
    formats: str = "json,md,csv",
    include_hash: bool = False,
    inspect_zips: bool = True,
    verbose: bool = True,
) -> Tuple[str, List[Dict], Dict]:
    """
    High-level convenience function for automation and Claude Code commands.

    Parameters
    ----------
    target : str | Path
        Folder or ZIP file to inventory.
    output_dir : str | Path | None
        Where to write outputs. Defaults to ``{target}/_inventory``.
    formats : str
        Comma-separated output formats: ``json``, ``md``, ``csv``.
    include_hash : bool
        Compute SHA256 checksums (slower).
    inspect_zips : bool
        Peek inside .zip files without extracting.
    verbose : bool
        Print progress messages.

    Returns
    -------
    (markdown, rows, stats) : Tuple[str, List[Dict], Dict]
        The markdown report string, raw inventory rows, and summary stats.
    """
    target = Path(target).resolve()
    if output_dir is None:
        output_dir = target / "_inventory"
    else:
        output_dir = Path(output_dir).resolve()

    stamp = dt.datetime.now(dt.UTC).strftime("%Y%m%dT%H%MZ")
    fmt_set = {f.strip().lower() for f in formats.split(",")}

    cfg = InventoryConfig(
        target=target,
        write_json_path=output_dir / f"inventory_{stamp}.json" if "json" in fmt_set else None,
        write_markdown_path=output_dir / f"inventory_{stamp}.md" if "md" in fmt_set else None,
        write_csv_path=output_dir / f"inventory_{stamp}.csv" if "csv" in fmt_set else None,
        include_hash=include_hash,
        inspect_zips=inspect_zips,
        verbose=verbose,
    )

    return render_and_write(cfg)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="inventory",
        description="Inventory a delivery folder or ZIP into Markdown + CSV/JSON.",
    )
    p.add_argument(
        "target",
        type=Path,
        help="Path to a directory or a single .zip file",
    )
    p.add_argument(
        "-o", "--output-dir",
        type=Path,
        default=None,
        help="Output directory (default: TARGET/_inventory)",
    )
    p.add_argument(
        "--formats",
        default="json,md,csv",
        help="Comma-separated output formats: json, md, csv (default: all)",
    )
    p.add_argument(
        "--hash",
        action="store_true",
        default=False,
        help="Compute SHA256 checksums (slower)",
    )
    p.add_argument(
        "--no-zip-inspect",
        action="store_true",
        default=False,
        help="Skip peeking inside .zip files",
    )
    p.add_argument(
        "-q", "--quiet",
        action="store_true",
        default=False,
        help="Suppress progress messages",
    )
    return p


def main(argv: List[str] | None = None) -> None:
    """CLI entry point."""
    parser = _build_parser()
    args = parser.parse_args(argv)

    _md, _rows, stats = inventory_folder(
        target=args.target,
        output_dir=args.output_dir,
        formats=args.formats,
        include_hash=args.hash,
        inspect_zips=not args.no_zip_inspect,
        verbose=not args.quiet,
    )

    print(f"\n[done] {stats['files']} files, {stats['zip_entries']} zip entries, {stats['total_human']}")


if __name__ == "__main__":
    main()
