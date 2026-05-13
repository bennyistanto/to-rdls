"""
validate_records.py - RDLS v1.0 Metadata Validation and Enrichment CLI
========================================================================
Two modes in one script:

  Audit mode (default):
    Three-layer validation for RDLS v1.0 JSON metadata files.
    Thin CLI wrapper over src/audit.py.

    Usage:
        python scripts/validate_records.py <metadata.json> [--schema <schema.json>] [--codelists <dir>]

    Defaults:
        --schema    : rdls_schema.json from sibling rdl-standard repo (falls back to schema/rdls_schema_v1.0.json)
        --codelists : ../rdl-standard/schema/codelists (local rdl-standard clone)

  Enrich mode (--enrich):
    Post-conversion enrichment for RDLS v1.0 JSON files.
    Applies mechanical fixes (unit normalization, URI fixes, license cleanup, etc.).
    Thin CLI wrapper over src/enrich.py.

    Usage:
        python scripts/validate_records.py --enrich "output/new-collection/**/*.json"
        python scripts/validate_records.py --enrich output/some/file.json
        python scripts/validate_records.py --enrich output/new-collection/
"""

import argparse
import json
import sys
from pathlib import Path

# Add project root to path so src/ is importable when run as script
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.audit import validate, CodelistRegistry
from src.enrich import fix_file, resolve_files


# ---------------------------------------------------------------------------
# Audit mode
# ---------------------------------------------------------------------------

def run_audit(args: argparse.Namespace) -> int:
    """Three-layer audit validation. Returns exit code."""
    script_dir = Path(__file__).parent
    repo_root = script_dir.parent

    # Resolve schema path
    schema_dir = repo_root / "schema"
    if args.schema:
        schema_path = Path(args.schema)
    else:
        rdl_standard_schema = repo_root.parent / "rdl-standard" / "schema" / "rdls_schema.json"
        if rdl_standard_schema.exists():
            schema_path = rdl_standard_schema
        else:
            schema_path = schema_dir / "rdls_schema_v1.0.json"
    if not schema_path.exists():
        print(f"ERROR: Schema file not found: {schema_path}")
        return 1

    # Resolve codelists path
    if args.codelists:
        codelists_dir = Path(args.codelists)
    else:
        rdl_standard = repo_root.parent / "rdl-standard" / "schema" / "codelists"
        if rdl_standard.exists():
            codelists_dir = rdl_standard
        else:
            codelists_dir = schema_dir / "codelists"

    if not codelists_dir.exists():
        print(f"WARNING: Codelists directory not found: {codelists_dir}")
        print("  Layer 2 and parts of Layer 3 will be skipped.")
        print(f"  Clone rdl-standard repo or specify --codelists path")

    if not args.metadata:
        print("ERROR: Specify a metadata file to validate, or use --enrich for enrichment mode.")
        return 1

    metadata_path = Path(args.metadata)
    if not metadata_path.exists():
        print(f"ERROR: Metadata file not found: {metadata_path}")
        return 1

    print(f"Validating: {metadata_path.name}")
    print(f"Schema:     {schema_path.name}")
    print(f"Codelists:  {codelists_dir}")
    print()

    with open(schema_path, encoding="utf-8") as f:
        schema = json.load(f)

    with open(metadata_path, encoding="utf-8") as f:
        raw = json.load(f)

    # Handle both wrapped {"datasets": [...]} and unwrapped formats
    if "datasets" in raw and isinstance(raw["datasets"], list):
        datasets = raw["datasets"]
        print(f"Found {len(datasets)} dataset(s) in wrapper\n")
    else:
        datasets = [raw]
        print("Single dataset (unwrapped)\n")

    registry = CodelistRegistry(codelists_dir)

    all_valid = True
    for i, dataset in enumerate(datasets):
        if len(datasets) > 1:
            print(f"\n{'#'*70}")
            print(f"Dataset {i+1}: {dataset.get('id', '(no id)')}")
            print(f"{'#'*70}")

        result = validate(dataset, schema, registry)
        print(result.summary())

        if not result.is_valid:
            all_valid = False

    return 0 if all_valid else 1


# ---------------------------------------------------------------------------
# Enrich mode
# ---------------------------------------------------------------------------

def run_enrich(paths: list) -> int:
    """Post-conversion enrichment. Returns exit code."""
    files = resolve_files(paths, PROJECT_ROOT)
    if not files:
        print("No matching files found.")
        return 1

    print(f"\n=== Post-conversion enrichment: {len(files)} file(s) ===\n")

    from collections import Counter
    total: Counter = Counter()
    all_warnings = []

    for path in files:
        try:
            result = fix_file(path)
        except Exception as e:
            print(f"  ERROR {path.name}: {e}")
            continue

        c = result["counts"]
        parts = []
        if c["unit_count"]:       parts.append(f"unit=count x{c['unit_count']}")
        if c["uri_fix"]:          parts.append(f"URI x{c['uri_fix']}")
        if c["scheme_custom_removed"]: parts.append(f"Custom-removed x{c['scheme_custom_removed']}")
        if c["license_removed"]:  parts.append(f"license-removed x{c['license_removed']}")
        if c["im_fix"]:           parts.append(f"IM=wd:m x{c['im_fix']}")
        if c["format_removed"]:   parts.append(f"format-removed x{c['format_removed']}")
        if c["conforms_to_added"]:parts.append(f"conforms_to x{c['conforms_to_added']}")
        status = ", ".join(parts) if parts else "no changes needed"
        print(f"  {path.name}: {status}")

        for k in c:
            total[k] += c[k]

        if result["warnings"]:
            all_warnings.append((path.name, result["warnings"]))

    print(f"\nTotal: unit_count={total['unit_count']} uri={total['uri_fix']} "
          f"custom-removed={total['scheme_custom_removed']} license={total['license_removed']} "
          f"im={total['im_fix']} format-removed={total['format_removed']} "
          f"conforms_to={total['conforms_to_added']}")

    if all_warnings:
        print("\n=== Manual attention required ===\n")
        for fname, warns in all_warnings:
            print(f"  {fname}:")
            for w in warns:
                print(w)
        print(
            "\nFor each item above, create a dataset-specific enrichment script in temp/\n"
            "following the pattern in temp/enrich_wbgufra_asset_type.py.\n"
            "See .claude/v1.0-reference.md -> Post-conversion checklist for the full guide."
        )
    else:
        print("\nNo manual fixes needed for asset_type fields.")

    print("\n=== Reminders (manual checks always required) ===")
    print("  [ ] asset_type: id/title/description/scheme/uri -> dataset-specific enrichment script")
    print("  [ ] GED4ALL scheme: set in specific script (NOT auto-detected by this script)")
    print("  [ ] media_type: if format has no IANA code, keep format and leave media_type absent")
    print("      Check rdl-standard/schema/codelists/open/media_type.csv for available codes")
    print("  [ ] conforms_to: OGC_API maps to Features 1.0.1 - override if resource is Coverages/Tiles")
    print("  [ ] Backup/rename: original v0.3 -> <stem>_v03.json, v1.0 keeps original name")
    print("  [ ] Layer 1 validation: python scripts/validate_records.py <file>")

    return 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="RDLS v1.0 Metadata Validator and Enrichment Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Examples:\n"
            "  Validate:  python scripts/validate_records.py output/my_record.json\n"
            "  Enrich:    python scripts/validate_records.py --enrich output/new-collection/**/*.json\n"
            "  Enrich:    python scripts/validate_records.py --enrich output/some/file.json"
        ),
    )
    parser.add_argument(
        "metadata",
        nargs="?",
        help="Path to RDLS JSON metadata file (audit mode)"
    )
    parser.add_argument("--schema", default=None, help="Path to RDLS v1.0 JSON schema file")
    parser.add_argument("--codelists", default=None, help="Path to codelists directory (with closed/ and open/ subdirs)")
    parser.add_argument(
        "--enrich",
        nargs="+",
        metavar="PATH",
        help="Enrich mode: apply post-conversion fixes to file(s), directory, or glob pattern"
    )
    args = parser.parse_args()

    if args.enrich:
        sys.exit(run_enrich(args.enrich))
    else:
        sys.exit(run_audit(args))


if __name__ == "__main__":
    main()
