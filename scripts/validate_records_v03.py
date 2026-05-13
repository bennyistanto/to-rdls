"""
validate_records_v03.py - RDLS v0.3 Semantic Validation CLI
============================================================
Thin CLI wrapper over src/validate_v03.py.

Complements JSON Schema validation by checking semantic rules that
jsonschema cannot enforce: open codelist values, single-value fields,
IANA link relations, cross-field consistency.

Usage:
    python scripts/validate_records_v03.py <metadata.json> [<metadata2.json> ...]
    python scripts/validate_records_v03.py output/rdls/*.json
    python scripts/validate_records_v03.py output/rdls/my_record.json --schema schema/rdls_schema_v0.3.json
"""

import argparse
import json
import sys
from pathlib import Path

# Add project root to path so src/ is importable when run as script
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.validate_v03 import validate_semantic, SemanticIssue


def main():
    parser = argparse.ArgumentParser(description="RDLS v0.3 Semantic Validator")
    parser.add_argument("metadata", nargs="+", help="Path(s) to RDLS v0.3 JSON metadata file(s)")
    parser.add_argument("--schema", default=None, help="Path to RDLS v0.3 JSON schema (optional, for future use)")
    parser.add_argument("--errors-only", action="store_true", help="Show only errors, not warnings or info")
    args = parser.parse_args()

    schema = None
    if args.schema:
        schema_path = Path(args.schema)
        if not schema_path.exists():
            print(f"ERROR: Schema file not found: {schema_path}")
            sys.exit(1)
        with open(schema_path, encoding="utf-8") as f:
            schema = json.load(f)

    all_clean = True

    for metadata_arg in args.metadata:
        metadata_path = Path(metadata_arg)
        if not metadata_path.exists():
            print(f"ERROR: File not found: {metadata_path}")
            all_clean = False
            continue

        with open(metadata_path, encoding="utf-8") as f:
            record = json.load(f)

        issues = validate_semantic(record, schema)

        if args.errors_only:
            issues = [i for i in issues if i.severity == "error"]

        errors = [i for i in issues if i.severity == "error"]
        warnings = [i for i in issues if i.severity == "warning"]
        infos = [i for i in issues if i.severity == "info"]

        print(f"\n{'='*60}")
        print(f"File: {metadata_path.name}")
        print(f"{'='*60}")

        if not issues:
            print("PASSED - no semantic issues found")
        else:
            for issue in issues:
                print(issue)
            print()
            print(f"Summary: {len(errors)} error(s), {len(warnings)} warning(s), {len(infos)} info(s)")

        if errors:
            all_clean = False

    sys.exit(0 if all_clean else 1)


if __name__ == "__main__":
    main()
