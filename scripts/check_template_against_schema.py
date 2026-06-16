"""Audit a template against the schema-derived ground truth: report every schema
property MISSING from the template and every template field NOT in the schema
(stale / wrong). Uses the generated template (scripts/generate_template_from_schema.py
output) as the authoritative field set, since that is built directly from the schema.

Usage:
    python scripts/check_template_against_schema.py [--current PATH] [--generated PATH]
"""
from __future__ import annotations
import json, sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def load(p):
    return json.loads(Path(p).read_text(encoding="utf-8"))


def paths(node, prefix=""):
    """Set of dotted field paths in an annotated template. Skips _-keys; '_item'
    descends as the array element ('[]')."""
    out = set()
    if isinstance(node, dict):
        for k, v in node.items():
            if k == "_item":
                out |= paths(v, prefix + "[]")
                continue
            if k.startswith("_"):
                continue
            p = f"{prefix}.{k}" if prefix else k
            out.add(p)
            out |= paths(v, p)
    elif isinstance(node, list):
        # a real JSON list is the same as an _item array element: normalise to '[]'
        for v in node:
            out |= paths(v, prefix + "[]")
    return out


def record_of(doc):
    return doc["datasets"][0] if isinstance(doc, dict) and "datasets" in doc else doc


def main():
    args = sys.argv[1:]
    cur = PROJECT_ROOT / "schema" / "rdls_template_v1.0.json"
    gen = PROJECT_ROOT / "schema" / "rdls_template_v1.0.generated.json"
    if "--current" in args:
        cur = Path(args[args.index("--current") + 1])
    if "--generated" in args:
        gen = Path(args[args.index("--generated") + 1])

    cur_paths = paths(record_of(load(cur)))
    gen_paths = paths(record_of(load(gen)))

    missing = sorted(gen_paths - cur_paths)   # schema has it, template lacks it
    extra = sorted(cur_paths - gen_paths)     # template has it, schema doesn't

    print(f"current template : {cur}")
    print(f"ground truth      : {gen}")
    print(f"schema field paths: {len(gen_paths)}   current template paths: {len(cur_paths)}\n")

    print(f"MISSING from current template ({len(missing)}) - schema fields not represented:")
    for p in missing:
        print(f"   - {p}")
    print(f"\nNOT IN SCHEMA ({len(extra)}) - stale / wrong fields in current template:")
    for p in extra:
        print(f"   ! {p}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
