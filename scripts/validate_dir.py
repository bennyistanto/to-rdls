"""Batch-validate every RDLS v1.0 JSON under a directory against the BAKED
schema (schema/rdls_schema_v1.0.json) + codelists + semantic layer.

Usage:
    python scripts/validate_dir.py <dir-or-glob> [--show N] [--errors-only]

Prints a per-file PASS/FAIL summary, a deduplicated error-pattern tally
(so you can see the shape of the failures across many files), and a final
count. Read-only — never edits files.
"""
from __future__ import annotations
import json, re, sys
from collections import Counter, defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from src.utils import load_json
from src.audit import (CodelistRegistry, ValidationResult,
    validate_layer1_schema, validate_layer2_codelists, validate_layer3_semantic,
    validate_layer4_consistency, validate_layer5_mde_rules)

SCHEMA = PROJECT_ROOT / "schema" / "rdls_schema_v1.0.json"
CODELISTS = PROJECT_ROOT.parent / "rdl-standard" / "schema" / "codelists"


def iter_records(path: Path):
    """Yield (file, record) for each dataset. Handles {datasets:[...]} + bare."""
    raw = load_json(str(path))
    if isinstance(raw, dict) and isinstance(raw.get("datasets"), list):
        for rec in raw["datasets"]:
            yield path, rec
    else:
        yield path, raw


def norm_msg(layer, path, msg):
    """Collapse a message to a pattern for tallying (strip indices + values)."""
    p = re.sub(r"\b\d+\b", "*", path)
    if layer == "schema":
        # keep the rule, drop the instance dump
        if "is not valid under any of the given schemas" in msg:
            return (layer, p, "resource fails anyOf (missing media_type/format or url)")
        m = re.match(r"'([^']+)' is not one of", msg)
        if m:
            return (layer, p, f"value not in enum")
        m2 = re.match(r"'([^']+)' is a required property", msg)
        if m2:
            return (layer, p, f"missing required '{m2.group(1)}'")
        return (layer, p, msg[:60])
    return (layer, p, msg[:70])


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    show = 8
    errors_only = "--errors-only" in sys.argv
    if "--show" in sys.argv:
        show = int(sys.argv[sys.argv.index("--show")+1])
    if not args:
        print("Usage: python scripts/validate_dir.py <dir-or-glob> [--show N] [--errors-only]")
        return 1

    include_backups = "--include-backups" in sys.argv
    target = args[0]
    p = Path(target)
    if p.is_dir():
        files = sorted(p.rglob("*.json"))
    else:
        # glob relative to project root
        files = sorted(PROJECT_ROOT.glob(target)) if not Path(target).is_absolute() else sorted(Path().glob(target))
    # Skip v0.3 backup files (convention: <stem>_v03.json / _v0.3.json) — they
    # are intentional pre-conversion backups, NOT v1.0 upload candidates.
    backup_re = re.compile(r"_v0\.?3\.json$", re.I)
    if not include_backups:
        skipped = [f for f in files if backup_re.search(f.name)]
        files = [f for f in files if not backup_re.search(f.name)]
        if skipped:
            print(f"(skipping {len(skipped)} v0.3 backup file(s); use --include-backups to include)\n")
    if not files:
        print(f"No JSON files found at: {target}")
        return 1

    schema = load_json(str(SCHEMA))
    reg = CodelistRegistry(CODELISTS)

    npass = nfail = 0
    pattern_tally = Counter()
    failing = []
    total_records = 0

    for f in files:
        try:
            recs = list(iter_records(f))
        except Exception as e:
            nfail += 1; failing.append((f, [("load", "(file)", str(e)[:80])])); continue
        file_errs = []
        for _, rec in recs:
            total_records += 1
            r = ValidationResult()
            validate_layer1_schema(rec, schema, r)
            validate_layer2_codelists(rec, reg, r)
            validate_layer3_semantic(rec, reg, r)
            validate_layer4_consistency(rec, r); validate_layer5_mde_rules(rec, r)
            for e in r.errors:
                file_errs.append((e["layer"], e["path"], e["message"]))
                pattern_tally[norm_msg(e["layer"], e["path"], e["message"])] += 1
        if file_errs:
            nfail += 1; failing.append((f, file_errs))
        else:
            npass += 1

    print(f"Schema: {SCHEMA.name} (baked)   Files: {len(files)}   Records: {total_records}")
    print(f"PASS: {npass}   FAIL: {nfail}")
    print()
    if pattern_tally:
        print("=== error patterns (deduplicated, by frequency) ===")
        for (layer, path, msg), n in pattern_tally.most_common(40):
            print(f"  {n:>5}x  [{layer}] {path}: {msg}")
        print()
    if not errors_only and failing:
        print(f"=== first {show} failing files ===")
        for f, errs in failing[:show]:
            print(f"### {f.name}  ({len(errs)} errors)")
            seen=set()
            for layer, path, msg in errs:
                k=(layer,path,msg[:40])
                if k in seen: continue
                seen.add(k)
                print(f"     [{layer}] {path}: {msg[:90]}")
    return 0 if nfail == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
