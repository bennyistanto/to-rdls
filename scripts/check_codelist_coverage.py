"""Guard against the "template silently misses a codelist" failure: verify that
EVERY codelist CSV on disk (rdl-standard/schema/codelists/{open,closed}) is
represented in the template's _codelists section. FAILS (exit 1) listing any
codelist not covered.

Run after every schema/codelist resync. If the resync adds a new codelist, this
fails until it is wired into the template - so "the template is complete" becomes
a checkable fact, not an assumption. Catches exactly the class that let
taxonomy_ged4all slip (a codelist on disk the schema never references).

Usage:
    python scripts/check_codelist_coverage.py [--template PATH] [--codelists DIR]
"""
from __future__ import annotations
import json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_TEMPLATE = ROOT / "schema" / "rdls_template_v1.0.json"
DEFAULT_CODELISTS = ROOT.parent / "rdl-standard" / "schema" / "codelists"


def main():
    args = sys.argv[1:]
    tpl = DEFAULT_TEMPLATE
    cld = DEFAULT_CODELISTS
    if "--template" in args:
        tpl = Path(args[args.index("--template") + 1])
    if "--codelists" in args:
        cld = Path(args[args.index("--codelists") + 1])

    doc = json.loads(tpl.read_text(encoding="utf-8"))
    section = doc.get("_codelists", {})
    covered = {k.lower() for k in section.keys() if not k.startswith("_")}

    on_disk = {}
    for sub in ("open", "closed"):
        d = cld / sub
        if d.exists():
            for f in d.glob("*.csv"):
                on_disk[f.name.lower()] = sub

    missing = sorted(n for n in on_disk if n not in covered)
    extra = sorted(c for c in covered if c not in on_disk)  # in template but not on disk (stale)

    print(f"template     : {tpl}")
    print(f"codelists dir: {cld}")
    print(f"on disk: {len(on_disk)}   covered by template _codelists: {len(covered)}\n")

    if missing:
        print(f"FAIL - {len(missing)} codelist(s) on disk NOT in template _codelists:")
        for m in missing:
            print(f"   [{on_disk[m]}] {m}")
    else:
        print("OK - every codelist on disk is represented in the template _codelists.")
    if extra:
        print(f"\nNOTE - {len(extra)} entry(ies) in template _codelists not found on disk (stale?):")
        for e in extra:
            print(f"   {e}")
    return 1 if missing else 0


if __name__ == "__main__":
    sys.exit(main())
