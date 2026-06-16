"""Merge the schema-derived generated template INTO the hand-written template:
keep the rich human annotations, add every missing field, fix the stale flat-loss
structure. Output replaces schema/rdls_template_v1.0.json.

Strategy:
  1. Canonicalise both templates' arrays to the {"_req", "_item"} form so the same
     array isn't list in one and _item-dict in the other.
  2. Merge dict-by-dict: the current (rich) value wins for keys it already has;
     missing keys are grafted from the generated (schema-accurate) template;
     stub objects (no _item) get their _item filled from generated.
  3. Fix the loss block explicitly: drop the flat impact_type/impact_modelling/
     impact_metric/measurement (and the stale _note); the nested `impact` is grafted
     by the merge.
  4. Preserve top-level _comment / _legend / _cross_field_rules from the current doc.

Usage:
    python scripts/merge_template.py [--current P] [--generated P] [--out P]
Then verify with scripts/check_template_against_schema.py (expect 0 missing / 0 stale).
"""
from __future__ import annotations
import json, sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def load(p):
    return json.loads(Path(p).read_text(encoding="utf-8"))


def canon(node):
    """Normalise list arrays to {'_req':'(array)','_item':<canon item>}."""
    if isinstance(node, list):
        item = node[0] if node else None
        return {"_req": "(array)", "_item": canon(item)}
    if isinstance(node, dict):
        return {k: canon(v) for k, v in node.items()}
    return node


def is_obj(n):
    return isinstance(n, dict)


def merge(cur, gen):
    """current wins for existing scalar values; recurse into dicts; graft gen-only keys."""
    if is_obj(cur) and is_obj(gen):
        out = {}
        for k, v in cur.items():       # keep current order + values, recurse where gen has same key
            if k in gen:
                out[k] = merge(v, gen[k])
            else:
                out[k] = v
        for k, v in gen.items():        # graft fields the current template is missing
            if k not in cur:
                out[k] = v
        return out
    if is_obj(cur) and not is_obj(gen):
        return cur  # current is the richer structure
    if not is_obj(cur) and is_obj(gen):
        return gen  # current was a stub/leaf; take the full generated structure
    # both leaves -> keep current (richer annotation), unless empty
    return cur if (isinstance(cur, str) and cur.strip()) else gen


def fix_loss(rec):
    """Drop the v0.3 flat impact_* keys; nested impact is already grafted."""
    try:
        ial = rec["loss"]["losses"]["_item"]["impact_and_losses"]
    except (KeyError, TypeError):
        return
    for stale in ("impact_type", "impact_modelling", "impact_metric", "measurement", "_note"):
        ial.pop(stale, None)
    # order: impact first, then loss_type/approach/frequency_type
    if "impact" in ial:
        ordered = {"_req": ial.get("_req", "REQUIRED"), "impact": ial["impact"]}
        for k, v in ial.items():
            if k not in ("_req", "impact"):
                ordered[k] = v
        rec["loss"]["losses"]["_item"]["impact_and_losses"] = ordered


def main():
    args = sys.argv[1:]
    cur_p = ROOT / "schema" / "rdls_template_v1.0.json"
    gen_p = ROOT / "schema" / "rdls_template_v1.0.generated.json"
    out_p = ROOT / "schema" / "rdls_template_v1.0.json"
    if "--current" in args: cur_p = Path(args[args.index("--current") + 1])
    if "--generated" in args: gen_p = Path(args[args.index("--generated") + 1])
    if "--out" in args: out_p = Path(args[args.index("--out") + 1])

    cur_doc = load(cur_p)
    gen_doc = load(gen_p)
    cur_rec = canon(cur_doc["datasets"][0])
    gen_rec = canon(gen_doc["datasets"][0])

    merged = merge(cur_rec, gen_rec)
    fix_loss(merged)
    # the asset_type.id GED4ALL data-model hint is authoritative from the generator
    # (the schema can't express it); the merge would otherwise keep the terse current leaf
    try:
        gen_id = gen_rec["exposure"]["_item"]["asset_type"]["id"]
        if isinstance(gen_id, str) and "taxonomy_ged4all" in gen_id:
            merged["exposure"]["_item"]["asset_type"]["id"] = gen_id
    except (KeyError, TypeError):
        pass

    out_doc = {}
    for k in ("_comment", "_legend", "_cross_field_rules"):
        if k in cur_doc:
            out_doc[k] = cur_doc[k]
    # carry the comprehensive codelist reference from the generated (schema+disk) template
    if "_codelists" in gen_doc:
        out_doc["_codelists"] = gen_doc["_codelists"]
    out_doc["_comment"] = (cur_doc.get("_comment", "") +
        " | REVISED 2026-06: merged with schema-derived fields (nested loss impact, rights, "
        "hazard_secondary, fragility/damage_to_loss/engineering_demand function types). "
        "Verify with scripts/check_template_against_schema.py.").strip(" |")
    out_doc["datasets"] = [merged]
    out_p.write_text(json.dumps(out_doc, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {out_p}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
