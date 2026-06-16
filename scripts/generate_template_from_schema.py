"""Generate the annotated RDLS v1.0 template DIRECTLY from the published schema,
so the template is provably complete (every property, correct nesting, current
codelists) and cannot drift from the schema the way the hand-built template did.

Why: the previous schema/rdls_template_v1.0.json was built from an outdated MDE /
schema snapshot. It encoded the FLAT loss structure (impact_type/impact_metric on
impact_and_losses) and stale impact_metric examples, and the translator propagated
that to every loss record. Deriving the template from the schema removes the
guesswork: walk $defs, resolve $ref / allOf, emit each property with its codelist
and required-ness exactly as the schema declares it.

The output is an ANNOTATED template (not a concrete instance): objects are dicts
with a "_req" annotation; leaves are placeholder strings "{type/codelist} REQUIRED|
optional [notes]"; arrays are {"_req": ..., "_item": <template>}. A companion
checker (scripts/check_template_against_schema.py) walks template+schema in
parallel to prove no property is missing or extra.

Usage:
    python scripts/generate_template_from_schema.py [--schema PATH] [--out PATH]
"""
from __future__ import annotations
import csv, json, sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CODELISTS_DIR = PROJECT_ROOT.parent / "rdl-standard" / "schema" / "codelists"
DISPLAY_CAP = 60  # list all values up to this many; above, sample + count + source


def load(p):
    return json.loads(Path(p).read_text(encoding="utf-8"))


def _read_csv(path):
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def _codes(rows):
    return [r["Code"].strip() for r in rows if (r.get("Code") or "").strip()]


def _grouped(rows, by_col, split=False):
    """Group Code values by a column. If split, the column may hold a
    comma-separated list of keys (e.g. imt.csv Hazard = 'flood,tsunami')."""
    out = {}
    for r in rows:
        raw = (r.get(by_col) or "").strip()
        c = (r.get("Code") or "").strip()
        if not (raw and c):
            continue
        keys = [k.strip() for k in raw.split(",")] if split else [raw]
        for k in keys:
            if k:
                out.setdefault(k, []).append(c)
    return out


def build_codelists(schema):
    """Comprehensive codelist reference covering EVERY CSV on disk (open+closed)
    plus the schema's conditional per-type maps and the two data-model linkages
    (taxonomy_ged4all, imt) that a pure schema-walk cannot see."""
    defs = schema.get("$defs", {})
    out = {}
    for sub in ("closed", "open"):
        d = CODELISTS_DIR / sub
        if not d.exists():
            continue
        for f in sorted(d.glob("*.csv")):
            rows = _read_csv(f)
            cs = _codes(rows)
            name = f.name
            if name == "imt.csv":
                out[name] = {
                    "type": sub, "count": len(cs), "custom_allowed": True,
                    "note": "intensity_measure values; depends on hazard type (validator filters by Hazard column)",
                    "by_hazard_type": _grouped(rows, "Hazard", split=True),
                }
            elif name == "taxonomy_ged4all.csv":
                out[name] = {
                    "type": sub, "count": len(cs),
                    "applies_to": "asset_type.id (exposure/loss/vuln) when scheme='GED4ALL' - NOT referenced by the JSON Schema",
                    "by_exposure_category": _grouped(rows, "Exposure_category"),
                }
            elif name == "process_type.csv":
                # base list + per-hazard-type subset from the schema conditional
                proc = {}
                for e in defs.get("conditional_hazard_type_to_process", {}).get("allOf", []):
                    k = e.get("if", {}).get("properties", {}).get("type", {}).get("const")
                    v = e.get("then", {}).get("properties", {}).get("process", {}).get("enum")
                    if k and v:
                        proc[k] = v
                out[name] = {"type": sub, "count": len(cs), "by_hazard_type": proc}
            else:
                entry = {"type": sub, "count": len(cs)}
                if sub == "open":
                    entry["custom_allowed"] = True
                if len(cs) <= DISPLAY_CAP:
                    entry["values"] = cs
                else:
                    entry["values_sample"] = cs[:20]
                    entry["note"] = f"{len(cs)} codes - full list at rdl-standard/schema/codelists/{sub}/{name}"
                out[name] = entry
    # map: quantity_kind -> which unit_*.csv (from the schema conditional)
    qk_unit = {}
    for e in defs.get("conditional_measurement_quantity_kind_to_unit", {}).get("allOf", []):
        k = e.get("if", {}).get("properties", {}).get("quantity_kind", {}).get("const")
        u = e.get("then", {}).get("properties", {}).get("unit", {}).get("codelist")
        if k and u:
            qk_unit[k] = u
    out["_unit_by_quantity_kind"] = qk_unit
    return out


class Generator:
    def __init__(self, schema):
        self.schema = schema
        self.defs = schema.get("$defs", {})

    def _deref(self, node):
        """Return (resolved_dict, sibling_overrides). Follows a single $ref and
        merges sibling keys (e.g. asset_type adds a 'required' override)."""
        if "$ref" in node:
            name = node["$ref"].split("/")[-1]
            target = dict(self.defs.get(name, {}))
            siblings = {k: v for k, v in node.items() if k != "$ref"}
            # sibling 'required'/'properties' override/extend the target
            for k, v in siblings.items():
                if k == "properties":
                    merged = dict(target.get("properties", {}))
                    merged.update(v)
                    target["properties"] = merged
                else:
                    target[k] = v
            return target
        return dict(node)

    def _collect(self, node):
        """Resolve $ref + allOf into a single effective schema with merged
        properties, required set, codelist info, and conditional notes."""
        node = self._deref(node)
        props = dict(node.get("properties", {}))
        required = set(node.get("required", []))
        notes = []
        for sub in node.get("allOf", []):
            sub = self._deref(sub)
            # conditional_* dependency refs
            if "if" in sub or "then" in sub:
                notes.append("conditional (see schema allOf if/then)")
                # still surface any unconditionally-required props in then
                then = sub.get("then", {})
                required |= set(then.get("required", []))
                continue
            cprops, creq, cnotes, _ = self._collect_raw(sub)
            props.update(cprops)
            required |= creq
            notes += cnotes
        return props, required, notes, node

    def _collect_raw(self, node):
        props = dict(node.get("properties", {}))
        required = set(node.get("required", []))
        notes = []
        for sub in node.get("allOf", []):
            sub = self._deref(sub)
            if "if" in sub or "then" in sub:
                notes.append("conditional")
                required |= set(sub.get("then", {}).get("required", []))
                continue
            cp, cr, cn, _ = self._collect_raw(sub)
            props.update(cp); required |= cr; notes += cn
        return props, required, notes, node

    def _leaf_annot(self, sch, req_word):
        sch = self._deref(sch)
        # codelist?
        cl = sch.get("codelist")
        enum = sch.get("enum")
        t = sch.get("type")
        fmt = sch.get("format")
        pat = sch.get("pattern")
        if enum:
            kind = "enum:" + "|".join(map(str, enum[:8])) + ("|..." if len(enum) > 8 else "")
        elif cl:
            kind = ("open:" if sch.get("openCodelist") else "closed:") + cl
        elif fmt:
            kind = f"format:{fmt}"
        elif pat:
            kind = f"pattern:{pat}"
        elif t:
            kind = t
        else:
            kind = "value"
        extra = []
        if sch.get("title"):
            extra.append(sch["title"])
        return f"{{{kind}}} {req_word}" + (f"  - {extra[0]}" if extra else "")

    def build(self, node, req_word="optional", depth=0, name_hint=None):
        props, required, notes, raw = self._collect(node)
        t = raw.get("type")

        # array
        if t == "array" or "items" in raw:
            item = raw.get("items", {})
            arr_note = "REQUIRED" if req_word.startswith("REQUIRED") else "optional"
            mi = raw.get("minItems")
            if mi:
                arr_note += f" (array, min {mi} item{'s' if mi != 1 else ''})"
            else:
                arr_note += " (array)"
            return {"_req": arr_note, "_item": self.build(item, "REQUIRED", depth + 1)}

        # object
        if props or t == "object":
            out = {}
            note = req_word
            if notes:
                note += " | " + "; ".join(sorted(set(notes)))
            out["_req"] = note
            for name, psch in props.items():
                child_req = "REQUIRED" if name in required else "optional"
                child = self._collect(psch)
                cprops, _, _, craw = child
                if cprops or craw.get("type") == "object" or "items" in craw or craw.get("type") == "array":
                    sub = self.build(psch, child_req, depth + 1, name_hint=name)
                    out[name] = sub
                else:
                    out[name] = self._leaf_annot(psch, child_req)
            # data-model linkage the JSON Schema does not encode: asset_type is a
            # Classification whose `id` should be a taxonomy_ged4all code when
            # scheme=GED4ALL (recommended). Make that explicit at the field level.
            if name_hint == "asset_type" and isinstance(out.get("id"), str):
                out["id"] = (out["id"] +
                    "  - when scheme='GED4ALL', use a taxonomy_ged4all.csv code for the "
                    "exposure_category (e.g. buildings='bui', population='sei-pop', "
                    "roads='trs-rod', power='pwr-lin'); see _codelists.taxonomy_ged4all.csv")
            return out

        # leaf
        return self._leaf_annot(node, req_word)

    def generate(self):
        root_req = set(self.schema.get("required", []))
        props = self.schema.get("properties", {})
        rec = {"_req": "RDLS v1.0 dataset record"}
        for name, psch in props.items():
            req_word = "REQUIRED" if name in root_req else "optional"
            child = self._collect(psch)
            cprops, _, _, craw = child
            if cprops or craw.get("type") in ("object", "array") or "items" in craw:
                rec[name] = self.build(psch, req_word, name_hint=name)
            else:
                rec[name] = self._leaf_annot(psch, req_word)
        _inject_hazard_ids(rec)
        return rec


_HAZ_ID_ANNOT = ("{string}  REQUIRED  - local hazard identifier; REQUIRED on EVERY hazard object "
                 "by the JKAN/DDH consumer (it reads hazard.id unconditionally). The JSON Schema "
                 "only requires it on event_set.hazards[], so it must be added everywhere else too.")


def _inject_hazard_ids(node):
    """Data-model linkage the schema doesn't encode: every hazard object needs `id`
    (consumer requirement). The schema defines `id` only on event_set.hazards[], so
    loss.hazard / event.hazard / vuln hazard_primary|secondary / trigger lack it in a
    pure schema-walk. A hazard node is a dict whose `type` annotation is the
    hazard_type enum (contains 'dust_sand_storm', a hazard-only code)."""
    if isinstance(node, dict):
        # a hazard object has a `type` leaf plus `process` and/or `intensity_measure`
        # (SimpleHazard/Hazard/trigger); impact.type has neither sibling, so excluded.
        is_hazard = ("type" in node and isinstance(node.get("type"), str)
                     and ("process" in node or "intensity_measure" in node))
        if is_hazard and "id" not in node:
            # put id first
            items = [("_req", node.get("_req", "optional"))] if "_req" in node else []
            items.append(("id", _HAZ_ID_ANNOT))
            items += [(k, v) for k, v in node.items() if k not in ("_req", "id")]
            node.clear(); node.update(items)
        for v in node.values():
            _inject_hazard_ids(v)
    elif isinstance(node, list):
        for v in node:
            _inject_hazard_ids(v)


def main():
    args = sys.argv[1:]
    schema_path = PROJECT_ROOT / "schema" / "rdls_schema_v1.0.json"
    out_path = PROJECT_ROOT / "schema" / "rdls_template_v1.0.generated.json"
    if "--schema" in args:
        schema_path = Path(args[args.index("--schema") + 1])
    if "--out" in args:
        out_path = Path(args[args.index("--out") + 1])

    schema = load(schema_path)
    gen = Generator(schema)
    record = gen.generate()
    codelists = build_codelists(schema)
    doc = {
        "_comment": "RDLS v1.0 annotated template - GENERATED from schema/rdls_schema_v1.0.json "
                    "(= https://docs.riskdatalibrary.org/en/1__0__0/rdls_schema.json) AND the "
                    "rdl-standard codelists directory. Do not hand-edit; regenerate via "
                    "scripts/generate_template_from_schema.py. Verify with "
                    "scripts/check_template_against_schema.py + scripts/check_codelist_coverage.py.",
        "_legend": "Each object has a _req note (REQUIRED/optional + conditionals). Leaves are "
                   "'{type|enum|open:codelist|closed:codelist|format} REQUIRED|optional'. Arrays use "
                   "_item for the element template. _codelists lists EVERY codelist (open+closed) with "
                   "its values, plus the conditional per-type maps and the GED4ALL/IMT data-model linkages.",
        "_codelists": codelists,
        "datasets": [record],
    }
    out_path.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    # quick stats
    def count(node):
        n = 0
        if isinstance(node, dict):
            for k, v in node.items():
                if k.startswith("_"):
                    continue
                n += 1 + count(v)
        elif isinstance(node, list):
            for v in node:
                n += count(v)
        return n
    print(f"wrote {out_path}")
    print(f"total field nodes: {count(record)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
