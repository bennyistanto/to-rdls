"""Migrate loss `impact_and_losses` from the v0.3 FLAT shape to the v1.0 NESTED
shape, and migrate superseded impact_metric vocabulary to the current codelist.

v0.3 (what we wrongly emitted):
    "impact_and_losses": {
      "impact_type": "direct", "impact_modelling": "simulated",
      "impact_metric": "economic_loss_value",
      "measurement": {"quantity_kind": "currency", "unit": "EUR"},
      "loss_type": ..., "loss_approach": ..., "loss_frequency_type": ...
    }

v1.0 ($defs/Impact, schema lines 1369-1382, 1823-1854):
    "impact_and_losses": {
      "impact": {
        "type": "direct", "modelling": "simulated", "metric": "loss",
        "measurement": {"quantity_kind": "currency", "unit": "EUR"}
      },
      "loss_type": ..., "loss_approach": ..., "loss_frequency_type": ...
    }

This is a pure structural relocation (no impact VALUES invented) plus an
impact_metric rename using the authoritative map in src/audit.IMPACT_METRIC_OBSOLETE.
Values not in that map (e.g. casualty_count) are KEPT verbatim - they are valid
custom open-codelist values. Vulnerability function impact.metric is migrated too.

valuation_year is NOT added: it cannot be fabricated (the source year is
unknown); it is optional in the schema. Currency losses lacking it are reported.

Usage:
    python scripts/fix_loss_impact_structure.py <dir> [...] [--dry-run]
"""
from __future__ import annotations
import json, re, sys
from pathlib import Path
from collections import Counter

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
from src.audit import IMPACT_METRIC_OBSOLETE

BACKUP = re.compile(r"_v0\.?3\.json$", re.I)
# v0.3 flat key -> v1.0 nested Impact key
_FLAT_TO_NESTED = {
    "impact_type": "type",
    "impact_modelling": "modelling",
    "impact_metric": "metric",
    "impact_loss_statistic": "loss_statistic",
    "loss_statistic": "loss_statistic",
    "measurement": "measurement",
}


def _migrate_metric(metric, metric_renames: Counter):
    if metric and metric in IMPACT_METRIC_OBSOLETE:
        new = IMPACT_METRIC_OBSOLETE[metric]
        metric_renames[f"{metric}->{new}"] += 1
        return new
    return metric


def fix_record(rec, stats: Counter, cur_no_vyear: list):
    changed = 0
    loss = rec.get("loss")
    if isinstance(loss, dict):
        for l in loss.get("losses", []) or []:
            if not isinstance(l, dict):
                continue
            ial = l.get("impact_and_losses")
            if not isinstance(ial, dict):
                continue
            flat_present = [k for k in _FLAT_TO_NESTED if k in ial]
            if flat_present and "impact" not in ial:
                impact = {}
                for flat_key in list(ial.keys()):
                    if flat_key in _FLAT_TO_NESTED:
                        nested_key = _FLAT_TO_NESTED[flat_key]
                        val = ial.pop(flat_key)
                        if nested_key == "metric":
                            val = _migrate_metric(val, stats)
                        impact[nested_key] = val
                # place impact first for readability
                rebuilt = {"impact": impact}
                rebuilt.update(ial)
                l["impact_and_losses"] = rebuilt
                ial = rebuilt
                stats["loss_nested"] += 1
                changed += 1
            elif isinstance(ial.get("impact"), dict):
                # already nested -- still migrate a stale metric if present
                im = ial["impact"]
                new = _migrate_metric(im.get("metric"), stats)
                if new != im.get("metric"):
                    im["metric"] = new
                    changed += 1
            # report currency losses without valuation_year (cannot fabricate)
            meas = (ial.get("impact") or {}).get("measurement") or {}
            if isinstance(meas, dict) and meas.get("quantity_kind") == "currency" and "valuation_year" not in meas:
                cur_no_vyear.append(1)
    # Vulnerability function impact.metric
    vuln = rec.get("vulnerability")
    if isinstance(vuln, dict):
        for arr in (vuln.get("functions") or {}).values():
            if isinstance(arr, list):
                for fn in arr:
                    if isinstance(fn, dict) and isinstance(fn.get("impact"), dict):
                        im = fn["impact"]
                        new = _migrate_metric(im.get("metric"), stats)
                        if new != im.get("metric"):
                            im["metric"] = new
                            changed += 1
    return changed


def main():
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry-run" in sys.argv
    if not args:
        print("Usage: python scripts/fix_loss_impact_structure.py <dir> [...] [--dry-run]")
        return 1
    files = []
    for a in args:
        p = Path(a)
        if p.is_dir():
            files += [f for f in p.rglob("*.json") if not BACKUP.search(f.name)]
        else:
            files += [f for f in Path().glob(a) if not BACKUP.search(f.name)]
    files = sorted(set(files))

    stats = Counter()
    cur_no_vyear = []
    nfiles = 0
    for f in files:
        try:
            doc = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        recs = doc["datasets"] if isinstance(doc, dict) and isinstance(doc.get("datasets"), list) else [doc]
        changed = 0
        for rec in recs:
            if isinstance(rec, dict):
                changed += fix_record(rec, stats, cur_no_vyear)
        if changed:
            nfiles += 1
            if not dry:
                f.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"{'(dry-run) ' if dry else ''}files changed: {nfiles}")
    print(f"  loss blocks nested (flat->impact): {stats.pop('loss_nested', 0)}")
    if stats:
        print("  impact_metric migrated:")
        for k, n in stats.most_common():
            print(f"     {n:>5}x  {k}")
    if cur_no_vyear:
        print(f"  NOTE: {len(cur_no_vyear)} currency losses have no valuation_year "
              f"(optional; cannot fabricate - flag for source).")
    return 0


if __name__ == "__main__":
    sys.exit(main())
