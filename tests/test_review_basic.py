"""Run review on both Chattogram and Monrovia, print summary."""
import sys, os, traceback, time
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from src.review import review_folder, render_review_markdown

targets = {
    "Chattogram": os.path.join("..", "output", "tomorrow-cities", "data", "chattogram"),
    "Monrovia": os.path.join("..", "output", "wbg-ufra", "lbr-monrovia-floodrisk", "data"),
}

# Resolve relative to this script's location
script_dir = os.path.dirname(os.path.abspath(__file__))
targets = {k: os.path.normpath(os.path.join(script_dir, v)) for k, v in targets.items()}

for name, target in targets.items():
    print(f"\n{'='*70}")
    print(f"  {name}")
    print(f"{'='*70}")
    t0 = time.time()
    try:
        result = review_folder(target, max_inspect=30, verbose=True)
        elapsed = time.time() - t0
        print(f"\n  Completed in {elapsed:.1f}s")
        print(f"  Files inspected   : {len(result.inspections)}")
        print(f"  Groups found      : {len(result.file_groups)}")
        print(f"  Datasets suggested: {len(result.suggested_datasets)}")
        print(f"  Quality issues    : {len(result.quality_issues)} groups")
        print(f"\n  Groups:")
        for g in result.file_groups:
            hevl = ",".join(g.hevl) if g.hevl else "?"
            naming = getattr(g, "_naming", None)
            extras = []
            if naming:
                if naming.get("scenarios"): extras.append(f"scenarios={naming['scenarios']}")
                if naming.get("return_periods"): extras.append(f"RPs={naming['return_periods']}")
                if naming.get("hazard_subtypes"): extras.append(f"subtypes={naming['hazard_subtypes']}")
                if naming.get("gmpes"): extras.append(f"GMPEs={naming['gmpes']}")
                if naming.get("intensity_measures"): extras.append(f"IMs={naming['intensity_measures']}")
                if naming.get("asset_types"): extras.append(f"assets={naming['asset_types']}")
            extra_str = f"\n      [{'; '.join(extras)}]" if extras else ""
            print(f"    {g.name}: {hevl} ({g.confidence}, {len(g.files)} files){extra_str}")
    except Exception as e:
        print(f"\n  FAILED: {type(e).__name__}: {e}")
        traceback.print_exc()

print(f"\n{'='*70}")
print("Done.")
