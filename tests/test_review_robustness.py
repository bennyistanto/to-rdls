"""Run review on all Tomorrow Cities + Phu Quoc + Sierra Leone datasets."""
import sys, os, traceback, time, json
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
from src.review import review_folder, render_review_markdown

script_dir = os.path.dirname(os.path.abspath(__file__))
BASE_TC = os.path.normpath(os.path.join(script_dir, "..", "output", "tomorrow-cities", "data"))
targets = {}

# All Tomorrow Cities datasets
if os.path.isdir(BASE_TC):
    for city in sorted(os.listdir(BASE_TC)):
        city_path = os.path.join(BASE_TC, city)
        if os.path.isdir(city_path):
            targets[f"TC-{city}"] = city_path

# Phu Quoc
pq_path = os.path.normpath(os.path.join(script_dir, "..", "output", "wbg-ufra", "vnm-phuquoc", "data"))
if os.path.isdir(pq_path):
    targets["PhuQuoc"] = pq_path

# Sierra Leone - Freetown
sle_path = os.path.normpath(os.path.join(script_dir, "..", "output", "wbg-ufra", "sle-floodrisk", "data"))
if os.path.isdir(sle_path):
    targets["SLE-Freetown"] = sle_path

print(f"Running review on {len(targets)} datasets...\n")
results_summary = []

for name, target in targets.items():
    print(f"\n{'='*70}")
    print(f"  {name}")
    print(f"  {target}")
    print(f"{'='*70}")
    t0 = time.time()
    try:
        result = review_folder(target, max_inspect=30, verbose=True)
        elapsed = time.time() - t0
        summary = {
            "name": name,
            "elapsed": round(elapsed, 1),
            "files": len(result.inspections),
            "groups": len(result.file_groups),
            "datasets": len(result.suggested_datasets),
            "quality_issues": len(result.quality_issues),
            "intermediate": result.intermediate_files.get("total_excluded", 0),
            "hevl": sorted(set(c for g in result.file_groups for c in g.hevl)),
            "status": "OK",
        }
        results_summary.append(summary)
        print(f"\n  Completed in {elapsed:.1f}s")
        print(f"  Files inspected   : {summary['files']}")
        print(f"  Groups found      : {summary['groups']}")
        print(f"  Datasets suggested: {summary['datasets']}")
        print(f"  Quality issues    : {summary['quality_issues']} groups")
        print(f"  Intermediate excl : {summary['intermediate']}")
        print(f"  HEVL coverage     : {','.join(summary['hevl'])}")
        print(f"\n  Groups:")
        for g in result.file_groups:
            hevl = ",".join(g.hevl) if g.hevl else "?"
            print(f"    {g.name}: {hevl} ({g.confidence}, {len(g.files)} files)")
    except Exception as e:
        elapsed = time.time() - t0
        results_summary.append({
            "name": name,
            "elapsed": round(elapsed, 1),
            "status": f"FAILED: {type(e).__name__}: {e}",
        })
        print(f"\n  FAILED in {elapsed:.1f}s: {type(e).__name__}: {e}")
        traceback.print_exc()

# Print summary table
print(f"\n\n{'='*70}")
print("  SUMMARY")
print(f"{'='*70}")
print(f"{'Dataset':<20} {'Status':<8} {'Time':>6} {'Groups':>7} {'Inspect':>8} {'RDLS DS':>8} {'Interm':>7} {'HEVL':<10}")
print("-" * 90)
for s in results_summary:
    if s["status"] == "OK":
        print(f"{s['name']:<20} {'OK':<8} {s['elapsed']:>5.1f}s {s['groups']:>7} {s['files']:>8} {s['datasets']:>8} {s['intermediate']:>7} {','.join(s['hevl']):<10}")
    else:
        print(f"{s['name']:<20} {'FAIL':<8} {s['elapsed']:>5.1f}s {s['status']}")

print(f"\n{'='*70}")
print("Done.")
