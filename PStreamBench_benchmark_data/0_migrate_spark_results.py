#!/usr/bin/env python3
"""Move Spark benchmark sweep result files from
PStreamBench_controller/dsp_be/<Job Class>/ into the organized
artifacts/ structure, matching the existing Flink/Storm layout:
<app>/<hardware>/spark/[<query-variant>/]<event-rate>/

Only moves files whose metaInfo matches one of the real sweep's event rates
(100000/500000/1000000) -- ad-hoc validation/reproduction test runs (e.g.
producer_event_per_second=4000) are left in place. Safe to re-run: copies
rather than deletes the originals, and re-running just overwrites the same
destination files.
"""
import json
import shutil
from pathlib import Path

SRC_ROOT = Path("/Users/pratyushagnihotri/AI_Agent/PStreamBench/PStreamBench_controller/dsp_be")
DST_ROOT = Path(__file__).resolve().parent / "artifacts"

SWEEP_EVENT_RATES = {"100000", "500000", "1000000"}

# job_class folder name (under dsp_be/) -> artifacts app folder name
APP_NAME = {
    "Ad Analytics": "ad-analytics",
    "Bargain Index": "bargain-index",
    "Click Analytics": "click-analytics",
    "Google cloud Monitoring": "google-cloud-monitoring",
    "Linear Road": "linear-road",
    "Log Processing": "log-processing",
    "Machine Outlier": "machine-outlier",
    "Sentiment Analysis": "sentiment-analysis",
    "Smart Grid": "smart-grid",
    "Spike Detection": "spike-detection",
    "TPCH": "tpch",
    "Traffic Monitoring": "traffic-monitoring",
    "Trending Topics": "trending-topics",
    "Word Count": "word-count",
}

# job_class -> {job_query_number: query-variant folder name}, or absent if
# the workload has only one query (no variant subfolder).
QUERY_VARIANT = {
    "Ad Analytics": {"1": "with_join", "2": "without_join"},
    "Click Analytics": {"1": "time-based", "2": "geo-based", "3": "count-based"},
    "Google cloud Monitoring": {"1": "cpu-per-category", "2": "cpu-per-job"},
    "Linear Road": {
        "1": "toll-notification",
        "2": "accident-detection",
        "3": "daily-expenditure",
        "4": "travel-time",
    },
    "TPCH": {"1": "without_window", "2": "with_window"},
}


def main():
    moved = 0
    skipped_non_sweep = 0
    skipped_no_pair = 0
    per_dest = {}

    for job_class, app_name in APP_NAME.items():
        src_dir = SRC_ROOT / job_class
        if not src_dir.is_dir():
            continue

        for meta_path in sorted(src_dir.glob("metaInfo-*.json")):
            try:
                meta = json.loads(meta_path.read_text())
            except (json.JSONDecodeError, OSError):
                continue

            event_rate = str(meta.get("producer_event_per_second", ""))
            hardware = meta.get("cluster_hardware_type", "")
            query_num = str(meta.get("job_query_number", ""))

            if event_rate not in SWEEP_EVENT_RATES or hardware != "m510":
                skipped_non_sweep += 1
                continue

            eval_path = src_dir / meta_path.name.replace("metaInfo-", "evaluationMetrics-", 1)
            if not eval_path.exists():
                skipped_no_pair += 1
                continue

            dest_dir = DST_ROOT / app_name / hardware / "spark"
            variant_map = QUERY_VARIANT.get(job_class)
            if variant_map:
                variant = variant_map.get(query_num)
                if variant is None:
                    print(f"WARNING: unknown query_number={query_num!r} for {job_class} "
                          f"in {meta_path.name}, skipping")
                    continue
                dest_dir = dest_dir / variant
            dest_dir = dest_dir / event_rate
            dest_dir.mkdir(parents=True, exist_ok=True)

            shutil.copy2(meta_path, dest_dir / meta_path.name)
            shutil.copy2(eval_path, dest_dir / eval_path.name)
            moved += 2
            per_dest[str(dest_dir)] = per_dest.get(str(dest_dir), 0) + 2

    print(f"\nMoved {moved} files ({moved // 2} metaInfo/evaluationMetrics pairs)")
    print(f"Skipped (not a sweep event rate / not m510): {skipped_non_sweep}")
    print(f"Skipped (missing evaluationMetrics pair): {skipped_no_pair}")
    print("\nPer-destination counts:")
    for dest, count in sorted(per_dest.items()):
        print(f"  {dest}: {count} files")


if __name__ == "__main__":
    main()
