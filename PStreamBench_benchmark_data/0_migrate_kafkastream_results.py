#!/usr/bin/env python3
"""Move Kafka Streams benchmark sweep result files from
PStreamBench_controller/dsp_be/<Job Class>/ into the organized
artifacts/ structure, matching the existing Flink/Storm/Spark layout:
<app>/<hardware>/kafkastream/[<query-variant>/]<event-rate>/

Query-variant folder names and the overall <app>/<hardware>/<engine>/...
layout match 0_migrate_spark_results.py exactly, so the two scripts should
be kept in sync if a new workload/query-variant naming is introduced.

Only moves files whose metaInfo matches one of the real sweep's event rates
(100000/500000/1000000) -- ad-hoc validation/reproduction test runs are left
in place. Unlike 0_migrate_spark_results.py, this dedups per (query,
parallelism, event-rate) combo and keeps only the latest (by the epoch
embedded in the filename) -- the Kafka Streams sweep retried failed combos
far more often than Spark's did, so copying every matching file would
routinely carry forward superseded/broken retries alongside the good result.
Safe to re-run: copies rather than deletes the originals, and re-running
just overwrites the same destination files.
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
# the workload has only one query (no variant subfolder). Kept identical to
# 0_migrate_spark_results.py's QUERY_VARIANT.
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


def epoch_of(meta_filename: str) -> int:
    # metaInfo-i0s0-<Job>-<epoch>-<timestamp>.json
    parts = meta_filename.split("-")
    try:
        return int(parts[3])
    except (IndexError, ValueError):
        return -1


def main():
    moved = 0
    skipped_non_sweep = 0
    skipped_no_pair = 0
    skipped_stale = 0
    per_dest = {}

    for job_class, app_name in APP_NAME.items():
        src_dir = SRC_ROOT / job_class
        if not src_dir.is_dir():
            continue

        # group candidate files by (query, parallelism, event_rate) so only
        # the latest (highest embedded epoch) survives per combo
        combos = {}  # key -> (epoch, meta_path, eval_path, dest_dir)
        for meta_path in sorted(src_dir.glob("metaInfo-*.json")):
            try:
                meta = json.loads(meta_path.read_text())
            except (json.JSONDecodeError, OSError):
                continue

            event_rate = str(meta.get("producer_event_per_second", ""))
            hardware = meta.get("cluster_hardware_type", "")
            query_num = str(meta.get("job_query_number", ""))
            parallelization = str(meta.get("job_parallelization", ""))

            if event_rate not in SWEEP_EVENT_RATES or hardware != "m510":
                skipped_non_sweep += 1
                continue

            eval_path = src_dir / meta_path.name.replace("metaInfo-", "evaluationMetrics-", 1)
            if not eval_path.exists():
                skipped_no_pair += 1
                continue

            dest_dir = DST_ROOT / app_name / hardware / "kafkastream"
            variant_map = QUERY_VARIANT.get(job_class)
            if variant_map:
                variant = variant_map.get(query_num)
                if variant is None:
                    print(f"WARNING: unknown query_number={query_num!r} for {job_class} "
                          f"in {meta_path.name}, skipping")
                    continue
                dest_dir = dest_dir / variant
            dest_dir = dest_dir / event_rate

            combo_key = (query_num, parallelization, event_rate)
            epoch = epoch_of(meta_path.name)
            if combo_key not in combos or epoch > combos[combo_key][0]:
                combos[combo_key] = (epoch, meta_path, eval_path, dest_dir)

        for epoch, meta_path, eval_path, dest_dir in combos.values():
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
