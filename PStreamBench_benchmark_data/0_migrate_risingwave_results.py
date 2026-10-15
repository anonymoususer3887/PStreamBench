#!/usr/bin/env python3
"""Move RisingWave benchmark sweep result files from
PStreamBench_controller/dsp_be/<Job Class>/ into the organized
artifacts/ structure, matching the existing Flink/Storm/Spark/Kafka
Streams layout: <app>/<hardware>/risingwave/[<query-variant>/]<event-rate>/

Modeled directly on 0_migrate_kafkastream_results.py (RisingWave's raw
data comes from the same metaInfo/evaluationMetrics-pair layout under
dsp_be/, produced by the same kind of getprom_*.py collector script) --
kept in sync with that script's APP_NAME/QUERY_VARIANT conventions so the
same app/variant folder names are reused across engines.

Two differences from the Kafka Streams migration:

1. RisingWave's own benchmark_sweep/run_sweep_risingwave.py ran EVERY
   query variant of a workload as a SEPARATE top-level dsp_be/<Job Class>
   folder (e.g. "Ad Analytics" and "Ad Analytics Q1" are two different
   folders, one per job_query_number), unlike Kafka Streams which stores
   every variant of a workload under one folder distinguished only by
   job_query_number inside metaInfo. APP_NAME below therefore has more
   keys than Kafka Streams' version -- one per RisingWave dsp_be folder
   name -- but every key still maps to the SAME app_name as its sibling
   variant, and QUERY_VARIANT is still looked up by the job_query_number
   *field value* (not by folder name), so this produces the identical
   destination layout either way.

2. RisingWave ran a second query for Smart Grid and Log Processing
   (Smart Grid Q2, Log Processing Q2) that no other engine has tested --
   QUERY_VARIANT deliberately has NO entry for these two (matching every
   other engine's flat, no-variant-subfolder layout for them), so their
   Q1 and Q2 files land side by side in the same flat <event_rate>
   folder. This is safe: 1_data_extractor.py treats every
   evaluationMetrics file as an independent row (no folder-level
   aggregation), and each row's own `job_query_number` column (already
   captured per-row from metaInfo) still distinguishes Q1 from Q2 even
   without a variant subfolder -- confirmed by checking
   object_prefix_for() in run_sweep_risingwave.py: it strips the " Q<N>"
   suffix before computing the SQL-object prefix used in job_id/filenames
   (bug #22's fix), so Smart Grid Q2's files use the exact same
   "smart_grid_p<N>_r<rate>" filename stem as Smart Grid Q1's, differing
   only by their embedded timestamp -- no filename collision, no
   overwrite.

Only moves files whose metaInfo matches one of the real sweep's event
rates (100000/500000/1000000) and hardware (m510) -- ad-hoc
validation/reproduction test runs (e.g. the Bargain Index threshold
confirmation tests, which used isolated topics and were cleaned up
separately) are left in place since they were never written under one of
these dsp_be/<Job Class>/ folders in the first place.

Dedups per (job_query_number, parallelism, event_rate) combo and keeps
only the latest (by the timestamp embedded in the filename), matching
0_migrate_kafkastream_results.py's rationale: RisingWave's sweep also
retried failed/superseded combos (bug #20's outage, bugs #21-24's fixes,
the Bargain Index re-run), so copying every matching file would carry
forward stale/broken results alongside the good ones.

Safe to re-run: copies rather than deletes the originals, and re-running
just overwrites the same destination files.
"""
import json
import shutil
from pathlib import Path

SRC_ROOT = Path("/Users/pratyushagnihotri/AI_Agent/PStreamBench/PStreamBench_controller/dsp_be")
DST_ROOT = Path(__file__).resolve().parent / "artifacts"

SWEEP_EVENT_RATES = {"100000", "500000", "1000000"}

# dsp_be/<Job Class> folder name -> artifacts app folder name. Every
# RisingWave query-variant folder maps to the SAME app_name as its base
# workload -- see module docstring point 1.
APP_NAME = {
    "Word Count": "word-count",
    "Ad Analytics": "ad-analytics",
    "Ad Analytics Q1": "ad-analytics",
    "Smart Grid": "smart-grid",
    "Smart Grid Q2": "smart-grid",
    "Log Processing": "log-processing",
    "Log Processing Q2": "log-processing",
    "Machine Outlier": "machine-outlier",
    "Bargain Index": "bargain-index",
    "Trending Topics": "trending-topics",
    "Click Analytics": "click-analytics",
    "Click Analytics Q2": "click-analytics",
    "Click Analytics Q3": "click-analytics",
    "Traffic Monitoring": "traffic-monitoring",
    "Spike Detection": "spike-detection",
    "Sentiment Analysis": "sentiment-analysis",
    "TPCH": "tpch",
    "TPCH Q2": "tpch",
    "Google cloud Monitoring": "google-cloud-monitoring",
    "Google cloud Monitoring Q2": "google-cloud-monitoring",
    "Linear Road": "linear-road",
}

# job_query_number -> query-variant folder name, per app_name. Kept
# identical to 0_migrate_kafkastream_results.py/0_migrate_spark_results.py
# for every app they also cover, so the same variant names line up across
# engines. Smart Grid and Log Processing are deliberately absent (see
# module docstring point 2).
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


def timestamp_of(meta_filename: str) -> int:
    # metaInfo-i0s0-<prefix>_p<N>_r<rate>-<YYYYMMDDTHHMMSS>.json -- unlike
    # Kafka Streams' metaInfo-i0s0-<Job>-<epoch>-<timestamp>.json, there is
    # no separate bare-epoch field, so sort by the embedded timestamp
    # instead (digits-only strips the "T", and the format is already
    # chronologically sortable as a plain string of digits).
    stem = meta_filename.removesuffix(".json").rsplit("-", 1)[-1]
    digits = "".join(c for c in stem if c.isdigit())
    try:
        return int(digits) if digits else -1
    except ValueError:
        return -1


def main():
    moved = 0
    skipped_non_sweep = 0
    skipped_no_pair = 0
    per_dest = {}

    for job_class, app_name in APP_NAME.items():
        src_dir = SRC_ROOT / job_class
        if not src_dir.is_dir():
            continue

        # group candidate files by (query, parallelism, event_rate) so only
        # the latest (highest embedded timestamp) survives per combo
        combos = {}  # key -> (ts, meta_path, eval_path, dest_dir)
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

            dest_dir = DST_ROOT / app_name / hardware / "risingwave"
            # app_name is kebab-case ("ad-analytics"); QUERY_VARIANT is
            # keyed by the human-readable base workload name ("Ad
            # Analytics") -- _QUERY_VARIANT_BY_APP resolves via APP_NAME's
            # own values rather than a lossy kebab->title guess.
            variant_map = _QUERY_VARIANT_BY_APP.get(app_name)
            if variant_map:
                variant = variant_map.get(query_num)
                if variant is None:
                    print(f"WARNING: unknown query_number={query_num!r} for {job_class} "
                          f"in {meta_path.name}, skipping")
                    continue
                dest_dir = dest_dir / variant
            dest_dir = dest_dir / event_rate

            combo_key = (query_num, parallelization, event_rate)
            ts = timestamp_of(meta_path.name)
            if combo_key not in combos or ts > combos[combo_key][0]:
                combos[combo_key] = (ts, meta_path, eval_path, dest_dir)

        for ts, meta_path, eval_path, dest_dir in combos.values():
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


# QUERY_VARIANT is keyed by human-readable base workload name; build a
# kebab-case-app_name -> variant_map lookup once, since dest folders are
# organized by app_name (kebab-case), not the display name.
_QUERY_VARIANT_BY_APP = {
    APP_NAME[base_name]: variant_map
    for base_name, variant_map in QUERY_VARIANT.items()
}


if __name__ == "__main__":
    main()
