"""
Migrates the completed Flink non-uniform-parallelism strategy sweep
(random/DS2/Dhalion across 100k/500k/1M events/sec, all 19 workload/query
combos) from its working location under PStreamBench_controller/dsp_be/
into PStreamBench_benchmark_data/artifacts/, matching the directory
convention every other engine (Storm/Spark/RisingWave) already
established there:

    artifacts/<workload-slug>/m510/flink/[<query-subfolder>/]<strategy>/<event_rate>/

`<query-subfolder>` only exists for workload families with more than one
query -- confirmed by inspecting the EXISTING Storm/RisingWave artifacts
directly (e.g. linear-road/m510/storm/{toll-notification,accident-
detection}/, ad-analytics/m510/storm/{with_join,without_join}/,
click-analytics/m510/storm/{count-based,geo-based,time-based}/) -- reused
verbatim here for cross-engine comparability, since that's the whole
point of this multi-engine benchmark suite. Smart Grid Q1/Q2 use
"global-avg-load"/"local-avg-load" (own choice, named after Flink's own
operator .name() strings "global average load"/"local average load" in
SmartGridJob.java -- Storm never had a Q2 to establish a precedent
against, and RisingWave's own naming ("median-per-house") is tied to its
own SQL view name, not a cross-engine standard).

Files are COPIED, not moved (confirmed this matches the existing
migration convention -- PStreamBench_controller/dsp_be/<job_class>/storm/
still has all its original files locally even though artifacts/ also has
copies of the same data).

Source location per workload: PStreamBench_controller/dsp_be/<folder>/
flink/<strategy>/evaluationMetrics-*.json, where <folder> is normally the
workload's job_class, EXCEPT Smart Grid Q2, which run_strategy_sweep_
flink.py's DEST_JOB_CLASS_OVERRIDE redirects to its own "Smart Grid Q2"
folder (reused verbatim here, imported directly rather than
reimplemented, so this script can never drift out of sync with the sweep
script's own folder-routing logic).
"""
import glob
import json
import os
import shutil
from pathlib import Path

import run_strategy_sweep_flink as sw

ARTIFACTS_ROOT = Path("/Users/pratyushagnihotri/AI_Agent/PStreamBench/PStreamBench_benchmark_data/artifacts")
HARDWARE = "m510"
STRATEGIES = ["random", "uniform", "ds2", "dhalion"]

WORKLOAD_SLUG = {
    "Word Count": "word-count",
    "Ad Analytics Q1": "ad-analytics",
    "Ad Analytics Q2": "ad-analytics",
    "Bargain Index": "bargain-index",
    "Click Analytics Q1": "click-analytics",
    "Click Analytics Q2": "click-analytics",
    "Click Analytics Q3": "click-analytics",
    "Google cloud Monitoring": "google-cloud-monitoring",
    "Linear Road Q1": "linear-road",
    "Linear Road Q2": "linear-road",
    "Log Processing": "log-processing",
    "Machine Outlier": "machine-outlier",
    "Sentiment Analysis": "sentiment-analysis",
    "Smart Grid Q1": "smart-grid",
    "Smart Grid Q2": "smart-grid",
    "Spike Detection": "spike-detection",
    "TPCH": "tpch",
    "Traffic Monitoring": "traffic-monitoring",
    "Trending Topics": "trending-topics",
}

# Only multi-query workload families need a subfolder -- matches the
# EXISTING Storm/RisingWave artifacts convention (see module docstring).
QUERY_SUBFOLDER = {
    "Ad Analytics Q1": "with_join",
    "Ad Analytics Q2": "without_join",
    "Click Analytics Q1": "time-based",
    "Click Analytics Q2": "geo-based",
    "Click Analytics Q3": "count-based",
    "Linear Road Q1": "toll-notification",
    "Linear Road Q2": "accident-detection",
    "Smart Grid Q1": "global-avg-load",
    "Smart Grid Q2": "local-avg-load",
}


def source_folder(workload_key, job_class, strategy):
    folder_name = sw.DEST_JOB_CLASS_OVERRIDE.get(workload_key, job_class)
    return sw.CONTROLLER_DIR / folder_name / "flink" / strategy


def dest_folder(workload_key, strategy, event_rate):
    parts = [ARTIFACTS_ROOT, WORKLOAD_SLUG[workload_key], HARDWARE, "flink"]
    if workload_key in QUERY_SUBFOLDER:
        parts.append(QUERY_SUBFOLDER[workload_key])
    parts += [strategy, str(event_rate)]
    dest = Path(*parts)
    dest.mkdir(parents=True, exist_ok=True)
    return dest


def read_meta(eval_file):
    meta_file = eval_file.replace("evaluationMetrics-", "metaInfo-")
    if not os.path.exists(meta_file):
        return None
    with open(meta_file) as f:
        return json.load(f)


def main():
    total_copied = 0
    total_skipped_existing = 0
    total_no_rate = 0
    total_wrong_query = 0

    for workload_key, (job_class, query_number, config_file, flink_job_name, stage_names, has_sink) in sw.WORKLOADS.items():
        for strategy in STRATEGIES:
            src = source_folder(workload_key, job_class, strategy)
            if not src.exists():
                continue
            eval_files = sorted(glob.glob(str(src / "evaluationMetrics-*.json")))
            for eval_file in eval_files:
                meta = read_meta(eval_file)
                if meta is None or meta.get("producer_event_per_second") is None:
                    print(f"  SKIP (no event rate in metaInfo): {eval_file}")
                    total_no_rate += 1
                    continue

                # CRITICAL: workloads that share one job_class across
                # multiple queries (Ad Analytics Q1/Q2, Click Analytics
                # Q1/Q2/Q3, Linear Road Q1/Q2) also share ONE physical
                # source folder -- confirmed live 2026-09-18, a first
                # migration attempt without this check copied BOTH
                # queries' files into BOTH destination query-subfolders
                # (e.g. every Ad Analytics file, Q1 and Q2 alike, ended up
                # in both "with_join" and "without_join"). Every file's
                # own metaInfo carries its real job_query_number
                # regardless of which folder it happens to sit in, so
                # filter on that rather than trusting the source folder
                # alone to mean "this query only".
                if str(meta.get("job_query_number")) != str(query_number):
                    total_wrong_query += 1
                    continue

                rate = int(meta["producer_event_per_second"])
                dest = dest_folder(workload_key, strategy, rate)
                meta_file = eval_file.replace("evaluationMetrics-", "metaInfo-")

                dest_eval = dest / Path(eval_file).name
                dest_meta = dest / Path(meta_file).name

                if dest_eval.exists():
                    total_skipped_existing += 1
                    continue

                shutil.copy2(eval_file, dest_eval)
                if os.path.exists(meta_file):
                    shutil.copy2(meta_file, dest_meta)
                total_copied += 1

        print(f"[{workload_key}] done")

    print(f"\n=== MIGRATION DONE: {total_copied} new files copied, "
          f"{total_skipped_existing} already present, {total_no_rate} skipped (no rate), "
          f"{total_wrong_query} skipped (belongs to a sibling query sharing this folder) ===")


if __name__ == "__main__":
    main()
