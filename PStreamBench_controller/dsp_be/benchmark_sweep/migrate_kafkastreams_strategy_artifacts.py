"""
Migrates the completed Kafka Streams non-uniform-parallelism strategy
sweep (random/DS2/Dhalion across 100k/500k/1M events/sec, all 19 workload/
query combos) from its working location under PStreamBench_controller/
dsp_be/ into PStreamBench_benchmark_data/artifacts/, matching the
directory convention every other engine (Storm/Spark/Flink/RisingWave)
already established there:

    artifacts/<workload-slug>/m510/kafkastream/[<query-subfolder>/]<strategy>/<event_rate>/

`<query-subfolder>` naming and which workloads need one is reused
VERBATIM from migrate_flink_strategy_artifacts.py's own QUERY_SUBFOLDER
dict -- not re-derived from this engine's own pre-existing uniform-
PARALLELISM-sweep artifacts (which, confirmed live 2026-10-01, split some
single-query-in-our-sweep workloads by query anyway, e.g. Google Cloud
Monitoring's cpu-per-category/cpu-per-job and TPCH's with_window/
without_window -- OUR sweep only ever covers query "1" for both, so Flink
correctly skipped a subfolder for them, and this script does too, for
cross-engine comparability).

CRITICAL SAFETY RULE (per explicit user instruction, 2026-10-01): this
engine's PStreamBench_benchmark_data/artifacts/<slug>/m510/kafkastream/
tree already has real data from an older, unrelated UNIFORM-PARALLELISM
sweep (swept parallelism, not strategy) -- confirmed live that
word-count/m510/kafkastream/uniform/ already holds 79+ files from that
older effort, predating this strategy sweep entirely. Our strategy
sweep's "uniform" STRATEGY TAG (the DS2/Dhalion baseline, min-parallelism
assignment -- an unrelated concept that happens to share the English
word "uniform") must never be copied into that same folder and mixed
with it. This script checks, for every (workload, query-subfolder)
destination, whether a "uniform" folder already exists there; if so (and
only then), the strategy-sweep's baseline data is instead written to
"uniform_1" -- the exact same disambiguation already used for Flink's
word-count migration (confirmed by inspecting that folder directly: its
"uniform_1" contains exactly 3 files, one per event rate, matching
baseline semantics precisely, while "uniform" has 79+ files, matching a
real parallelism sweep). Workloads with no pre-existing "uniform" folder
keep the plain "uniform" name, unchanged from every other engine's
convention.

Files are COPIED, not moved (same convention as every other engine's
migration -- the local PStreamBench_controller/dsp_be/<job_class>/
kafkastream/ copies are left in place).

Source location per workload: PStreamBench_controller/dsp_be/<job_class>/
kafkastream/<strategy>/evaluationMetrics-*.json (note: ENGINE_NAME in
run_strategy_sweep_kafkastreams.py is "kafkastream", matching every other
engine's artifacts folder name exactly -- the mixed-case "kafkaStream"
seen on-disk for a couple of workloads earlier this session was a
filesystem case-insensitivity quirk on macOS, not a real second engine
name; Path()'s own case-preserving-but-insensitive lookup means both
spellings resolve to the same directory there, so it doesn't affect this
script's glob patterns, which use the ENGINE_NAME constant's exact casing
consistently).
"""
import glob
import json
import os
import shutil
from pathlib import Path

import run_strategy_sweep_kafkastreams as sw

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

# Reused VERBATIM from migrate_flink_strategy_artifacts.py -- see module
# docstring for why this is NOT re-derived from kafkastream's own older
# uniform-parallelism-sweep artifacts.
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


def source_folder(job_class, strategy):
    return sw.CONTROLLER_DIR / job_class / sw.ENGINE_NAME / strategy


def _uniform_collision_base(workload_key):
    """The directory the "uniform" strategy folder would normally live in
    for this workload (before applying the uniform-vs-uniform_1 safety
    check) -- i.e. .../kafkastream/[<query-subfolder>/].
    """
    parts = [ARTIFACTS_ROOT, WORKLOAD_SLUG[workload_key], HARDWARE, "kafkastream"]
    if workload_key in QUERY_SUBFOLDER:
        parts.append(QUERY_SUBFOLDER[workload_key])
    return Path(*parts)


def resolve_uniform_strategy_name(workload_key):
    """Returns "uniform" normally, or "uniform_1" if a pre-existing,
    non-empty "uniform" folder already sits at this workload's
    destination (see module docstring's CRITICAL SAFETY RULE). Checked
    once per workload via this function rather than inline, so the same
    decision is used consistently for every event rate's files.
    """
    candidate = _uniform_collision_base(workload_key) / "uniform"
    if candidate.exists() and any(candidate.rglob("evaluationMetrics-*.json")):
        return "uniform_1"
    return "uniform"


def dest_folder(workload_key, strategy, event_rate):
    parts = [ARTIFACTS_ROOT, WORKLOAD_SLUG[workload_key], HARDWARE, "kafkastream"]
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
    uniform_redirects = []

    for workload_key, (job_class, query_number, config_file, stage_name) in sw.WORKLOADS.items():
        resolved_uniform = resolve_uniform_strategy_name(workload_key)
        if resolved_uniform != "uniform":
            uniform_redirects.append(workload_key)

        for strategy in STRATEGIES:
            src = source_folder(job_class, strategy)
            if not src.exists():
                continue
            dest_strategy_name = resolved_uniform if strategy == "uniform" else strategy

            eval_files = sorted(glob.glob(str(src / "evaluationMetrics-*.json")))
            for eval_file in eval_files:
                meta = read_meta(eval_file)
                if meta is None or meta.get("producer_event_per_second") is None:
                    print(f"  SKIP (no event rate in metaInfo): {eval_file}")
                    total_no_rate += 1
                    continue

                # Same cross-query-sharing hazard the Flink migration
                # found and fixed: Ad Analytics Q1/Q2, Click Analytics
                # Q1/Q2/Q3, Linear Road Q1/Q2, Smart Grid Q1/Q2 all share
                # ONE physical source folder (flat_folder_name() in
                # run_strategy_sweep_kafkastreams.py returns job_class
                # verbatim) -- filter on each file's own real
                # job_query_number rather than trusting the source folder
                # alone to mean "this query only".
                if str(meta.get("job_query_number")) != str(query_number):
                    total_wrong_query += 1
                    continue

                rate = int(meta["producer_event_per_second"])
                dest = dest_folder(workload_key, dest_strategy_name, rate)
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

        print(f"[{workload_key}] done" + (f" (uniform -> uniform_1, pre-existing data detected)" if resolved_uniform != "uniform" else ""))

    print(f"\n=== MIGRATION DONE: {total_copied} new files copied, "
          f"{total_skipped_existing} already present, {total_no_rate} skipped (no rate), "
          f"{total_wrong_query} skipped (belongs to a sibling query sharing this folder) ===")
    if uniform_redirects:
        print(f"uniform -> uniform_1 redirected for: {', '.join(uniform_redirects)} (pre-existing unrelated 'uniform' folder detected -- NOT overwritten)")
    else:
        print("No uniform/uniform_1 redirects were needed.")


if __name__ == "__main__":
    main()
