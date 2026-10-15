"""
One-off backfill for Smart Grid Q2's 100k-events/sec strategy-sweep data,
after fixing two real bugs found live 2026-09-15:
  1. SmartGridJob.java's Q2 branch sunk ProcessOutputQ2 records through a
     KafkaSink built with ProcessOutputQ1SerializationSchema (shared raw
     `Sink` field with Q1) -- threw ClassCastException on every record, so
     zero output ever reached the sink regardless of window timing. Fixed
     with a dedicated ProcessOutputQ2SerializationSchema + its own
     KafkaSink<ProcessOutputQ2>, jar rebuilt and redeployed.
  2. configs/smartgrid_q2.json's window_size_max=1000 (inherited from Q1's
     config, which uses milliseconds) meant Q2 -- which uses
     Time.seconds() in SmartGridJob.java, not milliseconds -- could draw a
     window up to 1000 SECONDS, far longer than the 5-minute job run.
     Fixed by tightening window_size_max to 100 (seconds).

All 10 pre-fix Smart Grid Q2 random-sample files (100k) had zero real
throughput and were deleted; their manifest entries were cleared. This
script regenerates that data with the corrected jar/config, matching
run_random()/run_baseline_then_nudge()'s exact logic in
run_strategy_sweep_flink.py but scoped to just the "Smart Grid Q2"
workload key, and -- per explicit user request -- writes the corrected
output into its own "Smart Grid Q2/flink/<strategy>/" folder rather than
the shared "Smart Grid/flink/<strategy>/" folder Q1 already occupies, so
the two queries' data is never ambiguous by folder alone.
"""
import os
import time

import run_strategy_sweep_flink as sw
import parallelism_strategies as strat

WORKLOAD_KEY = "Smart Grid Q2"
JOB_CLASS, QUERY, CONFIG_FILE, FLINK_JOB_NAME, STAGE_NAMES, HAS_SINK = sw.WORKLOADS[WORKLOAD_KEY]
EVENT_RATE = 100000
DEST_JOB_CLASS = "Smart Grid Q2"  # separate folder from plain "Smart Grid" (Q1)


def relocate(result):
    """run_one() already moved the file into 'Smart Grid/flink/<strategy>/'
    (job_class="Smart Grid" is required for correct API submission) --
    move it one more hop into 'Smart Grid Q2/flink/<strategy>/'.
    """
    if result.get("status") != "ok":
        return result
    src = result["file"]
    strategy_name = os.path.basename(os.path.dirname(src))
    dest_dir = sw.strategy_folder(DEST_JOB_CLASS, strategy_name)
    dest = dest_dir / os.path.basename(src)
    os.rename(src, dest)
    src_meta = src.replace("evaluationMetrics-", "metaInfo-")
    if os.path.exists(src_meta):
        os.rename(src_meta, dest_dir / os.path.basename(src_meta))
    result["file"] = str(dest)
    return result


def main():
    manifest = sw.load_manifest()
    num_stages = len(STAGE_NAMES)

    assignments = strat.random_strategy(sw.RANDOM_LEVELS, num_stages, sw.RANDOM_SAMPLES, seed=sw.RANDOM_SEED)
    for i, assignment in enumerate(assignments):
        key = sw.strategy_key(WORKLOAD_KEY, "random", f"s{i}", EVENT_RATE)
        if manifest.get(key) == "ok":
            sw.log(f"SKIP (already done): {key}")
            continue
        full_pds = sw.build_full_pds(assignment, HAS_SINK)
        sw.log(f"[backfill random] {WORKLOAD_KEY} sample {i}/{len(assignments)} -> {full_pds}")
        try:
            result = sw.run_one(JOB_CLASS, QUERY, CONFIG_FILE, FLINK_JOB_NAME, full_pds, EVENT_RATE, "random")
            result = relocate(result)
        except Exception as e:
            sw.log(f"  UNEXPECTED EXCEPTION: {e!r}")
            result = {"status": f"exception: {e!r}"}
        manifest[key] = result.get("status", "unknown")
        if result.get("status") == "ok":
            manifest[key + "|file"] = result["file"]
        sw.save_manifest(manifest)
        sw.log(f"  sleeping {sw.GAP_SECONDS}s gap...")
        time.sleep(sw.GAP_SECONDS)

    baseline_assignment = [strat.MIN_PARALLELISM] * num_stages
    baseline_full_pds = sw.build_full_pds(baseline_assignment, HAS_SINK)
    baseline_key = sw.strategy_key(WORKLOAD_KEY, "uniform", "baseline", EVENT_RATE)
    baseline_file = None
    if manifest.get(baseline_key) == "ok":
        sw.log(f"SKIP (already done): {baseline_key}")
        baseline_file = manifest.get(baseline_key + "|file")
    else:
        sw.log(f"[backfill baseline] {WORKLOAD_KEY} -> {baseline_full_pds}")
        try:
            result = sw.run_one(JOB_CLASS, QUERY, CONFIG_FILE, FLINK_JOB_NAME, baseline_full_pds, EVENT_RATE, "uniform")
            result = relocate(result)
        except Exception as e:
            sw.log(f"  UNEXPECTED EXCEPTION: {e!r}")
            result = {"status": f"exception: {e!r}"}
        manifest[baseline_key] = result.get("status", "unknown")
        if result.get("status") == "ok":
            baseline_file = result["file"]
            manifest[baseline_key + "|file"] = baseline_file
        sw.save_manifest(manifest)
        sw.log(f"  sleeping {sw.GAP_SECONDS}s gap...")
        time.sleep(sw.GAP_SECONDS)

    if not baseline_file:
        sw.log("  no baseline file available -- skipping DS2/Dhalion nudge")
        return

    stage_metrics = strat.load_stage_metrics_flink(baseline_file, STAGE_NAMES)
    for strategy_name, strategy_fn, extra_args in [
        ("ds2", strat.ds2_strategy, (EVENT_RATE,)),
        ("dhalion", strat.dhalion_strategy, ()),
    ]:
        key = sw.strategy_key(WORKLOAD_KEY, strategy_name, "nudge", EVENT_RATE)
        if manifest.get(key) == "ok":
            sw.log(f"SKIP (already done): {key}")
            continue
        assignment = strategy_fn(stage_metrics, *extra_args, baseline_assignment)[0]
        full_pds = sw.build_full_pds(assignment, HAS_SINK)
        sw.log(f"[backfill {strategy_name}] {WORKLOAD_KEY} -> {full_pds}")
        try:
            result = sw.run_one(JOB_CLASS, QUERY, CONFIG_FILE, FLINK_JOB_NAME, full_pds, EVENT_RATE, strategy_name)
            result = relocate(result)
        except Exception as e:
            sw.log(f"  UNEXPECTED EXCEPTION: {e!r}")
            result = {"status": f"exception: {e!r}"}
        manifest[key] = result.get("status", "unknown")
        sw.save_manifest(manifest)
        sw.log(f"  sleeping {sw.GAP_SECONDS}s gap...")
        time.sleep(sw.GAP_SECONDS)

    sw.log("=== BACKFILL DONE ===")


if __name__ == "__main__":
    main()
