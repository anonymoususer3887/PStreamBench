"""
Runs ADDITIONAL_QUERY_VARIANTS through the same sweep loop as
run_sweep_risingwave.py's `run` command, against the same
manifest_risingwave.json, by reassigning WORKLOADS before calling the
same run() logic -- mirrors run_sweep_risingwave_priority.py's existing
pattern for reusing run_one_combo/load_manifest/save_manifest/run_key.
"""
import time

import run_sweep_risingwave as base

base.WORKLOADS = base.ADDITIONAL_QUERY_VARIANTS

if __name__ == "__main__":
    manifest = base.load_manifest()
    results = []
    total = len(base.WORKLOADS) * len(base.PARALLELISM_LEVELS) * len(base.EVENT_RATES)
    done = 0
    for workload_name, query_number, sql_relpath, in_topic, out_topic, app_type, needs_window in base.WORKLOADS:
        for parallelism in base.PARALLELISM_LEVELS:
            for event_rate in base.EVENT_RATES:
                key = base.run_key(workload_name, parallelism, event_rate)
                done += 1
                if manifest.get(key) == "ok":
                    base.log(f"[{done}/{total}] SKIP (already done): {key}")
                    continue
                base.log(f"[{done}/{total}] {key}")
                try:
                    result = base.run_one_combo(
                        workload_name, query_number, sql_relpath, in_topic, out_topic,
                        app_type, needs_window, parallelism, event_rate, dry_run=False,
                    )
                except Exception as e:
                    base.log(f"  UNEXPECTED EXCEPTION: {e!r}")
                    result = {"label": key, "status": f"exception: {e!r}"}
                results.append(result)
                manifest[key] = result.get("status", "unknown")
                base.save_manifest(manifest)
                base.log(f"  sleeping {base.GAP_SECONDS}s gap before next combo...")
                time.sleep(base.GAP_SECONDS)

    base.log("\n=== SUMMARY ===")
    for r in results:
        base.log(f"{r['label']} -> {r.get('status')}")
