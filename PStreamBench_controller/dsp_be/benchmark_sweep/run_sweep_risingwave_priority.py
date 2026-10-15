"""
Time-boxed follow-up to run_sweep_risingwave.py -- the cluster has ~2 hours
left, not the ~5.7 hours the full 36-combo sweep needs. Reduces to one
representative event rate (500_000, the middle value) for the remaining
combos so BOTH workloads get full parallelism coverage instead of Word
Count finishing completely while Ad Analytics gets nothing.

Remaining combos (skips anything already in manifest_risingwave.json):
  - Word Count: parallelism 16, 32, 40 @ rate 500_000 (finishes the
    parallelism dimension Word Count was already partway through)
  - Ad Analytics: parallelism 2, 4, 8, 16, 32, 40 @ rate 500_000 (full
    parallelism sweep at one rate, since it had zero coverage otherwise)

Gap between jobs reduced from 120s to 60s to fit within the time budget --
still "some minutes" separation as originally specified, just tighter.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import run_sweep_risingwave as base

base.GAP_SECONDS = 60

PRIORITY_COMBOS = (
    [("Word Count", "1", "wordcount/word_count.sql", "WordCountIn", "WordCountOut", "Word_Count", False, p, 500_000)
     for p in (16, 32, 40)]
    + [("Ad Analytics", "2", "AdsAnalytics/ads_analytics_q2.sql", "AdAnalyticsIn", "AdAnalyticsOut", "Ad_Analytics", True, p, 500_000)
       for p in (2, 4, 8, 16, 32, 40)]
)


def main():
    manifest = base.load_manifest()
    results = []
    total = len(PRIORITY_COMBOS)
    for i, (workload_name, query_number, sql_relpath, in_topic, out_topic, app_type, needs_window, parallelism, event_rate) in enumerate(PRIORITY_COMBOS, 1):
        key = base.run_key(workload_name, parallelism, event_rate)
        if manifest.get(key) == "ok":
            base.log(f"[{i}/{total}] SKIP (already done): {key}")
            continue
        base.log(f"[{i}/{total}] {key}")
        try:
            result = base.run_one_combo(
                workload_name, query_number, sql_relpath, in_topic, out_topic,
                app_type, needs_window, parallelism, event_rate,
            )
        except Exception as e:
            base.log(f"  UNEXPECTED EXCEPTION: {e!r}")
            result = {"label": key, "status": f"exception: {e!r}"}
        results.append(result)
        manifest[key] = result.get("status", "unknown")
        base.save_manifest(manifest)
        base.log(f"  sleeping {base.GAP_SECONDS}s gap before next combo...")
        import time
        time.sleep(base.GAP_SECONDS)

    base.log("\n=== PRIORITY SUMMARY ===")
    for r in results:
        base.log(f"{r['label']} -> {r.get('status')}")


if __name__ == "__main__":
    main()
