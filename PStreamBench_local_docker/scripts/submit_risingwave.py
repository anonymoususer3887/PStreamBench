#!/usr/bin/env python3
"""Submits a RisingWave workload (Word Count or Ad Analytics) to the local
docker-compose RisingWave playground instance.

Reuses the project's OWN parallelism-injection logic
(PStreamBench_controller/dsp_be/benchmark_sweep/run_sweep_risingwave.py::
build_sql_with_parallelism) instead of reimplementing it, so the SET
streaming_parallelism staging (source=2, aggregation=swept value, sink=1)
stays exactly consistent with the real sweep -- see that function's own
docstring for why source/sink are pinned rather than swept.

Unlike Flink/Kafka Streams, RisingWave jobs are NOT self-terminating --
CREATE SOURCE/MATERIALIZED VIEW/SINK are persistent catalog objects that
keep running until explicitly dropped (see runner.py's own comment on this,
which documents a real infinite-loop bug that assumption caused in the
Django controller). This script submits and returns immediately; use
--drop to tear a workload's objects down afterward.

Usage:
    ./venv/bin/python scripts/submit_risingwave.py word_count --parallelism 4
    ./venv/bin/python scripts/submit_risingwave.py ad_analytics --parallelism 4
    ./venv/bin/python scripts/submit_risingwave.py word_count --drop
"""
import argparse
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
REPO_ROOT = ROOT.parent  # .../PStreamBench
SWEEP_DIR = REPO_ROOT / "PStreamBench_controller/dsp_be/benchmark_sweep"
RW_JOBS_DIR = REPO_ROOT / "PStreamBench_workload_generator/risingwave/dsp_risingwave_jobs"

sys.path.insert(0, str(SWEEP_DIR))
from run_sweep_risingwave import build_sql_with_parallelism  # noqa: E402

WORKLOADS = {
    "word_count": dict(sql=RW_JOBS_DIR / "wordcount/word_count.sql", prefix="word_count"),
    "ad_analytics": dict(sql=RW_JOBS_DIR / "AdsAnalytics/ads_analytics.sql", prefix="ad_analytics"),
}

PSQL = "psql"
HOST = "localhost"
PORT = "4566"


def run_psql(sql_text):
    print(f"+ submitting via psql ({len(sql_text)} chars)")
    result = subprocess.run(
        [PSQL, "-h", HOST, "-p", PORT, "-d", "dev", "-U", "root"],
        input=sql_text, text=True, capture_output=True,
    )
    print(result.stdout)
    if result.returncode != 0 or result.stderr.strip():
        print(result.stderr, file=sys.stderr)
    return result.returncode == 0


def drop_workload(prefix):
    stmts = [
        f"DROP SINK IF EXISTS {prefix}_sink;",
        f"DROP MATERIALIZED VIEW IF EXISTS {prefix}_mv CASCADE;",
        f"DROP SOURCE IF EXISTS {prefix}_source CASCADE;",
    ]
    for s in stmts:
        subprocess.run([PSQL, "-h", HOST, "-p", PORT, "-d", "dev", "-U", "root", "-c", s],
                       capture_output=True, text=True)
    print(f"+ dropped {prefix}_sink / {prefix}_mv / {prefix}_source (if they existed)")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("workload", choices=list(WORKLOADS))
    ap.add_argument("--parallelism", type=int, default=4, help="streaming_parallelism for the aggregation stage")
    ap.add_argument("--drop", action="store_true", help="drop this workload's objects instead of creating them")
    args = ap.parse_args()

    w = WORKLOADS[args.workload]
    if args.drop:
        drop_workload(w["prefix"])
        return

    sql_text = build_sql_with_parallelism(
        w["sql"], kafka_bootstrap="kafka:9092", needs_window=False,
        parallelism=args.parallelism, workload_name=args.workload,
    )
    ok = run_psql(sql_text)
    if not ok:
        print("FAILED to submit -- see psql output above.", file=sys.stderr)
        sys.exit(1)
    print(f"+ {args.workload} submitted (prefix={w['prefix']}_*, parallelism={args.parallelism}). "
          f"It runs persistently -- use --drop to tear it down when done.")


if __name__ == "__main__":
    main()
