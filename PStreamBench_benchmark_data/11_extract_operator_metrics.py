#!/usr/bin/env python3
"""Per-operator metric extractor for the 19G plot family.

Pulls throughput_per_sec, selectivity, backpressure_ratio and busy_ratio PER OPERATOR
directly from each run's evaluationMetrics-*.json (operatorMetrics field), for the
enumeration-strategy sweep (uniform/random/ds2/dhalion), m510 only.

Deliberately does NOT use operator_templates.csv: that mapping is for configuration
identity (matching vector positions to canonical stage names, needed for B/C/E/F's
parallelism-based views) and several workloads are marked "unresolved" there. These are
PERFORMANCE metrics, not parallelism -- the engine's own raw operator key (e.g. "source",
"parser", "repeatvisit") is used as the stage label directly, which also works for Kafka
Streams and RisingWave: their own CONFIGURATION is job-level (one thread/parallelism value
for the whole job), but their operatorMetrics still report throughput/selectivity/etc. per
logical operator, so they are NOT excluded here the way they are from the parallelism
views.

Output: operator_level_metrics.csv, one row per (run, operator), wide (one column per
metric). Joinable back to query_performance_data.csv via framework/strategy/query/variant/
hardware_type/producer_rate/parallelism.
"""
import json
import os

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
CSV_PATH = os.path.join(HERE, "query_performance_data.csv")
OUT_PATH = os.path.join(HERE, "operator_level_metrics.csv")

METRICS = ["throughput_per_sec", "selectivity", "backpressure_ratio", "busy_ratio"]


def deep_scalar(v):
    """operatorMetrics values are usually plain floats, but selectivity is nested one level
    under a job-id key (confirmed in the raw JSON) -- unwrap any nesting safely."""
    seen = 0
    while isinstance(v, dict):
        if not v or seen > 5:
            return None
        v = list(v.values())[0]
        seen += 1
    if isinstance(v, list):
        v = v[0] if v else None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def main():
    df = pd.read_csv(CSV_PATH, low_memory=False)
    df["query_variant"] = df["query_variant"].fillna("")
    sub = df[(df["hardware_type"] == "m510")
             & (df["enumeration_strategy"].isin(["uniform", "random", "ds2", "dhalion"]))]
    print(f"scanning {len(sub)} m510 enumeration-strategy rows")

    rows_out = []
    missing_file = no_op_metrics = 0
    for _, r in sub.iterrows():
        jf = os.path.join(HERE, str(r["path"]), f"evaluationMetrics-{r['artifact']}.json")
        if not os.path.exists(jf):
            missing_file += 1
            continue
        try:
            with open(jf) as f:
                data = json.load(f)
        except Exception:
            missing_file += 1
            continue
        op_metrics = data.get("operatorMetrics", {})
        if not op_metrics:
            no_op_metrics += 1
            continue
        for idx, (op_key, m) in enumerate(op_metrics.items()):
            if not isinstance(m, dict):
                continue
            out = dict(
                framework=r["framework"], strategy=r["enumeration_strategy"],
                query=r["query"], query_variant=r["query_variant"],
                hardware_type=r["hardware_type"], producer_rate=r["producer_rate"],
                parallelism=r["parallelism"], operator_index=idx, operator_raw_name=op_key,
            )
            for metric in METRICS:
                out[metric] = deep_scalar(m.get(metric))
            rows_out.append(out)

    out_df = pd.DataFrame(rows_out)
    out_df.to_csv(OUT_PATH, index=False)
    print(f"wrote {OUT_PATH}: {len(out_df)} operator-level rows")
    print(f"skipped: missing_file={missing_file}, no_operatorMetrics_field={no_op_metrics}")
    if len(out_df):
        cov = out_df.groupby("framework")[METRICS].apply(lambda d: d.notna().mean())
        print("\nnon-null coverage per metric, by framework:")
        print(cov.round(2).to_string())


if __name__ == "__main__":
    main()
