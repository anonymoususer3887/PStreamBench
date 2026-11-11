# PStreamBench — Metrics

> **Status: PENDING full write-up.** Needs a direct read of each
> `getprom_*.py` collector plus `operator_level_metrics.csv`'s actual
> columns before this is complete. What's confirmed so far:

## Confirmed metric categories (from the "Contributions So Far" doc and
collector script names)

- **Throughput** (p50/95/98) — query-level, from `getprom*.py`.
- **End-to-end latency** — per-engine implementation differs
  structurally: Flink's own latency markers travel the whole pipeline to
  the sink; Spark's closest equivalent (per `WordCountJob.java`'s own
  comment) measures *ingestion-to-processing* lag (average seconds
  between Kafka's own ingestion timestamp and "now" when the batch is
  processed) — **not the same thing**, and should not be treated as
  directly comparable across engines without stating this difference.
- **CPU / memory / network utilization** — hardware metrics, per node.
- **Operator-level metrics**: `throughput_per_sec`, `selectivity`,
  `backpressure_ratio`, `busy_ratio` — extracted per logical pipeline
  stage. Purpose-built to show *mechanism*, not just aggregate numbers —
  see the "New Insights" doc's Insight D for why this matters (e.g.
  showing which specific operator's backpressure drops when a
  heterogeneous configuration wins, not just that throughput went up).
- **Efficiency metrics** — *[inferred: likely throughput-per-core or
  similar derived metric; the "New Insights" doc explicitly flags
  "CPU/network efficiency metrics for the selected/configuration-based
  views" as **not yet built** — "blocked on porting a derived-metric
  calculation from the original baseline script, not yet done" — so this
  category may be partially aspirational, not fully implemented. Needs
  direct confirmation.]**

## Known collector bugs already found and fixed (worth citing here, not
just in the issue log, since they directly affect what a given metric
actually means if uncorrected)

- Flink's `getprom.py` had 5 separate unguarded `IndexError` cases
  (source operators have no latency-marker series; a hardcoded
  Kafka-sink operator name crashed for non-Kafka jobs; metrics evicted
  quickly after job cancellation) — see `docs/14_issue_log.md` and
  `docs/04_frameworks/flink.md`.
- Storm's `getprom_storm.py` has a known, still-open `IndexError` on a
  zero-result Prometheus query (STORM-012) — a metric can legitimately
  have no data point in the queried window; this currently crashes that
  one collection attempt rather than returning a clear "missing" value.
- Storm's exporter can silently deadlock under label-cardinality growth
  (STORM-009) — any metric collected during a deadlocked window is
  empty/zero, not absent-with-an-error; **this is a measurement-error
  mode worth flagging explicitly in any paper's limitations section**,
  since it would otherwise look like a legitimate zero.

**Still needed for this file:** a table of every metric with its exact
Prometheus query (or REST endpoint), units, aggregation method
(mean/p50/p95/p98 — which one, confirmed per metric not assumed), and a
dedicated subsection on the Flink-vs-Spark latency definition mismatch
noted above, since it's a real cross-engine comparability limitation.
