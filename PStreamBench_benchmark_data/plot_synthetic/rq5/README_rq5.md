# RQ5 figures (draft, 2026-10-30)

Scripts: `rq5_metrics.py` (values) then `rq5_plot.py` (figures). Data: `datasets_v2/synthetic_*_level.csv`, read only.

- Folders: Flink, Kafka Streams, RisingWave `uniform`; Spark `uniform_e2e_rerun`; Storm `distributed_fixed_exporter_fix`. Rows with a folder flag excluded.
- DoP = uniform level from the config folder (the sources stay at 2 in Flink, Kafka Streams and Spark, and at 1 in RisingWave).
- One value per system × shape × rate × DoP = median over runs (Spark: over attempts that consumed input).
- Capacity = consumed / offered. Offered = rate × number of inputs, except Storm (the topology splits the rate over the spouts).
- Consumed:
  - Flink, Kafka Streams, RisingWave: source operator totals.
  - Spark: processing rate.
  - Storm: first bolts (pre-filters; the join bolt for 4–6-way joins).
- Plateau DoP = lowest DoP with capacity ≥ 0.9 × the maximum. Latency = `endtoend_latency_ms`, compared only within a system.
- Nothing is hidden. Zeros on the log axis are drawn at the floor (0.003), and runs without input are drawn as ×.

Caveats drawn in the figures:
- **Storm 2F/3F/4F** (dotted, hollow in the summary): the first bolt is the window, and its metric counts emitted aggregates, not input.
- **RisingWave**: the source metric is unverified (~1 % of offered). Its latency is barrier latency.
- **Kafka Streams**: capacity > 1 at 100k for the filter shapes (more consumed than offered; backlog?).
- **Storm joins**: throughput grows in proportion to DoP (per-executor limit); its plateau DoP of 40 reflects that limit.
- **Wide joins**: sink output ∝ f^N under the calibrated key space, so "no output" is not the same as failure.
