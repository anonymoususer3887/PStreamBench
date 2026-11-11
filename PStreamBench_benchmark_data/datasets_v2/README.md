# PStreamBench datasets v2

Built by `build_datasets.py` (in this folder) on 2026-10-26. All inputs were only read; nothing outside `datasets_v2/` was written.

| File | One row per | Rows |
|---|---|---|
| `real_query_level.csv` | run | 7,888 |
| `real_operator_level.csv` | run × operator | 25,616 |
| `synthetic_query_level.csv` | run | 3,218 |
| `synthetic_operator_level.csv` | run × operator | 25,959 |

**Joining:** `run_id` (the path of the `evaluationMetrics` file) links operator rows to their run. Operator files also repeat the main key columns, so most uses need no join.

## Sources and scope

- **Real:** only `artifacts/<application>/<hardware>/<engine>/…`. Not used: `artifacts/workloads_hardwares_additional/` and `artifacts/query_and_operator_metrics/`.
- **Synthetic:** `PStreamBench_workload_generator/<system>/dsp_<system>_synthetic_jobs/data/**`, all five systems.
- **Every run is kept.** Nothing is dropped, rounded or thresholded. A missing value is empty (NaN), never 0.
- **Folder names are kept as found** in `strategy_folder`, e.g. `uniform_1`, `uniform_2`, `uniform_e2e_rerun`, `uniform_rocksdb_retry`, `baseline`, `distributed_fixed`.
- **`folder_flag`** marks runs from special folders: `others`, `extra`, `archive`, `local`, `diagnostics`, and `outside_rate_folder` (no rate folder in the path). Filter these out for the standard analysis.
- **`bad_json` and `missing_meta`** mark unreadable files. There are none at present.

## Uniform and uniform_1 (real data)

- **`strategy`** is the normalized strategy. A configuration (same system, application, variant, hardware, rate and parallelism vector) that exists **only in `uniform_1`** gets `strategy = uniform`. Its `fill_note` reads "configuration absent in uniform; run taken from uniform_1". This applies to 78 runs.
- **A metric missing in a `uniform` run** is taken from the matching `uniform_1` run. `fill_note` names the metrics and the source `run_id`. This applies to 6 runs: throughput for Bargain Index on Flink (3) and Storm (2), and Log Processing on Storm (1).
- **All other `uniform_1` rows keep `strategy = uniform_1`**, so they are not counted twice.

## Column dictionary (query level)

### Configuration
All from `metaInfo`:
- `producer_rate`, `parallelism` (vector), `nodes`, `slots_per_node`, `run_time_min`, `window_size`, `window_slide`, `job_query_number`, `iteration`, `sample`, `meta_enumeration_strategy`, `meta_hardware`.
- `parallelism_avg`, computed as in `query_performance_data.csv` (`compute_parallelism_avg` from `1_data_extractor.py`).
- `parallelism_source`, `parallelism_sink`.

From the path: `application`, `variant` (same mapping as `1_data_extractor.py`), `hardware_type`, `engine_folder`, `rate_folder`, `config_folder` (synthetic level or sample).

### Throughput

**`throughput_50/95/98`** uses the same definition per system as `query_performance_data.csv`:

| System | Source |
|---|---|
| Flink, Storm | `queryMetrics.throughput` series, summed per timestamp, leading and trailing zeros trimmed, then percentiles. `throughput_n` and `throughput_trim_*` describe the series |
| Spark | `sparkStreamingMetrics.processing_rate_*th`: records **processed** per second, not sink output |
| Kafka Streams, RisingWave | sink operator `throughput_per_sec`, one value repeated in all three columns |

`throughput_definition` states the definition in each row. Other throughput columns:

| Column | Meaning |
|---|---|
| `sink_operator_throughput` | Sink operator rate, every system, as an operator **total**. **For Flink** it is the operator directly before the sink (Flink's sink reports records *out*, which is 0), multiplied by that operator's DoP; `sink_operator_throughput_note` names the operator |
| `source_operator_throughput` | Sum of the source/spout operators' `throughput_per_sec` (consumed input), where the system reports it |
| `source_output_50/95/98/avg/max` | Flink/Storm `queryMetrics.recPerSecOutSource*`, summed over sources. **Storm synthetic:** unreliable under load; the p50 is 0 in many runs whose bolts are busy |
| `broker_input_rate_*` | Kafka broker: messages written **by the producer** into the input topic (`kafka_event_rate`). This is *not* what the system consumes. Old name: `event_rate` |
| `broker_output_rate_*` | Kafka broker: messages written into the output topic (`kafka_throughput`) |
| `spark_input_rate_*`, `spark_processing_latency_ms_*` | Spark's own streaming metrics |

### End-to-end latency
**One column family for all systems:** `endtoend_latency_ms`, plus `_95_ms`, `_98_ms` or `_max_ms` where the system reports them, all in **milliseconds**.
- `latency_stat`: which statistic the value is (p50, avg, reported value).
- `latency_metric`: what the system measures. **The five are not the same quantity:**

| System | Raw field | Statistic | What it measures |
|---|---|---|---|
| Flink | `endtoend_latency_50/95/98` | p50 | latency markers; exclude waiting in Kafka, operator processing and window waits |
| Storm | `endtoend_latency_storm` | reported value | Storm-reported end-to-end latency. **Synthetic runs: 0 in all runs, not collected** |
| Spark | `endToEndLatency.end_to_end_latency_seconds*_50th/95th/98th` × 1000 | p50 | ingestion-to-processing lag; stops before the sink. Several input streams: the **maximum** over inputs (the old CSV **summed** them) |
| Kafka Streams | `streamsMetrics.end_to_end_latency_avg` (`_max`) | avg | native record end-to-end latency; includes waiting in Kafka |
| RisingWave | `actorMetrics.barrier_inflight_latency_avg` (`_max`) × 1000 | avg | barrier in-flight latency (p99 barrier duration); internal, excludes waiting in Kafka |

Compare latency as a trend within one system. Across systems, compare only after reading this table.

### Hardware
From node_exporter (`hardwareMetrics`):

| Column | Meaning |
|---|---|
| `cpu_50/95/98` | Average over all nodes of each node's quantile over time |
| `cpu_busiest_node_50` | Maximum over nodes of the per-node median, from the stored per-node series (where present) |
| `memory_50/95/98` | Node memory |
| `network_control_50/95/98` | `node_network_out`, control interface (eno1). **Old CSV `network_*` = this column**; it does not carry data traffic |
| `network_experiment_50/95/98` | `experiment_network_out`, experiment network (eno1d1): data traffic |
| `flink_tm_cpu_*`, `flink_tm_network_*`, `flink_tm_heap_*` | Flink TaskManager's own CPU load, network and heap metrics (real Flink runs) |

## Operator level

- **Identification:** `operator_index` (order in `operatorMetrics`, which is not always the pipeline order) and `operator_name` (the engine's raw key).
- **`operator_dop`:** the operator's parallelism where the raw data states it. That is Flink only, parsed from the metric keys. Elsewhere it is empty, not guessed.
- **`throughput_total_per_sec`:** the operator's total rate. **Flink's `throughput_per_sec` is the mean over subtasks** (`getprom.py`: `avg(avg_over_time(numRecordsOutPerSecond))`), so its total is `throughput_per_sec` × `operator_dop`. The other systems already report totals, so the two columns are equal for them. `source_operator_throughput` and `sink_operator_throughput` use the totals.
- **Other columns:** every scalar the system reports per operator:
  - `throughput_per_sec`, `selectivity`, `busy_ratio`, `backpressure_ratio`;
  - `processing_latency_50/95/98th` (Flink markers);
  - `execution_latency_*` (Storm);
  - `memory_usage_bytes` (RisingWave);
  - `rows` (Spark).
- **Known limitations:**
  - **Kafka Streams:** `busy_ratio` is one thread-level number copied to all operators, and `backpressure_ratio` is 0 (pull-based).
  - **Storm:** `backpressure_ratio` is 0 everywhere.

## Differences from `query_performance_data.csv`

1. **Missing is NaN, not 0.** The old extractor wrote 0 where Spark, Kafka Streams or RisingWave lacked a value: 34 Spark, 59 Kafka Streams and 3 RisingWave throughput values, and 128 Kafka Streams latency values.
2. **Spark latency with several inputs** is the maximum over inputs, not the sum.
3. **`uniform_1` is included,** as described above. The old CSV excluded it.
4. **18 runs** that the old CSV listed as Google Cloud Monitoring `uniform` are now in `google-cloud-monitoring/m510/flink/cpu-per-category/uniform_2/`. Their `job_name` is "Flink smart grid job", so they are Smart Grid runs filed under GCM. Keep them out of GCM analyses.
5. **Not in the old CSV:**
   - all operator-level data;
   - `uniform_1` (162 runs);
   - `uniform_2` (18);
   - consumed input, sink-operator throughput, broker rates, the experiment network, busiest-node CPU;
   - per-run flags.
6. **Unchanged:** for the 7,708 runs present in both, CPU is identical in every run. Throughput is identical except for the 0 → NaN cases, plus 6 Flink/Storm runs where the old CSV had no value and the recomputed series now gives one. Latency differs only in the 0 → NaN cases and in about 60 multi-input Spark runs (maximum instead of sum).
