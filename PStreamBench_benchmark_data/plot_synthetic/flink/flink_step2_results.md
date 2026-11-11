# Step 2, hypothesis 4 (Flink): results

Plan: `step2_plan_flink.md`. Decisions of 2026-10-26:
- all strategies included;
- data from the new `datasets_v2` files;
- job code read for edge types;
- failed runs skipped.

Files are in `PStreamBench_benchmark_data/plot_synthetic/flink/`:
- `flink_prepare.py`, `flink_summary.py`: the scripts.
- `flink_runs.csv`, `flink_operator_rows.csv`: every value used.
- `flink_failed_runs.csv`: the 27 runs without consumed input, listed and not analysed.
- `flink_uniform_summary.csv`, `flink_p5_random_latency_rank.csv`, `flink_p6_marker_latency_per_operator.csv`, `flink_p2_p7_backpressure.csv`, `flink_p10_real_random_operator_scaling.csv`: the tables behind each verdict.

Data: 486 synthetic Flink runs (uniform, random, ds2, dhalion); 459 analysed after removing the 27 failed runs.

## Three things found in the code and the collector that change earlier results

1. **Flink's operator `throughput_per_sec` is the mean over subtasks, not the operator total.** `getprom.py` computes it as `avg(avg_over_time(numRecordsOutPerSecond))` over subtasks; the other systems report totals. Consequences:
   - The Flink sink throughput in the synthetic figures of 24 October was a per-subtask value. **Its fall with DoP** (e.g. TwoFilters 216 → 11 rows/s) **was this artefact.** As an operator total, sink output is flat (≈ 431 rows/s at every DoP).
   - `datasets_v2` has been rebuilt with a new column, `throughput_total_per_sec` = mean × operator DoP. `source_operator_throughput` and `sink_operator_throughput` are now operator totals for Flink.
   - `query_performance_data.csv` throughput is not affected: it uses the summed throughput series.
2. **Synthetic Flink sources are in-job data generators, not Kafka.** `DataGeneratorSource` is rate-limited to the offered rate. So there is no waiting in Kafka in these runs, and the latency markers cover the whole path from the source.
3. **One node (the master) sits at about 53 % CPU in every run**, independent of load. The job client busy-waits until it cancels the job (`runAndSelfCancel`). This inflates `cpu_busiest_node_50`; `cpu_50` is less affected.

## Verdicts

### P1 The fixed source (DoP 2) caps consumed input: not supported

- Single-input shapes and 2- and 3-way joins consume **0.865–0.870 of the offered rate** at every rate and every DoP. Some configurations have no value because their runs failed. The 3-way join at 1M falls to 0.60–0.70 at DoP 2 and 4.
- The constant fraction means the source is not a cap. A cap would show a constant *absolute* rate. The 13 % gap is the same at 100k and 1M, so it is most likely the rate limiter, or the 5-minute averaging window including start-up.
- **Only the 4-, 5- and 6-way joins fall short.** 4-way at 1M: 0.29 at DoP 2 → 0.87 at DoP 40. 5-way at 1M: 0.27 → 0.67 at DoP 32. 6-way stays at 0.17–0.37 at every DoP.

### P2 Where input falls short, the source is not backpressured: contradicted, and that is the explanation

- Where consumed input falls short (4–6-way joins at 1M), **source backpressure is 0.40–0.89**, and the busiest join operator is busy 0.70–0.99 at DoP 2–4.
- The bottleneck is the join. Credit-based flow control propagates it upstream and throttles the generator.

### P7 Backpressure appears upstream and falls as the bottleneck gets more subtasks: holds for 4- and 5-way joins

| Shape at 1M | Source backpressure | Busy ratio of the busiest join | Consumed input (fraction of offered) |
|---|---|---|---|
| 4-way, DoP 2 → 40 | 0.80 → 0.17 | 0.70 → 0.30 | 0.29 → 0.87 |
| 5-way, DoP 2 → 32 | 0.83 → 0.55 | 0.99 → 0.15 | 0.27 → 0.67 |

**Exception: the 6-way join.** Source backpressure stays at 0.81–0.89 although the joins are only 0.07–0.19 busy at DoP 16–40. The limit there is not operator compute. Candidates: exchange or credit limits on its five chained keyed exchanges (the network buffers between joins), or the window aggregation.

### P8 No collapse at high DoP: holds for 24 of 27 shape-rate pairs

- Sink output at DoP 40 is ≥ 0.88 of its maximum in 24 of 27 pairs.
- **Exceptions:**
  - 5-way at 1M: 0.43. Consumed input drops from 0.67 at DoP 32 to 0.49 at DoP 40.
  - 6-way at 1M: 0.28, with its maximum at DoP 2.
  - FourFilters at 1M: its output is about 0.004 rows/s, too selective to compare.
- In contrast to Storm's inverted U, Flink degrades through backpressure, not through collapse.

### P3 Latency rises with DoP at fixed input: holds only where a keyed edge is p × p

| Shape | DoP 40 / DoP 2 | Rank correlation with DoP |
|---|---|---|
| LinearQuery | 1.6–2.3× | 0.80–1.00 |
| TwoWayJoin at 500k | 2.2× | 0.94 |
| Filter shapes | 0.96–1.12 (flat) | — |
| Overloaded 4- and 5-way joins | 0.43–0.66 (falls) | — |

For the overloaded joins, clearing the backpressure removes queueing.

### P4 The rise grows with the number of operator boundaries: contradicted

FourFilters (6 exchanges) is flat, while LinearQuery (4 exchanges) rises the most. The **kind** of exchange matters, not the number:
- LinearQuery has a keyed exchange between two operators at DoP p (filter1 → window-agg: p × p channels).
- The filter shapes key only on the source edge (2 × p channels); their other edges are 1:1 forward channels.

### P5 Random configurations: latency follows channel count better than mean DoP: mixed

- Channel count correlates more strongly than mean DoP in 17 of 27 shape-rate groups.
- But the medians are ρ = 0.20 (channels) against 0.37 (mean DoP).
- Clear for LinearQuery (0.69–0.85 against 0.36–0.54) and ThreeWayJoin at 100k. Not for the filter shapes and the wide joins.
- With 5–10 configurations per group, this cannot be settled.

### P6 Per-hop marker latency: consistent with P3

At 100k, marker latency at the last operator:
- LinearQuery: rises 122 → 181 ms.
- Filter shapes: flat at 240–290 ms.
- TwoWayJoin: 137–214 ms.
- SixWayJoin: rises 1.5 s → 17 s.

### P9 CPU rises at flat input: holds, but small

- `cpu_50` from DoP 2 to 40: +0.5–1.3 pp for filter shapes, +1–3 pp for LinearQuery, +2–23 pp for joins.
- Busy ratios stay ≤ 0.3 at high DoP.
- Much smaller than Storm (+30–50 pp at flat input).

### Network: rises steeply with DoP

Experiment-network traffic at DoP 40 against DoP 2:
- LinearQuery: 22–176×.
- 4- and 5-way joins: up to 240–310×.

A plausible reason: with slot sharing, a DoP-2 pipeline fits into few slots on one TaskManager, so its exchanges stay local. At high DoP the subtasks are spread over all five TaskManagers, and the exchanges cross the network. The data agrees but cannot prove it; slot placement was not recorded.

### P10 (real applications) Single-key applications: not conclusive

- Operator throughput exists only for the newer real runs: random (10 configurations per rate), ds2, dhalion and uniform_1.
- In Bargain Index and Spike Detection, the parser upstream of the keyed operator is backpressured (0.58 and 0.81 at 1M).
- So is the parser in many-key applications (Machine Outlier 0.82, Word Count 0.91). The backpressure is not specific to a single key.
- Busy ratios are 0 in all real Flink runs (not collected).

## Contribution-table row (Flink)

| Architectural property | Execution mechanism under higher DoP | Measured effect | Condition |
|---|---|---|---|
| Pipelined subtasks with **operator chaining disabled** (every boundary is an exchange); keyed edges are **all-to-all** (p × q channels); **credit-based backpressure**; slot sharing packs low-DoP jobs onto few TaskManagers | More subtasks relieve an overloaded operator, and backpressure releases the source. On keyed edges between parallel operators, channels grow with p², each carrying fewer records. As subtasks spread over TaskManagers, exchanges move from local to network | Input never collapses: light shapes keep up at every DoP. Overloaded joins recover from 0.29 to 0.87 of the offered input as the join's DoP rises (source backpressure 0.80 → 0.17). Latency rises 1.6–2.3× only where a p × p keyed exchange exists, and falls 0.43–0.66× where backpressure clears. Network traffic grows by up to two orders of magnitude; CPU by only 1–3 pp for light shapes | Higher DoP helps Flink exactly where an operator is the bottleneck, and the backpressure ratios show which operator. Beyond that, more subtasks cost latency only on all-to-all keyed exchanges, and network traffic as the job spreads across machines. Flink degrades through backpressure rather than collapse |

## Caveats

1. The 0.87 consumed fraction is the same at all rates. It reflects the rate limiter or the measurement window, not the system.
2. Slot placement is not recorded, so the local-to-network explanation for the network growth is inferred.
3. Latency markers skip operator processing and window waits. In these synthetic runs there is no Kafka waiting to miss.
4. All jobs run with chaining disabled, so the effect of chaining itself is not measured.
