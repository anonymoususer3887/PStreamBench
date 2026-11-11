# Step 2, hypothesis 2 (Storm): results

Plan: `step2_plan_storm.md`, recommendations D1–D7 taken as approved ("continue with storm", 2026-10-25).

**Change from the plan:** the spout metric was dropped as unreliable. Instead, **stage-1 input** is used: the rows/s executed by the first bolt stage, which receives every input row (filter shapes: `window-agg`; 2-/3-way joins: sum of `filter-<i>`; 4–6-way joins: `join`). **Throughput** stays the sink metric (`throughput_50`).

Files are in `PStreamBench_benchmark_data/plot_synthetic/storm/`:
- `storm_extract.py`, `storm_summary.py`: the scripts.
- `storm_syn_runs.csv`, `storm_syn_configs.csv`: every synthetic value.
- `storm_real_runs.csv`, `storm_real_configs.csv`: every real value.
- `storm_uniform_summary.csv`, `storm_p3_rank_correlation.csv`, `storm_p4_peak_dop_real.csv`: the tables behind each verdict.

Data: 487 synthetic runs (486 configurations), 1401 real runs (1391 configurations). Nothing is dropped. Rounding happens only in this text.

## Verdicts

### P1 Throughput peaks and then falls: holds at 500k/1M for 10 of 18 pairs, not at 100k

Stage-1 input over the uniform sweep:

| | 100k | 500k / 1M |
|---|---|---|
| Filter shapes, LinearQuery | flat at about 78k rows/s from DoP 4 on (DoP 40 / peak = 0.98–0.99) | **inverted U.** Peak at DoP 8 (65k–395k rows/s), then down to **6–27 % of the peak at DoP 40** |
| 2-/3-way joins | flat (0.90–0.98) | 500k: 0.82 / 0.97; **1M: 0.11 / 0.11** (peak at DoP 8 about 785k, DoP 40 about 83k) |
| 4–6-way joins | flat (0.92–0.99) | **no fall** (0.79–1.00) |

**Too little parallelism also fails.** At DoP 2, the filter shapes execute only 4–50 rows/s at every rate, including 100k. At 500k/1M, the 4–6-way joins execute 0. LinearQuery at DoP 2 handles 20k–78k. So the curve is an inverted U: near zero at DoP 2, a peak at DoP 8, a fall at 32–40.

Sink throughput follows the same pattern but is noisier, because the window aggregation sets its level.

### P2 The fall comes with CPU near saturation: holds, and the CPU is not spent in the operators

- At DoP 40, `cpu_95` is 82–100 % and the busiest worker node's median CPU is 46–86 %.
- At the same time, the most-loaded bolt's busy ratio (Storm "capacity", the share of time a bolt executes tuples) is **at most 0.21, mostly below 0.06**.
- So most of the CPU goes to work outside operator logic: queue handling, serialization between workers, acking, thread scheduling.
- At 100k, where the input does not change with DoP, `cpu_50` still rises from 8–16 % at DoP 2 to 43–63 % at DoP 40. That is the pure cost of more executors, with no throughput gain.

### P3 The drop lines up better with total executors than with per-operator DoP: not supported

Rank correlation between normalized stage-1 input and either measure is weak for both (|ρ| ≤ 0.30). Total executors does not explain the drop better than per-operator DoP. The data cannot separate the two explanations.

### P4 More cores per node move the peak to a higher DoP: partly holds

Median DoP with the highest sink throughput, real applications:

| Rate | m510 (40 slots) | c6525-25g (80 slots) | c6525-100g (120 slots) |
|---|---|---|---|
| 100k | 24 | 48 | 32 |
| 500k | 36 | 72 | 64 |
| 1M | 40 | 40 | 40 |

The peak shifts upward on larger hardware at 100k and 500k, but not at 1M. c6525-100g (most cores) peaks below c6525-25g. Consistent with the hypothesis for 2 of 3 rates. Not a clean core-count effect.

### P7 Network traffic rises with DoP: holds

At 100k, where the input stays flat, traffic on the experiment network at DoP 40 is **1.1–25×** that at DoP 2, for 9 of 9 shapes. Filter shapes rise 6–25×, joins 1.1–3×. Bytes sent per input row therefore grow with DoP.

The CSV's `network_*` columns measure the control interface (`node_network_out`, eno1), not the experiment network. Real-application network numbers in the CSV therefore do not show data traffic.

### Memory (no prediction)

`memory_50` rises 2.5–4× from DoP 2 to DoP 40 at every rate, independent of load. This is a per-executor overhead.

### H2 (stall under overload): not testable

The spout metric is broken. The near-zero processing at DoP 2 fits overload with unbounded in-flight tuples, but without ack/fail counts that cannot be shown.

## Contribution-table row (Storm)

| Architectural property | Execution mechanism under higher DoP | Measured effect | Condition |
|---|---|---|---|
| Executors are threads in a **fixed set of worker JVMs** sharing the cores; every tuple is **acked**; **no limit on in-flight tuples**; shuffle groupings spread tuples over all workers | More DoP adds threads but no cores; more tuples cross worker boundaries (serialization, Netty); more threads compete for CPU and ack traffic grows | **Inverted U** at 500k/1M: peak at DoP 8, down to 6–27 % of the peak at DoP 40 for cheap stages. CPU p95 82–100 % while bolts execute < 6 % of the time. At 100k, DoP 2→40 multiplies CPU 3–6×, memory 2.5–4× and network up to 25× without any throughput gain | Higher DoP helps Storm only until each stage has enough executors for its load. Beyond that, the per-tuple transfer and scheduling cost grows faster than the work it parallelizes. The penalty is largest for **cheap per-tuple stages** (filters, linear query) and absent for **expensive stages** (4–6-way joins), where operator work dominates the overhead |

## Caveats

1. Stage-1 input is an operator metric (executed rows/s). It is a proxy for consumed input, used because the spout metric is broken.
2. Contention cannot be separated from ack timeouts and replays (no ack/fail counts).
3. The DoP grid is coarse (2, 4, 8, 16, 32, 40). "Peak at DoP 8" means "between 4 and 16".
4. The m510 hardware specification (8 cores per node) is still to be verified against CloudLab's documentation.
