# Step 2, hypothesis 5 (Spark): results

Plan: `step2_plan_spark.md`. Decisions of 2026-10-27: D1–D6 as recommended, and D4, reading the job code, allowed.

Files are in `PStreamBench_benchmark_data/plot_synthetic/spark/`:
- `spark_prepare.py`, `spark_summary.py`: the scripts.
- `spark_runs.csv`: every attempt with a success flag. `spark_configs.csv`: medians over successful attempts, with attempt and failure counts.
- `spark_uniform_summary.csv`, `spark_failures_by_cpu.csv`, `spark_real_uniform_summary.csv`: the tables behind each verdict.

Data: 1081 synthetic Spark runs. Uniform = `uniform_e2e_rerun` (419 attempts), random/ds2/dhalion from the original folders. RocksDB (125 runs) is kept as a separate configuration. 9 runs whose processing-rate p50 was 0 use the flagged p95 instead.

## From the job code (`SyntheticTopology.java`)

- **Source:** Spark's built-in `rate` format, at the offered rate; **no Kafka**. It generates rows for the elapsed time, so a slow batch makes the next one larger. There is no per-trigger cap and no trigger.
- **Sink:** `noop`, in append mode.
- **Parallelism:** narrow stages use `repartition(n)`. **All shuffles share one `spark.sql.shuffle.partitions` value**, the last one set before `start()`, so every join and the window aggregation get the same width.

## Verdicts

### P1 Processing rate rises until tasks cover the cores, then is flat: plateau reached at DoP 4, far below 40 cores

- **Single-input shapes:** at their maximum from DoP 2 or 4. At 1M they drop slightly towards DoP 40 (DoP 40 / maximum 0.89–0.93).
- **Joins:** clearly limited at DoP 2, then flat from DoP 4. For example, TwoWayJoin at 500k: 464k → 607k rows/s; ThreeWayJoin at 1M: 252k → 712k rows/s.
- The limit is the 2-task stages at DoP 2, not the 40 cores.

### P2 CPU rises with DoP while tasks are fewer than cores: not supported

`cpu_50` stays at 1–5 % for single-input shapes and 5–20 % for joins, with no consistent trend over DoP. The cores are never the limiting resource.

### P3 Latency follows batch duration: holds

The rank correlation between the end-to-end lag and the batch duration over all 95 successful uniform configurations is **ρ = 0.95**.

### P4 Latency falls with DoP and then flattens or rises: the "rises" side holds for light shapes

- **Light shapes at 100k and 500k:** from DoP 2 to DoP 40, batch duration grows **2.8–4.6×** (e.g. LinearQuery at 100k: 0.8 → 2.6 s) and latency grows **1.9–2.8×**. There is no initial fall.
- **1M:** batches are 20–35 s long at every DoP, and latency stays roughly flat (0.6–1.1×).
- **Mechanism, consistent with the data:** the per-record work is small, so with more partitions each batch pays more task-scheduling and launch cost than it gains in parallel work.
- **Real applications** (heavier per-record work) show the opposite tendency: batch duration at DoP 40 against DoP 2 has a median of 0.90× and is shorter in 36 of 54 cases.

### P5, P6 Wide joins with the default store fail at every DoP: holds for 5- and 6-way joins, mostly for 4-way

| Shape | Configurations without output |
|---|---|
| SixWayJoin | 18 of 18 |
| FiveWayJoin | all 12 at 500k and 1M; 2 of 6 at 100k |
| FourWayJoin | all 6 at 1M; at 500k, DoP 2, 32 and 40 (DoP 4–16 succeed) |

So failure is set by join width and rate. DoP matters only at the margin (4-way at 500k).

**In most failed uniform attempts the cluster was idle** during the measurement window (busiest node < 15 % CPU): 6-way 121 of 121 attempts, 5-way 87 of 93. The job had already died or never completed a batch. The data cannot separate "out of memory in the first batch" from "never started".

### P7, P8 RocksDB lets wide joins succeed: not testable with the stored runs

- **None of the 125 RocksDB runs produced output.** Their busiest node had a median CPU of 1.1 % (max 9 %), so the jobs were not processing during the measurement.
- The successful test your agent reported (FourWayJoin at 500k, DoP 8) is **not** among the stored runs. The `uniform_rocksdb_retry/500000` folder only contains DoP 2, 32 and 40.

### P9 Network and memory rise with DoP: weak

- At 100k, experiment-network traffic rises 1.4–2.1× for single-input shapes and up to 9.6–47× for joins from DoP 2 to DoP 40.
- At 1M it is near zero at every DoP.
- Memory changes little (about 5–8 GB).

### Real-data check

In the real Spark runs, `cpu_50` is 5.9–6.5 % in every configuration, almost constant. The CPU column of the real Spark data looks unreliable and should be checked before use.

## Contribution-table row (Spark)

| Architectural property | Execution mechanism under higher DoP | Measured effect | Condition |
|---|---|---|---|
| **Micro-batches** on a **fixed pool of 40 cores**; DoP sets the number of tasks per stage, and **all shuffles share one partition count**; the next batch takes everything that arrived meanwhile (no cap); default state lives **on the executor heap** | More tasks add no cores. With light per-record work, each batch pays more per-task scheduling cost, so batches and latency get longer. A slow batch makes the next one larger, and join state for that batch must fit on the heap | Processing rate plateaus from DoP 4 (only the 2-task stages at DoP 2 limit it). For light shapes, batch duration rises 2.8–4.6× and latency 1.9–2.8× from DoP 2 to 40, and latency tracks batch duration (ρ = 0.95). CPU stays 1–20 %. 5- and 6-way joins fail at every DoP above 100k. Real applications, with heavier per-record work, get slightly shorter batches at high DoP | Spark gains from DoP only until the tasks cover the per-batch work. Beyond that, extra partitions lengthen every batch, and so latency, unless per-record work is heavy. Whether wide stateful joins run at all is decided by batch size and state placement, which DoP cannot change |

## Caveats

1. Only run-level percentiles exist, not individual batches. Batch growth is inferred from batch duration and failures.
2. Spark's latency (ingestion-to-processing lag) stops before the sink.
3. The RocksDB comparison needs new runs that actually process data, plus 2- and 3-way joins under both stores for overlap.
4. The original `uniform` folder (162 runs) has output in only 9 runs, which reflects the earlier collector problems. It was kept separate and not used.
