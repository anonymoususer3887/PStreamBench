# Step 2, hypothesis 3 (Kafka Streams): results

Plan: `step2_plan_kafkastreams.md`. Decisions of 2026-10-26:
- all strategies included;
- the real data's producer-rate column (`event_rate_*`) not used;
- **6 instances** (master + 5 workers), confirmed in `cluster-simple-kafkastreams-job.yaml` (`hosts: alpha:betas`);
- commit interval taken as 30 s (2.8 docs; to be confirmed for 3.8).

The same playbook also shows that **the producers run on the worker nodes** (`hosts: betas`), next to the Kafka Streams instances. Worker CPU therefore includes producer load.

Files are in `PStreamBench_benchmark_data/plot_synthetic/kafkastreams/`:
- `ks_extract.py`, `ks_summary.py`: the scripts.
- `ks_syn_runs.csv`, `ks_syn_configs.csv`: every synthetic value.
- `ks_real_runs.csv`, `ks_real_configs.csv`: every real value.
- `ks_uniform_summary.csv`, `ks_random_vs_uniform.csv`, `ks_real_uniform_summary.csv`: the tables behind each verdict.

Data: 523 synthetic runs (486 configurations) and 1068 real runs (735 configurations). Nothing dropped; rounding only in this text.

**Assumption:** in the synthetic node series, `node0` is taken to be the master. 521 of 523 runs have it.

## Verdicts

### P1 Consumed input rises to a low thread count and is flat beyond it: holds

- **Single-input shapes** (linear, filters): consumed input reaches its plateau at **4 threads** and stays within 1 % of it up to 40 threads.
- **Joins:** most of the gain comes by **8 threads**. Consumed input at 40 threads is 0.83–1.00 of the maximum.
- **The plateau sits below the offered rate at 1M:**
  - single-input shapes: 0.65–0.70 of the offered rate at every thread count;
  - joins: 0.36–0.62.
- At 1M the single-input shapes leave the cluster mostly idle (busiest worker node 15–30 % CPU, master about 30 %). The cap there is therefore **not** the CPU of the stream threads. Candidates the data cannot settle: the broker, or the producer not reaching 1M. The synthetic Kafka input-rate field would answer the second; it is not used here.

**Real applications** show the same plateau at 100k and 500k. **At 1M, throughput at DoP 40 falls well below its maximum in 8 of 23 applications** (DoP 40 / maximum ≤ 0.28). Examples:

| Application | DoP 40 / maximum | `cpu_50` at DoP 40 |
|---|---|---|
| Machine Outlier | 0.15 | 97 % |
| Word Count | 0.11 | 89 % |
| Ad Analytics (with join) | 0.17 | 81 % |
| Click Analytics (count-based and time-based) | 0.20–0.28 | 85 % |
| Linear Road accident-detection, Traffic Monitoring | about 0 | — |

The real queries do more work per record than the synthetic shapes, so this is where more threads turn into contention.

### P2 CPU keeps rising after the plateau: holds

- **Synthetic,** from 8 to 40 threads at nearly unchanged input:
  - `cpu_50` rises by 2 pp (filters) to 11 pp (joins);
  - the busiest worker reaches **85–99 %** for the 4-, 5- and 6-way joins.
- **Real:** `cpu_50` is higher at DoP 40 than at DoP 2 in 69 of 69 application–rate pairs. At 1M it reaches 47–97 %.

### P3 The plateau point does not move with the input rate: holds

The plateau starts at 4 threads (single input) or about 8 threads (joins) at all three rates.

### P4 Windowed latency stays at seconds or more at every DoP: holds

- The lowest synthetic latency in any configuration is 1.05 s; filters 2.7–40 s, joins 3.6–650 s.
- Filter latencies lie mostly **below** the 30 s commit interval. This fits the documented rule that results are forwarded at the earlier of commit and cache pressure: at these rates the cache fills before the commit is due.

### P5 Smaller cache share per thread means more forwarded updates: partly holds

At unchanged consumed input, sink output at 40 threads is higher than at 8 threads for ThreeFilters and FourFilters at all three rates (6 of 6 pairs; for example FourFilters at 500k: 7.6 → 49 rows/s). It is unchanged for LinearQuery and TwoFilters (0 of 6). The mechanism is consistent, but the data cannot isolate it.

### P6 Latency rises where consumed input falls below offered: not supported

- The highest synthetic latencies occur at **100k**, where joins consume about 90 % of the offered rate. 2-way and 3-way joins rise from 37–47 s at 2 threads to 520–550 s at 40 threads.
- At 1M, where joins consume only 30–60 %, latencies are lower (5–100 s).
- In the real data, latency often **falls** with DoP while throughput falls (Machine Outlier at 1M: 61 s → 1.7 s; Word Count: 54 s → 0.12 s). Kafka Streams measures latency only on records that are processed, so lower output and lower latency can come together.

So latency here does not track the Kafka backlog. The low-load rise with threads is unexplained. Candidates: longer consumer-group rebalancing with 240 threads at start-up, and per-thread commit and punctuation scheduling. Both would need a startup trace.

### P7 Repartition traffic: holds

- **Joins** move 8–41 MB/s on the experiment network, **filters** 1.3–12.6 MB/s.
- Join traffic rises 1.1–1.7× from 2 to 40 threads; filter traffic stays flat. It never falls with DoP.
- The **master** node reaches 82–87 % CPU for the 5- and 6-way joins at 500k and 1M. If the broker runs there (topics are created from the master, `hosts: alpha`; the broker placement itself is not yet checked), this is the node every repartition round trip passes through.

### Random strategy: inconsistent with uniform, do not pool

Only the thread count is applied in our setup, so a random configuration should behave like the uniform configuration with the same thread count. It does not: consumed input in random runs is a **median of 0.65×** the uniform value at the same thread count (10th–90th percentile 0.41–1.03). This matches the earlier observation that Kafka Streams' random runs differ from uniform. Either the random runs were made under different conditions, or something besides `num.stream.threads` changed. Check before using them.

## Contribution-table row (Kafka Streams)

| Architectural property | Execution mechanism under higher DoP | Measured effect | Condition |
|---|---|---|---|
| Parallelism = **stream threads per instance**; tasks are fixed by the **100 input partitions**; every key change is a **broker round trip** (repartition topic); results are forwarded at **commit or cache pressure**, with the cache shared by all threads of an instance | More threads do not create more tasks. Beyond about 4–8 threads they compete for CPU, with producers co-located on the workers, and share the cache. Repartition traffic is concentrated on the broker host | Consumed input plateaus at 4–8 threads at every rate. CPU keeps rising (busiest worker up to 99 %). At 1M, real-application throughput at DoP 40 drops to 0–0.28 of its maximum in 8 of 23 applications while `cpu_50` reaches up to 97 %. Windowed latency never falls below about 1 s, and the master reaches 82–87 % CPU for wide joins | Raising threads helps Kafka Streams only until they cover the active tasks and the cores. After that, the cost is CPU and, under high load and heavier per-record work, lower throughput. For repartition-heavy queries the broker host becomes the shared limit, independent of DoP |

## Caveats

1. Consumed input is the sum of source-operator throughput, an operator metric.
2. Latency is an average (the only value reported); the "max" field is itself an average, according to the collector.
3. Tasks versus cores as the cause of the plateau cannot be separated (both are passed at about DoP 16–17 in total threads).
4. The producers share the worker nodes, so worker CPU is not Kafka Streams alone.
