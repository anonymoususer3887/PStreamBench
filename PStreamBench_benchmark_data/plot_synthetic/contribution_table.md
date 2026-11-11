# From architecture to performance: combined contribution table

Basis: the five step-2 analyses (`*_step2_results.md`) and `dsps_architecture_parallelism_sourced.md`. Every effect below has a CSV and a script behind it. "Unverified" marks what still rests on an assumption.

## 1. One row per system

| System | Architectural property | Mechanism when DoP rises | Measured effect | Condition: when more DoP helps or hurts |
|---|---|---|---|---|
| **Flink** | Pipelined subtasks; chaining disabled in our jobs (every boundary is an exchange); keyed edges all-to-all; credit-based backpressure | More subtasks relieve an overloaded operator and release the source; keyed edges between parallel operators grow to p² channels; subtasks spread from one TaskManager to many | Input never collapses. Overloaded joins recover from 0.29 to 0.87 of offered input as source backpressure falls (0.80 → 0.17). Latency rises 1.6–2.3× only where a p × p keyed exchange exists, and falls 0.43–0.66× where backpressure clears. Network traffic grows up to ~300×; CPU only +1–3 pp for light shapes | Helps exactly where one operator is the bottleneck; backpressure ratios identify it. Beyond that it costs latency on all-to-all keyed exchanges and network traffic. **Degrades through backpressure, not collapse** |
| **Storm** | Executor threads in a fixed set of worker JVMs; every tuple acked; no limit on in-flight tuples | More DoP adds threads, not cores; more tuples cross worker boundaries; threads compete for CPU | **Inverted U** at 500k/1M: peak at DoP 8, down to 6–27 % of the peak at DoP 40 for cheap stages. CPU p95 82–100 % while bolts execute < 6 % of the time. At 100k, DoP 2 → 40 multiplies CPU 3–6× without throughput gain | Helps until each stage has enough executors for its load; beyond that, per-tuple transfer and scheduling cost grows faster than useful work. Worst for cheap stages, absent for expensive ones (wide joins). Unverified: contention vs. ack replays (no ack/fail counts) |
| **Spark** | Micro-batches on a fixed pool of 40 cores; DoP = tasks per stage, one shared shuffle width; no per-batch cap; default state on the executor heap | More tasks add no cores; with light per-record work each batch pays more scheduling cost; a slow batch makes the next one larger | Processing rate plateaus from DoP 4. Light shapes: batch duration +2.8–4.6×, latency +1.9–2.8× from DoP 2 to 40; latency tracks batch duration (ρ = 0.95). CPU 1–20 %. 5-/6-way joins fail at every DoP above 100k. Real applications: batches slightly shorter at high DoP | Helps only until tasks cover the per-batch work; beyond that every batch, and so latency, gets longer unless per-record work is heavy. **Whether wide stateful joins run at all is set by batch size and state placement, not by DoP** |
| **Kafka Streams** | Parallelism = stream threads per instance; tasks fixed by the 100 input partitions; key changes go through the broker; output at commit or cache flush | Threads beyond the active tasks and cores add no capacity; they compete for CPU (producers co-located) and share the cache | Consumed input plateaus at 4–8 threads at every rate; CPU keeps rising (busiest worker up to 99 %). At 1M, real applications lose throughput at DoP 40 (to 0–0.28 of the maximum in 8 of 23) at up to 97 % CPU. Windowed latency ≥ ~1 s at every DoP | Helps until threads cover the active tasks and cores; after that the cost is CPU and, under high load and heavier per-record work, lower throughput. For repartition-heavy queries the broker host becomes the shared limit. Unverified: broker location; Random runs inconsistent |
| **RisingWave** | Fragments run as actors; DoP set per statement (in our setup only the stateful view takes the swept DoP; parsing fused into the source fragment at DoP 2); epoch/barrier commits | Extra actors help only if the stateful fragment is the bottleneck; the source side and the barrier cycle stay as they are | Input flat across DoP; synthetic source emits ~6–10k rows/s regardless of offered rate. Barrier latency takes a few recurring levels, independent of DoP. Exception where the stateful fragment is the bottleneck: Click Analytics (geo) sink output ×6–11 with DoP | Helps only where the scaled fragment is the bottleneck; otherwise the fixed source side and the epoch cycle cap throughput and set latency. Unverified: the ~9k rows/s source metric (possible under-count); no true end-to-end latency |

## 2. General conditions across systems

Each condition names the systems where it is shown and the evidence.

1. **Parallelism helps only where the scaled stage is the bottleneck.**
   - **Shown in:** Flink (backpressure clears as the join's DoP rises), RisingWave (only geo-based gains), Spark (only the DoP-2 stages limit joins).
   - **Meaning:** configured DoP is not effective DoP. A parallelism setting is useful only if it changes the bottleneck stage.
2. **Without more cores, more threads turn into overhead, and the execution model decides how that overhead appears.**
   - **Storm,** with no limit on in-flight work, collapses (inverted U).
   - **Kafka Streams,** pull-based, wastes CPU and, under heavy load, loses throughput.
   - **Spark,** micro-batched, gets longer batches.
   - **Flink,** with credit-based backpressure, degrades least.
   - **Evidence:** CPU rises at flat input in all four. Storm and Flink show it directly: high CPU, low operator busy ratios.
3. **Fixed parts of the job cap what extra parallelism can achieve.**
   - **Examples:** the source fragment (RisingWave), the partition count bounding the tasks (Kafka Streams), the fixed core pool (Spark).
   - **Meaning:** the cap is part of the deployment as much as of the system.
4. **All-to-all exchanges, not the number of operators, set the latency cost of parallelism in record-at-a-time systems.**
   - **Shown in:** Flink. Latency rises only on p × p keyed edges.
   - **Supporting:** network traffic grows steeply with DoP in Flink and Storm.
5. **Batch, epoch and commit cycles set a latency floor that parallelism cannot lower and may raise.**
   - **Spark:** batch duration.
   - **RisingWave:** barrier levels.
   - **Kafka Streams:** about 1 s or more, driven by commit and cache.
6. **For wide stateful queries, feasibility comes before performance.** Whether the query runs at all depends on state placement and batch or ingestion control, not on DoP.
   - **Spark:** 5-/6-way joins fail at every DoP.
   - **Flink:** the 6-way join stays backpressured with idle operators.
7. **Resource use is not useful work.** CPU rises with DoP without throughput gain in Storm, Kafka Streams and Flink. CPU-based scaling decisions would therefore add resources where they do not help (links to RQ1 §CPU).

## 3. What the paper should state as limitations

- One run per configuration in most cases; the median where repeated.
- Latency semantics differ per system: compare within systems only.
- Setup choices that shape results:
  - chaining disabled (Flink);
  - fixed workers and cores (Storm, Spark);
  - source at DoP 2 and per-statement control (RisingWave);
  - in-job sources without Kafka (synthetic Flink and Spark).
- Open data issues:
  - Storm collector (stall test, latency);
  - RisingWave source metric and latency;
  - Spark RocksDB runs without output;
  - Kafka Streams Random runs inconsistent with Uniform.
