# PStreamBench — Workload Implementation

> **Status: PENDING.** Not yet written. Needs, for every one of the 14
> real workloads (Ad Analytics, Bargain Index, Click Analytics, Google
> Cloud Monitoring, Linear Road, Log Processing, Machine Outlier,
> Sentiment Analysis, Smart Grid, Spike Detection, TPCH, Traffic
> Monitoring, Trending Topics, Word Count) plus the 9 synthetic shapes:
> logical query semantics, operator topology, source/sink behavior,
> window semantics, filter/join/aggregation semantics, per-engine
> implementation differences, and how semantic equivalence across engines
> was (or wasn't yet) verified.
>
> Real source material to read and ground this in:
>
> - `PStreamBench_workload_generator/{flink,storm,spark,kafkastreams,risingwave}/`
>   — the actual job source per engine, one subfolder per workload. This
>   is the primary source; nothing here should be written from the
>   workload *name* alone without reading its implementation per engine.
> - `PStreamBench_benchmark_data/operator_templates.csv` — already
>   contains a verified (not guessed) operator-to-stage mapping for 125
>   (framework, query, variant) combinations, built by reading job source
>   directly. This is a ready-made cross-engine topology reference — see
>   the "Contributions So Far" doc's "Operator-to-stage mapping" section
>   for the methodology (382 template rows, 124 verified against source,
>   206 inferred, 50 for job-level engines, 2 explicitly left unresolved).
> - `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §2 — full design rationale and
>   structure table for the 9 synthetic shapes (already written to
>   reconstruction depth for Storm + Flink; needs folding in here, not
>   rewriting).
> - The "Rigor principles" section of the "Contributions So Far" doc —
>   **semantic equivalence is checked, not assumed**: Kafka Streams and
>   RisingWave are only compared head-to-head on a workload once both
>   jobs' source has been read and confirmed to compute the same logical
>   result — currently confirmed only for Word Count. This file should
>   state plainly, per workload pair, which comparisons are verified and
>   which are withheld pending that check, per the user's explicit
>   instruction to document non-equivalence cases rather than skip them.
> - Cross-engine structural differences already found while building
>   `operator_templates.csv` (worth a dedicated subsection, not buried):
>   Flink sometimes merges a source and its parser into one parallelism
>   slot where Storm keeps them separate; some jobs have a parallelism
>   vector slot that's never actually read by any operator (a "dead"
>   configuration dimension); whether the sink gets its own independent
>   parallelism value differs by engine and by query. Linear Road
>   specifically: the Kafka sink's `.sinkTo(sink)` call has no
>   `.setParallelism()` at all (Flink), so it silently inherits the
>   source's parallelism — a genuine structural difference from every
>   other workload, found by reading `LinearRoad.java` directly.

Needs: one subsection per workload (14 real + 9 synthetic), each with a
topology diagram (see the user's request for diagrams —
`docs/` as a whole should gain a `diagrams/` subfolder once this file and
`01_architecture.md`'s data-flow diagram have a consistent visual style
to extend).
