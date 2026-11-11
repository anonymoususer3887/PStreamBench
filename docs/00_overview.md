# PStreamBench — Project Overview

> **Status of this document:** written from the actual repository (code,
> configs, and the project's own working notes — `KAFKA_STREAMS_RISINGWAVE_PLAN.md`,
> `MULTI_ENGINE_LOCAL_TESTING_AND_BENCHMARK_SWEEP.md`, `SPARK_VALIDATION.md`,
> `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md`), not from memory. Where something
> could not be directly confirmed in code, it is marked **[inferred]**.
> This is the first file in a larger reconstruction-guide effort (see
> `docs/` for the rest); later files go deeper on each area this one only
> introduces.

## 1. What PStreamBench is

PStreamBench is a benchmarking platform for Distributed Stream Processing
Systems (DSPSs). It exists to answer one practical question with real,
reproducible measurements rather than vendor claims or a single engine's
own numbers: **under what workload, hardware, and configuration conditions
does each engine's performance actually hold up, and how does that change
as you scale parallelism?**

Concretely, it is three things working together:

1. **A workload generator** — real-world benchmark queries (14 of them,
   see `docs/03_workloads.md`) and, as of this project's extension, a
   **synthetic workload suite** (9 fixed query shapes — linear, chained
   filters, multi-way joins) implemented independently for each supported
   engine.
2. **A controller + WUI** — a Django backend (`PStreamBench_controller`)
   and Vue frontend (`PStreamBench_wui`) that provision CloudLab clusters,
   deploy an engine, submit a job, collect metrics, and tear everything
   down again — either through the browser or by driving the same REST
   API directly.
3. **An analysis pipeline** (`PStreamBench_benchmark_data`) — scripts that
   turn raw per-run metric JSON into the CSVs and figures a paper actually
   cites.

## 2. Research questions it supports

Derived from the actual plot families and sweep scripts in the repo
(`parallelism_strategies.py`, the `run_strategy_sweep_*.py` scripts, and
the enumeration-analysis plotting modes — see `docs/05` and `docs/08`):

- **RQ1 — Does performance scale monotonically with parallelism, or does
  it saturate/degrade, and where?** The original (v1) motivation: sweep a
  single *uniform* degree of parallelism (DoP) across every workload and
  observe the shape of the throughput/latency curve.
- **RQ2 — Does *which* operator gets more parallelism matter, not just
  *how much* total parallelism is provisioned?** This is why
  `parallelism_strategies.py` exists at all: `uniform` alone can't ask
  this question, because it gives every operator the same value by
  construction.
- **RQ3 — Can automated, metric-driven search (DS2, Dhalion) find a
  better heterogeneous configuration than blind random search, and where
  does that advantage actually matter?**
- **RQ4 — Does an engine's architecture (per-operator parallelism vectors
  — Flink/Storm/Spark — vs. a single job-level scalar — Kafka
  Streams/RisingWave) change how much heterogeneous configuration is even
  worth?** This split did not exist in the original two-engine (Flink,
  Storm) evaluation; it only becomes askable once engines on both sides of
  that architectural line are in the benchmark.
- **RQ5 (synthetic-workload specific) — Is the multi-way join operator
  specifically the most parallelism-sensitive stage in a DSPS pipeline,
  independent of any one real workload's data characteristics?** This is
  why the synthetic suite exists instead of relying solely on the 14 real
  workloads, whose join operators (Ad Analytics, Click Analytics) are
  confounded with that workload's specific data shape.

## 3. Why the benchmark is designed the way it is

A few design choices only make sense in light of these research questions,
and are easy to misread as arbitrary if you don't know why they exist:

- **Per-run configuration identity, not an averaged "mean parallelism."**
  Averaging a run's parallelism vector into one number is only meaningful
  for `uniform` (every operator has the same value, so the mean *is* the
  configuration). For `random`/`ds2`/`dhalion`, two runs with very
  different per-operator allocations can average to the same number — the
  analysis pipeline instead tracks each run's exact vector as a
  `config_id` string. See `docs/05_parallelism_and_enumeration.md`.
- **Job-level engines never get a fabricated per-operator breakdown.**
  Kafka Streams and RisingWave expose one scalar knob, not a vector. The
  pipeline represents that honestly as a single value, rather than
  inventing a fake "operator-level" parallelism split for them to make the
  plots look uniform across engines. Their *performance* metrics are still
  tracked per logical operator — configuration and runtime performance are
  two different axes.
- **Semantic equivalence is checked before cross-engine comparison, not
  assumed.** Two engines' jobs for "the same" workload are only compared
  head-to-head once their source code has been read and confirmed to
  compute the same logical result. See `docs/03_workloads.md` and
  `docs/09_validation.md`.
- **Synthetic workloads are fixed-shape, not randomly generated.** An
  earlier/related tool (`pdspbench-plan-generation`) generates query plans
  by randomly deciding which operators to include — useful for training an
  ML performance-prediction model, useless for controlled benchmarking,
  where every run of "the same shape" must have an identical operator
  graph so only parallelism and rate vary. See
  `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §1–2 for the full comparison.

## 4. Scope

**In scope** (confirmed built and in active use):
- 5 streaming engines: Apache Flink, Apache Storm, Apache Spark
  (Structured Streaming), Kafka Streams, RisingWave.
- 14 real-world workloads (Ad Analytics, Bargain Index, Click Analytics,
  Google Cloud Monitoring, Linear Road, Log Processing, Machine Outlier,
  Sentiment Analysis, Smart Grid, Spike Detection, TPCH, Traffic
  Monitoring, Trending Topics, Word Count) plus their query variants.
- 9 synthetic workload shapes (Linear, Chained Filters ×2/3/4, N-way Join
  ×2/3/4/5/6), ported for Storm and Flink.
- 4 parallelism-enumeration strategies: Uniform, Random, DS2, Dhalion
  (plus `exhaustive`/`minavgmax`/`increasing`/`heuristic` in
  `parallelism_strategies.py`, used less centrally — see `docs/05`).
- CloudLab-based distributed deployment via a Django controller + Ansible
  playbooks, driven by a Vue WUI or directly via its REST API.
- A local, non-CloudLab Docker Compose environment
  (`PStreamBench_local_docker/`) for proof-of-life testing of all 5
  engines without a cluster.
- A metrics → CSV → figure analysis pipeline covering query-level,
  operator-level, and configuration/strategy-level plot families.

**Out of scope / not built** (explicit, not an oversight):
- Synthetic workload ports for Spark, Kafka Streams, RisingWave —
  Storm and Flink were chosen first; the other three are future work
  (`SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §10).
- Native (non-Docker, non-CloudLab) local execution of any engine from the
  WUI's interactive wizard — only Flink/Kafka Streams/RisingWave/Storm are
  wired into `PStreamBench_local_docker/scripts/pstreambench_cli.py`
  today; Spark is listed as a menu choice but not functional there (see
  `docs/04_frameworks/spark.md` for why).
- Exposing DS2/Dhalion (or any `parallelism_strategies.py` strategy) as a
  selectable option in the WUI's own Submit Job form — they currently only
  run via the command-line `run_strategy_sweep_*.py` scripts, which reuse
  the WUI's own job-submission endpoint in "Custom" mode rather than
  having their own. See `WUI_IMPROVEMENTS_TODO.md` item 11.
- Live Grafana dashboards for Spark, Kafka Streams, or RisingWave — only
  Flink and Storm have one today (`WUI_IMPROVEMENTS_TODO.md` item 10).
- A genuinely multi-node RisingWave deployment on fresh infrastructure as
  of October 2026 — blocked on MinIO's discontinuation of free binary/image
  distribution (`LOCAL_DOCKER_TODO.md`; `WUI_IMPROVEMENTS_TODO.md` item 9).

## 5. How to read the rest of this documentation set

| File | Covers |
|---|---|
| `01_architecture.md` | Repository structure, components, data flow, local vs. cluster vs. cloud execution |
| `02_environment_setup.md` | CloudLab provisioning, controller/WUI setup, local Docker setup |
| `03_workloads.md` | Every real workload's logical semantics, topology, and cross-engine equivalence |
| `04_frameworks/{flink,storm,spark,kafka_streams,risingwave}.md` | Per-engine integration detail |
| `05_parallelism_and_enumeration.md` | Uniform/Random/DS2/Dhalion, config IDs, vector mapping |
| `06_execution_pipeline.md` | What happens, step by step, during one benchmark run |
| `07_metrics.md` | Every collected metric: source, calculation, units, limitations |
| `08_analysis_and_plots.md` | The plotting pipeline, every figure family |
| `09_validation.md` | How correctness was verified |
| `10_troubleshooting.md` | Problems encountered — symptom, root cause, fix, validation |
| `11_design_decisions.md` | Meaningful choices, alternatives considered, trade-offs |
| `12_known_limitations.md` | Resolved / partially resolved / unresolved / accepted / future work |
| `13_rebuild_from_scratch.md` | End-to-end reconstruction guide |
| `14_issue_log.md` | Running engineering log — issue ID, date, component, root cause, fix, remaining risk |

**This set is a living document**, written incrementally rather than all
at once, given its scope. See each file's own status note at the top for
what's confirmed-from-code vs. still pending.
