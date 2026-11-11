# Framework Integration — RisingWave

> **Status: PENDING full write-up.** `KAFKA_STREAMS_RISINGWAVE_PLAN.md`
> Part B is the primary design source to fold in here.

## Install & launch
- Version: **v3.0.1** binary (single self-contained Rust binary, no
  JVM/Maven toolchain needed — unlike every other engine here).
- Two deployment modes, both used in this project:
  - **`playground`** — meta+compute+frontend+embedded (non-shared) state
    store, all one process. Confirmed sufficient for single-node testing
    (local Docker path, `risingwavelabs/risingwave:v3.0.1` image).
  - **Real multi-node** — meta+frontend on the master node, one compute
    process per worker node (mirrors Flink's JobManager/TaskManager
    placement for a fair resource comparison). Requires a **shared
    object store** (MinIO) for Hummock, because `hummock+fs://` (a
    local-disk path) is not a shared/networked filesystem — confirmed
    live: a compute node on a different physical machine cannot read
    Hummock SST files written by another node's local disk (see
    `risingwave-minio-start.yaml`'s own comment, and
    `docs/14_issue_log.md` INFRA-ENV-003 for why this path is currently
    blocked for *fresh* nodes as of October 2026).

## Configuration
- Jobs are **SQL**, not a jar — `CREATE SOURCE` / `CREATE MATERIALIZED
  VIEW` / `CREATE SINK`, submitted via `psql`.
- `PStreamBench_workload_generator/risingwave/dsp_risingwave_jobs/` — one
  `.sql` file per workload/variant, no build step at all.

## Workload / query implementation
- Reuses `build_sql_with_parallelism()`
  (`PStreamBench_controller/dsp_be/benchmark_sweep/run_sweep_risingwave.py`)
  to template the parallelism value into each `.sql` file at submit time
  — this function is imported and reused directly by the local Docker
  path's `submit_risingwave.py` rather than reimplemented.
- `ads_analytics.sql`'s own file header said "NOT YET VALIDATED against a
  live RisingWave instance" before this project's local Docker testing
  pass — that pass was its first real end-to-end validation.

## Parallelism representation
- **Single job-level scalar**, same architectural category as Kafka
  Streams — not a per-operator vector.

## Source/sink
- Kafka source/sink via RisingWave's native Kafka connector syntax in
  the `CREATE SOURCE`/`CREATE SINK` statements.

## State / window
- *Not yet confirmed in this pass* — needs direct reading of the `.sql`
  files' windowing clauses (RisingWave uses SQL-native windowing
  functions, structurally different from the other 4 engines' API-based
  windowing).

## Framework-specific limitations found
- **Does not self-terminate** — persistent catalog objects
  (source/materialized view/sink) keep running until explicitly dropped
  (`DROP SOURCE/MATERIALIZED VIEW/SINK`), unlike Flink/Kafka Streams/Spark.
  The local Docker submit script ignores the `duration` argument other
  engines use for exactly this reason.
- **No live Grafana dashboard today** — `WUI_IMPROVEMENTS_TODO.md` item 10.
- **Real multi-node deployment currently blocked** on fresh
  infrastructure — MinIO's discontinuation of free binary/image
  distribution (INFRA-ENV-003). `playground` single-node mode is
  unaffected.
- A 4-core trial license auto-activates once meta detects more cores
  than that in the cluster — **[inferred, from a related finding in a
  different engine's notes; needs direct confirmation for RisingWave
  specifically before stating as established fact.]**

## Metrics collection
- `infra/reporting_scripts/getprom_risingwave.py` — RisingWave exposes
  Prometheus metrics **natively** (no JMX-bridge exporter needed, unlike
  Kafka Streams/the Kafka broker), confirmed live via its own
  `:1222`/`:1250`/`:2222` endpoints.

## Run termination
- Explicit `DROP` statements required — see above. Not optional; skipping
  this leaks persistent catalog objects the way Storm leaks `ACTIVE`
  topologies.

## Known differences from other engines
- SQL-based job definition, not a compiled jar — the only engine here
  with no build step.
- Single-scalar configuration, shared only with Kafka Streams.
- Does not self-terminate, shared only with Storm (for a different
  reason: Storm's is a bug, RisingWave's is architectural — persistent
  catalog objects are the intended design).

**Still needed for this file:** window-clause semantics (direct `.sql`
source read), the meta/compute/frontend process topology diagram, and
confirmation (not inference) of the trial-license core-count behavior.
