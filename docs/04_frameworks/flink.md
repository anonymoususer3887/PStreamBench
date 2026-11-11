# Framework Integration — Apache Flink

> **Status: PENDING full write-up.** Substantial real material already
> exists scattered across this project's docs and code; this file's job
> is to consolidate it into one place, not re-derive it. What's already
> confirmed and ready to fold in:

## Install & launch
- Version: Flink **1.16.2** tarball (`archive.apache.org/dist/flink/flink-1.16.2/...`),
  matching `PStreamBench_Cloud_setup/install_all_tools.sh` / the new
  `install_flink_only.sh`. The local Docker path uses `apache/flink:1.16.0-java11`
  (closest available image tag, not an exact version match — flagged in
  `PStreamBench_local_docker/docker-compose.yml`'s own comment).
- Distributed: controller's Ansible playbooks
  (`cluster-complex-jm-create.yaml`, `cluster-complex-tm-create.yaml`,
  then `-start.yaml` variants) — JobManager on the cluster's main node,
  TaskManagers on every slave node, one task slot config via
  `_flink_config_manipulation()` (`infra/views.py`) templated from
  `infra/flink_files/sample-flink-conf.yaml`.
- REST API port: **8086** (not Flink's default 8081) — hardcoded in
  `getprom.py`; must match on both sides.

## Configuration / important parameters
- `taskmanager.numberOfTaskSlots` ← `cluster_tm_slots` (WUI form field).
- `jobmanager.rpc.address` / `taskmanager.host` — **must** be the real
  per-node address, not left to auto-detection; see `docs/14_issue_log.md`
  FLINK-004 for why.
- Prometheus reporter: `metrics.reporter.prom.factory.class` must be the
  exact `...PrometheusReporterFactory` class name; see FLINK-002.

## Workload / query implementation
- `PStreamBench_workload_generator/flink/dsp_flink_jobs/` (14 real
  workloads) + `dsp_flink_synthetic_jobs/` (9 synthetic shapes, separate
  project).
- CLI contract: Flink's own `ParameterTool`-style arguments
  (`-kafka-server`, `-parallelism`, `-mode`, `-input`, `-output`,
  `-waitTimeToCancel`, etc.) — **different convention** from
  Storm/Spark/Kafka Streams' shared `ArgParser.java` single-dash
  convention (see `docs/11_design_decisions.md` once written).

## Parallelism representation
- Per-operator vector, e.g. `2,2,2,2` for Word Count
  (source, tokenizer, counter, sink) — see
  `WordCountJob.java`'s own class comment for the exact mapping, and
  `operator_templates.csv` for every other workload's mapping.
- Default Flink scheduling packs one subtask per operator into the same
  slot per parallel slice (no `slotSharingGroup()` override anywhere in
  this project's jobs) — slot need is `max(vector)`, not `sum(vector)`.
  **This was gotten wrong once already** — see FLINK-005 in the issue log
  before assuming otherwise.

## Source/sink
- Kafka source/sink via Flink's Kafka connector, standard
  `KafkaSource`/`KafkaSink` builders.
- Linear Road: sink has **no independent parallelism** — inherits the
  source's value (see `docs/03_workloads.md` once written).

## State/checkpoint/window
- *Not yet confirmed in this pass* — needs reading each workload's window
  operator calls (tumbling/sliding, size/slide params) directly.
  **[inferred]** this project does not appear to use Flink checkpointing
  for fault tolerance (jobs are short-lived benchmark runs, not
  long-running production jobs) — needs direct confirmation before
  stating as fact.

## Framework-specific limitations found
- See `docs/14_issue_log.md` FLINK-001 through FLINK-005 — hardcoded
  bootstrap server (now fixed), metrics reporter class name, SSH
  detachment quirk specific to `flink run`, TaskManager host
  advertisement, and the slot-allocation model correction.

## Metrics collection
- `infra/reporting_scripts/getprom.py` — scrapes Flink's REST API (job
  overview) + Prometheus (per-operator metrics). Known bugs found and
  fixed in this file while extending it for synthetic workloads: 5
  separate unguarded `IndexError` cases (see
  `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §9's "getprom.py extended"
  section) — latent in the shared collector used for all 14 real
  workloads too, just never triggered by their specific job shapes.

## Run termination
- **Self-terminating**, in-process: `executeAsync()` + a busy-wait on
  `job_run_time` + `client.cancel()` (see `WordCountJob.java`). The
  Python side discovers the job id via a polling thread started
  *concurrently* with the blocking submission call — a real
  structural-race fix from 2026-09-13 (see
  `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §9), not something to
  reinvent if porting this pattern to another engine.

## Known differences from other engines
- Per-operator parallelism vector, like Storm/Spark; unlike Kafka
  Streams/RisingWave's single job-level scalar.
- Genuinely self-terminates, unlike Storm (see `storm.md`).
- Different CLI argument convention from Storm/Spark/Kafka Streams.

**Still needed for this file:** state/window semantics (direct source
read required), a worked example of the full job-submission JSON/REST
call sequence, and a diagram of the JobManager/TaskManager topology.
