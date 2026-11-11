# Framework Integration — Apache Storm

> **Status: PENDING full write-up**, but more real material already
> exists for Storm than any other engine (it's the most heavily
> battle-tested path in this project's history — the synthetic workload
> sweep alone surfaced a dozen real bugs, see `docs/14_issue_log.md`
> `STORM-*` and `INFRA-ENV-001`). This file's job is consolidation.

## Install & launch
- Version: **Storm 2.6.2** (`archive.apache.org/dist/storm/apache-storm-2.6.2/...`),
  matching `install_all_tools.sh` / `install_storm_only.sh`. Local Docker
  path uses the official `storm:2.6.2` image with `entrypoint: []`
  overridden — see STORM-001 in the issue log for why.
- Real distributed mode only — **not** `LocalCluster`, because the job
  code calls `StormSubmitter.submitTopology()` directly
  (`AbstractTopology.java` hardcodes `clusterMode="distributed"`),
  requiring a real Nimbus to submit to.
- Submission: `storm jar <jar> <MainClass> -name <topology-name> <args...>`
  via the controller's Ansible tasks (distributed) or directly inside the
  `storm-nimbus` container (local Docker).

## Configuration
- `storm.yaml`: `storm.zookeeper.servers`, `nimbus.seeds`,
  `storm.local.dir`, `supervisor.slots.ports` (how many worker slots —
  **must** be sized for concurrent submissions plus Storm's non-self-
  terminating topologies, see STORM-002/003; this project bumped it from
  2 to 6 after hitting exactly that).
- Worker count must scale with total executor count — a fixed
  `numWorkers` regardless of configured parallelism causes thread
  oversubscription at high levels (STORM-007), not a real scaling result.

## Workload / query implementation
- `PStreamBench_workload_generator/storm/dsp_storm_jobs/` (14 real) +
  `dsp_storm_synthetic_jobs/` (9 synthetic shapes, separate Gradle
  project — deliberately isolated so synthetic-workload work could never
  break the 14 real jobs).
- CLI convention: single-dash `ArgParser.java`, shared with Spark and
  Kafka Streams (`-kafka-server`, `-parallelism`, etc.) — different from
  Flink's own convention.

## Parallelism representation
- Per-operator vector, same shape as Flink/Spark. `supervisor.slots.ports`
  count must be ≥ the sum of a topology's executor-to-worker mapping;
  see `-numWorkers` in the job's own submit args.

## Source/sink
- Kafka source/sink, standard Storm Kafka spout/bolt pattern.
- Ad Analytics specifically: `Q1RollingCTRMapper` emits a **null** Kafka
  key, so output lands on an unpredictable partition of a 4-partition
  topic — a real methodology trap, not a bug (STORM-004).

## State / window
- *Not yet confirmed in this pass* — needs direct source read of each
  workload's windowing (tumbling/sliding bolt implementation).

## Framework-specific limitations found
- **Topologies never self-terminate** — `AbstractTopology.java` logs
  "Killing query" but calls no actual kill API (STORM-002). Every caller
  of a Storm topology in this project explicitly calls `storm kill`
  itself afterward as a result; this is not optional.
- Official Docker image's entrypoint does `chown -R` under `set -e`,
  fails on bind-mounted configs on Docker Desktop/macOS (STORM-001).

## Metrics collection
- `storm_exporter.py` (Prometheus exporter) + `getprom_storm.py`
  (scrapes Storm UI REST API `/api/v1/topology/<id>` + Prometheus).
- **Known failure mode**: the exporter's `/metrics` HTTP endpoint can
  silently deadlock after enough unique `TopologyId` label values
  accumulate across repeated submissions, while its polling loop keeps
  visibly running (STORM-009). An automatic health-check-and-restart
  step before every submission is required, not optional, for any
  multi-run sweep.
- Storm UI's own stats (`bolts[].executed`/`.acked`) are the right tool
  for checking whether data is really flowing before assuming a "no
  output" result is a processing bug (STORM-004).

## Run termination
- **Explicit, external kill required** — see above. The controller's
  production path kills jobs via its own Ansible step; any other caller
  (local Docker scripts, the synthetic-workload sweep script) must do the
  same itself.

## Known differences from other engines
- Does not self-terminate (unlike Flink/Kafka Streams/Spark).
- Requires a real Nimbus, cannot run in an embedded/local-cluster mode
  the way some of this project's other engines can (RisingWave's
  `playground` mode, Kafka Streams needing no cluster at all).

**Still needed for this file:** window semantics (direct source read),
a Nimbus/Supervisor/Worker topology diagram, and the full disk-usage/log-
rotation capacity-planning note from INFRA-ENV-001 turned into an actual
operational runbook entry (how much disk headroom a long sweep needs).
