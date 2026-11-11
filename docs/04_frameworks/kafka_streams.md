# Framework Integration — Kafka Streams

> **Status: PENDING full write-up.** `KAFKA_STREAMS_RISINGWAVE_PLAN.md`
> (the original design/planning doc for adding this engine) is the
> primary source to fold in here, not re-derived from scratch.

## Install & launch
- **No separate cluster software** — Kafka Streams is a client library,
  not a daemon. "Deploying" it is just a plain JVM container/host with
  the app jar present, idling until a job is launched
  (`java -cp <jar> <MainClass> <args>`). This is the single biggest
  structural difference from Flink/Storm/Spark, and it's why
  `install_kafkastreams_only.sh` installs **no engine binary at all** —
  only Kafka + Prometheus + the JMX exporter (see
  `install_kafkastreams_only.sh`'s own header comment).
- Local Docker: `eclipse-temurin:11-jre` container running
  `tail -f /dev/null`, job launched via `docker compose exec`.
- Distributed: controller's `cluster-complex-kafkastreams-master-create.yaml`
  / `-worker-create.yaml` — every node just runs the shared Kafka broker
  (started for every engine regardless) and launches its own job JVM at
  submit time; Prometheus/Grafana/node_exporter setup is *not* shared
  infra the way it is for the other engines, so it still needs its own
  (monitoring-only) playbook equivalent.

## Configuration
- A required, **unique** `-application-id` per run — doubles as both the
  Kafka consumer-group id and the RocksDB state-store directory name.
  Must be unique across runs or state collides; the local Docker submit
  scripts timestamp it automatically.

## Workload / query implementation
- `PStreamBench_workload_generator/kafkastreams/dsp_kafkastreams_jobs/`.
- Word Count output format: `word,count` (no parentheses) — a visible,
  deliberate tell distinguishing it from Flink's `(word, count)` when
  both engines' output lands on a shared topic.
- `WordCountTopology.java`/`AdsAnalyticsTopology.java` already read
  `-kafka-server` correctly — **no hardcoded-localhost bug** like the
  Flink version had (`docs/14_issue_log.md` FLINK-001 did not have a
  Kafka Streams counterpart).

## Parallelism representation
- **Single job-level scalar** (stream threads), not a per-operator
  vector — the other half of the architectural split this whole project
  is built to respect (see `docs/00_overview.md` §3 and
  `docs/05_parallelism_and_enumeration.md`). In
  `operator_templates.csv`'s convention: `vector[1]` = stream threads,
  `vector[0]` = job parallelism — mechanical by project-wide convention,
  not read per-workload.

## Source/sink
- Kafka source/sink natively (it's a Kafka-native library) — no separate
  connector layer to configure.

## State / window
- *Not yet confirmed in this pass* — RocksDB-backed state stores are used
  (implied by the `-application-id` note above); window semantics need
  direct source confirmation.

## Framework-specific limitations found
- None found yet in this project's testing (confirmed working for Word
  Count + Ad Analytics in the local Docker pass with no code fixes
  needed) — this itself is worth stating plainly rather than omitting;
  Kafka Streams is the simplest integration of the 5 engines precisely
  because it needs no cluster daemon at all.

## Metrics collection
- `infra/reporting_scripts/getprom_kafkastreams.py` + a JMX exporter
  (port **7076**, separate from the shared Kafka broker's own JMX port)
  per app instance.

## Run termination
- **Self-terminates in-process**: `streams.close()`, same reliability
  class as Flink/Spark, not Storm.

## Known differences from other engines
- No cluster to install/start/stop — a fundamentally different
  "deployment" model from the other four.
- Single-scalar configuration, shared only with RisingWave.
- No live Grafana dashboard today — `WUI_IMPROVEMENTS_TODO.md` item 10.

**Still needed for this file:** state-store/window semantics (direct
source read), and the exact `-application-id` collision-avoidance
convention written out as a reusable rule for anyone adding a new
workload.
