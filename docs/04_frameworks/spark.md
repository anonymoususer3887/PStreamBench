# Framework Integration — Apache Spark (Structured Streaming)

> **Status: PENDING full write-up.** `SPARK_VALIDATION.md` (37KB,
> existing project doc) already has deep live-validation detail for the
> 14 real workloads and should be the primary source for this file, not
> re-derived. This stub captures what's confirmed from this session's
> own direct testing plus that file's headline findings.

## Install & launch
- Version: **Spark 3.5.1** (`spark-3.5.1-bin-hadoop3`), Standalone mode
  (no Hadoop/YARN cluster required). Local Docker path uses
  `apache/spark:3.5.9-scala2.12-java11-ubuntu` (closest available image
  tag — Docker Hub doesn't carry 3.5.1; 3.5.x is binary-compatible).
- Submission: `spark-submit --master spark://<master>:7077
  --total-executor-cores N --class <MainClass> <jar> <args>`.

## Configuration
- `spark-defaults.conf` / `spark-env.sh`, templated by
  `_spark_config_manipulation()` the same way Flink/Storm's configs are
  (see `infra/spark_files/sample-spark-*`).
- `spark.metrics.conf` enables Spark's own `PrometheusServlet`, scraped
  by Prometheus for both built-in `spark_streaming_*` metrics and custom
  `OperatorMetricsSource` gauges.

## Workload / query implementation
- `PStreamBench_workload_generator/spark/dsp_spark_jobs/` — 14 real
  workloads built against the same CLI convention (`ArgParser.java`,
  shared with Storm/Kafka Streams) and the same Kafka topics as every
  other engine, so the same producer/data works across engines
  unmodified.
- `WordCountJob.java`'s own class comment documents the parallelism
  mapping explicitly: Spark has no per-operator `setParallelism()` — the
  closest native controls are `repartition()` (fixes a stage's task
  count) and `spark.sql.shuffle.partitions` (fixes a groupBy shuffle's
  width). `parallelism_degree[0..3]` maps the same way Flink does:
  `[0]` source, `[1]` tokenizer, `[2]` counter/shuffle partitions,
  `[3]` sink.

## Distributed (CloudLab) deployment: synthetic shapes

- Standalone cluster: one master + N workers, matching every other
  engine's node-role convention on this project's CloudLab allocations.
  Submitted via `spark-submit --deploy-mode client` from the master node
  itself, so the driver (and its metrics-servlet/Prometheus scrape
  target) always runs on a known, fixed node.
- Six real, confirmed-live issues were found and fixed standing this up
  for the 9 synthetic shapes' distributed sweep (checkpoint sharing
  across nodes, REST API reachability, a Master UI crash under load, two
  distinct Prometheus-query-ambiguity bugs, plus generic fresh-CloudLab-
  node bootstrap issues) — see `docs/14_issue_log.md` SPARK-001 through
  SPARK-005 (Spark-specific) and INFRA-ENV-004/005 (generic CloudLab
  node setup, not Spark-specific) for full root-cause/fix detail. Not
  repeated here to avoid the two files drifting out of sync.
- The full 9-shape × 3-rate × 6-level uniform sweep, plus the
  random/ds2/dhalion non-uniform strategy sweeps, all completed with
  zero outstanding failures after SPARK-005's fix.

## Two real bugs found and fixed during live validation
(per `SPARK_VALIDATION.md` and the top-level `README.md`'s Spark status
note — full investigation detail lives in `SPARK_VALIDATION.md`, not
repeated here):
1. A `flatMapGroupsWithState` bean-encoding issue.
2. A Gson/Guava classpath conflict with Spark's own bundled versions.

## Open issue: second micro-batch not observed in local testing
- In `PStreamBench_local_docker`'s environment, Spark's job submits
  correctly, connects to Kafka, and processes exactly one micro-batch —
  a second was not observed within the test window.
- Ruled out: resource allocation (2 vs. 8 executor cores), startup-timing
  race (data produced 15/30/55s after submission), and the job's chained
  `.observe()` calls (tested with an isolated diagnostic copy that had
  every `.observe()` call stripped — same result).
- Not yet tried: explicit `Trigger.ProcessingTime(...)`, checking the
  `apache/spark:3.5.9` image's bundled Kafka-source version against the
  job's own 3.5.1 client libraries, Spark driver thread dumps.
- **This is a local-environment finding, not evidence Spark itself
  doesn't work** — see `LOCAL_DOCKER_TODO.md` for the full detail and
  explicit framing.

## Metrics collection
- `infra/reporting_scripts/getprom_spark.py` — scrapes Spark's own
  streaming-query metrics + Prometheus.

## Run termination
- **Genuinely self-terminates in-process**: `query.stop()` is called
  explicitly after `awaitTermination()`, confirmed to actually work
  (unlike Storm) — see `PStreamBench_local_docker/README.md`'s per-engine
  comments.

## Known differences from other engines
- No per-operator parallelism control — `repartition()`/shuffle-partition
  count is the closest analog, a structurally different knob from
  Flink/Storm's `setParallelism()`.
- No live Grafana dashboard today (unlike Flink/Storm) —
  `WUI_IMPROVEMENTS_TODO.md` item 10.
- Not wired into the local Docker interactive wizard
  (`pstreambench_cli.py`) — listed as a menu choice but not functional
  there, pending the micro-batch issue above.

**Still needed for this file:** state/window semantics (direct source
read), the Spark driver/executor topology diagram, and resolution of the
micro-batch open issue once investigated further.
