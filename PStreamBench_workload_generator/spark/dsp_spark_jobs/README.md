# dsp_spark_jobs

Apache Spark Structured Streaming counterpart to `dsp_jobs` (Flink) and
`dsp_storm_jobs` (Storm) — all 14 PStreamBench workloads are implemented.
Validated: real Maven build against Spark 3.5.1, a full controller-driven
deployment + run of Word Count on a live CloudLab cluster, and direct
`spark-submit` runs of all other 13 workloads against a live cluster with
synthetic per-workload input, checking real output on the Kafka sink
topic. Two real bugs were found and fixed only during that live pass (not
by compilation): `flatMapGroupsWithState`'s state class needs
`Encoders.javaSerialization()`, not `Encoders.bean()`, when the state class
uses bare public fields (affected Machine Outlier, Linear Road); and Gson's
newer static `JsonParser.parseString()` / Guava's `Resources.getResource()`
both fail at runtime on Spark executors because Spark bundles its own
older Gson/Guava that shadows the shaded versions in this job's jar —
fixed by using Gson's older instance-style `parse()` and loading classpath
resources via this job's own class's classloader instead of Guava's
`Resources` helper (affected Click Analytics, Sentiment Analysis).

## Why this exists

PStreamBench characterizes how operator parallelism (DoP) behaves across
DSPS architectures. Spark Structured Streaming has no per-operator
`setParallelism()` the way Flink does, or per-bolt executor counts the way
Storm does — its only native parallelism controls are `repartition(n)`
(fixes a stage's task count) and `spark.sql.shuffle.partitions` (fixes a
shuffle's width). To keep the same experimental protocol — one DoP value
per pipeline stage, driven by the same `job_pds` vector and enumeration
strategies already used for Flink/Storm — every job here maps
`parallelism_degree[i]` onto the same stage Flink's own `.setParallelism()`
call at index `i` controls:

| Index | Stage      | Flink mechanism             | Spark mechanism                     |
|-------|------------|------------------------------|--------------------------------------|
| any   | non-aggregating stage (source, map, filter, sink) | `.setParallelism()` | `.repartition(n)` |
| any   | aggregating stage (`keyBy`+window/sum/count) | `.setParallelism()` | `spark.sql.shuffle.partitions` set to that stage's value |

This is an approximation, not an equivalent guarantee: Spark's scheduler
still decides how those partitions are packed onto executor cores, whereas
Flink's parallelism is a hard per-task slot assignment. Treat this as a
stated limitation in any write-up comparing DoP scaling across the three
systems, not hidden.

## Workloads and known deviations from the Flink source

Every job was translated field-for-field from its Flink counterpart
(same CSV/JSON parsing, same output format where Spark can produce it),
but a handful of real Structured Streaming API limitations forced
documented deviations — each job's class-level Javadoc spells out its own
in detail. Summary:

| Workload | Spark class | Notable deviation |
|---|---|---|
| Word Count | `wordcount.WordCountJob` | none |
| Trending Topics | `TrendingTopics.TrendingTopics` | processing-time window approximated via `current_timestamp()` |
| Log Processing | `LogsAnalyzer.LogsAnalyzer` | hand-rolled per-event counter replaced with windowed `count()` |
| Spike Detection | `SpikeDetection.SpikeDetection` | spike condition evaluated at window granularity, not per-event |
| Bargain Index | `BargainIndex.BargainIndex` | `last(close)` approximates Flink's iteration-order-dependent "last quote" |
| Smart Grid | `smartgrid.SmartGridJob` | query 2's window units (seconds, not ms) and skipped `parallelism_degree[3]` preserved from source |
| TPC-H | `TPCH.TPCH` | query 2's windowed filter approximated with a keyed repartition |
| Google Cloud Monitoring | `GoogleCloudMonitoring.GoogleCloudMonitoring` | none structurally |
| Machine Outlier | `MachineOutlier.MachineOutlier` | uses `flatMapGroupsWithState` (Structured Streaming's equivalent of Flink's keyed ProcessWindowFunction) to run the ported BFPRT algorithm |
| Traffic Monitoring | `TrafficMonitoring.TrafficMonitoring` | road-matching GeoJSON loaded once per Spark partition via `mapPartitions`, not per record |
| Ad Analytics | `AdsAnalytics.AdsAnalytics` | query 1's join reordered to join-then-aggregate (Structured Streaming can't join two already-aggregated streams); two independent consumer groups replace Flink's accidental shared-group-id split |
| Click Analytics | `ClickAnalytics.ClickAnalytics` | queries 1/3 use `count`/`count_distinct` instead of manual cross-window visit-tracking state; query 3's count-window approximated with the same time-window as query 1 (Structured Streaming has no count-window) |
| Sentiment Analysis | `SentimentAnalysis.SentimentAnalysis` | classifier loaded once per partition via `mapPartitions` instead of once per record |
| Linear Road | `LinearRoad.LinearRoad` | queries 1/3/4 use `flatMapGroupsWithState` keyed by vehicleId; query 2 (accident detection) maps directly onto `groupBy+count` since it has no per-vehicle state in the Flink source either |

Machine Outlier and Linear Road carried the most translation risk, since
both port genuinely custom per-key stateful algorithms (BFPRT selection;
toll/expenditure accumulation) via `flatMapGroupsWithState` rather than a
built-in aggregate — everything else reduces to `repartition`/windowed
`groupBy`. Both are now confirmed working live (see the validation note
above for the state-encoding bug that surfaced only under a real Spark
executor, not at compile time).

## Build

```bash
cd dsp_spark_jobs
mvn package
cp ./target/dsp_spark_jobs-1.0-SNAPSHOT.jar ../../../PStreamBench_controller/dsp_be/infra/spark_files/
```

(or `make build`, which does the same thing.)

## Run (Kafka mode only, Word Count example)

```bash
spark-submit \
  --master spark://<master-host>:7077 \
  --name WordCount \
  --total-executor-cores <cluster-core-count> \
  --class com.kom.dsp.wordcount.WordCountJob \
  dsp_spark_jobs-1.0-SNAPSHOT.jar \
  -parallelism 5,5,5,5 \
  -mode KAFKA \
  -input WordCountIn \
  -output WordCountOut \
  -kafka-server localhost:9092 \
  -waitTimeToCancel 360
```

`-parallelism` takes one comma-separated value per stage (matching each
workload's Flink `parallelism_degree[]` length) — the same `job_pds`
vector / enumeration configs PStreamBench already uses for Flink/Storm
work unmodified, since `constants_spark.py` uses the exact same topic
names as `constants_flink.py`.

File mode (`-mode FILE`) is not implemented; PStreamBench always runs
workloads against Kafka in practice.
