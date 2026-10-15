# PStreamBench — local Docker smoke test

Minimal, local proof that a PStreamBench job actually runs end-to-end: real
data in, a real job processing it, real results out. Deliberately narrow
scope — Word Count + Ad Analytics per engine, no Prometheus/Grafana, no
Django controller, no React frontend, no CloudLab. This doesn't need the
distributed controller/WUI; each engine is wired in and tested one at a time,
same rigor every time (real datasets, real bugs fixed as found, verified with
an actual run before being called done).

## Architecture — what `docker compose up -d` actually brings up

One `docker-compose.yml`, 11 containers, one shared Kafka broker all 5 engines
read/write through:

| Service(s) | Image | Purpose | Host port(s) |
|---|---|---|---|
| `zookeeper` | `confluentinc/cp-zookeeper:7.5.0` | coordination for Kafka + Storm | — |
| `kafka` | `confluentinc/cp-kafka:7.5.0` | the one shared broker every engine talks to | `9092` |
| `flink-jobmanager`, `flink-taskmanager` | `apache/flink:1.16.0-java11` | single-node Flink cluster (8 task slots) | `8081` (Web UI) |
| `kafkastreams-app` | `eclipse-temurin:11-jre` | plain JVM container, idles until a job is launched via `docker compose exec` (no cluster — Kafka Streams is a library, not a service) | — |
| `risingwave` | `risingwavelabs/risingwave:v3.0.1` | single-binary `playground` mode (meta+compute+frontend+embedded state, all one process) | `4566` (psql) |
| `storm-nimbus`, `storm-supervisor`, `storm-ui` | `storm:2.6.2` | real Nimbus+Supervisor+UI (not `LocalCluster` — the job jars call `StormSubmitter.submitTopology()` directly, which needs a real Nimbus) | `6627` (Thrift), `8089` (UI) |
| `spark-master`, `spark-worker` | `apache/spark:3.5.9-scala2.12-java11-ubuntu` | single-node Standalone cluster | `7077` (RPC), `8088` (Master UI) |

Two things worth knowing before you poke at it yourself:

- **Kafka exposes two listeners on purpose** — `BROKER` (`kafka:9092`, used by
  anything *inside* the compose network: Flink/Storm/Spark/Kafka
  Streams/RisingWave containers) and `HOST` (`localhost:9092`, used by
  anything on your *host* machine: the producer script, `verify_output.sh`,
  `psql`). This exists because the project's own producer script hardcodes
  `localhost:9092` for Word Count regardless of what's passed to it (see
  "Known upstream quirks" below) — publishing Kafka's host listener on
  exactly `9092` means that hardcoded value works by accident, without
  needing a further patch.
- **Storm's 3 services override `entrypoint: []`** — the official image's
  entrypoint does `chown -R storm:storm /conf` under `set -e`, which fails
  against this project's bind-mounted `storm.yaml` on Docker Desktop/macOS
  (exit 254, container dies before Storm starts). We supply a complete
  `storm.yaml` ourselves, so the image's auto-chown/auto-config step isn't
  needed anyway — confirmed by running `storm nimbus` directly without it.

Every other service's image is used as-is, no entrypoint override. Full
reasoning for every version pin and design choice is also in the inline
comments of `docker-compose.yml` itself, if you want more detail than the
table above.

## What's here

```text
PStreamBench_local_docker/
├── docker-compose.yml       # Zookeeper + Kafka + Flink (JM+TM) + Kafka Streams
│                            # (plain JVM) + RisingWave (playground) + Storm
│                            # (nimbus+supervisor+ui) + Spark (master+worker)
├── storm_conf/storm.yaml     # minimal single-node Storm cluster config
├── jars/
│   ├── flink/                # Flink job jar (rebuilt, see "Known upstream
│   │                          #  quirks fixed" below -- don't copy a prebuilt one)
│   ├── kafkastreams/          # Kafka Streams app jar (rebuilt with Java 11)
│   ├── storm/                 # Storm topology shadow jar (rebuilt with Java 11)
│   └── spark/                 # Spark job jar (rebuilt with Java 11; see
│                               #  LOCAL_DOCKER_TODO.md for a local finding
│                               #  worth knowing about before trusting its output)
├── data/                     # real Kaggle datasets (gitignored; see setup_datasets.sh)
├── venv/                     # Python venv for the producer + kaggle CLI
├── requirements.txt
└── scripts/
    ├── setup_datasets.sh                   # one-time: download real data from Kaggle
    ├── create_topics.sh                    # create the 4 Kafka topics used here
    ├── submit_wordcount.sh                 # Flink: submit the Word Count job
    ├── submit_adanalytics.sh               # Flink: submit the Ad Analytics (with_join) job
    ├── submit_wordcount_kafkastreams.sh    # Kafka Streams: submit the Word Count app
    ├── submit_adanalytics_kafkastreams.sh  # Kafka Streams: submit the Ad Analytics (with_join) app
    ├── submit_risingwave.py                # RisingWave: submit/drop via psql (reuses the
    │                                        #  real sweep's build_sql_with_parallelism())
    ├── submit_wordcount_risingwave.sh      # thin wrapper -> submit_risingwave.py word_count
    ├── submit_adanalytics_risingwave.sh    # thin wrapper -> submit_risingwave.py ad_analytics
    ├── submit_wordcount_storm.sh           # Storm: submit via `storm jar` + self-kill after
    ├── submit_adanalytics_storm.sh         # Storm: same, Ad Analytics (with_join)
    ├── submit_wordcount_spark.sh           # Spark: submit via spark-submit
    ├── submit_adanalytics_spark.sh         # Spark: same, Ad Analytics (with_join)
    ├── run_producer.sh        # feed real events into an input topic (engine-agnostic)
    ├── verify_output.sh       # prove the output topic actually received data
    ├── custom_producer.py     # copy of the project's producer, two bugs fixed (below)
    └── pstreambench_cli.py    # interactive wizard -- asks engine/workload/
                                # parallelism/rate one at a time, then drives
                                # the scripts above. No frontend, no controller.
```

## Known upstream quirks fixed to make this work locally

Both were found while wiring this up, not invented for it — worth fixing
upstream too, not just here:

1. **`WordCountJob.java`** hardcoded its Kafka bootstrap server to
   `"localhost:9092"`, silently ignoring the `--kafka-server` CLI argument
   (the real line was commented out just above it). Harmless on the original
   single-host CloudLab setup; breaks the moment Flink runs in its own
   container. Fixed in
   `PStreamBench_workload_generator/flink/dsp_flink_jobs/src/.../wordcount/WordCountJob.java`,
   and the jar in `jars/flink/` was **rebuilt with Java 11** (matching the
   project's `pom.xml` target) and the fix was verified directly in the
   compiled bytecode with `javap`, not just trusted from a successful
   `mvn package` — a prebuilt jar can silently drift from source (see this
   project's own notes on that).
2. **`custom_producer.py`** had the identical bug in *two* places (the
   dedicated `wordcount()` generator and the generic `event_producer()` used
   by Ad Analytics and everything else) — both hardcoded
   `'bootstrap.servers': 'localhost:9092'` instead of using the
   `kafka_bootstrap_server` parameter actually passed in. Also hardcoded its
   dataset directory to `BASE_DIR = "/home/playground/"` (the CloudLab path).
   The copy in `scripts/custom_producer.py` fixes both; `BASE_DIR` now
   resolves to `PStreamBench_local_docker/data/` regardless of caller cwd.

Because Kafka's host-facing listener in `docker-compose.yml` is published on
exactly port 9092 (see the comment in that file), the *original* hardcoded
producer value would actually have worked by accident too — but don't rely
on that elsewhere; the fix is the correct behavior going forward.

Two more were found wiring up `run_producer.sh` itself (not engine-specific):
a bash syntax error (an apostrophe inside an unquoted `${var:?message}`
expansion), and a dependency on GNU `timeout`, which doesn't ship on macOS by
default — replaced with a background-process-plus-`kill` pattern.

Kafka Streams needed no code fixes — `WordCountTopology.java`/`AdsAnalyticsTopology.java`
already read `-kafka-server` correctly (no hardcoded-localhost bug like the
Flink version had).

## Prerequisites

- Docker Desktop running (`docker ps` should not error)
- Kaggle API credentials — this reuses the project's existing
  `PStreamBench-risingwave/PStreamBench_Cloud_setup/kaggle.json`; point
  `KAGGLE_CONFIG_DIR` elsewhere if you don't have access to that file

## Run it

```bash
cd PStreamBench_local_docker

# one-time setup
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
./scripts/setup_datasets.sh          # downloads real data into data/

# bring up the whole stack -- Zookeeper, Kafka, Flink, Kafka Streams,
# RisingWave, Storm (9 containers total). Pass specific service names
# instead (e.g. `docker compose up -d kafka flink-jobmanager flink-taskmanager`)
# if you only want one engine running.
docker compose up -d
./scripts/create_topics.sh

# --- Word Count ---
./scripts/submit_wordcount.sh &                        # runs ~40s, let it start
sleep 5
./scripts/run_producer.sh "Word_Count" WordCountIn 1000 20
./scripts/verify_output.sh WordCountOut

# --- Ad Analytics (with_join) ---
./scripts/submit_adanalytics.sh &                       # runs ~40s, let it start
sleep 5
./scripts/run_producer.sh "Ad_Analytics" AdAnalyticsIn 1000 20
./scripts/verify_output.sh AdAnalyticsOut
```

Flink Web UI (job status, parallelism, task slots): <http://localhost:8081>

### Kafka Streams

Same topics, same producer script, different submission (a plain `java`
process in the `kafkastreams-app` container, not a cluster submission) and a
required unique `-application-id` per run (it doubles as the consumer-group
id and RocksDB state-store directory — the submit script timestamps it for
you, so you don't need to):

```bash
./scripts/submit_wordcount_kafkastreams.sh &            # runs ~40s, let it start
sleep 5
./scripts/run_producer.sh "Word_Count" WordCountIn 1000 20
./scripts/verify_output.sh WordCountOut

./scripts/submit_adanalytics_kafkastreams.sh &
sleep 5
./scripts/run_producer.sh "Ad_Analytics" AdAnalyticsIn 1000 20
./scripts/verify_output.sh AdAnalyticsOut
```

Note: Kafka Streams' Word Count output format is `word,count` (no
parentheses) — visibly different from Flink's `(word, count)`, since each
engine serializes its own tuple independently. Both engines can safely read
the *same* input topic concurrently (each uses its own consumer group), but
if you want to be certain which engine's output you're looking at on a
shared output topic, that format difference is the tell for Word Count; Ad
Analytics' `RollingCTR{...}` format is identical across engines by design,
so distinguish those by timing/offset instead.

### RisingWave

Jobs are SQL (`CREATE SOURCE`/`MATERIALIZED VIEW`/`SINK`), submitted via
`psql`, not a jar. Unlike Flink/Kafka Streams, **these objects don't
self-terminate** — they keep running until dropped:

```bash
./venv/bin/python scripts/submit_risingwave.py word_count --parallelism 4
./scripts/run_producer.sh "Word_Count" WordCountIn 1000 20
./scripts/verify_output.sh WordCountOut
# or query the materialized view directly:
psql -h localhost -p 4566 -d dev -U root -c "SELECT * FROM word_count_mv ORDER BY cnt DESC LIMIT 10;"
./venv/bin/python scripts/submit_risingwave.py word_count --drop   # tear down when done

./venv/bin/python scripts/submit_risingwave.py ad_analytics --parallelism 4
./scripts/run_producer.sh "Ad_Analytics" AdAnalyticsIn 1000 20
./scripts/verify_output.sh AdAnalyticsOut
./venv/bin/python scripts/submit_risingwave.py ad_analytics --drop
```

Note: `ads_analytics.sql`'s own file header said "NOT YET VALIDATED against a
live RisingWave instance" before this was run — this was its first real
end-to-end test, not a known-good path being re-confirmed like the others.

### Storm

Submitted via `storm jar` inside the `storm-nimbus` container (it has the
`storm` CLI and is already configured to talk to itself). Real Nimbus +
Supervisor + UI, not Storm's in-process `LocalCluster` mode — see the
docker-compose.yml comment on why (`StormSubmitter.submitTopology()` is
hardcoded in the job code, which needs a real Nimbus to submit to).

```bash
./scripts/submit_wordcount_storm.sh &                   # blocks ~40s (see note below), let it start
sleep 10
./scripts/run_producer.sh "Word_Count" WordCountIn 1000 15
./scripts/verify_output.sh WordCountOut

./scripts/submit_adanalytics_storm.sh &
sleep 10
./scripts/run_producer.sh "Ad_Analytics" AdAnalyticsIn 1000 15
./scripts/verify_output.sh AdAnalyticsOut
```

Storm UI: <http://localhost:8089> (moved off 8080 to avoid clashing with
other local UIs). Its REST API (`/api/v1/topology/<id>`) is genuinely useful
for checking whether data is really flowing through each stage —
`bolts[].executed`/`.acked` — before assuming a "no output" result means a
processing failure (see `LOCAL_DOCKER_TODO.md` for why that distinction
matters on this particular output topic).

Note: each `submit_*_storm.sh` call blocks for the job's full
`waitTimeToCancel` (the wait loop runs client-side, same as Flink) *and*
then explicitly kills the topology itself afterward — Storm topologies do
not self-terminate the way Flink/Kafka Streams jobs do, so skipping that
kill step leaks an `ACTIVE` topology that permanently occupies worker
slots.

### Spark

`submit_wordcount_spark.sh` / `submit_adanalytics_spark.sh` invoke
`spark-submit` against the Standalone master the same way the other
engines' submit scripts do: the job starts, connects to Kafka, and
processes the first micro-batch. Spark Master UI: <http://localhost:8088>.

In local testing so far, a second micro-batch wasn't observed after the
first within the test window, so data produced after that point wasn't
picked up — see `LOCAL_DOCKER_TODO.md` for what's been ruled out and what
hasn't been tried yet. This is a local-environment finding, not a
statement that Spark itself doesn't work.

### Or: the interactive wizard (one parameter at a time)

If you'd rather not remember the exact script order/args above, `pstreambench_cli.py`
asks for engine, workload, parallelism, rate, and durations one at a time, then
runs the same steps for you:

```bash
./venv/bin/python scripts/pstreambench_cli.py
```

`flink`, `kafka_streams`, `risingwave`, and `storm` are wired to a real local
setup today; `spark` is listed as a menu choice (so the tool's shape doesn't
need rework later) but currently prints "not available yet" instead of
pretending to run. Notes: the wizard doesn't auto-drop RisingWave's
persistent objects afterward — run `submit_risingwave.py <workload> --drop`
yourself when done with it; it also doesn't auto-kill Storm topologies if
you interrupt it mid-run (a normal finish does kill them, via the submit
script itself) — check `storm list` and `storm kill <name> -w 0` if in doubt.

`verify_output.sh` prints `PASS: N message(s) found` with a sample of the
actual output records if data flowed end-to-end, or `FAIL` if not — that's
the full proof-of-life this is meant to give, nothing more.

## Tear down

```bash
docker compose down -v
```

## Native (non-Docker) local run

No container runtime at all — install each engine directly on your machine
and run the exact same job jars (`jars/flink/`, `jars/kafkastreams/`,
`jars/storm/`, `jars/spark/`) and the exact same `scripts/run_producer.sh` /
`scripts/verify_output.sh` against a locally-running Kafka on
`localhost:9092`. Versions below are pinned to match this project's own
Cloud setup (`PStreamBench_Cloud_setup/install_all_tools.sh`), not generic
"latest" versions, so behavior matches the distributed deployment as closely
as possible.

**Caveat, stated plainly:** this guide is written from the project's known
install procedures and this repo's existing CLI contracts, but **it has not
been live-verified end-to-end on this machine** the way every Docker-path
engine was (each of those was actually run, with real bugs found and fixed
along the way — see "Known upstream quirks" above). Treat these steps as a
solid starting point, not a proven-working path yet. If you hit something
that doesn't match, say which step and it'll get fixed the same way the
Docker path's bugs were — one real run at a time.

### 0. Shared prerequisite: Java 11 + a local Kafka

```bash
brew install openjdk@11
export JAVA_HOME="$(/usr/libexec/java_home -v 11)"

# Same Kafka version the Cloud setup uses (kafka_2.12-3.8.0) -- a plain JVM
# process, no container, works on macOS as-is.
mkdir -p ~/pstreambench_native && cd ~/pstreambench_native
curl -L https://archive.apache.org/dist/kafka/3.8.0/kafka_2.12-3.8.0.tgz -o kafka.tgz
tar xzf kafka.tgz && cd kafka_2.12-3.8.0

# Zookeeper first, then the broker (both block -- run each in its own terminal/background)
bin/zookeeper-server-start.sh config/zookeeper.properties &
sleep 5
bin/kafka-server-start.sh config/server.properties &
```

The default `config/server.properties` already advertises
`localhost:9092`, so no listener juggling is needed here (that dual-listener
trick in the "Architecture" section above exists only because Docker
networking requires it). Create the topics exactly as the Docker path does:

```bash
cd /path/to/PStreamBench_local_docker
KAFKA_BIN=~/pstreambench_native/kafka_2.12-3.8.0/bin ./scripts/create_topics.sh   # or run the 4 kafka-topics.sh --create commands by hand
```

(`create_topics.sh` was written against `docker compose exec kafka ...` — for
a native Kafka, run its 4 `kafka-topics.sh --create` commands directly
against `kafka_2.12-3.8.0/bin/kafka-topics.sh --bootstrap-server
localhost:9092` instead; same topic names: `WordCountIn`, `WordCountOut`,
`AdAnalyticsIn`, `AdAnalyticsOut`.)

Run the producer and verifier unchanged — both already default to
`localhost:9092`:

```bash
./scripts/run_producer.sh "Word_Count" WordCountIn 1000 20
./scripts/verify_output.sh WordCountOut
```

### 1. Flink (native)

```bash
curl -L https://archive.apache.org/dist/flink/flink-1.16.2/flink-1.16.2-bin-scala_2.12.tgz -o flink.tgz
tar xzf flink.tgz && cd flink-1.16.2
./bin/start-cluster.sh          # Web UI: http://localhost:8081

cp /path/to/PStreamBench_local_docker/jars/flink/dsp_jobs-1.0-SNAPSHOT.jar .
./bin/flink run -c com.kom.dsp.wordcount.WordCountJob dsp_jobs-1.0-SNAPSHOT.jar \
  -parallelism 2,2,2,2 -mode kafka -input WordCountIn -output WordCountOut \
  -kafka-server localhost:9092 -waitTimeToCancel 40
```

Same jar, same CLI args as the Docker path's `submit_wordcount.sh` — just
`-kafka-server localhost:9092` instead of `kafka:9092`, and `flink run`
called directly instead of through `docker compose exec`. Ad Analytics:
swap in `com.kom.dsp.AdsAnalytics.AdsAnalytics`, `AdAnalyticsIn`/`AdAnalyticsOut`,
and the 7-value parallelism vector (see `submit_adanalytics.sh` for the exact args).

### 2. Kafka Streams (native)

Already the simplest engine in the Docker path (just `java -cp`, no
cluster) — natively it's identical minus the container:

```bash
java -cp /path/to/PStreamBench_local_docker/jars/kafkastreams/dsp_kafkastreams_jobs-1.0-SNAPSHOT.jar \
  com.kom.dsp.wordcount.WordCountTopology \
  -parallelism 2 -mode kafka -input WordCountIn -output WordCountOut \
  -kafka-server localhost:9092 -application-id wordcount-native-$(date +%s) \
  -waitTimeToCancel 40
```

### 3. RisingWave (native)

```bash
brew tap risingwavelabs/risingwave
brew install risingwave
risingwave playground      # psql endpoint: localhost:4566
```

Then the exact same workflow as the Docker path's RisingWave section:

```bash
cd /path/to/PStreamBench_local_docker
./venv/bin/python scripts/submit_risingwave.py word_count --parallelism 4
./scripts/run_producer.sh "Word_Count" WordCountIn 1000 20
./scripts/verify_output.sh WordCountOut
./venv/bin/python scripts/submit_risingwave.py word_count --drop
```

Note: Homebrew's `risingwave playground` auto-shuts down after 30 minutes
of inactivity (loses all state) — fine for a quick proof-of-life run, not
for leaving it up long-term.

### 4. Storm (native)

```bash
curl -L https://archive.apache.org/dist/storm/apache-storm-2.6.2/apache-storm-2.6.2.tar.gz -o storm.tar.gz
tar xzf storm.tar.gz && cd apache-storm-2.6.2
cp /path/to/PStreamBench_local_docker/storm_conf/storm.yaml conf/storm.yaml
# storm.yaml's storm.local.dir -- change "/storm-local" to a real local path
# first (that value is a Docker container path, not valid on your host)

./bin/storm nimbus &
sleep 10
./bin/storm supervisor &
./bin/storm ui &             # Web UI: http://localhost:8080

./bin/storm jar /path/to/PStreamBench_local_docker/jars/storm/<storm-jar> \
  com.kom.dsp.wordcount.WordCountTopology \
  -name wordcount-native-$(date +%s) -parallelism 2,2,2,2 -mode kafka \
  -input WordCountIn -output WordCountOut -kafka-server localhost:9092 \
  -waitTimeToCancel 40
# then explicitly: ./bin/storm kill <topology-name> -w 0
# (Storm topologies don't self-terminate -- true here too, not just
# in Docker; see the Storm note in the "### Storm" section above)
```

The Storm CLI script itself needs Python 3 on `PATH` (unrelated to this
project's own venv) — macOS ships one by default, but confirm with
`storm version` before troubleshooting anything else if `storm jar` fails
oddly.

### 5. Spark (native)

```bash
curl -L https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz -o spark.tgz
tar xzf spark.tgz && cd spark-3.5.1-bin-hadoop3
./sbin/start-master.sh           # Web UI: http://localhost:8080
./sbin/start-worker.sh spark://$(hostname):7077

./bin/spark-submit --master spark://$(hostname):7077 --total-executor-cores 2 \
  --class com.kom.dsp.wordcount.WordCountJob \
  /path/to/PStreamBench_local_docker/jars/spark/dsp_spark_jobs-1.0-SNAPSHOT.jar \
  -parallelism 2,2,2,2 -mode kafka -input WordCountIn -output WordCountOut \
  -kafka-server localhost:9092 -waitTimeToCancel 40
```

The same second-micro-batch finding noted in the "### Spark" section above
applies here too, since it was reproduced independently of Docker (same
result with 2 cores and 8, with different startup delays, and with the
job's `.observe()` calls stripped out) — see `LOCAL_DOCKER_TODO.md` for
details.
