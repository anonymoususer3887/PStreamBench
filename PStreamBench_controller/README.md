# PStreamBench Controller (Backend)

The **PStreamBench Controller** is the backend service of PStreamBench. It exposes API endpoints used by the Web UI (WUI) to:

- register and manage CloudLab nodes
- create and manage benchmark clusters
- deploy a selected **System Under Test (SUT)**:
  - **Apache Flink**
  - **Apache Storm**
  - **Apache Spark**
  - **Kafka Streams**
  - **RisingWave**
- execute jobs/topologies and manage experiment lifecycle
- collect experiment artifacts and metrics for analysis/visualization

The controller is implemented in **Django (Python)** and uses **Ansible playbooks** for cluster provisioning and service management.

---

## Contents

- [Prerequisites](#prerequisites)
- [Run the Controller](#run-the-controller)
- [CloudLab SSH Access](#cloudlab-ssh-access)
- [Controller Structure](#controller-structure)
- [SUT Support: Flink, Storm, Spark, Kafka Streams, and RisingWave](#sut-support-flink-storm-spark-kafka-streams-and-risingwave)
- [Job Binaries](#job-binaries)
- [Monitoring and Metrics Collection](#monitoring-and-metrics-collection)
- [Related Components](#related-components)

---

## Prerequisites

Recommended versions:

- Python 3.8.10
- Java 11

Install controller dependencies using `requirements.txt`.

> Notes  
> - Maven/Gradle are only required if you want to compile job binaries locally (see [Job Binaries](#job-binaries)).  
> - For CloudLab orchestration, password-less SSH access is required (see [CloudLab SSH Access](#cloudlab-ssh-access)).

---

## Run the Controller

From the repository root:
```bash
cd dsp_be
pip install -r requirements.txt

python manage.py makemigrations
python manage.py migrate

python manage.py runserver
```     

### Remote Access (Controller)

To expose the controller outside localhost:
```bash
python manage.py runserver 0.0.0.0:8000
```

## CloudLab SSH Access

The controller provisions and configures CloudLab nodes using SSH/Ansible. Ensure:

1. A local SSH key exists (e.g., `~/.ssh/id_rsa.pub`).
2. The public key is added in CloudLab under **Manage SSH Keys**.
3. Password-less SSH works from the controller machine to CloudLab nodes:
```bash
ssh <cloudlab_user>@<node_fqdn>
```

Also ensure the CloudLab user default shell is set to bash to avoid hostname binding issues in some setups.

## Controller Structure

Main folders in `dsp_be/`:

| Folder | Purpose |
|---|---|
| `dsp_be/` | Django project configuration (settings/urls) |
| `auth/` | Authentication and user info storage |
| `infra/` | Cluster creation and job execution logic, framework assets, reporting scripts |
| `report/` | Views/endpoints for live monitoring + artifact-based analysis |
| `utils/` | Ansible playbooks and provisioning utilities |

Common framework asset paths:

- `dsp_be/infra/flink_files/` — Flink job jar and related files
- `dsp_be/infra/storm_files/` — Storm topology jar and related files
- `dsp_be/infra/spark_files/` — Spark job jar and related files
- `dsp_be/infra/kafkastreams_files/` — Kafka Streams app jar and related files
- `dsp_be/infra/reporting_scripts/` — scripts for parsing/collecting metrics/artifacts

RisingWave has no jar to upload — its jobs are `.sql` files submitted via `psql`, read directly from `PStreamBench_workload_generator/risingwave/dsp_risingwave_jobs/`.

Job execution is abstracted through:

- `runner.py` — framework-independent execution wrapper (routes execution based on `cluster.cluster_type`: Flink, Storm, Spark, Kafka Streams, or RisingWave)

---

## SUT Support: Flink, Storm, Spark, Kafka Streams, and RisingWave

When creating a cluster in the Web UI, the controller deploys the selected SUT.

### Apache Flink

- **Cluster roles:** JobManager (main node) + TaskManagers (worker nodes)
- **Execution:** submit jar jobs to Flink
- **Monitoring:** live metrics via Prometheus/Grafana + exported artifacts

### Apache Storm

- **Cluster roles:** Nimbus + Storm UI (main node), Supervisors (worker nodes), ZooKeeper (cluster)
- **Execution:** submit topologies from the Storm jar
- **Monitoring:**
  - Storm UI provides operational visibility
  - metrics are collected primarily through exporter/scripts and artifacts
  - Storm metrics may update at coarse granularity (often ~1 minute)

### Apache Spark (Structured Streaming) — all 14 workloads implemented and validated live

- **Cluster roles:** Standalone Master (main node) + Workers (worker nodes). Master UI runs on port 8087 (not the Spark default 8080, which collides with Storm's UI port on this same node role).
- **Execution:** `spark-submit --master spark://<main>:7077 --name <job> --total-executor-cores <n> --class <program> dsp_spark_jobs-1.0-SNAPSHOT.jar ...`
- **Monitoring:** Spark's built-in Prometheus servlet + Prometheus/Grafana; query-level throughput is read from Kafka's own topic metrics (SUT-agnostic) rather than Spark-internal streaming-query metrics, since Structured Streaming does not expose an operator/vertex REST API the way Flink does.
- **Status:** all 14 real-world workloads are implemented, build cleanly against Spark 3.5.1, and have been run end-to-end on a live CloudLab cluster (see [PStreamBench_workload_generator/spark/dsp_spark_jobs](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_workload_generator/spark/dsp_spark_jobs#readme) for how DoP vectors are approximated via `repartition()`/`spark.sql.shuffle.partitions`, per-workload deviations, and notes on two real bugs the live-validation pass surfaced and fixed: a `flatMapGroupsWithState` state-bean encoding issue affecting Machine Outlier/Linear Road, and a Gson/Guava classpath conflict with Spark's own bundled versions affecting Click Analytics/Sentiment Analysis).

### Kafka Streams

- **Cluster roles:** no master/worker split — Kafka Streams is a library, and each worker node runs an independent app instance (a `Topology`, Kafka Streams' equivalent of a Flink job/Storm topology) that connects straight to the shared Kafka cluster. Instances coordinate scaling among themselves via Kafka's own consumer-group rebalancing protocol, not via anything PStreamBench manages.
- **Execution:** submit the app jar to each worker node; the app registers as `NUM_STREAM_THREADS` threads against the input topic's consumer group.
- **Parallelism:** job-level only (`NUM_STREAM_THREADS`) — there's no way to give one pipeline stage more parallelism than another, unlike Flink/Storm/Spark's per-operator vectors.
- **Monitoring:** JMX exporter per app instance (port 7076, separate from the shared Kafka broker's own JMX port).
- **Status:** Word Count is implemented and validated through this full controller + Ansible path. Collected results (`PStreamBench_benchmark_data/query_performance_data.csv`) already cover more workloads than just Word Count, but — unlike Spark's dedicated live-validation pass above — those haven't been individually re-confirmed against the current job code workload by workload; treat results beyond Word Count as not yet re-verified, not as unavailable.

### RisingWave

- **Cluster roles:** a real distributed deployment: **meta** node (metadata + coordination), **compute** nodes (actual stream execution), **frontend** (the Postgres-wire SQL endpoint jobs are submitted through), and **MinIO** (S3-compatible object storage backing state) — see `utils/playbook/tasks/risingwave/`.
- **Execution:** jobs are `.sql` files (`CREATE SOURCE` / `CREATE MATERIALIZED VIEW` / `CREATE SINK`), not compiled programs, run via `psql` against the frontend node.
- **Parallelism:** job-level only (`JobParallelism`), same constraint as Kafka Streams.
- **Monitoring:** RisingWave's own Prometheus endpoint + Prometheus/Grafana.
- **Status:** same as Kafka Streams — Word Count fully validated through the controller; wider workload coverage exists in collected data but hasn't been individually re-verified since.

---

## Job Binaries

PStreamBench uploads compiled jars to the cluster main node to execute jobs/topologies.

Because GitHub limits large files, binaries may not be included and must be compiled locally.

### Build Flink job jar

```bash
cd dsp_jobs
mvn package
cp ./target/dsp_jobs-1.0-SNAPSHOT.jar ../dsp_be/infra/flink_files/
```


### Build Storm topology jar

```bash
cd dsp_storm_jobs
./gradlew shadowJar
cp ./build/libs/dsp_storm_jobs-1.0-SNAPSHOT-all.jar ../dsp_be/infra/storm_files/
```

### Build Spark job jar

```bash
cd dsp_spark_jobs
mvn package
cp ./target/dsp_spark_jobs-1.0-SNAPSHOT.jar ../dsp_be/infra/spark_files/
```

### Build Kafka Streams app jar

```bash
cd dsp_kafkastreams_jobs
mvn package
cp ./target/dsp_kafkastreams_jobs-1.0-SNAPSHOT.jar ../dsp_be/infra/kafkastreams_files/
```

### RisingWave — no build step

RisingWave jobs are plain `.sql` files, not compiled binaries — nothing to build or copy; the controller reads them directly from `PStreamBench_workload_generator/risingwave/dsp_risingwave_jobs/`.

> Tip: all four JVM-based directories (Flink/Storm/Spark/Kafka Streams) include Makefiles that run these commands directly.

## Monitoring and Metrics Collection

PStreamBench supports both **live monitoring** and **artifact-based analysis**.

### Prometheus + Grafana (Flink + Hardware)

Typical flow:

- Flink and node exporters expose metrics  
- Prometheus scrapes metrics and stores time series  
- Grafana dashboards query Prometheus via PromQL  

This repository extends Prometheus support to include **hardware metrics** (CPU/memory/network) using **Prometheus Node Exporter**.

### Storm Metrics

Storm metrics are collected through Storm-specific scripts/exporter and are primarily supported through artifacts:

- `getprom_storm.py` — Storm metrics collection  
- Storm exporter script on main node (restart if needed):  
  - `/home/playground/apache-storm-2.6.2/bin/storm_exporter.sh`

### Kafka Streams & RisingWave Metrics

Query-level throughput for both is read from Kafka's own topic metrics (SUT-agnostic), same approach as Spark above, since neither exposes a Flink-style operator/vertex REST API. Kafka Streams additionally exposes per-instance JMX metrics (port 7076); RisingWave exposes its own Prometheus endpoint from the meta/compute nodes.

---

## Related Components

- Web UI (frontend): `PStreamBench_wui/`
- CloudLab profile setup: `PStreamBench_Cloud_setup/`

See also:

- `PStreamBench_wui/README.md` (frontend setup and remote access)
- `PStreamBench_Cloud_setup/README.md` (CloudLab node provisioning)
