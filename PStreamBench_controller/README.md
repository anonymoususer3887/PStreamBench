# PStreamBench Controller (Backend)

The **PStreamBench Controller** is the backend service of PStreamBench. It exposes API endpoints used by the Web UI (WUI) to:

- register and manage CloudLab nodes
- create and manage benchmark clusters
- deploy a selected **System Under Test (SUT)**:
  - **Apache Flink**
  - **Apache Storm**
- execute jobs/topologies and manage experiment lifecycle
- collect experiment artifacts and metrics for analysis/visualization

The controller is implemented in **Django (Python)** and uses **Ansible playbooks** for cluster provisioning and service management.

---

## Contents

- [Prerequisites](#prerequisites)
- [Run the Controller](#run-the-controller)
- [CloudLab SSH Access](#cloudlab-ssh-access)
- [Controller Structure](#controller-structure)
- [SUT Support: Flink, Storm, and Spark](#sut-support-flink-storm-and-spark)
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
- `dsp_be/infra/reporting_scripts/` — scripts for parsing/collecting metrics/artifacts

Job execution is abstracted through:

- `runner.py` — framework-independent execution wrapper (routes Flink vs Storm execution based on cluster SUT)

---

## SUT Support: Flink, Storm, and Spark

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
> Tip: All three directories include Makefiles that run these commands directly.

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

---

## Related Components

- Web UI (frontend): `PStreamBench_wui/`
- CloudLab profile setup: `PStreamBench_Cloud_setup/`

See also:

- `PStreamBench_wui/README.md` (frontend setup and remote access)
- `PStreamBench_Cloud_setup/README.md` (CloudLab node provisioning)
