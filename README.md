<h2 align="center">
  Experiment, Analysis, and Benchmark on PStreamBench for DSPS
</h2>

This repository contains the experimental setup and workload suite used in the paper:

**“Is Parallelism a Silver Bullet? An Empirical Study of Parallelism Behavior in Distributed Stream Processing Systems.”**

It provides a collection of representative streaming applications, configurations for varying degrees of parallelism (DoP), and scripts for executing experiments across different distributed stream processing systems (DSPS), including **Apache Flink**, **Apache Storm**, **Apache Spark**, **Kafka Streams**, and **RisingWave**.

The setup enables controlled evaluation of parallelism behavior across:

- diverse workloads,
- multiple system architectures, and  
- heterogeneous hardware configurations.

It supports running experiments, collecting performance metrics such as **throughput**, **end-to-end latency**, and **resource utilization**, and exporting results for further analysis.

## Key Concepts

- SUT (System Under Test): The stream processing engine being benchmarked (Flink, Storm, Spark, Kafka Streams, or RisingWave).
- PQP (Parallel Query Plan): A parallelization configuration for an application/topology (e.g., per-operator DoP vector).
- Workload Configuration: Stream parameters such as event rate, query parameters, execution duration, iterations, etc.
- Heterogeneous Hardware: Clusters where nodes differ in CPU, memory, storage, or accelerators.

## Supported Stream Processing Engines (SUTs)

PStreamBench is designed to benchmark multiple SPEs under a unified workflow.

### Apache Flink

- **Cluster roles:** JobManager + TaskManagers

- **Parallelism mapping:** PQP/DoP settings map to Flink operator/subtask parallelism

- **Experiment lifecycle:** deploy cluster → submit job → run for duration → collect metrics → analyze/export

### Apache Storm

- **Cluster roles:** ZooKeeper + Nimbus + Supervisors

- **Parallelism mapping:** PQP/DoP settings map to spout/bolt parallelism (executors/tasks)

- **Experiment lifecycle:** deploy cluster → submit topology → run for duration → collect metrics → analyze/export

### Apache Spark (Structured Streaming) — in progress

- **Cluster roles:** Standalone Master + Workers

- **Parallelism mapping:** Spark has no per-operator parallelism setting. PQP/DoP vectors are approximated with an explicit `repartition(n)` at each pipeline stage boundary (and `spark.sql.shuffle.partitions` for the aggregation stage), matching the same `job_pds` vector length/semantics used for Flink/Storm. This is a stated approximation, not an equivalent guarantee — see [PStreamBench_workload_generator/spark/dsp_spark_jobs/README.md](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_workload_generator/spark/dsp_spark_jobs#readme).

- **Experiment lifecycle:** deploy cluster → spark-submit → run for duration → collect metrics → analyze/export

- **Status:** all 14 workloads are implemented and validated on a live CloudLab cluster (Word Count through the real controller + Ansible automation; the other 13 via direct `spark-submit` against the same built jar). See [PStreamBench_workload_generator/spark/dsp_spark_jobs/README.md](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_workload_generator/spark/dsp_spark_jobs#readme) for per-workload deviations from the Flink source and live-validation notes (including two real bugs found and fixed only during that pass: a `flatMapGroupsWithState` bean-encoding issue, and a Gson/Guava classpath conflict with Spark's own bundled versions). Query-level (Kafka topic throughput) and hardware metrics are collected; per-operator metric breakdowns (as available for Flink) are not yet implemented for Spark.

### Kafka Streams

- **Cluster roles:** Kafka Streams is a library, not a standalone cluster service — each "worker" node runs an independent app instance that connects directly to the shared Kafka cluster and coordinates scaling via Kafka's own consumer-group protocol, rather than a dedicated master/worker split like Flink or Storm.

- **Parallelism mapping:** PQP/DoP maps to the app's configured number of stream threads (`NUM_STREAM_THREADS`) per instance — a **job-level** knob, not a per-operator one: unlike Flink/Storm/Spark, Kafka Streams does not expose independent parallelism per pipeline stage.

- **Experiment lifecycle:** deploy app instances across worker nodes → submit via the controller → run for duration → collect metrics → analyze/export.

- **Status:** Word Count is implemented and validated through the full controller + Ansible automation path; broader workload coverage already exists in collected results (see `PStreamBench_benchmark_data`), but hasn't been individually reconfirmed against the latest job code for every workload the way Spark's live-validation pass was — treat non–Word Count Kafka Streams results as not yet re-verified rather than unavailable.

### RisingWave

- **Cluster roles:** a genuine distributed deployment, not a single process — meta node (metadata/coordination), compute nodes (stream execution), frontend (Postgres-wire SQL interface jobs are submitted through), and MinIO (S3-compatible object storage for state). See `PStreamBench_controller/dsp_be/utils/playbook/tasks/risingwave/`.

- **Parallelism mapping:** jobs are `CREATE SOURCE` / `CREATE MATERIALIZED VIEW` / `CREATE SINK` SQL statements (not JVM programs), submitted via `psql`. Like Kafka Streams, RisingWave exposes one **job-level** parallelism knob, not independent per-operator control.

- **Experiment lifecycle:** deploy the meta/compute/frontend/MinIO roles → submit the workload's SQL via the controller → run for duration → collect metrics → analyze/export.

- **Status:** same as Kafka Streams — Word Count fully validated through the controller; wider coverage exists in collected data but hasn't been individually re-verified workload by workload.

## Workloads

PStreamBench supports benchmarking using:

- 14 real-world applications

Applications can be executed under different PQPs, workload settings, and resource configurations.

## Repository Structure

- [PStreamBench_Cloud_setup:](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_Cloud_setup#readme)
CloudLab setup instructions and scripts to provision resources and install dependencies.

- [PStreamBench_controller:](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_controller#readme)
  - Backend controller. Provides APIs to automate:

  - cluster creation and SUT deployment (Flink/Storm/Spark/Kafka Streams/RisingWave)

  - job/topology submission and experiment lifecycle

  - persistence of metadata and results (e.g., SQLite/MongoDB depending on configuration)

- [PStreamBench_wui:](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_wui#readme)
Web UI for provisioning inputs, workload configuration, experiment execution, and visualization.

- [PStreamBench_workload_generator:](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_workload_generator) Workload generator for <strong>Apache Flink</strong>, <strong>Apache Storm</strong>, <strong>Apache Spark</strong>, <strong>Kafka Streams</strong>, and <strong>RisingWave</strong> (one subfolder per engine). It produces benchmark executions for real-world applications, and enumerates <em>parallel query plans</em> by varying operator (Flink/Storm/Spark) or job-level (Kafka Streams/RisingWave) parallelism and workload parameters.

- [PStreamBench_benchmark_data](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_benchmark_data)
Selected sample experiment outputs (real-world and synthetic applications).

> Note: Detailed steps about each component of PStreamBench is provided in their respective README.md files.

## Ways to Run PStreamBench

PStreamBench can be run four ways, depending on whether you want a full distributed CloudLab cluster or a quick local check, and whether you want to drive it from the Web UI or the command line.

| | Frontend (WUI) | Command line |
|---|---|---|
| **Distributed** (CloudLab, any of the 5 SUTs, live monitoring) | ✅ the primary workflow — see [Step-by-Step Operations](#step-by-step-operations) below | the WUI talks to the controller's Django REST API; nothing stops calling those same endpoints directly (e.g. with `curl`/a script) instead of the UI, but there isn't a dedicated CLI for this today — possible, not yet scripted |
| **Local** (single machine, proof-of-concept) | not supported — the WUI only targets CloudLab-provisioned clusters | ✅ [PStreamBench_local_docker/](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_local_docker#readme) — a `docker-compose` stack (Kafka + the SUT) plus shell scripts (`submit_*.sh`, `run_producer.sh`, `verify_output.sh`) or an interactive wizard (`pstreambench_cli.py`) that prompts for engine/workload/parallelism one at a time. Currently wired for Flink with Word Count and Ad Analytics as proof-of-life workloads; see that folder's README for scope and how to extend it |

If you just want to confirm a job actually runs — without provisioning CloudLab nodes, without the controller or WUI — start with `PStreamBench_local_docker/`. If you're running the full parallelism-enumeration benchmark across hardware tiers, use the distributed CloudLab + controller + WUI path described next.

## PStreamBench Workflow (High-Level)

1. Provision CloudLab resources (cluster nodes).
1. Add node hostnames in the Web UI.
1. Create a cluster and deploy an SUT (Flink, Storm, Spark, Kafka Streams, or RisingWave).
1. Select an application (real-world or synthetic).
1. Configure PQP/parallelism and workload parameters (event rate, duration, iterations, enumeration strategy).
1. Run the experiment.
1. Monitor real-time performance metrics.
1. Analyze historical results and export artifacts for offline analysis or ML.

## Step-by-Step Operations

>  Detailed instructions for each component are provided in the component-level README files.
1. Create a **CloudLab** account and set up a cluster using [PStreamBench_Cloud_setup/](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_Cloud_setup#readme).
2. Clone the repository into your home directory.
3. Set up and start [PStreamBench_controller/:](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_controller#readme).
4. Set up and start [PStreamBench_wui/](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_wui#readme).
5. In the WUI, open Explore Node and add hostnames from all CloudLab nodes.
6. Go to **Create Cluster** and select:
    - number of nodes
    - node allocation / roles
    - SUT: **Apache Flink**, **Apache Storm**, **Apache Spark**, **Kafka Streams**, or **RisingWave**
7. Deploy the selected SUT:
    - Flink: JobManager + TaskManagers
    - Storm: ZooKeeper + Nimbus + Supervisors
    - Spark: Standalone Master + Workers
    - Kafka Streams: independent app instances per worker node (no master role — coordination happens via Kafka's consumer-group protocol)
    - RisingWave: meta + compute + frontend + MinIO roles
8. Select the cluster and configure the experiment:
    - application/topology
    - event rate
    - parallelism / PQP / DoP vector
    - execution time
    - iterations
    - enumeration strategy
9. Run the experiment and monitor:
    - end-to-end latency
    - throughput
    - resource utilization
10. Download/export results:
    - configurations and metrics as JSON
    - optional graph representation artifacts
    - persisted records in configured databases (e.g., MongoDB/SQLite)

## Outputs and Artifacts

Each experiment run typically produces:

- run metadata (SUT, nodes, versions, workload parameters, PQP settings)

- time-series performance metrics (throughput, latency, utilization)

- exported JSON artifacts for offline analysis

- optional PQP/topology graph representations

- persisted results in configured databases (depending on configuration)

## Troubleshooting

- **Deployment succeeds but jobs/topologies fail:** verify all required services are running on the correct nodes (Flink JM/TM; Storm ZK/Nimbus/Supervisors; Spark Master/Workers; Kafka Streams app instances; RisingWave meta/compute/frontend/MinIO).

- **Throughput is capped:** confirm PQP/parallelism does not exceed available slots/executors and verify the source generator configuration.

- **Metrics missing in UI:** confirm controller database connectivity and metric collectors are enabled.