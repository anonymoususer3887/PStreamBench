<h2 align="center">
  Experiment, Analysis, and Benchmark on PStreamBench for DSPS
</h2>

This repository contains the experimental setup and workload suite used in the paper:

**“Is Parallelism a Silver Bullet? An Empirical Study of Parallelism Behavior in Distributed Stream Processing Systems.”**

It provides a collection of representative streaming applications, configurations for varying degrees of parallelism (DoP), and scripts for executing experiments across different distributed stream processing systems (DSPS), including **Apache Flink** and **Apache Storm**.

The setup enables controlled evaluation of parallelism behavior across:

- diverse workloads,
- multiple system architectures, and  
- heterogeneous hardware configurations.

It supports running experiments, collecting performance metrics such as **throughput**, **end-to-end latency**, and **resource utilization**, and exporting results for further analysis.

## Key Concepts

- SUT (System Under Test): The stream processing engine being benchmarked (e.g., Flink, Storm).
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

## Workloads

PStreamBench supports benchmarking using:

- 14 real-world applications

Applications can be executed under different PQPs, workload settings, and resource configurations.

## Repository Structure

- [PStreamBench_Cloud_setup:](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_Cloud_setup#readme)
CloudLab setup instructions and scripts to provision resources and install dependencies.

- [PStreamBench_controller:](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_controller#readme)
  - Backend controller. Provides APIs to automate:

  - cluster creation and SUT deployment (Flink/Storm)

  - job/topology submission and experiment lifecycle

  - persistence of metadata and results (e.g., SQLite/MongoDB depending on configuration)

- [PStreamBench_wui:](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_wui#readme)
Web UI for provisioning inputs, workload configuration, experiment execution, and visualization.

- [PStreamBench_workload_generator:](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_workload_generator) Workload generator for <strong>Apache Flink</strong> and <strong>Apache Storm</strong>. It produces benchmark executions for real-world applications, and enumerates <em>parallel query plans</em> by varying operator (Flink) or spout/bolt (Storm) parallelism and workload parameters.

- [PStreamBench_benchmark_data](https://github.com/anonymoususer3887/PStreamBench/tree/main/PStreamBench_benchmark_data)
Selected sample experiment outputs (real-world and synthetic applications).

> Note: Detailed steps about each component of PStreamBench is provided in their respective README.md files.

## PStreamBench Workflow (High-Level)

1. Provision CloudLab resources (cluster nodes).
1. Add node hostnames in the Web UI.
1. Create a cluster and deploy an SUT (Flink or Storm).
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
    - SUT: **Apache Flink** or **Apache Storm**
7. Deploy the selected SUT:
    - Flink: JobManager + TaskManagers
    - Storm: ZooKeeper + Nimbus + Supervisors
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

- **Deployment succeeds but jobs/topologies fail:** verify all required services are running on the correct nodes (Flink JM/TM; Storm ZK/Nimbus/Supervisors).

- **Throughput is capped:** confirm PQP/parallelism does not exceed available slots/executors and verify the source generator configuration.

- **Metrics missing in UI:** confirm controller database connectivity and metric collectors are enabled.