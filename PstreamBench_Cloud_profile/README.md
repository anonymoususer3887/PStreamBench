# PStreamBench — CloudLab Cluster Setup (Apache Flink + Apache Storm)

This README describes how to provision **CloudLab** nodes for **PStreamBench** and prepare them for benchmarking **Apache Flink** and **Apache Storm** on a multi-node cluster. The CloudLab profile installs all required system dependencies and bootstrap services so that PStreamBench can later deploy and run Flink/Storm jobs from the controller/UI.

---

## Table of Contents

- [What CloudLab Provides](#what-cloudlab-provides)
- [Prerequisites](#prerequisites)
- [Create a CloudLab Profile](#create-a-cloudlab-profile)
- [Instantiate a Cluster](#instantiate-a-cluster)
- [Node Roles and Services](#node-roles-and-services)
- [Essential Setup Scripts](#essential-setup-scripts)
- [Verify Setup on the Nodes](#verify-setup-on-the-nodes)
- [Ports](#ports)
- [Troubleshooting](#troubleshooting)
- [Next Steps](#next-steps)

---

## What CloudLab Provides

CloudLab is a research testbed that provides **bare-metal servers** for building isolated clusters (“experiments”). PStreamBench uses CloudLab to:
- provision a multi-node cluster (e.g., 4–10 nodes),
- prepare each node with required dependencies,
- ensure foundational services (Kafka, ZooKeeper, monitoring) can run reliably,
- support benchmarking of **Flink** and **Storm** using PStreamBench orchestration (controller + UI).

---

## Prerequisites

### CloudLab Account
- Create an account at: https://www.cloudlab.us/
- Ensure your **default shell** is set to `bash` in CloudLab user settings (important for hostname handling and scripts).

### SSH Access (Required)
PStreamBench requires **password-less SSH** from the machine running the backend/controller to CloudLab nodes.

1. Ensure you have an SSH key locally (typical default: `~/.ssh/id_rsa.pub`).
2. Add the public key in CloudLab: **Manage SSH Keys**.
3. Test SSH access to a CloudLab node (should **not** ask for password):
   ```bash
   ssh <cloudlab_user>@<node_fqdn>

### Recommended Hardware / OS

- Choose a supported node type (commonly m510 works well).

- Use the OS image defined by the profile (commonly Ubuntu 22.04/20.04 depending on your profile version).

## Create a CloudLab Profile

PStreamBench uses a repository-based profile. CloudLab will clone the repository to each node at:

- `/local/repository`

### Option A — Use This Repository Directly (Public)

1. Upload this CloudLab profile folder to a repository (recommended separate repo for profile):

`dsp_cloudlab_profile/` (or your equivalent CloudLab profile directory)

2. In CloudLab, create a new profile from that repository.

3. CloudLab requires one of the following at the top-level of the repo:

`profile.py (geni-lib)` or

`profile.rspec`

### Option B — Keep the Repo Private

If the profile repository is private, configure CloudLab access using a supported token-based method (e.g., GitLab access token) so CloudLab can clone it.

> Tip: If you update your repository later, use the profile Update button in CloudLab (Edit Profile page) to refresh branches/tags and update cached content.

## Instantiate a Cluster

1. Go to CloudLab and select your profile.
2. Create a new experiment (instantiate the profile).
3. Choose:
    - Number of nodes (example: 6)
    - Hardware type (example: m510)

4. Start the experiment.
5. Wait until the cluster is ready and all startup services have finished.
6. Open a shell on node0 and check:

    ```bash
    hostname

###  Hostname Patterns

Other nodes typically share the same suffix; only the prefix changes:

- `node0.<exp>.<site>.cloudlab.us`
- `node1.<exp>.<site>.cloudlab.us`
- `node2.<exp>.<site>.cloudlab.us`
- ...

You will use these hostnames later in PStreamBench UI under Explore Nodes.

## Node Roles and Services

PStreamBench assumes the first node (`node0`) acts as the main node for cluster-level services and dashboards. Other nodes act as workers.

### Shared Foundation Services (All Nodes)

- baseline tools + runtime dependencies
- dataset directories and repo clone
- monitoring agent (Node Exporter, if enabled in your profile/scripts)

### Kafka + ZooKeeper Cluster (All Nodes)

This setup extends PStreamBench to form Kafka and ZooKeeper clusters across all nodes, supporting higher input rates and improved stability for multi-node experiments.

### Flink Cluster (Deployed later by PStreamBench)

- `node0:` JobManager
- `node1..n:` TaskManagers

### Storm Cluster (Deployed later by PStreamBench)

- `node0:` Nimbus + Storm UI (and typically a ZooKeeper endpoint)
- `node1..n:` Supervisors

> Important
The CloudLab profile prepares dependencies and service scaffolding. Actual Flink/Storm cluster deployment and job execution is performed by PStreamBench (controller/playbooks) when you create a cluster in the UI.

## Essential Setup Scripts

Your CloudLab profile typically orchestrates the following scripts (names may differ slightly depending on your repository layout):

`profile.py`

- Defines the cluster topology:
    - number of nodes (configurable at instantiation)
    - node types / disk images
- Defines Execute Services that run after nodes clone the repository.

`preconditioning.sh`

- Creates directory structure and base environment
- Installs core dependencies (commonly):
    - Java / Python tooling
    - system packages required by playbooks and collectors

`install_all_tools.sh`

Installs and prepares services/tools used during benchmarking, including:
- Kafka (multi-node cluster support)
- ZooKeeper (multi-node cluster support)
- Monitoring stack prerequisites (Prometheus/Grafana and/or exporters)
- Dependencies needed for both Flink and Storm runtime deployments

`download_dataset.sh (optional)`

- Downloads datasets (e.g., from Kaggle) because CloudLab limits repo size.
- If Kaggle rate limits occur, see troubleshooting.

## Verify Setup on the Nodes

After the experiment is fully ready (CloudLab shows it as Ready and startup services finished), verify on node0:

 1. Repository clone exists

    ```
    ls -la /local/repository
    ```

 2. Basic networking between nodes

From node0, ping another node:
    ```
    ping -c 2 node1
    ```

 3. Check installed tool versions (examples)

    ```
    java -version
    python3 --version
    ```

 4. Check that background services (if started by profile) are running
    
    Depending on your profile, some services may be started immediately. Use:

    ```
        ps aux | egrep -i "kafka|zookeeper|prometheus|grafana|node_exporter|storm|flink" | head
    ```   


> Note: Flink/Storm processes may not be running yet if you deploy them later via PStreamBench UI. That is expected.

## Ports

Most dashboards/services are hosted on the main node (node0) of a PStreamBench cluster.

Common ports:

- 8086 — Apache Flink Dashboard
- 8080 — Apache Storm UI
- 3000 — Grafana
- 9090 — Prometheus

Example:
If your main node is
`node0.bob-123456.maki-test-pg0.utah.cloudlab.us`, then:

Flink Dashboard: `http://node0.bob-123456.maki-test-pg0.utah.cloudlab.us:8086`

Storm UI: `http://node0.bob-123456.maki-test-pg0.utah.cloudlab.us:8080`

Grafana: `http://node0.bob-123456.maki-test-pg0.utah.cloudlab.us:3000`

Prometheus: `http://node0.bob-123456.maki-test-pg0.utah.cloudlab.us:9090`

## Troubleshooting

### Cluster Creation / Startup Services Fail

- Check the CloudLab Execution Log for your experiment.
- Most common failures are missing downloads or transient network issues.

### Missing Downloads (Kafka, Storm, etc.)

- Some projects do not keep all versions available forever.
- If an archive is missing, update the download URL in:
    - CloudLab profile scripts (e.g., `install_all_tools.sh`)
    - Ansible tasks used by PStreamBench playbooks (if applicable)

### Kaggle Download Fails (Rate Limit)

- Reload nodes in small batches (CloudLab) and wait for setup to finish.
- Consider hosting datasets in an alternative location if failures are frequent.

### SSH Connection to Nodes Not Working

- Ensure your SSH public key is added in CloudLab.
- If you added a new key recently:
    - existing nodes may need reload or the experiment recreated.

### Flink TaskManagers Don’t Join JobManager (Later, after PStreamBench deploys)

- Check JobManager logs for hostname binding issues.
- Ensure CloudLab user default shell is `bash` (prevents `$HOST` issues in some cases).
- If you see FQDN problems, use explicit hostname binding in startup commands.

### Storm Metrics / Exporter Issues (Later, after PStreamBench deploys)
-  Storm metrics collection may rely on exporter scripts.
- If metrics are missing, restart the exporter process on `node0` (if used in your setup).
- Keep in mind: Storm exports some metrics at coarse granularity (often ~1 min), limiting short-run observability.

### Grafana Crashes / Charts Missing

Grafana can be unstable on some nodes/setups. If it stops:

- restart it manually (path depends on your installation)
- use `tmux` to keep it running

## Next Steps

After CloudLab nodes are provisioned and ready:
1. Set up and start the PStreamBench Controller
    See: PStreamBench_controller/README.md
2. Set up and start the PStreamBench Web UI
    See: PStreamBench_wui/README.md
3. In the UI:
    - add your CloudLab nodes in **Explore Nodes**
    - go to **Create Cluster**
    - select the **SUT (Flink or Storm)**
    - deploy and execute jobs/topologies