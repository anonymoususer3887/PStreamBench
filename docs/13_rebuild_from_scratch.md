# PStreamBench — Reconstruction Guide

> **Status: substantially grounded** for the local Docker path (every
> command below was actually run and validated this session or in
> `PStreamBench_local_docker/README.md`) and for CloudLab node
> provisioning (validated live on 15 fresh nodes, 2026-10-14/15 — see
> `docs/14_issue_log.md` INFRA-ENV-002/003). The controller/WUI setup
> steps are confirmed from their own READMEs but not re-validated from
> absolute scratch in this pass. **Someone following this section should
> get further than starting from nothing, but should expect to hit the
> open issues flagged inline** (Spark's micro-batch stall,
> RisingWave's MinIO-blocked multi-node path) rather than a fully clean
> run on every engine.

## 0. Machine requirements

- A machine with Docker Desktop (for the local path) **or** SSH access to
  provisionable Linux nodes (CloudLab or equivalent, for the distributed
  path) — both paths can be set up independently; neither requires the
  other.
- For the distributed path: Ubuntu 22.04 nodes with passwordless `sudo`,
  at least ~60GB free disk (the `mkextrafs.pl`-provisioned `/home/
  playground` filesystem alone needs this for one engine's binaries +
  datasets), 16+ cores recommended (this project's CloudLab nodes were
  `m510`-class, Xeon D-1548, 16 cores/62GB RAM).
- Java 11, Python 3.10+, Node.js 16+/npm 9+ (controller + WUI).

## 1. Repository checkout

```bash
git clone <this repository>
cd PStreamBench
```

## 2. Local Docker path (fastest way to prove the whole pipeline works)

```bash
cd PStreamBench_local_docker
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
./scripts/setup_datasets.sh          # downloads real data into data/
docker compose up -d                 # 11 containers: Zookeeper, Kafka, Flink,
                                      # Kafka Streams, RisingWave, Storm, Spark
./scripts/create_topics.sh

# Word Count on Flink, as a first smoke test
./scripts/submit_wordcount.sh &
sleep 5
./scripts/run_producer.sh "Word_Count" WordCountIn 1000 20
./scripts/verify_output.sh WordCountOut
```

See `PStreamBench_local_docker/README.md` for every other engine's exact
commands (Kafka Streams, RisingWave, Storm all confirmed working the
same session; Spark submits correctly but has an open micro-batch issue —
see `docs/12_known_limitations.md`) and the interactive wizard
(`pstreambench_cli.py`) as an alternative to remembering script order.

## 3. Distributed/CloudLab path

### 3.1 Provision nodes

For each node, stage the required assets and run the two-stage install
(pre-condition, then the specific engine(s) that node will run):

```bash
# From PstreamBench_Cloud_profile/ (the actual CloudLab profile repo —
# holds the real binary assets: job jars, kaggle.json, jmx exporter, etc.)
NODE=<hostname>
ssh $NODE "sudo mkdir -p /local/repository && sudo chown \$(whoami) /local/repository"
scp custom_producer.py dsp_jobs-1.0-SNAPSHOT.jar jmx_exporter*.yml \
    jmx_prometheus_javaagent.jar jna*.jar kaggle.json oshi-core-*.jar \
    roads.geojson server.properties spark-metrics.properties \
    storm_exporter.py pre_conditioning.sh \
    install_flink_only.sh install_storm_only.sh install_spark_only.sh \
    install_kafkastreams_only.sh install_risingwave_only.sh \
    $NODE:/local/repository/

ssh $NODE "cd /local/repository && sudo bash pre_conditioning.sh"
# then exactly one of, depending on which engine this node will run:
ssh $NODE "cd /local/repository && sudo bash install_flink_only.sh"      # or install_storm_only.sh /
                                                                           # install_spark_only.sh /
                                                                           # install_kafkastreams_only.sh /
                                                                           # install_risingwave_only.sh
```

**Important, validated live:** `archive.apache.org` (the source for
Flink/Storm/Spark/Kafka's pinned versions) is extremely slow — download
each engine's tarball **once**, then distribute it to that engine's other
nodes via `scp`/LAN transfer rather than re-downloading per node. See
`docs/14_issue_log.md` INFRA-ENV-002 for the exact pattern that took 15
nodes from "would take many hours serially" to about an hour.

**Known blocker, not yet resolved:** if a node needs RisingWave in real
multi-node mode, its MinIO step will fail — the official `minio` binary
and Docker image are no longer freely distributed (INFRA-ENV-003).
`playground` (single-node) mode is unaffected.

### 3.2 Start the controller

```bash
cd PStreamBench_controller/dsp_be
python3 -m venv venv && ./venv/bin/pip install -r requirements.txt
./venv/bin/python manage.py migrate
./venv/bin/python manage.py runserver 0.0.0.0:8000
```

Check nothing else is already using port 8000 first
(`lsof -i :8000`) — a real, repeated collision in this project's own
development (see `docs/14_issue_log.md` WUI-001's context).

### 3.3 Start the WUI

```bash
cd PStreamBench_wui/dsp_fe
npm install
npm run serve          # http://localhost:8080
```

### 3.4 Add nodes and create a cluster, via the WUI

1. **Explore Nodes** → Add Cloudlab Node → domain name + SSH user, for
   each node provisioned in §3.1.
2. **Create Cluster** → pick engine type, cluster name, resource sizing,
   hardware type. The controller auto-assigns whichever nodes are
   currently free (not user-selectable) — see
   `docs/14_issue_log.md` for why you may want to add nodes in a
   specific order if you need a specific assignment.
3. Wait for the Ansible playbook chain to complete (several minutes —
   the WUI's "please wait" dialog does not show real step-by-step
   progress; see `WUI_IMPROVEMENTS_TODO.md` item 4). **Check the
   controller's own log/the cluster's listed status afterward** — a
   failed step does not currently raise an error back to the UI
   (CTRL-001).

### 3.5 Submit a job

Explore Clusters → click into the cluster → Submit Job tab → pick
workload (Word Count is the simplest first test), parallelism, run time
→ Submit Job.

### 3.6 Validate the result

Check the cluster's Grafana dashboard (Flink/Storm only, confirmed
working today — `WUI_IMPROVEMENTS_TODO.md` item 10), or check the raw
`evaluationMetrics-*.json`/`metaInfo-*.json` files written to the results
tree directly.

## 4. Full benchmark execution (sweeps)

```bash
cd PStreamBench_controller/dsp_be/benchmark_sweep
./venv/bin/python run_strategy_sweep_flink.py       # or _storm/_spark/_kafkastreams/_risingwave
```

Resumable via a JSONL manifest — safe to re-run after any failure; only
combos not already `status=='ok'` are retried.

## 5. Plot generation

```bash
cd PStreamBench_benchmark_data
python3 12_run_full_enumeration_batch.py    # the full 31,851-figure batch;
                                             # expect this to take a while —
                                             # it's run across 10 parallel workers
```

Or a narrower, faster pass via `10_run_narrow_sweeps.py` / the individual
`6_make_all_plots*.sh` / `7_make_enumeration_plots.sh` scripts — see
`docs/08_analysis_and_plots.md` for what each produces.

## 6. Expected output structure

```text
PStreamBench_benchmark_data/
├── artifacts/<workload>/<hardware>/<engine>/<variant>/<rate>/
│   ├── evaluationMetrics-*.json
│   └── metaInfo-*.json
├── query_performance_data.csv
├── operator_level_metrics.csv
├── operator_templates.csv
└── plots_todate/ , plots_enumeration/ , plots_pdsp_<engine>/
    └── <figure families as PNG + PDF + SVG, plus a CSV config mapping>
```

## 7. If something doesn't match this guide

This guide was validated against the real, live state of the project as
of October 2026 for the sections marked above — but it is not a
substitute for `docs/14_issue_log.md`'s growing list of real gotchas.
Before concluding something is broken, check that file first; if it's a
genuinely new issue, add it there using its structured format before
working around it silently.
