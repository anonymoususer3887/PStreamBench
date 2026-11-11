# PStreamBench — Architecture

> **Status:** written directly from the repository layout and the
> controller's actual URL/view dispatch (`dsp_be/urls.py`, `infra/views.py`,
> `infra/runner.py`), confirmed by reading code, not inferred. Peripheral
> subsystems noted as **[inferred]** where only lightly checked (their
> purpose is clear from imports/naming but their internals weren't read
> line-by-line for this pass).

## 1. Repository structure

```text
PStreamBench/
├── PStreamBench_Cloud_setup/        # CloudLab profile scripts: provision a node, install
│                                     # Java/Python/Kafka/each engine's binary, Prometheus/Grafana
├── PStreamBench_controller/
│   └── dsp_be/                      # Django project — the benchmark's control plane
│       ├── infra/                   # THE core app: Cluster/Nodes models, Ansible orchestration,
│       │                            # job submission/termination (see §3)
│       ├── auth/                    # user accounts [inferred: standard Django auth app]
│       ├── report/                  # result reporting views — reads Cluster model + Kafka
│       │                            # directly, uses constants_flink/constants_storm [inferred]
│       ├── datagen/                 # a SEPARATE cluster type ("PQP" — zero-shot data
│       │                            # generator), its own DataGenCluster model, SSH-driven
│       │                            # (paramiko) rather than Ansible [inferred, lightly checked]
│       ├── utils/                   # per-engine constants (constants_flink.py, _storm.py,
│       │                            # _spark.py, _kafkastreams.py, _risingwave.py) +
│       │                            # utils/playbook/ (every Ansible playbook + task file)
│       ├── benchmark_sweep/         # command-line sweep automation — reuses the SAME
│       │                            # /infra/jobcreate REST endpoint the WUI uses (see
│       │                            # docs/05 and docs/06), does not reimplement submission
│       ├── job_automate.py          # a simpler, older batch-submission script (loops over
│       │                            # a hardcoded job_query_mapping, hits the REST API directly)
│       └── db.sqlite3               # Cluster/Nodes/User state — NOT the benchmark results
│                                     # themselves (those are files, see §2.4)
├── PStreamBench_wui/dsp_fe/         # Vue 2 + Vuetify 2 frontend, talks to the controller's
│                                     # REST API (port 8000, hardcoded per-call — see
│                                     # WUI_IMPROVEMENTS_TODO.md item 6)
├── PStreamBench_workload_generator/ # job SOURCE CODE, one subfolder per engine:
│   ├── flink/dsp_flink_jobs/         #   + dsp_flink_synthetic_jobs/ (9 synthetic shapes)
│   ├── storm/dsp_storm_jobs/         #   + dsp_storm_synthetic_jobs/
│   ├── spark/dsp_spark_jobs/
│   ├── kafkastreams/dsp_kafkastreams_jobs/
│   └── risingwave/dsp_risingwave_jobs/   # plain .sql files, no build step
├── PStreamBench_benchmark_data/      # the analysis pipeline: numbered scripts 0–12 (see
│                                     # docs/08), consume per-run JSON, emit CSV + figures
├── PStreamBench_local_docker/       # docker-compose stack for all 5 engines, no CloudLab/
│                                     # controller — proof-of-life testing (see docs/02)
└── docs/                            # this documentation set
```

## 2. Main components and how they relate

### 2.1 The controller (`PStreamBench_controller/dsp_be`)

A Django project exposing a REST API under four route prefixes
(`dsp_be/urls.py`): `/infra/`, `/auth/`, `/report/`, `/datagen/`. **`infra`
is the one this whole benchmark is actually built around** — it owns two
models (`Cluster`, `Nodes`) and the views that:

- register a CloudLab node (`CreateNode`/`DeleteNode`),
- create a cluster of a given engine type on N free nodes
  (`AnsibleClusterCreate` — picks nodes, writes an Ansible inventory,
  runs the engine's install/config/start playbook chain),
- start/stop a cluster's engine processes (`AnsibleClusterStart`/`Stop`),
- submit, wait on, and collect metrics for a job
  (`JobRunner`/the job-create view — see `docs/06_execution_pipeline.md`
  for the exact sequence), and
- delete a cluster (`AnsibleClusterDelete` — runs a teardown playbook,
  then removes the DB row regardless of whether that playbook actually
  succeeded; see `docs/10_troubleshooting.md` for why this matters).

All Ansible execution goes through one helper,
`_configure_and_run_playbooks()` (`infra/views.py`), which loads
`utils/playbook/inventory.yml`, runs the named playbook via Ansible's
Python API (`PlaybookExecutor`), and **does not check its return code** —
a failed play does not raise an exception or stop execution. This is a
confirmed, significant architectural fact, not a minor detail — see
`docs/10_troubleshooting.md` and `WUI_IMPROVEMENTS_TODO.md` item 1 for its
consequences.

### 2.2 The WUI (`PStreamBench_wui/dsp_fe`)

A Vue 2 / Vuetify 2 single-page app. Every API call is made directly from
a component (`axios.post(this.url + ":8000/infra/...")`) — there is no
shared API-client module, which is why the port is hardcoded in many
places rather than configured once. Key views:
`ExploreCloudlabNode.vue` (add/remove nodes), `CreateCluster.vue` (create
a cluster — picks engine type, resource sizing; does **not** let the user
choose which specific nodes get used, see `docs/05`), `ExploreClusters.vue`
(list clusters + status), `ClusterInfo.vue` (the actual "Submit Job" form
— per-workload parallelism/window/enumeration-strategy inputs).

### 2.3 Job source code (`PStreamBench_workload_generator/`)

One independent build project per engine per workload family (real vs.
synthetic). "Independent" is a deliberate choice, confirmed in
`SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §3: the synthetic Storm project
(`dsp_storm_synthetic_jobs`) is a separate Gradle project from the real
one (`dsp_storm_jobs`) specifically so that building/wiring synthetic
workloads could never accidentally modify or break the 14 real ones. The
controller's Ansible playbooks copy the already-built jar (Flink/Storm/
Spark/Kafka Streams) or `.sql` file (RisingWave) onto the cluster at
job-submit time, not at cluster-create time — see `docs/06`.

### 2.4 The analysis pipeline (`PStreamBench_benchmark_data/`)

Not a database — every run's result is two JSON files
(`evaluationMetrics-*.json`, `metaInfo-*.json`) written to a
workload/hardware/engine/variant-named directory tree (see `docs/07` for
their exact schema). The numbered scripts (`1_data_extractor.py` through
`12_run_full_enumeration_batch.py` — see `docs/08`) read that tree
directly off disk and produce CSVs and figures; there is no step where
results get loaded into the controller's own `db.sqlite3` — the two
systems (controller DB, and the file-based results tree) are entirely
separate, connected only by convention (the controller's metric-collection
step writes into the same tree the analysis pipeline reads from).

## 3. Data flow, end to end

```text
 ┌──────────────┐     ┌───────────────────┐     ┌──────────────────────┐
 │  Job source   │     │   Controller       │     │   CloudLab nodes      │
 │  (workload_   │────▶│   (infra app)       │────▶│   (Ansible-managed)   │
 │  generator/)  │ jar/│   - Cluster/Nodes   │ SSH │   - engine binaries   │
 │  built once,  │ sql │     models          │     │   - Kafka broker      │
 │  copied at    │     │   - job submission  │     │   - Prometheus +      │
 │  submit time  │     │     + wait + kill   │     │     engine-specific   │
 └──────────────┘     └───────────┬─────────┘     │     exporter          │
                                   │ getprom_*.py  └──────────┬────────────┘
                                   │ (scrapes Prometheus            │
                                   │  over HTTP, per engine)        │ Prometheus
                                   ▼                                │ scrapes
                   ┌───────────────────────────┐                   │
                   │ evaluationMetrics-*.json   │◀──────────────────┘
                   │ metaInfo-*.json            │
                   │ (file tree, per workload/  │
                   │  hardware/engine/variant)  │
                   └──────────────┬──────────────┘
                                  │ read directly off disk
                                  ▼
                   ┌───────────────────────────┐
                   │ PStreamBench_benchmark_data │
                   │ numbered scripts 0–12       │
                   │ → CSV (query_performance_   │
                   │   data.csv, operator_level_ │
                   │   metrics.csv, ...)         │
                   │ → PNG/PDF/SVG figures        │
                   └───────────────────────────┘
```

Workload generation → deployment → execution → metric collection are all
driven by the controller (steps 1–3 of the diagram); aggregation →
plotting is a separate, later, offline step over the accumulated file
tree (step 4) — the two halves do not run in the same process and can be
re-run independently (you can regenerate every plot from existing JSON
without touching a cluster at all).

## 4. Local, cluster, and cloud/distributed execution — how they differ

| | **Local Docker** (`PStreamBench_local_docker/`) | **Cluster / Distributed** (controller + CloudLab) |
|---|---|---|
| Orchestration | `docker-compose.yml`, no Django/Ansible | Django controller + Ansible playbooks over SSH |
| Node provisioning | N/A — containers on one machine | CloudLab nodes, pre-conditioned via `PStreamBench_Cloud_setup/install_all_tools.sh` (or the `install_<engine>_only.sh` variants) |
| Job submission | Shell scripts calling `docker compose exec ... flink run / storm jar / spark-submit / java -cp` directly | The same binaries, submitted the same way, but via Ansible tasks run by the controller over SSH |
| Metrics | None by design — this path is proof-of-life only (did data flow end to end), not performance measurement | Prometheus + per-engine exporter, scraped by `getprom_*.py`, written to the real results tree |
| Parallelism strategies | Not applicable — fixed small vectors for a smoke test | Full `uniform`/`random`/`ds2`/`dhalion` sweep support |
| Typical use | Verifying a job/engine combination works at all, fast iteration, no CloudLab allocation needed | Actual benchmark data collection |

**[inferred]** "Cloud" as a third, distinct deployment target beyond
CloudLab was not found as a separate code path in this pass — CloudLab
*is* the cloud/distributed execution environment this project uses; there
is no separate AWS/GCP-style deployment path confirmed in the repository.
If one exists, it was not located during this documentation pass and
should be corrected here if found.

## 5. Script/artifact dependency summary

| Produces | Consumed by |
|---|---|
| `PStreamBench_Cloud_setup/install_all_tools.sh` (or `install_<engine>_only.sh`) run on a node | `infra/views.py`'s cluster-create playbooks (assume `/home/playground/<engine>` already exists — **do not install it themselves**, see `docs/10_troubleshooting.md`) |
| Engine job jar/`.sql` built in `PStreamBench_workload_generator/<engine>/` | Copied onto the cluster by the controller's job-submit playbook (not the cluster-create one) |
| A job run (controller or local Docker) | `evaluationMetrics-*.json` + `metaInfo-*.json` in the results tree |
| The results tree | `PStreamBench_benchmark_data`'s numbered scripts, in rough dependency order: `1_data_extractor.py` → `2_operator_build_data.py`/`11_extract_operator_metrics.py` → `3_plot_pdsp_query_metrics.py`/`4_plot_pdsp_operator_metrics.py`/`7_plot_pdsp_enumeration_analysis.py` (see `docs/08` for the full graph) |
| `parallelism_strategies.py`'s `STRATEGIES` dict | Both `run_strategy_sweep_*.py` (command-line sweeps) and, per-call, the controller's own `infra/runner.py` enumeration logic for `Random`/`MinMax`/`Exhaustive`/etc. — **these are two separate implementations that happen to overlap in strategy names**; see `docs/05` and `WUI_IMPROVEMENTS_TODO.md` item 11 for why they aren't the same code path |
