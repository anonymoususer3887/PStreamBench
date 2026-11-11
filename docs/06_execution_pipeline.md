# PStreamBench — Experimental Execution Pipeline

> **Status: PENDING full write-up**, but the overall shape is confirmed
> from `infra/views.py`/`infra/runner.py` and the `run_strategy_sweep_*.py`
> scripts. Needs a full line-by-line pass through `runner.py`'s
> `JobRunner` (or equivalent) class to confirm exact step ordering and
> file touch-points per engine before this is complete.

## Step-by-step sequence (confirmed shape, not yet exhaustively detailed)

1. **Prepare environment** — node already provisioned (see
   `docs/02_environment_setup.md`) and registered (`Nodes` model);
   cluster already created (`AnsibleClusterCreate` — picks free nodes,
   runs install/config/start playbooks; see `docs/01_architecture.md` §2.1
   and `docs/14_issue_log.md` CTRL-001 for a real failure mode here).
2. **Start system** — covered by cluster creation, not job submission;
   the engine's processes (JobManager/Nimbus/Spark master/Kafka broker/
   RisingWave meta) are already running before any job is submitted.
3. **Deploy workload** — the job jar/`.sql` is copied onto the cluster
   **at submit time**, not at cluster-create time (confirmed: the
   controller's job-submit playbooks, not the cluster-create ones, handle
   this — see `docs/01_architecture.md` §2.3).
4. **Start producer** — `custom_producer.py` (or the local Docker copy)
   feeds real Kaggle-sourced data into the input Kafka topic at a
   configured rate.
5. **Warm-up** — *[inferred: not yet confirmed whether a distinct
   warm-up phase exists separate from the measurement window, or whether
   the full `job_run_time` window is the measurement itself]*.
6. **Measurement window** — `job_run_time` / `waitTimeToCancel`, engine-
   dependent (see each `docs/04_frameworks/*.md` for self-termination
   behavior — Flink/Spark/Kafka Streams self-cancel in-process; Storm
   needs an explicit external kill; RisingWave needs an explicit `DROP`).
7. **Metric collection** — `getprom*.py` (per engine), scraping
   Prometheus + the engine's own REST API, run **after** the job
   completes but (per `docs/14_issue_log.md` FLINK's own note elsewhere)
   sometimes time-sensitive — Flink actually removes a cancelled job's
   metric series quickly, so collection must happen while the job is
   still running or shortly after, not an arbitrary delay later.
8. **Shutdown** — explicit kill (Storm), explicit `DROP` (RisingWave), or
   already self-terminated (Flink/Spark/Kafka Streams) by this point.
9. **Validation** — *[inferred: the sweep scripts' manifest-based
   `status=='ok'` check is the validation step; needs direct confirmation
   of what "ok" actually checks — did output land, or just "did the
   collector not crash"]*.
10. **Retry/failure handling** — a JSONL manifest makes the sweep scripts
    resumable; failed combos (anything not `status=='ok'`) are retried
    automatically on the next invocation — confirmed working live
    multiple times (see `docs/14_issue_log.md` INFRA-ENV-001's "no code
    changes needed" note, and STORM-009/010's retry-after-fix pattern).
11. **Result persistence** — `evaluationMetrics-*.json` +
    `metaInfo-*.json` written to the results tree (see
    `docs/07_metrics.md` for schema, pending).

**Still needed for this file:** the exact REST call sequence
(`POST /infra/jobcreate/<cluster_id>` body shape and response), a
sequence diagram, and direct confirmation of steps 5 and 9 above.
