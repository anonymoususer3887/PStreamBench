# PStreamBench — Known Limitations and Unresolved Issues

> **Status: substantially grounded** — aggregated directly from
> `docs/14_issue_log.md`, `WUI_IMPROVEMENTS_TODO.md`,
> `LOCAL_DOCKER_TODO.md`, and `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §10.
> This file should be kept in sync with those as they change — it is a
> **summary view**, not a separate source of truth.

## 🟢 Resolved (confirmed fixed and validated)

- Flink/custom producer hardcoded `localhost:9092` bootstrap servers —
  `docs/14_issue_log.md` FLINK-001.
- Flink Prometheus reporter misconfiguration, SSH detachment hang,
  TaskManager host advertisement, slot-allocation model — FLINK-002/003/
  004/005.
- Storm Docker entrypoint failure, worker-slot exhaustion, null-keyed
  topic false alarm, ZooKeeper incompatibility/port collision, worker-
  count scaling, short-window measurement artifact, exporter deadlock +
  its self-matching-`pkill` meta-bug, missing operator dispatch — all of
  STORM-001 through STORM-011 except STORM-012.
- Chained-filter threshold no-op bug, join key-space under-calibration —
  SYNTH-001/002.
- All planned Storm synthetic-workload sweeps: uniform (162/162) and
  Random/DS2/Dhalion (324/324) — `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md`
  §10.
- All planned Flink synthetic-workload sweeps, same scale, after the
  slot-allocation correction — same source.
- The `matplotlib.tight_layout()` NaN-axes-position bug in the full
  31,851-figure enumeration batch — "Contributions So Far" doc, Current
  status section.

## 🟡 Partially resolved (worked around, root cause not eliminated)

- **Storm topologies never self-terminate** (STORM-002) — every current
  caller explicitly kills them; the underlying library behavior is
  unchanged, so a *future* caller that doesn't know this will leak slots
  silently.
- **Unrotated Storm worker logs can fill a node's disk and cascade to a
  full cluster outage** (INFRA-ENV-001) — cleared manually once; no log
  rotation was actually configured, so a future multi-day sweep will hit
  this again.

## 🔴 Unresolved (open, not worked around)

- **`_configure_and_run_playbooks()` discards the Ansible exit code**
  (CTRL-001) — a failed cluster-create step currently reports success to
  the WUI.
- **`ds2`/`dhalion` not selectable from the WUI's Submit Job form**
  (WUI-002) — only available via command-line sweep scripts.
- **Port 8000 hardcoded independently across every WUI component**
  (WUI-001).
- **MinIO's discontinuation of free binary/image distribution blocks
  real multi-node RisingWave deployment on fresh infrastructure**
  (INFRA-ENV-003) — `playground` single-node mode is unaffected.
- **`getBoltMetrics` crashes on a zero-result Prometheus query**
  (STORM-012) — caught by the sweep's own error handling, surfaces as a
  retry, doesn't corrupt data.
- **Spark: a second micro-batch was not observed in local Docker
  testing** — `LOCAL_DOCKER_TODO.md`; explicitly a local-environment
  finding, several specific hypotheses already ruled out, several not
  yet tried.
- **No live Grafana dashboard for Spark, Kafka Streams, or RisingWave**
  — `WUI_IMPROVEMENTS_TODO.md` item 10.
- **No statistical-significance testing applied to cross-strategy/
  cross-framework comparison claims** — `docs/09_validation.md`; flagged
  explicitly in the "New Insights" project doc as needed before any
  such number is called a finding rather than an observation.
- **No oracle/near-exhaustive baseline for "% of best possible" framing**
  in strategy-search-quality claims — same source.

## ⚪ Accepted limitations (understood, deliberately not changed)

- **`dhalion_strategy` clamps every stage to a minimum of 2** even with
  no observed backpressure, so a true "no-change" baseline isn't
  representable — SYNTH-003. Not changed because it's shared code also
  used by real-workload sweeps whose existing results depend on the
  current behavior.
- **No per-operator parallelism breakdown for Kafka Streams/RisingWave**
  — a deliberate design decision (`docs/11_design_decisions.md`), not a
  gap to fill.

## Future work (explicitly scoped, not started)

- Synthetic workload ports for Spark, Kafka Streams, RisingWave —
  Storm and Flink were chosen first.
- A combined throughput+latency dual-axis chart for the *selected*
  configuration (currently only available as a scatter over *all*
  configurations).
- CPU/network efficiency metrics for the selected/configuration-based
  views — blocked on porting a derived-metric calculation from the
  original baseline script.
- A curated "intro" overview figure for the new configuration-based
  pipeline, analogous to the one that exists for the old mean-
  parallelism pipeline.
- A full old-family → configuration-based-view → selected/best-view
  mapping table covering all 14 original plot families — noted in the
  "Contributions So Far" doc as "in progress."
- Exposing all of `parallelism_strategies.py`'s strategies (not just
  DS2/Dhalion) via the WUI, per the corrected design in
  `WUI_IMPROVEMENTS_TODO.md` item 11.
- Native (non-Docker, non-CloudLab) local execution — written up as a
  guide (`PStreamBench_local_docker/README.md`'s "Native" section) but
  explicitly **not live-verified** end-to-end on real hardware yet.
