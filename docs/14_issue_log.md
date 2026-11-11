# PStreamBench — Issue & Decision Log

> **Purpose:** an engineering history, not a static manual. Every entry
> below was confirmed in code, a live run, or an existing project document
> (cited in **Source**) — nothing here is invented. This is a **living
> log**: append new entries at the bottom of their component's section,
> never edit history out, and give each a unique, never-reused ID.
>
> **ID scheme:** `<COMPONENT>-<NNN>`, zero-padded, assigned in discovery
> order within that component. Components used so far: `STORM`, `FLINK`,
> `SPARK`, `WUI`, `CTRL` (controller/infra), `INFRA-ENV` (CloudLab/hosting
> environment, not engine-specific), `SYNTH` (synthetic workload design).

## Status legend

- 🟢 **Resolved** — fixed and validated live.
- 🟡 **Partially resolved** — fixed for the case that mattered at the
  time; a related case is explicitly left open.
- 🔴 **Unresolved** — actively open, not worked around.
- ⚪ **Accepted limitation** — understood, deliberately not fixed (scope
  or cost reasons), documented so nobody re-discovers it from scratch.

---

## FLINK-001 — Hardcoded Kafka bootstrap server ignored `-kafka-server`

- **Date:** 2026 (local Docker testing pass)
- **Component:** `PStreamBench_workload_generator/flink/dsp_flink_jobs/.../wordcount/WordCountJob.java`
- **Observed behavior:** Flink's Word Count job connected to `localhost:9092`
  regardless of the `--kafka-server` CLI argument passed to it.
- **Root cause:** `bootstrapServer = "localhost:9092"` was hardcoded; the
  line reading the real CLI parameter was present but commented out just
  above it.
- **Fix:** changed to `bootstrapServer = params.get("kafka-server")`.
- **Validation:** jar rebuilt with Java 11 (matching the module's
  `pom.xml` target); fix confirmed directly in compiled bytecode via
  `javap`, not just trusted from a successful `mvn package` (per the
  project's own "stale jar" gotcha — a prebuilt jar can silently drift
  from source).
- **Script/commit:** fixed in the real source file (not a local-only
  patch); jar copied to `PStreamBench_local_docker/jars/flink/`.
- **Remaining risk:** none identified — harmless on the original
  single-host CloudLab setup (where `localhost:9092` happened to be
  correct by accident), but would have broken any setup where Flink runs
  off-host from Kafka.
- **Status:** 🟢 Resolved.
- **Source:** `PStreamBench_local_docker/README.md` "Known upstream
  quirks fixed" §1.

## FLINK-002 — Wrong Prometheus reporter factory class name

- **Component:** CloudLab Flink cluster config (`flink-conf.yaml`)
- **Observed behavior:** Flink metrics never appeared on port 9250; no
  error, just silence.
- **Root cause:** `metrics.reporter.prom.factory.class` was set to
  `...PrometheusReporter` instead of `...PrometheusReporterFactory`. The
  TaskManager log did record a clear `WARN` ("the reporter factory ...
  could not be found") once actually checked.
- **Fix:** used the exact class name from Flink's own "available
  factories" warning message.
- **Validation:** port 9250 opened and was scraped successfully after the fix.
- **Status:** 🟢 Resolved.
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §9.

## FLINK-003 — `ssh host 'cmd & disown'` hangs for the full job duration, only for `flink run`

- **Observed behavior:** backgrounding a remote `flink run` over SSH with
  `cmd & disown` hung for exactly the job's `waitTimeToCancel` duration
  every time (5s wait → ~10s hang; 30s wait → ~35s hang), even though the
  job itself reached `RUNNING` within ~6s.
- **Investigation:** ruled out network/host causes — a plain `sleep 30 &`
  over the identical SSH connection returned in ~1s. Neither explicit
  stdio redirection, `setsid cmd &`, `ssh -T` (no pty), nor disabling SSH
  multiplexing changed the hang.
- **Root cause:** `flink run`'s CLI process genuinely runs the user's full
  `main()` (including its self-cancel busy-wait) before exiting — unlike
  a shell daemon that backgrounds itself internally and returns
  immediately, so the invoking SSH channel had something real to wait on
  regardless of shell-level backgrounding tricks.
- **Fix:** `setsid -f cmd > log 2>&1 < /dev/null` (fork-and-detach in one
  step, no trailing `&`, no `disown`) — returns in ~2s.
- **Validation:** confirmed via precise timing before/after; the
  `nohup ... & disown` pattern used for Storm's shell daemons was
  confirmed to still be correct for *those* (they background themselves),
  this fix is specific to processes like `flink run` that don't.
- **Status:** 🟢 Resolved.
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §9.

## FLINK-004 — TaskManagers advertised `localhost` instead of their real IP

- **Observed behavior:** jobs worked at the lowest parallelism level
  (Flink happened to schedule everything onto one node) but failed with
  `java.net.ConnectException: Connection refused` at `level=4` and above,
  once the scheduler spread tasks across multiple TaskManagers.
- **Root cause:** `taskmanager.host` was left unset (relying on
  auto-detection) while only `taskmanager.bind-host: 0.0.0.0` was
  configured — a cross-node shuffle then tried to connect to
  `localhost:<port>` on the *wrong* machine.
- **Fix:** set `taskmanager.host` explicitly to each node's real
  `10.10.1.x` address (the one genuinely per-node config value, unlike
  every other key which is identical across nodes).
- **Validation:** forced a `level=16` submission (guaranteed multi-node
  spread) immediately after the fix — ran and self-cancelled cleanly.
- **Status:** 🟢 Resolved.
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §9.

## FLINK-005 — Wrong slot-allocation model assumed (per-shape parallelism ceiling was fictional)

- **Observed behavior:** none directly — this was a *design* error, not a
  runtime failure, caught by **user pushback**, not self-discovered.
- **Initial (wrong) assumption:** believed Flink allocates one cluster
  slot per operator-parallelism instance, *additively* across a job's
  stages (`total_slots = 2*sources + level*swept_stages + 1*sink`), and
  capped two synthetic shapes' sweep levels accordingly.
- **Root cause of the wrong assumption:** none of this project's Flink
  jobs call `slotSharingGroup(...)`; with no override, Flink's default
  behavior packs **one subtask from every operator into the same slot**
  per parallel "slice," so total slot need is `max(per-stage parallelism)`,
  not the sum.
- **Fix:** confirmed live — a job submitted with `pds=2,2,2,8,1` (sum=15)
  measured (via `/overview`, `slots-available` dropping 96→88) at
  exactly 8 slots, matching `max(...)=8`. Both sweep scripts corrected to
  drop the per-shape cap entirely.
- **Validation:** this also explained why the real Ad Analytics sweep
  could run `10,40,40,40,40,40,1` on a 40-slot cluster at all — the
  additive model would have wrongly demanded ~211 slots.
- **Remaining risk:** the ~36 strategy-sweep runs collected under the
  wrong cap were archived to `data/archive/deprecated/`, not deleted —
  make sure any future analysis script excludes that directory.
- **Status:** 🟢 Resolved.
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §9 ("Correction, found
  live 2026-10-13").

## STORM-001 — Official Storm Docker image's entrypoint fails under Docker Desktop/macOS

- **Component:** `PStreamBench_local_docker/docker-compose.yml`
- **Observed behavior:** all 3 Storm containers died immediately (exit
  254) before Storm itself ever started.
- **Root cause:** the official `storm` image's `docker-entrypoint.sh`
  runs `chown -R storm:storm /conf` under `set -e`, which fails against
  a bind-mounted `storm.yaml` on Docker Desktop/macOS.
- **Fix:** `entrypoint: []` override on all 3 Storm services — a complete
  `storm.yaml` is supplied directly, so the image's auto-chown/auto-config
  step was never actually needed.
- **Validation:** confirmed by running `storm nimbus` directly without the
  entrypoint — starts cleanly.
- **Status:** 🟢 Resolved.
- **Source:** `PStreamBench_local_docker/README.md` Architecture §.

## STORM-002 — Topologies never actually self-terminate

- **Observed behavior:** a topology logs "Killing query" but stays
  `ACTIVE` in Nimbus forever.
- **Root cause:** `AbstractTopology.java`'s distributed-mode path logs the
  kill message but never calls an actual kill API.
- **Why it wasn't caught sooner:** production deployments mask this —
  the controller's own Ansible step kills topologies externally, and that
  path was never missing. It only surfaced when driving Storm directly
  (local Docker testing, and the synthetic-workload CloudLab cluster).
- **Fix:** the local Docker submit scripts and the synthetic-workload
  sweep script both call `storm kill` themselves explicitly afterward.
  The underlying `AbstractTopology.java` behavior itself was **not**
  changed (shared code; the controller's own external-kill path already
  works around it).
- **Remaining risk:** any *new* caller of these topologies that doesn't
  know to kill them externally will leak worker slots silently, with no
  error anywhere.
- **Status:** 🟡 Partially resolved (worked around everywhere it's
  currently called from; the underlying library behavior is unchanged).
- **Source:** `PStreamBench_local_docker/README.md`,
  `LOCAL_DOCKER_TODO.md`.

## STORM-003 — Worker-slot exhaustion from leaked topologies, silent failure mode

- **Observed behavior:** every job submission after the first silently
  sat pending forever — accepted by Nimbus, never actually scheduled, no
  error anywhere. `storm list` showed topologies running fine, but their
  worker log directories never appeared on the supervisor at all.
- **Root cause:** combination of STORM-002 (leaked `ACTIVE` topologies)
  and too few configured worker slots (2) for the number of concurrent
  submissions expected.
- **Fix:** `supervisor.slots.ports` bumped from 2 to 6 slots; explicit
  `storm kill` added after every submission (see STORM-002).
- **Status:** 🟢 Resolved (for this project's current usage pattern).
- **Source:** `PStreamBench_local_docker/storm_conf/storm.yaml` inline
  comment.

## STORM-004 — "No output" false alarm on a null-keyed, multi-partition topic

- **Observed behavior:** Ad Analytics' output appeared missing on Storm,
  initially read as a processing failure.
- **Root cause of the *appearance* of failure:** `Q1RollingCTRMapper`
  returns `null` as the Kafka message key, so output lands on an
  unpredictable one of the output topic's 4 partitions. The initial check
  only sampled 1–2 partitions — not a real bug, an insufficient search.
- **Investigation that found the real state:** Storm UI's own per-component
  stats (`/api/v1/topology/<id>`, `bolts[].executed`/`.acked`) showed the
  sink bolt genuinely receiving and acking tuples (54,230 at one check),
  before a full 4-partition search confirmed real output with genuine
  non-zero CTR values.
- **Lesson generalized:** check an engine's own live stats before
  concluding a pipeline is broken on a shared, null-keyed, multi-partition
  topic.
- **Status:** 🟢 Resolved (was never actually broken).
- **Source:** `PStreamBench_local_docker/README.md`,
  `LOCAL_DOCKER_TODO.md` §2.

## STORM-005 — Ubuntu's packaged ZooKeeper incompatible with Storm 2.6.2

- **Component:** CloudLab synthetic-workload cluster bring-up
- **Observed behavior:** Nimbus failed at startup with
  `KeeperErrorCode = Unimplemented`.
- **Root cause:** Ubuntu's `apt`-packaged ZooKeeper (3.4.13) lacks
  container-znode support Storm 2.6.2's Nimbus requires.
- **Fix:** replaced with a standalone ZooKeeper 3.9.6 install.
- **Status:** 🟢 Resolved.
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §4.

## STORM-006 — ZooKeeper AdminServer port collision with Storm UI

- **Observed behavior:** ZooKeeper startup failure.
- **Root cause:** ZooKeeper's built-in AdminServer defaults to port 8080
  — the same port Storm's UI uses.
- **Fix:** `admin.enableServer=false`.
- **Status:** 🟢 Resolved.
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §4.

## STORM-007 — Fixed worker count made higher parallelism perform *worse*

- **Observed behavior:** increasing configured parallelism made
  throughput go down, not up.
- **Root cause:** `numWorkers` was fixed regardless of parallelism level,
  packing dozens of executor threads into a handful of JVMs at high
  levels — thread oversubscription, not a real scaling result.
- **Fix:** scale worker count with total executor count, capped at the
  cluster's real slot ceiling (96 = 6 nodes × 16 cores).
- **Status:** 🟢 Resolved.
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §5.

## STORM-008 — Short measurement window under-measured throughput by ~40%

- **Observed behavior:** diagnostic probes using a ~100s window reported
  throughput ~40% below target, consistently, regardless of source
  parallelism (2/4/8).
- **Root cause:** a fixed topology warm-up/scheduling delay dominates a
  short window.
- **Fix/validation:** re-measuring the identical configuration over the
  real sweep's full 5-minute window recovered 96.2% of a 1M events/s
  target — confirming source parallelism was never actually the
  bottleneck; the window length was.
- **Status:** 🟢 Resolved.
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §5.

## STORM-009 — `storm_exporter.py` silent HTTP deadlock under label-cardinality growth

- **Observed behavior:** ~64 of the first 165 synthetic sweep runs
  collected empty or literal-zero metrics, no error raised.
- **Root cause:** the exporter's internal polling loop keeps running
  (visibly logging) while its `/metrics` HTTP endpoint silently deadlocks
  after accumulating enough unique `TopologyId` label values across
  repeated submissions — a failure mode the project's own
  `parallelism_strategies.py` had already documented
  (`ensure_exporter_healthy()`) but which hadn't been ported into this
  newer sweep automation.
- **Fix:** added an automatic exporter health-check-and-restart step
  before every submission.
- **Validation:** all previously-failed runs succeeded on retry once the
  fix was in place — confirmed as the sole cause (not resource
  exhaustion or a shape-specific bug), including the four
  highest-executor-count shapes, which had failed 100% of the time
  before the fix.
- **Status:** 🟢 Resolved.
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §5.

## STORM-010 — Self-matching `pkill -f` defeated the exporter auto-heal (STORM-009's own fix)

- **Observed behavior:** 67 of 273 runs collected empty/partial metrics
  even *after* STORM-009's fix was deployed, concentrated on the
  widest-fan-in shapes.
- **Root cause:** the auto-heal's SSH command string was
  `sudo pkill -9 -f storm_exporter ...; ... nohup ./storm_exporter.sh ...`
  — since that whole string is itself the remote shell's command-line
  text, and it contains the substring `storm_exporter`, `pkill -f` killed
  that shell before it could reach the relaunch. Every "restart
  succeeded" check after that point was silently false. This is the same
  self-matching failure mode as INFRA-ENV-002 (below), in a different
  script.
- **Fix:** moved the kill+relaunch into a pre-written remote script file
  invoked by a generic filename, so the invoking shell's own command-line
  text never contains the pattern being searched for.
- **Validation:** verified live — SSH session survives the restart, the
  exporter returns HTTP 200 — before resuming the sweep.
- **General lesson (recorded for reuse):** any `pkill -f`/`pgrep -f`
  issued as part of a single inline SSH command string is unsafe if that
  pattern also appears anywhere in the command string itself.
- **Status:** 🟢 Resolved.
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §5.

## STORM-011 — `getprom_storm.py` missing bolt/spout dispatch for 8 of 9 synthetic shapes

- **Observed behavior:** 8 of 9 synthetic shapes silently produced empty
  `operatorMetrics` — no error, just missing data.
- **Root cause:** only `LinearQuery`'s bolt/spout names were wired into
  the per-stage dispatch table.
- **Fix:** added dispatch entries for the remaining 8 shapes, verified
  against the live topologies' actual bolt/spout IDs.
- **Status:** 🟢 Resolved.
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §5.

## STORM-012 — `getBoltMetrics` crashes on a zero-result Prometheus query

- **Observed behavior:** a residual low rate of `collect_failed` runs
  persisted even after STORM-009/010 were fixed.
- **Root cause:** `getBoltMetrics` indexes into
  `data["data"]["result"][0]` unconditionally — when a metric genuinely
  has no data point in the queried window (e.g. right after an exporter
  restart, before its first scrape), this is an `IndexError`, not a
  NaN/missing value (a different case from the NaN-in-baseline issue
  `sanitize_stage_metrics()` already handles).
- **Fix:** not yet patched — this is shared code also used by all
  real-workload collection, so a fix here needs the same care as any
  other change to a widely-used collector.
- **Status:** 🔴 Unresolved. Caught by the sweep's own error handling
  (surfaces as `collect_failed`, doesn't escalate further), so it doesn't
  corrupt data — it just loses the occasional run to a retry.
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §5.

## INFRA-ENV-001 — Disk exhaustion from unrotated Storm worker logs crashed ZooKeeper, cascading to the whole cluster

- **Observed behavior:** after ~300 topology submissions, the master
  node's disk hit 100% full; ZooKeeper's transaction log writes then
  failed with `IOException: No space left on device`, which ZooKeeper
  treats as severe and shuts down on — taking Nimbus, and the whole
  cluster's scheduling ability, with it.
- **Root cause:** Storm's per-worker log directories
  (`logs/workers-artifacts/`) are not rotated or cleaned by default; grew
  to 50GB on the master alone. One other node (not just the master) was
  also found at 100%.
- **Fix:** cleared `workers-artifacts/` and truncated oversized daemon
  logs on all 6 nodes, restarted the full daemon stack in dependency
  order (ZooKeeper → Nimbus → UI → supervisors → exporter — supervisors
  needed an *explicit* restart since they don't reconnect to ZooKeeper on
  their own after losing it).
- **Collateral finding:** one topology (`TwoFilters`) was left running as
  a zombie for 1h43m because the sweep's own kill-and-wait step couldn't
  reach Nimbus during the outage window — found via its uptime far
  exceeding any run's 5-minute window, killed manually.
- **Validation / what *didn't* need fixing:** the sweep's existing
  resumability (JSONL manifest, retry anything not `status=='ok'`) meant
  no completed data was lost — the ~23 runs that failed to activate
  during the outage were automatically retried once the cluster was
  healthy, no code changes needed.
- **Remaining risk:** this is specifically a capacity-planning finding
  for long-running (multi-day) sessions submitting hundreds of
  topologies back to back — not fixed at the root (no log rotation was
  added), just manually cleared. A future multi-day sweep will hit this
  again unless log rotation is actually configured.
- **Status:** 🟡 Partially resolved (worked around, root cause — no log
  rotation — not fixed).
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §5.

## INFRA-ENV-002 — `archive.apache.org` extremely slow for pinned EOL engine versions

- **Date:** 2026-10-14
- **Observed behavior:** downloading Flink 1.16.2 (452MB) from
  `archive.apache.org` ran at ~20-30KB/s single-connection — a 4-6 hour
  download for one file.
- **Investigation:** confirmed the node's general internet bandwidth was
  fine (GitHub: ~1.1MB/s; a generic CDN: ~70MB/s) — the slowness was
  specific to `archive.apache.org`. Confirmed the current fast mirror
  network (`dlcdn.apache.org`) and the official "closer.lua" redirector
  both 404/redirect back to the same slow archive host for these
  specific EOL versions — there is no faster official source.
- **Root cause:** `archive.apache.org` is Apache's intentionally
  deprioritized long-term archive for EOL releases, not a CDN — this is
  expected behavior for that host, not a bug.
- **Fix:** download each needed large file **once** (accepting the slow
  rate), then fan out to every node that needs it via fast `scp`/LAN
  transfer, instead of re-downloading per node. Also split
  `install_all_tools.sh` into per-engine `install_<engine>_only.sh`
  scripts so a node only downloads the one engine it actually needs
  (Spark/Kafka Streams/RisingWave variants already existed; Flink/Storm
  variants were written to complete the set).
- **Validation:** running the 5 engine-leader downloads in parallel
  (instead of serially per node) completed all 15 nodes' provisioning in
  about an hour, vs. a naive serial estimate of many hours.
- **Status:** 🟢 Resolved (as a process — the underlying host speed is
  unchanged and out of this project's control).
- **Source:** this session's live 15-node CloudLab provisioning work;
  `WUI_IMPROVEMENTS_TODO.md` does not yet have this entry — add there too
  if a general infra doc is written.

## INFRA-ENV-003 — MinIO discontinued all free public binary/image distribution

- **Date:** 2026-10-14
- **Observed behavior:** RisingWave's multi-node cluster creation failed
  — MinIO (needed for Hummock's shared object store across physically
  separate compute nodes) never came up; the downloaded `minio` binary
  turned out to be a 405-byte HTML error page, not the real binary.
- **Investigation:** `dl.min.io/server/minio/release/linux-amd64/minio`
  returns `410 Gone` (confirmed for the base path, `/archive/...`, and
  `.deb` paths alike); `github.com/minio/minio`'s latest release has
  **zero** binary assets; Docker Hub's `minio/minio` returns "object not
  found"; the commonly-used alternative `bitnami/minio` also returns "not
  found". The `mc` **client** (a separate repo) still publishes real
  binaries via GitHub releases — only the **server** distribution is
  gone.
- **Root cause:** an upstream MinIO business/distribution policy change,
  not a transient outage or a CloudLab-specific restriction.
- **Fix:** **not yet implemented.** Options identified: (a) build MinIO
  from source via Go (source is still public), (b) accept single-node
  RisingWave only (embedded storage, no Hummock sharing) until resolved.
  Explicitly deprioritized by project decision ("not so important" right
  now).
- **Status:** 🔴 Unresolved, deliberately deprioritized.
- **Source:** `WUI_IMPROVEMENTS_TODO.md` item 9, `LOCAL_DOCKER_TODO.md`.

## INFRA-ENV-004 — Fresh CloudLab allocation missing base tooling (`/home/playground`, Java) and unseen SSH host keys

- **Date:** 2026-10-16.
- **Observed behavior:** 4 of 6 newly-allocated nodes failed the first SSH
  connection attempt with host-key verification errors; separately,
  `/home/playground` (this project's standard working directory) did not
  exist on any of the 6 fresh nodes, and an initial `chown` attempt
  failed because the expected owning group (`pagnih03`) doesn't exist —
  the real CloudLab group is the project name (`maki-test-PG0`), not the
  username.
- **Root cause:** a genuinely fresh CloudLab allocation (a different
  base image/experiment than the one the earlier Flink/Storm work ran
  on), so none of that earlier allocation's one-time setup carried over.
  The SSH host-key failures were first-contact, not a stale/conflicting
  key — `ssh-keygen -R` found nothing cached for any of the 4 affected
  hosts, confirming it was never seen before rather than a changed/
  malicious key.
- **Fix:** `-o StrictHostKeyChecking=accept-new -o BatchMode=no` for the
  first connection to each node; `sudo mkdir -p /home/playground &&
  sudo chown pagnih03 /home/playground` (owner only, no `:group` clause).
- **Status:** 🟢 Resolved.
- **Source:** this session's own setup of the 6-node Spark/CloudLab
  cluster used for the synthetic-shapes distributed sweep.

## INFRA-ENV-005 — `node_exporter` hung on half the fresh nodes (filesystem collector stuck in uninterruptible sleep)

- **Date:** 2026-10-16.
- **Observed behavior:** 3 of 6 nodes' `node_exporter` processes stopped
  responding to scrapes entirely — not crashed, just hung, confirmed via
  `ps` showing the process in state `D` (uninterruptible sleep, normally
  I/O-wait).
- **Root cause:** the default collector set includes a filesystem-stat
  collector that blocks on a problematic mount specific to this CloudLab
  node image — not root-caused further, since the fix below made the
  actual block moot rather than diagnosing the underlying mount issue.
- **Fix:** restarted `node_exporter` on the affected nodes with
  `--collector.disable-defaults --collector.cpu --collector.meminfo
  --collector.netdev --collector.loadavg` — narrows to exactly the 4
  collectors this project's Tier-1 hardware metrics actually use,
  sidestepping the blocking one entirely.
- **Status:** 🟡 Partially resolved (worked around; the specific
  blocking mount/collector was never root-caused).
- **Source:** this session's own setup of the 6-node Spark/CloudLab
  cluster.

## INFRA-ENV-006 — Cryptocurrency-mining malware found running on a previously-allocated CloudLab node

- **Date:** 2026-10-16.
- **Observed behavior:** on `ms1001.utah.cloudlab.us` (a node from an
  earlier CloudLab allocation, not the fresh one eventually used for the
  Spark sweep), `km`/`kv` processes were found connecting to
  `auto.c3pool.org`/`mine.c3pool.com` with an embedded wallet address,
  running continuously since well before this session started (process
  uptime predated this project's own use of the node).
- **Root cause:** not investigated — outside this project's scope, and
  deliberately not probed further given the security sensitivity without
  the user's explicit direction.
- **Fix:** **none applied by this session.** Reported to the user
  verbatim with full detail (process names, C2 domains, wallet address,
  uptime) and explicitly took no remediation action — no process
  killing, no cleanup — pending the user's decision. The user's response
  was to provide 6 entirely new CloudLab nodes rather than ask for
  remediation of the compromised one.
- **Status:** 🔴 Unresolved by design (deliberately left for the user to
  decide, not a dropped task). The compromised node was not reused.
- **Source:** this session's own CloudLab node audit, reported directly
  to the user in chat; not written up anywhere else until now.

## CTRL-001 — `_configure_and_run_playbooks()` discards the Ansible exit code

- **Component:** `infra/views.py`
- **Observed behavior:** a RisingWave cluster-create run failed partway
  through (MinIO never came up — see INFRA-ENV-003's predecessor
  symptom), yet the API still returned `{"success": true}` and a
  `Cluster` row was created in the database; the WUI only ever showed
  "Stopped", with no indication anything had gone wrong.
- **Root cause:** `_configure_and_run_playbooks()` calls
  `playbook_executor.run()` and never inspects its return value. A failed
  Ansible play does not raise a Python exception, so execution falls
  through to `Cluster.objects.create()` regardless of what failed
  upstream.
- **Fix:** not yet implemented in the controller itself — see
  `docs/11_design_decisions.md` and `WUI_IMPROVEMENTS_TODO.md` item 1 for
  the proposed fix (capture the exit code, persist per-step status).
- **Status:** 🔴 Unresolved (documented, not yet fixed).
- **Source:** this session's live RisingWave cluster-creation test;
  `WUI_IMPROVEMENTS_TODO.md` item 1.

## WUI-001 — Port 8000 hardcoded independently in every component

- **Component:** `PStreamBench_wui/dsp_fe/src/**/*.vue`
- **Observed behavior:** a real local port-8000 collision (an unrelated
  process also wanting port 8000) had to be resolved by killing the other
  process, twice in one session, because there was no single place to
  point the WUI at a different port instead.
- **Root cause:** every component calls
  `axios.post(this.url + ":8000/...")` directly; there is no shared
  API-client module or configurable port.
- **Fix:** not yet implemented — recommended fix is a single configurable
  base-URL-including-port value.
- **Status:** 🔴 Unresolved.
- **Source:** `WUI_IMPROVEMENTS_TODO.md` item 6.

## WUI-002 — `ds2`/`dhalion` not available from the Submit Job form

- **Observed behavior:** the WUI's enumeration-strategy dropdown
  (`Random`/`MinMax`/`Exhaustive`/`Rulebased`/`Custom`/`None`) has no
  `ds2`/`dhalion` option, unlike the command-line sweep scripts.
- **Root cause:** two separate, disconnected implementations exist —
  `infra/runner.py`'s own enumeration logic (what the WUI form drives),
  and `parallelism_strategies.py`'s `STRATEGIES` dict (what
  `run_strategy_sweep_*.py` uses). The sweep scripts already submit
  through the *same* `/infra/jobcreate/<cluster_id>` endpoint the WUI
  uses, via its existing `Custom` mode — `ds2`/`dhalion` were just never
  exposed as a WUI-triggerable **background loop** the way the sweep
  scripts run them.
- **Fix:** not yet implemented — proposed design: factor the sweep
  scripts' submit→wait→collect→compute-next→resubmit loop into a
  background task the controller can run, triggered from a WUI action.
- **Status:** 🔴 Unresolved (design agreed, not built).
- **Source:** `WUI_IMPROVEMENTS_TODO.md` item 11 (as corrected after
  user clarification in the same conversation that proposed it).

## SYNTH-001 — Chained-filter thresholds were no-ops

- **Component:** synthetic workload design (`ChainedFiltersTopology`)
- **Observed behavior:** every filter passed ~100% of tuples
  (measured selectivity 1.000 across all filters) — the filters were
  structurally present but had no actual effect.
- **Root cause:** initial thresholds (0.9, 0.8, 0.7, 0.6) sat entirely
  above the natural distribution of window-averaged values for uniform
  `[0,1)` input, which clusters near 0.5.
- **Fix:** corrected to (0.52, 0.50, 0.48, 0.46), yielding real,
  decreasing selectivities (~0.77 → 0.65 → 0.47 → 0.34 for the 4-filter
  case).
- **Status:** 🟢 Resolved.
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §2.

## SYNTH-002 — Join key-space formula under-shot narrow joins

- **Observed behavior:** 2-way and 3-way joins produced far fewer than
  the targeted ~1000 matches per window.
- **Root cause:** the calibration formula used the **pre-filter**
  per-source tuple count, not the post-filter count — under-shooting by
  exactly the factor the pre-filter step removes.
- **Fix:** corrected to use the post-filter count (`n_w`).
- **Validation:** all five join widths (2–6-way) landed within ~±20% of
  the 1000-matches-per-window target once corrected.
- **Status:** 🟢 Resolved.
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §2.

## SYNTH-003 — `dhalion_strategy` clamps every stage to a minimum of 2, even with no backpressure

- **Observed behavior:** a true "no-change" baseline isn't representable
  as-is when using `dhalion_strategy`.
- **Root cause:** the strategy function clamps every stage's parallelism
  to a floor of 2 unconditionally.
- **Fix:** none — flagged, not changed, since it's shared code also used
  by real-workload sweeps; changing it would affect results already
  collected.
- **Status:** ⚪ Accepted limitation (known, deliberately not fixed).
- **Source:** `SYNTHETIC_WORKLOAD_CONTRIBUTIONS.md` §10.

## SPARK-001 — Streaming checkpoint `FileNotFoundException` on a real multi-node cluster

- **Date:** 2026-10-16.
- **Observed behavior:** every synthetic job failed on its first
  checkpoint commit with `FileNotFoundException` on the checkpoint's
  `commits` subdirectory, only once submitted to the real 6-node
  Standalone cluster — never seen in earlier single-node/local testing.
- **Root cause:** the checkpoint root was a plain local filesystem path;
  on a real cluster the driver and each executor can run on different
  physical machines, so an executor couldn't see a path that only
  existed under the driver node's own local disk.
- **Fix:** moved the checkpoint root to the project's existing
  NFS-shared mount (`/proj/<project>/spark-checkpoints`, visible
  identically from every node), pre-created the per-app checkpoint
  subdirectory before submission, and added a `Thread.sleep(3000)`
  afterward — even on the shared mount, a brand-new directory wasn't
  always immediately visible to a different node's read, an NFS
  attribute-cache propagation delay.
- **Status:** 🟢 Resolved.
- **Source:** this session's distributed Spark synthetic-sweep setup;
  `ConfigArgs.java`'s `checkpointRoot` option,
  `SyntheticTopology.java`'s `runAndSelfCancel()`.

## SPARK-002 — Spark Standalone's REST API not reachable from outside the CloudLab allocation

- **Date:** 2026-10-16.
- **Observed behavior:** every `curl` to the master's REST API (port
  8087) from outside CloudLab timed out with no response at all; every
  poll loop built on it reported jobs as "failed to activate" for jobs
  that, confirmed via direct SSH, were actually running correctly.
- **Root cause:** two compounding issues — port 8087 isn't opened
  externally on this CloudLab allocation at all (unlike an earlier
  allocation's Flink REST port, which was), and separately the Master
  WebUI process binds to the master's specific internal IP
  (`10.10.1.1`), not `127.0.0.1`/`localhost`.
- **Fix:** routed every REST call through SSH (`ssh master "curl -s
  http://10.10.1.1:8087/json/"`) instead of a direct HTTP request from
  this machine — sidesteps the external firewall entirely and doesn't
  depend on guessing which ports a given CloudLab allocation happens to
  expose.
- **Status:** 🟢 Resolved.
- **Source:** this session's distributed Spark synthetic-sweep setup.

## SPARK-003 — Spark Master's embedded Jetty web server crashed under repeated REST polling

- **Date:** 2026-10-16.
- **Observed behavior:** partway through the sweep, the Master
  UI/REST endpoint stopped responding entirely, while the underlying job
  scheduler kept working fine (jobs continued to run and self-cancel
  normally).
- **Root cause:** not root-caused in depth — the embedded Jetty server
  serving the Master UI/REST API died under the sustained polling load
  from the sweep's own `wait_active()`/`wait_terminated()` repeated
  `/json/` calls across hundreds of combos.
- **Fix:** `stop-master.sh` + `start-master.sh` to restart just the
  Master process (workers/running jobs unaffected); increased the poll
  loops' own timeout margins afterward as a precaution.
- **Status:** 🟡 Partially resolved (worked around via restart; the
  Jetty crash itself wasn't root-caused).
- **Source:** this session's distributed Spark synthetic-sweep setup.

## SPARK-004 — `getSparkStreamingMetrics()`/`_get_operator_rows()` crash (`KeyError: 'data'`) on an error-response Prometheus query

- **Date:** 2026-10-16.
- **Observed behavior:** the very first distributed synthetic sweep run
  crashed `collect()` on every combo past the first one, losing the
  entire `evaluationMetrics` payload (including `operatorMetrics`) for
  every subsequent combo, not just the one metric that actually had no
  data.
- **Root cause:** a Prometheus query against a metric-name regex that
  genuinely matches zero series (or, as SPARK-005 below later showed, an
  *ambiguous* match) comes back as an error response with no `"data"`
  key at all, not an empty `"result"` list — the code assumed the
  latter and indexed into `data['data']['result']` unconditionally.
- **Fix:** wrapped every such query in `try/except (KeyError,
  IndexError)`; added `_safe_result()`/`_safe_scalar()` helpers and
  applied them throughout `getHardwareMetrics()` and
  `_get_operator_rows()` as well, not just the one call site that
  crashed first.
- **Status:** 🟢 Resolved (though see SPARK-005 — this guard correctly
  prevents a *crash*, but silently returns `None` for the underlying
  data gap, which SPARK-005's root cause could still trigger until that
  was separately fixed).
- **Source:** this session's distributed Spark synthetic-sweep setup;
  `getprom_spark.py`.

## SPARK-005 — Prometheus query ambiguity (`"vector cannot contain metrics with the same labelset"`) when multiple job runs' metrics overlap in the lookback window

- **Date:** 2026-10-17.
- **Observed behavior:** during the ds2/dhalion strategy sweep
  specifically (far less often during the earlier uniform sweep, which
  only hit 5 stale `collect_failed` entries out of 167),
  `throughput_per_sec`/`busy_ratio`/`backpressure_ratio` came back
  `None` for every stage on 2 baseline runs in a row, each retried 3
  times with the same result.
- **Root cause:** every metric query in `getprom_spark.py` matched
  `__name__` against an **unscoped** `"metrics_app_.+_driver_..."`
  regex — i.e. *any* Spark app's metric series still within
  Prometheus's lookback window, not just the current job's. Confirmed
  live by issuing the exact same subquery
  (`avg_over_time({__name__=~"..."}[5m:1s] @ <ts>)`) by hand against the
  live Prometheus instance: it returns a real value when exactly one
  app-id series is in range, and a hard **query error**
  (`"vector cannot contain metrics with the same labelset"`) when two or
  more are — which SPARK-004's own `except KeyError` guard then silently
  swallows as `None`. The strategy sweep's own baseline-retry loop made
  this *worse*, not better: each failed attempt resubmits almost
  immediately, adding yet another candidate app-id series to the same
  5-minute window instead of letting it clear.
- **Fix:** added `_job_id_pattern_fragment(job_id)`, which reproduces
  Spark's Dropwizard metric-name sanitization (every non-alphanumeric
  character in the app id becomes `_`) to scope the regex to exactly
  one job run. Threaded `job_id` through `getSparkStreamingMetrics()`,
  `_get_operator_rows()`, `getOperatorMetrics()`,
  `getSyntheticOperatorMetrics()`, `getSyntheticCombo()`, and the 3
  synthetic-shape dispatch functions — deliberately **only** for the
  synthetic-shapes call path (`job_id=None` stays the default
  everywhere, preserving the old unscoped behavior for all 14
  real-workload collectors, since this bug was only confirmed live
  against the synthetic sweep's much tighter back-to-back submission
  cadence, and touching the real-workload path risks already-collected,
  already-analyzed data for a bug not confirmed to affect it).
- **Validation:** re-issued the same raw PromQL query scoped to the
  currently-running job's id immediately after the fix — clean result,
  no ambiguity error; restarted the running strategy-sweep process so it
  picked up the fix, and the very next retry (a combo that had just
  failed 3/3 times) succeeded immediately. The remainder of the
  ds2/dhalion sweep (81/81) and the subsequent random sweep (270/270)
  both completed with zero further `null_throughput` occurrences.
- **Status:** 🟢 Resolved (for the synthetic-shapes path only — see Fix
  for why the real-workload path is deliberately left unscoped).
- **Source:** this session's Spark synthetic-shapes ds2/dhalion/random
  strategy sweep; `getprom_spark.py`.

---

## Not yet logged here (flagged, not lost)

The following are confirmed real findings from project history that have
not yet been reformatted into the structured entries above — anyone
continuing this log should pull them in:

- `SPARK_VALIDATION.md` — two real bugs found and fixed during Spark's
  live-validation pass (a `flatMapGroupsWithState` bean-encoding issue,
  a Gson/Guava classpath conflict with Spark's own bundled versions) —
  see the top-level `README.md`'s Spark status note for the one-line
  summary; the full file has the investigation detail. These predate
  SPARK-001 through SPARK-005 above (which cover the later distributed
  synthetic-sweep setup, not this earlier local-validation pass) — would
  become `SPARK-006`/`SPARK-007` if backfilled, not a re-used number.
- `LOCAL_DOCKER_TODO.md`'s Spark micro-batch-stall finding — genuinely
  unresolved (not a quick log entry; it already has ruled-out hypotheses
  that deserve their own `SPARK-NNN` entry, next available `SPARK-008`,
  once that investigation resumes).
- Everything in `KAFKA_STREAMS_RISINGWAVE_PLAN.md` and
  `MULTI_ENGINE_LOCAL_TESTING_AND_BENCHMARK_SWEEP.md` not yet reviewed for
  this log.
