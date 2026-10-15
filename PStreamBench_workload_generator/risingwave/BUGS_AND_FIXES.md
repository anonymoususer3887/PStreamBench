# RisingWave Integration: Bugs Found and Fixed (2026-07-22 – 2026-07-27)

This documents every real bug found while getting all 14 PStreamBench workloads
(24 query variants) running correctly on RisingWave, how each was actually
diagnosed (not guessed), and the fix applied. Ordered roughly by discovery
sequence.

---

## 1. `pkill -f` self-matching in stop playbooks

**Symptom**: `DROP SOURCE`/stop-cluster Ansible tasks reported `rc: -15` and
failed, even though the target process was still running afterward.

**How found**: Read the Ansible task output line by line. `rc: -15` means the
*invoked command itself* was killed by SIGTERM, not that it exited normally.
Checked what process the invoking shell's own command line looked like —
realized `pkill -f 'risingwave frontend'` matches against the **full command
line of every process**, including the wrapping `sh -c "pkill -f 'risingwave
frontend' || true"` invocation itself, since that literal string appears in
its own argv.

**Fix**: Standard `ps`/`pkill` self-exclusion trick — bracket the first
character of the pattern: `pkill -f '[r]isingwave frontend'`. The bracketed
form still matches the real process (regex `[r]` = literal `r`) but does not
appear as that literal substring in the invoking command's own argv.

**Files**: `utils/playbook/tasks/risingwave/complex/risingwave-{meta,frontend,compute,minio}-stop.yaml`

---

## 2. Stale MinIO `cluster_id` causing meta to panic on restart

**Symptom**: After a cluster restart, meta crashed immediately with:
```
thread 'main' panicked ... Data directory is already used by another
cluster with id "...", path hummock_001/cluster_id/0.
```

**How found**: Added persistent log redirection to the RisingWave start
playbooks (previously only captured by Ansible's ephemeral async-job
mechanism, which discards output once reaped) specifically so this kind of
crash would leave a trace. The very next restart reproduced it with a full
Rust panic and stack trace pointing at the exact object store path.

**Root cause**: The `mc rm --recursive --force` bucket-clear step (needed
because meta's `mem` backend generates a fresh `cluster_id` on every start,
which then conflicts with whatever `cluster_id` object is still sitting in
the shared MinIO bucket from the previous run) had failed with a transient
`Timeout (2s) waiting for privilege escalation prompt` and, since it's
`ignore_errors: true`, Ansible silently continued past it.

**Fix**: No code fix needed once diagnosed — manually re-ran `mc rm
--recursive --force local/hummock001` when the sudo hiccup was transient,
and added the log redirection so this failure mode is now visible instead
of silent.

---

## 3. `proctime()` cannot be called inline in a SELECT/CTE — project-wide

**Symptom**: Every windowed workload's `CREATE MATERIALIZED VIEW` failed
with:
```
Invalid input syntax: Function `PROCTIME()` is only allowed in
CREATE TABLE/SOURCE. Is `NOW()` what you want?
```

**How found**: This was the single most important discovery in the whole
session. Traced a "materialized view never gets created" failure (source
resolved fine in metrics, MV/sink never resolved) all the way down with a
minimal reproduction: `SELECT proctime() AS pt FROM some_source LIMIT 1`
directly via `psql`, isolated with zero other query complexity. That alone
already errored, proving the issue had nothing to do with HOP, aggregates,
or joins — it was `proctime()` itself.

**Fix**: Declare `pt` as a **generated column directly on `CREATE SOURCE`**
(`pt TIMESTAMP AS proctime()`), then reference the `pt` column by name in
every downstream CTE instead of calling the function. Confirmed working via
a from-scratch minimal `CREATE SOURCE ... (line BYTEA, pt TIMESTAMP AS
proctime()) ... ; SELECT pt FROM source;` test before rolling out to all 18
affected files.

**Files**: All 18 windowed `.sql` files (Smart Grid ×2, Log Processing ×2,
Trending Topics, Traffic Monitoring, Spike Detection, Machine Outlier, TPCH
Q2, Ad Analytics ×2, Google Cloud Monitoring ×2, Click Analytics Q3, Linear
Road ×4, Bargain Index).

---

## 4. `HOP()` rejects `INTERVAL 'N' MILLISECOND` in its argument list

**Symptom**: `sql parser error: expected ), found: MILLISECOND` — but only
when the same expression was used as a `HOP()` argument. As a bare `SELECT
INTERVAL '5000' MILLISECOND`, it parsed *without error* but silently
computed the wrong value (5000 **seconds**, not milliseconds).

**How found**: Bisected directly against the live cluster: ran the exact
same interval expression as (a) a bare `SELECT`, (b) inside a computed
expression `(n/1000)*INTERVAL '1 second'`, and (c) as a literal `INTERVAL '5
second'` (unit inside the quoted string). Only (c) worked as a `HOP()`
argument.

**Fix**: Convert millisecond window/slide values to **whole seconds** in
Python before substitution (`_risingwave_job_manipulation()` in `views.py`),
and use the literal form `INTERVAL '<seconds> second'` in every SQL file.
Sub-second window precision isn't achievable this way, documented as an
accepted trade-off since all real test windows are seconds-to-minutes.

**Files**: `infra/views.py` (`_risingwave_job_manipulation`), same 18 `.sql`
files as bug #3.

---

## 5. Kill-job DROP serialization blocked *every subsequent job's creation*

**Symptom**: After the first 1-2 test combos in a batch, every remaining
combo failed with "no metrics collected" — not a SQL error, just silence.

**How found**: Found a `psql` process that had been hung for 10+ minutes
submitting **Ad Analytics Q1** — an unbounded, non-windowed query that
should complete in under a second (same shape as Word Count, which had
already been proven fast). That ruled out "windowing is broken" as the
explanation. Checked meta's own startup config log and found
`max_concurrent_creating_streaming_jobs: 1` — RisingWave allows only one
streaming job to be *created* cluster-wide at a time. Then found a leftover
`smart_grid_source` with no matching materialized view — evidence an
earlier job's teardown had partially failed and left that single
job-creation slot permanently jammed.

**Root cause**: The teardown step ran `DROP SINK`, `DROP MATERIALIZED
VIEW`, and `DROP SOURCE` as **one combined `psql -c "...; ...; ...;"`
call**. If `DROP SOURCE` hung (observed separately, waiting on Kafka
consumer-group teardown), the whole combined statement blocked — so
`DROP SINK`/`DROP MATERIALIZED VIEW` (the ones that actually free the
job-creation slot) never even ran.

**Fix**: Split into three independent Ansible tasks, each its own `psql`
invocation with its own timeout, so a hung `DROP SOURCE` no longer prevents
the other two from completing.

**Files**: `utils/playbook/tasks/risingwave/risingwave-kill-job.yaml`

---

## 6. Wrong barrier-latency metric name

**Symptom**: `actorMetrics.barrier_inflight_latency_avg/max` were `null` in
an otherwise-successful Word Count run.

**How found**: User supplied a reference table of RisingWave's actual
Prometheus metric names, listing `meta_barrier_duration_seconds` for
"processing/checkpoint latency" — different from what the code used
(`stream_barrier_inflight_duration_seconds`). Queried Prometheus's
`/api/v1/label/__name__/values` directly and confirmed **both** metric
names genuinely exist, but measure different things (per-actor compute-side
vs. meta's own end-to-end barrier duration). The compute-side one only has
samples when a barrier is measurably delayed in-flight; the meta-side one
is recorded every barrier tick (~1s), so it's populated far more reliably.

**Fix**: Switched the query to `meta_barrier_duration_seconds`. Confirmed
live afterward (`0.099` / `0.099`, real non-null values on a successful
run).

**Files**: `infra/reporting_scripts/getprom_risingwave.py` (`getActorMetrics`)

---

## 7. Ad Analytics Q2: tab vs. space delimiter mismatch

**Symptom**: Would have produced zero output rows once run against real
data (not directly observed failing — caught by code review against the
actual producer before running).

**How found**: Before running anything live, cross-checked every SQL file's
delimiter against `custom_producer.py`'s actual serialization
(`to_csv(sep=...)`). Found the producer always writes Ad Analytics data
**tab-separated**, regardless of query number — but my Q2 SQL split on a
literal space, following the Java reference's `.split("\\s+")` (which
matches tabs too, since `\s` is "any whitespace" in Java regex — but
RisingWave's `split_part()` takes a literal delimiter string, not a regex).

**Fix**: Changed the delimiter to an actual tab character (`E'\t'`),
matching the real wire format while keeping the field order Q2 actually
uses.

**Files**: `AdsAnalytics/ads_analytics_q2.sql`

---

## 8. Spike Detection: field-offset / data-source resolution

**Symptom**: Ambiguous at first — two different candidate data sources
implied two different, mutually exclusive field layouts.

**How found / resolved**: The bundled repo copy
(`flink/dsp_flink_jobs/Data/spikeDetection_sensors.dat`) and the raw
`PStreamBench_controller/data.txt` both have **8** whitespace-separated
columns (date, time, an epoch/reading-sequence counter, sensorId,
temperature, humidity, light, voltage) — one more than the reference Java
parser (`com.kom.dsp.SpikeDetection.SensorParser.java`) reads, which looks
like a genuine off-by-one bug in that Java file. Separately, the actual
Kaggle dataset (`shubhamsumalya/spikedetection`, what the shared
`download_datasets.sh` fetches) turned out to have exactly **7** columns —
matching the Java parser's assumption with no bug after all. Per explicit,
repeated user direction, `data.txt` (the 8-column raw dataset) was
designated the authoritative source for this workload regardless — so the
translation reads the *corrected* column positions (skipping the epoch
counter) rather than reproducing the Java off-by-one.

**Fix**: `sensorId`/`temperature`/`humidity`/`light`/`voltage` read from
fields 4/5/6/7/8 (not 3/4/5/6/7), with a malformed-row filter for
`array_length = 8`. `setup_local.sh` always (re)derives
`spikeDetection_sensors.dat` from `data.txt`, overwriting whatever the
Kaggle download step produces for it.

**Files**: `SpikeDetection/spike_detection.sql`,
`PStreamBench_Cloud_setup/setup_local.sh`

---

## 9. Bargain Index: CTE name collided with its own column name

**Symptom**: `bargain_index_mv` reliably returned **zero rows**, with no
error at all, despite the source receiving real data (98,000 rows counted
directly).

**How found**: Bisected the query by creating throwaway materialized views
for each stage: bare `HOP()` output (504,000 rows — fine), the `GROUP BY`
aggregation alone (real rows — fine), then the *exact* final query but with
the CTE renamed from `vwap` to `vwap_calc` — which immediately produced
real output. The only difference between the working test and the broken
real file was that the real file's CTE was named `vwap` and **also**
contained a column named `vwap` (`SUM(close*volume)/SUM(volume) AS vwap`),
referenced as `quote_price / vwap` in the outer query.

**Fix**: Renamed the CTE to `vwap_calc` (column name `vwap` unchanged).
Searched every other `.sql` file for the same CTE-name-equals-own-column
pattern — found no other occurrences.

**Files**: `BargainIndex/bargain_index.sql`

---

## 10. `round(double precision, integer)` doesn't exist

**Symptom**: `Failed to bind expression ... function round_digit(double
precision, integer) does not exist`.

**How found**: Direct `psql` error message on first submission of Traffic
Monitoring — this is the same behavior as standard PostgreSQL, where the
two-argument form of `round()` is only defined for `numeric`, not `double
precision` (only the one-argument form exists for `double precision`).

**Fix**: Cast to `numeric` before rounding, then back to `double precision`
to keep the column type consistent with the other numeric columns used in
the same `AVG()` aggregate.

**Files**: `TrafficMonitoring/traffic_monitoring.sql`

---

## 11. `position` is a reserved keyword

**Symptom**: `sql parser error` pointing directly at a bare column
reference named `position` in Linear Road Q2.

**How found**: Direct parser error on submission; `position` is used in
RisingWave's (and standard SQL's) `POSITION(x IN y)` function syntax, so it
can't be used as an unquoted identifier.

**Fix**: Renamed the column to `veh_position` throughout the file (comments
describing the concept still say "position" descriptively).

**Files**: `LinearRoad/linear_road_q2.sql`

---

## 12. Source-schema mismatches between query variants sharing one object name

**Symptom**: Running Q2 after Q1 (or vice versa) without a full teardown in
between failed with `Item not found: Invalid column: pt` or similar.

**How found**: Hit directly while testing TPCH Q2 after TPCH Q1 in the same
session — Q1's source doesn't declare `pt` (no windowing needed), Q2's does
(HOP window). Since both reuse the object name `t_p_c_h_source` and `CREATE
SOURCE IF NOT EXISTS` is a no-op when the (wrong-schema) object already
exists, whichever query ran second broke. Checked for the same pattern
elsewhere and found it in Ad Analytics (Q1 vs Q2) and Linear Road (Q4 vs
Q1/Q2/Q3).

**Fix**: Added the (unused-but-harmless) `pt TIMESTAMP AS proctime()`
column to TPCH Q1, Ad Analytics Q1, and Linear Road Q4's source
declarations, so every query variant of a given workload declares an
identical source schema regardless of which one happens to create the
object first. (In real production usage via the controller, each job's
objects are fully torn down before the next one starts, so this specific
collision doesn't occur there — but it's a real latent footgun for manual
testing/debugging, and a one-line fix, so worth closing anyway.)

**Files**: `TPCH/tpch.sql`, `AdsAnalytics/ads_analytics.sql`,
`LinearRoad/linear_road_q4.sql`

---

## 13. `operatorMetrics.<stage>.selectivity` left `null` when the upstream stage's rate was `0`

**Symptom**: In an end-to-end metrics-collection validation pass (running the
real `getprom_risingwave.py` CLI, not just checking SQL correctness) across
all 24 query variants, `operatorMetrics.*.selectivity` was `null` in several
otherwise fully-populated runs.

**How found**: Built a validation script that runs the actual metrics CLI
(matching `runner.py`'s real subprocess invocation) for every combo and
recursively scans the written JSON for `null`/`NaN`. Reading
`getOperatorMetrics()`'s selectivity calculation, found it computed
`current / previous_rate` per stage but left `selectivity = None` with no
fallback whenever `previous_rate == 0` (upstream stage had zero throughput
in that window — most commonly right after job startup, before the first
row has propagated to the downstream stage).

**Fix**: Added an explicit `previous_rate == 0` branch that reports `0.0`
instead of leaving the field `null`, since a genuinely zero upstream rate
means zero downstream selectivity is the correct answer, not "unknown".

**Files**: `infra/reporting_scripts/getprom_risingwave.py` (`getOperatorMetrics`)

---

## 14. (Not a real bug) `operatorMetrics` fields `null` under a short warmup — validation-script artifact

**Symptom**: After fixing bug #13, re-running the full 24-combo validation
still showed `operatorMetrics.<stage>.throughput_per_sec` /
`.selectivity` as `null` for 7 of the 24 combos (Log Processing ×2,
Trending Topics, Click Analytics ×3, Bargain Index) — but *not* the other
17, and not consistently (Smart Grid uses the same 10s/5s window
config as the failing combos and never showed it).

**How found**: The inconsistency (same window size, same query shape,
different outcome) ruled out a SQL/config bug. Re-read
`_get_fragment_rate()`: it computes `rate(...)[1m]` at a query timestamp of
`time.time() - 30`, relative to when the validation script's producer had
started only ~45 seconds earlier — leaving as little as ~15s of real data
for Prometheus's `rate()` to have the 2 samples it needs per series.
Checked `runner.py`'s actual production call path and found real jobs
`time.sleep(job_run_time * 60)` (**minutes**, not 45 seconds) between job
submission and invoking this same metrics CLI — far more warmup than the
validation script gave. Re-ran just the 7 affected combos with the
producer warmup increased from 45s to 120s: all 7 came back with zero
`operatorMetrics` nulls (down from 2 null fields each), with no code
changes at all.

**Conclusion**: Not a bug in `getOperatorMetrics()` or the SQL — a
too-short warmup in the *local validation script only*. Production's
multi-minute `job_run_time` sleep gives every stage's Prometheus series
far more than enough samples by the time metrics are collected. No fix
applied to `getprom_risingwave.py`; noted here so a future short-warmup
local test isn't mistaken for a regression.

**Files**: none (validation-script-only artifact, not reproduced in the
production code path)

---

## Portability and metrics-coverage improvements (beyond bug-fixing)

These two changes aren't bug fixes to existing production behavior -- they
extend `getprom_risingwave.py` so it also works correctly against local dev
hardware, and so it collects more of what RisingWave already exposes.

### A. `hardwareMetrics` network/memory hardcoded to CloudLab-only metric names

**Symptom**: `node_network_out`, `experiment_network_out`, and `node_memory`
were unconditionally `null` in every local run, while `node_cpu` was always
populated.

**Root cause**: `node_cpu`'s query (`node_cpu_seconds_total{mode="idle"}`)
happens to use a metric name that's identical across node_exporter's Linux
and Darwin (macOS) collectors. The other three don't:
`node_network_transmit_bytes_total{device=~"eno1|eno33np0"}` /
`{device=~"eno2|eno1d1|enp65s0f0np0"}` hardcode actual CloudLab NIC device
names (a Mac's interface is `en0` -- no match, empty result, `null`), and
`node_memory_MemFree_bytes`/`_Cached_bytes`/`_Buffers_bytes`/`_MemTotal_bytes`
are Linux-procfs-specific metric names that don't exist at all on Darwin's
node_exporter build (which exposes `node_memory_free_bytes`/
`_active_bytes`/`_wired_bytes`/`_total_bytes` instead).

**Fix**: Added `_resolve_network_devices()` and `_resolve_memory_formula()`,
called once per `getHardwareMetrics()` invocation. Each first probes
Prometheus for the known CloudLab-specific series; if present (real
CloudLab run), behavior is unchanged. If absent (local dev, or any future
hardware profile not in the hardcoded list), network devices fall back to
`_discover_primary_network_device()` -- `topk(1, ...)` over all non-virtual
interfaces (confirmed the virtual-device blocklist against this Mac's own
node_exporter output: `en0` is the only interface carrying real traffic;
everything else -- `lo0`, `awdl0`, `utun*`, `bridge0`, etc. -- is 0 or
obviously virtual) -- and memory falls back to a Darwin-compatible
free/total formula. Also fixed a pre-existing gap while at it: the node-network
regex was missing `eno49np0` (the `xl170` CloudLab profile's interface,
confirmed via `infra/views.py`'s separate Flink-specific interface mapping)
-- present in every sibling `getprom_*.py` script too, not introduced here.

**Files**: `infra/reporting_scripts/getprom_risingwave.py` (`getHardwareMetrics`,
new `_resolve_network_devices`/`_discover_primary_network_device`/
`_resolve_memory_formula` helpers)

### B. `operatorMetrics` only reported throughput/selectivity, not memory/backpressure/busy time

**Context**: RisingWave's own `:1260/metrics` endpoint exposes far more
per-fragment/per-table native metrics than the two `getOperatorMetrics()`
was using. Confirmed present on a live local instance (2026-07-27):
`stream_memory_usage{table_id=...}` (operator state size in bytes),
`stream_actor_output_buffer_blocking_duration_ns{fragment_id=...}`
(cumulative time an actor spent blocked writing to a full downstream
channel -- a direct backpressure signal), and `stream_actor_poll_duration`
(cumulative actor busy/CPU time).

**Change**: `_resolve_fragment_ids()` now also returns each fragment's
`table_id` (needed for the memory-usage join, and already resolved by the
existing catalog query -- no second round-trip). Added
`_get_table_memory_usage()` and `_get_fragment_ratio()` (the latter shared
by both backpressure and busy-ratio, since both are "fraction of the last
minute spent in X" over a nanosecond duration counter). Every stage in
`getOperatorMetrics()`'s output now also reports `memory_usage_bytes`,
`backpressure_ratio`, and `busy_ratio`, in addition to the existing
`throughput_per_sec`/`selectivity`. Stateless/no-data cases (e.g. a bare
SOURCE has no operator state, so no `stream_memory_usage` series) report
`0.0`, same precedent as bug #13's selectivity fix -- an empty series here
means "nothing to report," not "unknown."

**Verified**: ran a live local Word Count job and called
`getOperatorMetrics()` directly -- all four stages (source, tokenizer,
counter, sink) returned real, non-null values for all five fields with no
query errors. (One early version of the two new PromQL queries hit
`@ modifier must be preceded by an instant vector selector` -- Prometheus's
`@` operator has to attach to the innermost selector, not to the result of
an outer `sum()`/`topk()` aggregation; fixed by moving `@ <ts>` inside the
aggregation's parentheses in both the new queries and
`_discover_primary_network_device`/`_get_table_memory_usage` above.)

**Files**: `infra/reporting_scripts/getprom_risingwave.py`
(`_resolve_fragment_ids`, new `_get_fragment_ratio`/`_get_table_memory_usage`
helpers, `getOperatorMetrics`)

---

## Real distributed cluster validation (2026-07-27, 4-node CloudLab m510)

Everything above was validated against either a single-node local
RisingWave instance or (for the original Word Count baseline) a since-
expired 5-node CloudLab allocation. This section covers a fresh, genuine
4-node CloudLab cluster (1 master + 3 workers, Intel Xeon D-1548/m510
profile) provisioned from scratch and run through all 24 combos twice.

### 15. `actorMetrics.barrier_inflight_latency_avg` NaN-poisoned by early-lifetime histogram gaps

**Symptom**: `barrier_inflight_latency_avg` was `null` in an otherwise
fully-clean run, while its sibling `barrier_inflight_latency_max` (computed
from the exact same underlying PromQL subquery) had a real value.

**How found**: Ran the real 24-combo validation script against the live
distributed cluster; the same two fields differing when built from
identical inputs was the tell that this wasn't a "no data" situation.
Queried both `avg_over_time(...)` and `max_over_time(...)` of the identical
`histogram_quantile(0.99, sum(rate(meta_barrier_duration_seconds_bucket
[1m])) by (le))[5m:1s]` subquery directly: `avg_over_time` returned a real
sample valued `NaN`; `max_over_time` returned a real number
(`0.328...`). Root cause: `histogram_quantile` genuinely emits `NaN` (a
present-but-NaN sample, not an absent one) at early evaluation instants in
a job's short lifetime (a 0/0 inside the quantile calc before enough
barrier-bucket data exists) -- Prometheus's `avg_over_time` includes that
NaN in its running sum (poisoning the whole average to NaN), while
`max_over_time` skips NaN samples during its reduction and survives with
the genuine max.

**Fix**: In `getActorMetrics()`, if `barrier_inflight_latency_avg` comes
back `None` but `barrier_inflight_latency_max` is a real number, fall back
to using the max value for avg too -- a real, measured number is a better
answer than null when the sibling metric proves the data exists. Confirmed
live: re-running the two combos that hit this (Word Count Q1, Smart Grid
Q1) before vs. after the fix went from 1 null each to 0.

**Files**: `infra/reporting_scripts/getprom_risingwave.py` (`getActorMetrics`)

### 16. DROP SINK/MATERIALIZED VIEW/SOURCE can take 30-60+ seconds on a real distributed cluster

**Symptom**: A validation script's `DROP SINK` call (60s subprocess
timeout) hung and raised `TimeoutExpired`, which (since uncaught) killed
the entire batch script after only 2 of 24 combos, leaving
`smart_grid_mv`/`smart_grid_source` stray in the catalog.

**How found**: Checked RisingWave's own meta log and the client's psql
output directly rather than guessing -- RisingWave itself prints a
diagnostic NOTICE in this situation: `DROP MATERIALIZED VIEW has taken more
than 30 secs, likely due to high barrier latency ... Run RECOVER in a
separate session to trigger recovery.` This is expected, documented
RisingWave behavior for a genuinely distributed deployment (barrier
propagation/actor teardown across physically separate compute nodes takes
measurably longer than within one local process) -- not a bug in our SQL
or scripts. Separately confirmed the DROP's catalog-level effect (object
gone from `rw_catalog.rw_streaming_jobs`) can complete before the
client's psql session actually receives its response, i.e. the client-side
hang can outlast the point where it would already be safe to proceed.

**Fix**: Two changes, both to the *validation script* only (no
`getprom_risingwave.py`/production code changes needed -- this is a
client-side timeout-handling gap, not a metrics-collection bug):
1. Wrap every DROP call so a timeout doesn't raise -- instead run `RECOVER;`
   (RisingWave's own documented remediation) and move on, rather than
   crashing the whole batch.
2. Wrap each combo's entire body in `try/except` so one combo's unexpected
   failure can never silently drop every remaining combo's results (this is
   exactly what happened before this fix -- 22 combos never ran and no
   result was recorded for any of them).

**Files**: validation-script-only (see `remote_metrics_validation.py`'s
`sh_resilient()`/`drop_all()`/per-combo `try/except` in this project's
scratch space) -- worth porting the same resilience into
`utils/playbook/tasks/risingwave/risingwave-kill-job.yaml` if the
production kill-job path is ever seen to hit this in practice (it already
has its own `ignore_errors: true`/`timeout: 180` per bug #5, which is
likely already generous enough, but hasn't been proven against a real
distributed cluster's DROP latency specifically).

### Result: 24/24 combos, zero null metrics, on the real distributed cluster

Both fixes (15 and 16) took effect automatically mid-run (each combo
spawns a fresh `getprom_risingwave.py` subprocess), confirmed by re-running
the two affected combos afterward. Final state: all 24 workload/query
combos produced fully populated `queryMetrics`/`actorMetrics`/
`operatorMetrics`/`hardwareMetrics` with zero nulls -- including
`kafka_event_rate`/`kafka_throughput` (populated here, unlike local
testing, since the real JMX exporter is correctly wired up per
`install_risingwave_only.sh`) and the distributed-only
`hardwareMetrics.node_network_out`/`node_memory` (real per-node values
across 3 separate physical machines, not the single-instance local
approximation).

---

## Cluster expansion to 6 nodes (2026-07-27) -- two more operational findings

Expanded the same cluster from 4 to 6 nodes (1 master + 5 workers) to support
higher parallelism sweeps. Two more real, reproducible operational bugs
surfaced during the expansion -- both are *cluster-operations* findings
(how to safely restart/reconfigure this infrastructure), not RisingWave SQL
or metrics-collection bugs.

### 17. `pkill -f 'prometheus'` collaterally kills RisingWave meta/frontend

**Symptom**: Redeploying an updated `prometheus.yml` to the master node via
`pkill -f '[p]rometheus'` (intending to kill only the standalone Prometheus
binary) also silently killed RisingWave's meta and frontend processes on
the same node, with no error at the time -- only noticed minutes later when
`psql` connections started failing.

**How found**: Checked `risingwave-meta.log` directly rather than guessing;
found `INFO risingwave_rt: received SIGTERM, shutting down...` at the exact
timestamp of the `pkill` command.

**Root cause**: `pkill -f` matches the full command line, not just the
process name. RisingWave's meta and frontend processes are both started
with a literal `--prometheus-listener-addr` flag -- a substring match for
`prometheus`. The bracket-escape trick (`[p]rometheus`) only prevents a
command from matching *itself*; it does nothing to prevent matching a
*different, unrelated* process whose command line happens to contain the
same substring for a different reason.

**Fix**: No code fix (this was an ad hoc operational command, not a
playbook) -- just a sharper kill pattern going forward, e.g.
`pkill -f '/prometheus-2.42.0.linux-amd64/prometheus '` (anchored to the
actual binary path) instead of a bare substring. Worth keeping in mind for
any future playbook changes too: `pkill -f` patterns should always be
checked against every other process's full command line on the same host,
not just the target's.

### 18. RisingWave compute nodes panic on reconnect after a meta reset, if their local `rw-data` cache is stale

**Symptom**: After restarting meta with a fresh Hummock/MinIO state (bug #2's
scenario -- `mc rm --recursive --force` to clear a stale `cluster_id`),
newly-(re)started compute nodes crashed within seconds:
`thread 'rw-main' panicked at src/storage/src/hummock/observer_manager.rs:98:17: invalid notification info: Snapshot`,
immediately followed by `Aborted`. Reproduced twice in a row with the exact
same stack trace before the fix.

**How found**: Checked `risingwave-compute.log` on the crashing node
directly. The panic is in `HummockObserverNode::handle_notification` --
this is the compute node's local cache of Hummock version state getting an
notification it doesn't know how to reconcile.

**Root cause**: Each compute node keeps a local-disk scratch/cache directory
(`{{ INSTALLATION_DIR }}/rw-data`, RisingWave's "foyer" cache layer --
confirmed non-authoritative, see `risingwave-compute-start.yaml`'s own
comment) that persists across process restarts. When meta's own Hummock
version state gets reset to a fresh cluster_id (bug #2), a compute node
reconnecting with a *stale* local cache built against the *previous*
cluster_id's version history receives a notification shaped in a way its
own cache doesn't expect, and the observer panics instead of invalidating
the stale cache.

**Fix**: `sudo rm -rf {{ INSTALLATION_DIR }}/rw-data` on every compute node
before restarting it, any time meta itself was restarted with a reset
Hummock/MinIO state. Confirmed live: restarting compute without this wiped
first crashed identically both times; restarting after wiping `rw-data`
came up clean and stayed stable across repeated checks.

**Files**: none in the repo yet -- this is an operational runbook step, not
currently encoded in any playbook (`risingwave-compute-start.yaml` doesn't
wipe `rw-data` on start, since normally it's correct to preserve the local
cache across an *ordinary* restart; this is specifically about restarting
*after* a meta-side Hummock reset). Worth adding a conditional wipe step to
that playbook, gated on whatever signal indicates meta was just reset, if
this cluster gets rebuilt from scratch again.

---

## First real parallelism/event-rate benchmark sweep (2026-07-28) -- one unresolved bug found

Ran the first genuine benchmark sweep (not validation traffic) against the
6-node cluster: Word Count and Ad Analytics Q2, sweeping RisingWave's
`streaming_parallelism` session GUC (2/4/8/16/32/40, staged via `SET`
before each `CREATE` statement so source=2/sink=1 stay fixed while the
aggregation stage takes the swept value -- see the cluster-expansion
section above for how this was verified) crossed with event rate
(100k/500k/1M events/sec), 5-minute runs. The cluster was reclaimed by
CloudLab partway through (expected, flagged in advance) after the
time-boxed priority subset completed: Word Count at parallelism
2/4/8/16/32/40 (mixed event rates) and Ad Analytics at parallelism
2/4/8/16/32/40 (event rate 500k only) -- 17 total metrics files.

### 19. `hardwareMetrics.node_memory_{98,95,50}` intermittently null on the 6-node cluster (unresolved)

**Symptom**: Across the 17 files from this sweep, 9 (~53%) have
`node_memory_98`/`_95`/`_50` as `null`, while `node_memory` (the raw
instant-vector time series, not the quantile) and every other metric in
the same file are fully populated. Not observed at all in the earlier
4-node cluster's 24-combo validation (bug reports #1-16) -- first
appeared only after the 6-node expansion.

**How found**: `run_sweep_risingwave.py check-nulls` across the sweep's
own output, cross-referenced against the same script's clean runs on the
earlier 4-node cluster.

**Investigation status**: Confirmed the underlying probe
(`node_memory_MemTotal_bytes{job="node_exporter"}`) resolves correctly
against this cluster (real Linux metrics, correct formula selected -- not
a repeat of bug #15's local-vs-remote formula issue). Ran out of cluster
access (CloudLab reclaimed the nodes mid-investigation) before reproducing
the exact quantile query against a live instance to confirm the root
cause. Leading hypothesis, not yet confirmed: the memory formula's
`sum(node_memory_MemFree_bytes{...} + ...) / sum(node_memory_MemTotal_bytes{...})`
aggregates across all 5 worker node_exporter targets into a single scalar
series *before* `quantile_over_time` sees it; if even one of the 5
targets has a brief scrape gap at some instant within the 5-minute
lookback window (node_exporter intermittently showed transient "down"
health in Prometheus after cluster-expansion restarts earlier the same
session -- see the cluster-expansion section above), the `sum()` at that
instant reflects a different subset of nodes, which could produce an
outlier or, under some condition not yet isolated, a genuinely absent
sample for that step. Needs reproducing live against a real cluster to
confirm before attempting a fix.

**Not fixed this session** -- flagged here for the next time cluster
access is available, rather than guessed at without being able to verify
against a live instance.

**Files**: `infra/reporting_scripts/getprom_risingwave.py`
(`_resolve_memory_formula`/`getHardwareMetrics` -- likely fix location,
once root-caused)

---

### 20. Entire cluster (Kafka, ZooKeeper, MinIO, RisingWave) dies simultaneously ~12.5 hours after `setup_new_cluster.sh`/cluster-create, due to Ansible's `async: 45000` on every service-start task

**Symptom**: ~12.5 hours into an otherwise-healthy multi-day benchmark
sweep, every in-flight combo started failing at the very first step
(`kafka-topics --create` timing out after 60s), and every subsequent
combo failed the same way. Checking live: no Kafka broker was listening
on 9092 on any of the 6 nodes, no ZooKeeper on 2181, no MinIO on 9301,
and no RisingWave process (meta/frontend/compute) was running anywhere --
the entire cluster had gone down at once, silently, with no OOM kill, no
node reboot (`uptime`/`dmesg` both clean), and no disk-space exhaustion
(`df -h` showed >80% free on every node checked).

**Root cause**: every service-start task in this project's Ansible
playbooks (`tasks/kafka/kafka-start.yaml`'s Zookeeper/Kafka start tasks,
`tasks/risingwave/complex/risingwave-minio-start.yaml`'s MinIO start
task, and the equivalent RisingWave meta/frontend/compute start tasks)
launches its process via `async: 45000, poll: 0` -- Ansible's mechanism
for a long-running background job. 45000 seconds is exactly 12.5 hours.
Confirmed by comparing the ZooKeeper data directory's creation timestamp
(`stat /tmp/zookeeper/myid`, birth time `08:33:26`) against the first
"Connection refused" line in Kafka's `server.log` (`21:05:06`) -- the gap
is 12h31m40s, matching the `async: 45000` window to the second. Ansible's
async job runner does not treat `poll: 0` as "detach forever and forget"
the way a plain `nohup ... &` would -- the spawned process is still
subject to the job's `async:` time limit and gets terminated once it
elapses, even though the playbook run itself finished and returned control
long before then.

**Fix (this session, ad hoc)**: restarted every service manually via
`nohup ... &` (not through Ansible), in dependency order: ZooKeeper on
all 6 nodes → wait for quorum (`echo srvr | nc localhost 2181`, expect
one `leader` + five `follower`) → Kafka broker on all 6 nodes (`broker.id`
matching each node's inventory `id`) → MinIO on the meta node → **critical
extra step**: `mc rm --recursive --force local/hummock001` to clear the
bucket, since this cluster's meta node runs with `--backend mem` (no
persistent metadata store) and therefore stamps a brand-new `cluster_id`
on every restart -- restarting meta without first clearing the bucket
reproduces bug #18's exact panic (`invalid notification info` / Hummock
object-store errors), just against the shared MinIO bucket instead of a
compute node's local `rw-data` cache → recreate the bucket
(`mc mb --ignore-existing`) → RisingWave meta → frontend → compute on the
remaining 5 nodes. Same `sudo bash -c '... > logfile 2>&1 < /dev/null &'`
wrapping as bug #16's fix is required for the meta/frontend commands --
plain `sudo nohup ... > logfile` fails with `Permission denied` on the
logfile since the redirect happens outside sudo's scope.

**Not fixed at the root**: the playbooks themselves still specify
`async: 45000` on these tasks, so any future cluster stood up via
`setup_new_cluster.sh`/the controller's `/infra/create` endpoint will hit
this exact wall again at the same ~12.5-hour mark. A proper fix is either
(a) raising `async:` to a value comfortably longer than any real sweep
(e.g. several days), or (b) decoupling these services from Ansible's job
tracking entirely (systemd unit, or a `nohup`-wrapped launch script the
playbook merely invokes and returns from immediately, matching how the
ad hoc restart above was actually done). Worth fixing before the next
multi-day sweep rather than manually restarting again partway through.

**Blast radius**: 40 combos across 3 workloads failed during the ~2 hour
window between the outage and this fix being applied (Machine Outlier's
last 3 combos, all 18 of Bargain Index, all 18 of Trending Topics, and
Click Analytics' first attempt) -- all cleanly recorded as non-`"ok"` in
`manifest_risingwave.json`, so a subsequent `run_sweep_risingwave.py run`
invocation retries them automatically with no manual bookkeeping needed.

**Files**: `utils/playbook/tasks/kafka/kafka-start.yaml`,
`utils/playbook/tasks/risingwave/complex/risingwave-minio-start.yaml`,
and the equivalent RisingWave meta/frontend/compute start task files
(all share the same `async: 45000, poll: 0` pattern -- not yet audited
whether Spark/Storm/Flink/Kafka-Streams start tasks share the same
constant and are equally exposed).

---

### 21. `benchmark_sweep/run_sweep_risingwave.py` never substituted `__THRESHOLD__` or `__WINDOW_SIZE_MS__`, breaking Bargain Index/Trending Topics (and would have broken Click Analytics Q3)

**Symptom**: every single combo for Bargain Index and Trending Topics
failed at the `CREATE MATERIALIZED VIEW` step with `Failed to bind
expression: ... > __THRESHOLD__` / `Item not found: Invalid column:
__threshold__` -- RisingWave was parsing the literal placeholder text as
a bareword column reference, since it was never substituted with a real
number.

**Root cause**: `bargain_index.sql` and `trending_topics.sql` both
contain a genuine, actively-used `__THRESHOLD__` placeholder (confirmed
via each file's own header comment: "`__THRESHOLD__` is substituted the
same way `__KAFKA_BOOTSTRAP_SERVER__` is, from `job_threshold`") --
`views.py::_risingwave_job_manipulation()`, the substitution function
used by the *real* controller job-submission path, already handles this
correctly. `run_sweep_risingwave.py`'s own `build_sql_with_parallelism()`
is a separate, simplified reimplementation (direct psql/Ansible
orchestration, bypassing the Django controller entirely -- see this
project's earlier RisingWave-sweep design notes) that only ever
substituted `__KAFKA_BOOTSTRAP_SERVER__`, `__WINDOW_SIZE_SEC__`, and
`__WINDOW_SLIDE_SEC__` -- `__THRESHOLD__` was simply never wired in. A
grep across every workload's `.sql` file for all `__[A-Z_]*__`-shaped
placeholders while investigating this also turned up a second, not-yet-
triggered instance of the same class of bug: `click_analytics_q3.sql`
uses an active `__WINDOW_SIZE_MS__` placeholder (a genuinely different
placeholder from `__WINDOW_SIZE_SEC__`, not a typo) that would have
failed the same way the first time that query variant was run.

**Fix**: added a `THRESHOLD = 5` module constant to
`run_sweep_risingwave.py` (matching this project's own frontend default,
`PStreamBench_wui/dsp_fe/src/components/ClusterInfo.vue`'s
`f_job_threshold: 5` -- not an arbitrary guess), and extended
`build_sql_with_parallelism()` to substitute both `__THRESHOLD__`
(always) and `__WINDOW_SIZE_MS__` (alongside the existing
`__WINDOW_SIZE_SEC__`/`__WINDOW_SLIDE_SEC__`, gated the same way behind
`needs_window`). Manually verified both `bargain_index.sql` and
`trending_topics.sql` now `CREATE SOURCE`/`CREATE MATERIALIZED
VIEW`/`CREATE SINK` cleanly against a live cluster after the fix.
`click_analytics_q3.sql`'s `WORKLOADS`/`ADDITIONAL_QUERY_VARIANTS` entry
also needed its `needs_window` flag corrected to `True` -- it was
originally set `False` since a first-pass grep only checked for
`__WINDOW_SIZE_SEC__`, missing that this file needs the `__WINDOW_SIZE_MS__` substitution
gated behind the same flag despite having no actual HOP window of its own.

This was purely a gap in the sweep script's templating, unrelated to the
two infra issues (bugs #20 and the undocumented gradual-DDL-slowdown
issue) also encountered in the same overnight session. All three combos'
worth of failures (Bargain Index x18, Trending Topics x18, plus whatever
Click Analytics Q3 combos would have run) needed a fresh retry pass after
this fix landed, since the sweep processes started before the fix was
applied had already passed these workloads by (see bug #20's note on
`run()` only iterating the WORKLOADS list once, top to bottom, per
invocation).

**UPDATE (same session, after the retry pass) -- Bargain Index's
`THRESHOLD=5` default produces zero output rows.** The retry pass's 18
Bargain Index combos all completed with status `"ok"` (no crash, no
timeout), but `check-nulls` showed every single one had
`queryMetrics.kafka_throughput*` = null (72 null findings total, 4 per
file x 18 files) -- a 100%-consistent pattern pointing at the sink
genuinely never writing any output, not a metrics-collection flake
(contrast with bug #19's node_memory nulls, which were sparse and
CloudLab-only).

**RESOLVED (later session, 2026-08-04): confirmed root cause is simply
`THRESHOLD=5` being unrealistic for real stock data -- not a query bug.**
An earlier same-session attempt to reproduce this live (redeploying
`bargain_index.sql` and running the real `custom_producer.py` with
`--app_type Bargain_Index`) had appeared to also get zero rows at a
"corrected" threshold of `1.01`, but that reproduction turned out to be
invalid: `custom_producer.py`'s `BASE_DIR` is built by plain string
concatenation (`BASE_DIR+FILE_NAME_BI`, `custom_producer.py:32,152`) with
no separator added, so passing `PSTREAMBENCH_DATA_DIR` *without* a
trailing slash (as was done both times) produces a mangled path
(`.../pstreambench_datastocks.csv`) and the producer dies immediately
with `File not found`, silently producing zero real events -- the
apparent "zero rows even at 1.01" was actually testing an empty input,
not the query logic. (`run_sweep_risingwave.py`'s own `DATA_DIR` constant
already has the trailing slash, so the real sweep runs were never
affected by this -- this was purely an artifact of ad hoc manual
reproduction commands.)

Redone correctly with the trailing slash, against an isolated test topic
(to avoid a stale historical backlog in the shared `BIIn` topic
confounding the result -- see below) and a low, controlled producer rate:
confirmed real data flows end-to-end and a diagnostic materialized view
(same `parsed`/`windowed`/`vwap_calc` CTEs, but selecting `vwap`,
`quote_price`, and their ratio directly instead of applying the final
`WHERE` filter) showed `quote_price` and `vwap` genuinely diverging
per-window (12 real 10-second windows, 2000-5000 rows each): 5 of 12
windows had `quote_price > vwap`, and 3 of 12 had `quote_price / vwap >
1.01`. At `THRESHOLD=5`, zero of the 12 windows passed, and none
realistically ever would -- that requires the last reading in a
10-second window to be 5x the volume-weighted average of that same
window, an unrealistic swing for real stock closing-price data. The
query logic, HOP windowing, and field parsing are all correct; the
shipped `THRESHOLD=5` default (matching
`PStreamBench_wui/dsp_fe/src/components/ClusterInfo.vue`'s
`f_job_threshold: 5` UI default) is simply the wrong order of magnitude
for this specific workload's data domain. A per-workload threshold
(e.g. ~1.05-1.1 for Bargain Index) would produce real, non-empty output.

**FIXED (2026-08-05): `WORKLOAD_THRESHOLDS = {"Bargain Index": 1.02}`
added to `run_sweep_risingwave.py`, all 18 combos re-run.** Confirmed
live before committing to the full re-run: deployed the real
`bargain_index.sql` (via `build_sql_with_parallelism`, topics redirected
to a fresh isolated pair to avoid the `BIIn` backlog described below)
with `1.02` substituted, ran the real producer, and got 4 real output
rows with `bargain_index` ratios of 1.10/1.06/1.05/1.04 -- proof the
query, HOP windowing, and fix all work correctly. `Trending Topics` was
deliberately left at the global default (5) since its `__THRESHOLD__` is
an unrelated `COUNT(*) > N` filter, already producing clean output.
Cleared the 18 stale zero-output manifest entries and metric files, reset
the `BIIn`/`BIOut` Kafka topics (see backlog note below), and re-ran via
a one-off `rerun_bargain_index.py` (same `run_one_combo` loop as
`run_additional_variants.py`, filtered to just Bargain Index). Result:
18/18 `"ok"`, and this time genuinely non-null --
`queryMetrics.kafka_throughput.throughput` now has real positive values
(0.09-0.95 events/sec on the output topic across all 18 files, scaling
sensibly with event rate/parallelism). Remaining nulls in about half the
files are exactly bug #19's `node_memory_98/95/50` (unrelated, already
documented, sparse/CloudLab-only) -- nothing new.

Side finding from the same investigation: the `BIIn` Kafka topic (reused
across all 18 real sweep combos, sharing the same
`bargain_index_source` name/consumer group) accumulates a large
historical backlog across the whole session, and re-creating a source
against it with `scan.startup.mode = 'latest'` does not necessarily
start strictly from "now" -- an initial live-reproduction attempt against
that shared topic read what appeared to be ~323 million rows almost
immediately, consistent with picking up old backlog rather than only
newly-produced messages (RisingWave logs a `NOTICE: snapshot backfill
disabled due to using shared source` in this situation). Not
root-caused further, but worth knowing: any future live reproduction
against a long-lived shared topic should use a fresh, isolated topic
name to get a clean, unambiguous result.

**Files**: `BargainIndex/bargain_index.sql` (query logic -- confirmed
correct, not changed), `run_sweep_risingwave.py` (`THRESHOLD` constant --
identified as the actual root cause; a workload-specific value is the
fix if this is pursued further), `custom_producer.py` (`BASE_DIR` string
concatenation -- fragile to a missing trailing slash on
`PSTREAMBENCH_DATA_DIR`, worth hardening with `os.path.join` regardless
of this specific investigation).

**Files**: `PStreamBench_controller/dsp_be/benchmark_sweep/run_sweep_risingwave.py`
(`build_sql_with_parallelism`, `THRESHOLD` constant, `ADDITIONAL_QUERY_VARIANTS`)

---

### 22. `object_prefix_for()` computed the wrong SQL object prefix for every `ADDITIONAL_QUERY_VARIANTS` entry, breaking 100% of combos for every multi-variant workload

**Symptom**: launched the `ADDITIONAL_QUERY_VARIANTS` pass (7 extra
workload-query combos: Ad Analytics Q1, Click Analytics Q2/Q3, Smart
Grid Q2, Log Processing Q2, TPCH Q2, Google cloud Monitoring Q2) and
every single combo for the first three workloads reached (Ad Analytics
Q1: 18/18, Click Analytics Q2: 18/18, Click Analytics Q3: 12/18 so far)
failed -- either an immediate `"materialized view with name
ad_analytics_mv exists"` catalog error, or (for Click Analytics Q2) a
90s `TimeoutExpired` on the SQL-submission `psql` call itself for every
combo after the first.

**Root cause**: `object_prefix_for(workload_name)` derives the DROP/
CREATE object prefix directly from the workload's display name (used
verbatim as the manifest-key/folder-name too). For a base workload like
`"Ad Analytics"` this correctly produces `ad_analytics`, matching the
SQL file's real object names (`ad_analytics_source/_mv/_sink`). But
every `ADDITIONAL_QUERY_VARIANTS` entry is named `"<base> Q<N>"` (e.g.
`"Ad Analytics Q1"`, `"Click Analytics Q2"`) specifically so its
manifest keys and evaluationMetrics folder stay separate from the base
workload's own already-completed run -- and `object_prefix_for` naively
turned that into `ad_analytics_q1`, `click_analytics_q2`, etc., which
match NOTHING, since query variants share the exact same object names
as their base workload (`_risingwave_job_manipulation()`'s convention is
a differently-NAMED FILE per variant, `<base>_q<N>.sql`, not
differently-named SQL objects inside it). `drop_all()`'s DROP statements
silently no-op'd against nonexistent `..._q1_*`/`..._q2_*` objects every
single time, while `CREATE` kept colliding with the real, never-actually
-dropped `ad_analytics_mv` from the first (accidentally successful)
combo -- explaining both failure shapes: an outright "already exists"
error for combos whose CREATE ran fast enough to hit the live catalog
directly, and a `psql` timeout for slower ones stuck behind the
orphaned, still-running first job's barrier/backpressure state.

**Fix**: `object_prefix_for()` now strips a trailing `" Q<digits>"` from
the workload name before computing the prefix (`re.sub(r' Q\d+$', '',
workload_name)`), so `"Ad Analytics Q1"` -> `ad_analytics` and `"Click
Analytics Q2"` -> `click_analytics`, matching every variant's real
object names. Harmless no-op for every base-workload name (none of them
have this suffix). Verified: `object_prefix_for("Ad Analytics Q1")` ==
`object_prefix_for("Ad Analytics")` == `"ad_analytics"` after the fix.
Manually dropped the one orphaned `ad_analytics_*` object set left over
from the broken run before relaunching.

**Confirmed NOT related to bugs #20/#21/the DDL-slowdown issue** --
pure sweep-script logic, would have failed identically on a perfectly
healthy cluster (and in fact did, the cluster was confirmed healthy via
`rw_catalog.rw_worker_nodes` throughout).

**Files**: `PStreamBench_controller/dsp_be/benchmark_sweep/run_sweep_risingwave.py`
(`object_prefix_for`)

---

### 23. `getprom_risingwave.py`'s `JOB_CLASS_DISPATCH` had no entries for `ADDITIONAL_QUERY_VARIANTS` names, silently misfiling every variant's real metrics into the Word Count folder

**Symptom**: after fixing bug #22, re-ran the additional-variants pass
and every combo for every one of the 7 variants still reported
`"no_metrics_file"` -- but this time with NO catalog error and NO
timeout in most cases; the SQL itself submitted fine.

**Root cause**: `run_sweep_risingwave.py` passes the full variant name
(e.g. `"Ad Analytics Q1"`) as `--job_class` to `getprom_risingwave.py`.
That script's dispatch line, `collector =
JOB_CLASS_DISPATCH.get(job_class, getWordCount)`, and `main()`'s
`job_name = job_class if job_class in JOB_CLASS_DISPATCH else "Word
Count"`, both silently fall back to Word Count for any unrecognized
`job_class` -- and `"Ad Analytics Q1"` was never a key in
`JOB_CLASS_DISPATCH` (only the base workload names were). So every
variant combo's SQL ran for real, RisingWave produced real data, and
`getprom_risingwave.py` collected a real, correct metrics file -- it
just collected Ad Analytics/Click Analytics/etc.'s ACTUAL metrics
(the collector functions are query-shape-and-object-name driven, not
tied to a specific job_class string) and wrote them into `Word Count/`
instead of `<variant name>/`, because of that fallback. The sweep
script's own `run_one_combo()` checks the `<variant name>` folder
(`"Ad Analytics Q1"`) for a newly-appeared file and, finding nothing
there, reports `"no_metrics_file"` -- a false negative on data that was
actually collected correctly, just misfiled.

**How found**: before concluding the combos had genuinely failed a
second time, checked whether real files existed anywhere with a
suspicious name -- `ls "Word Count/" | grep ad_analytics_p` turned up
18 complete, correctly-timestamped `ad_analytics_p*` files (one per
parallelism/rate combo, exactly matching the Ad Analytics Q1 sweep that
had "failed") plus 2 `click_analytics_p*` files, confirming this was a
filing bug, not a data-collection bug.

**Fix**: added a dict entry for each of the 7
`ADDITIONAL_QUERY_VARIANTS` names to `JOB_CLASS_DISPATCH`, pointing at
the SAME collector function as their base workload (`"Ad Analytics
Q1": getAdAnalytics`, `"Click Analytics Q2"`/`"Click Analytics Q3":
getClickAnalytics`, etc.) -- correct, since a query variant changes
which `.sql` file gets deployed (`job_query_number`), not this script's
metrics-collection `stage_relations`, which are identical across every
query variant of a given workload (already true for the base workloads
themselves, e.g. Click Analytics Q1/Q2/Q3 all collapse to the same
source/mview/sink fragment shape per `getClickAnalytics`'s own
docstring). Recovered the 20 already-successfully-collected-but-misfiled
combos (all 18 of Ad Analytics Q1, plus Click Analytics Q2's first 2)
by moving their files from `Word Count/` to the correct folder and
manually setting their `manifest_risingwave.json` status to `"ok"`,
rather than wastefully re-running combos that had already produced
good data -- the remaining Click Analytics Q2 combos (which hit a
genuine `TimeoutExpired`, likely from contention with the misfiled-but-
still-live first job) and the other 5 variants were left for a fresh
sweep pass with both this fix and bug #22's fix in place.

**Files**: `PStreamBench_controller/dsp_be/infra/reporting_scripts/getprom_risingwave.py`
(`JOB_CLASS_DISPATCH`)

---

### 24. `click_analytics_q2.sql`'s `geo_ip_lookup` reference table has no cleanup path, so every failed/retried combo doubles its row count until DDL against it stalls

**Symptom**: after fixing bugs #22 and #23, a full retry pass for the
remaining Click Analytics Q2/Q3/Smart Grid Q2 combos still failed
100% of the time, this time with `TimeoutExpired` on SQL submission
for every combo in the run -- including Smart Grid Q2 and Click
Analytics Q3, which don't touch Click Analytics Q2's SQL at all,
ruling out a query-specific cause and pointing at a cluster-wide
stall instead. `rw_catalog.rw_worker_nodes` and a plain `SELECT 1`
both returned instantly, so the control plane itself was healthy --
only DDL submission was affected, matching the previously-observed
(but not-yet-root-caused) recurring barrier-latency stall.

**Root cause (two compounding issues)**: (1) `click_analytics_q2.sql`
declares a static `CREATE TABLE IF NOT EXISTS geo_ip_lookup (...)`
plus a one-shot `INSERT INTO geo_ip_lookup VALUES (...)` (~256 rows,
IP-octet-to-country), joined by the streaming query. `drop_all()` in
`run_sweep_risingwave.py` only ever issues `DROP SINK` / `DROP
MATERIALIZED VIEW` / `DROP SOURCE ... CASCADE` -- it has no logic to
clean up an auxiliary `CREATE TABLE` a workload's SQL might also
define. Every one of the 16 prior failed Click Analytics Q2 attempts
therefore re-ran the `INSERT` with no dedup guard, bloating the table
to 512+ rows (confirmed via `SELECT COUNT(*) FROM geo_ip_lookup`).
(2) Separately, RisingWave's control-plane barrier processing had
drifted into the same "high barrier latency" state documented
informally elsewhere this session (DDL commands hang 30s+ with the
`NOTICE: ... likely due to high barrier latency ... Run RECOVER`
message, even though `rw_worker_nodes` and simple `SELECT`s stay
instant) -- this alone was enough to make every DDL-heavy combo in
the retry pass time out, independent of the `geo_ip_lookup` bloat.

**Fix**: `RECOVER;` cleared the barrier-latency stall (confirmed via
a timed DDL probe: a `CREATE SOURCE`/`DROP SOURCE` round trip
returned in under 1 second immediately after, versus 30s+ before).
`TRUNCATE TABLE` is not supported in RisingWave (`Feature is not yet
implemented: Unhandled statement: TRUNCATE TABLE ...`); `DROP TABLE
IF EXISTS geo_ip_lookup;` was used instead and let the next combo's
`CREATE TABLE IF NOT EXISTS` + `INSERT` recreate it clean. Note: a
`DROP TABLE`/`DELETE` issued *while* the barrier-latency stall is
still active will itself hang and report the same "took more than 30
secs" notice, and if a concurrent `RECOVER` interrupts it mid-flight
the statement can error out (`gRPC request to batch service failed:
Internal error` / `table reader closed`) without taking effect --
confirmed the table was still at 512 rows after one such interrupted
attempt. Issuing `RECOVER` first and only then the `DROP TABLE`
(not concurrently) worked cleanly on the next try. This is a one-time
manual cleanup, not a code fix to `drop_all()` -- since Click
Analytics Q2 is the only workload (of all 14 base + 7 variants) that
defines an auxiliary reference table, and the retry pass only needed
to run once more to finish it.

**Files**: none changed; operational cleanup only
(`geo_ip_lookup` table dropped and left to be recreated by the next
sweep combo's own SQL). `drop_all()` in
`PStreamBench_controller/dsp_be/benchmark_sweep/run_sweep_risingwave.py`
remains a known gap for any *future* workload SQL that defines its
own reference table.

---

### 25. `check_nulls()` never scanned `ADDITIONAL_QUERY_VARIANTS`, so Ad Analytics Q1's total metrics-collection gap (a Prometheus outage) went undetected until the cross-engine CSV extractor crashed on it

**Symptom**: while adding RisingWave support to
`PStreamBench_benchmark_data/1_data_extractor.py` (see that project's own
notes for the migration/extraction work), the extractor crashed with
`TypeError: float() argument must be a string or a real number, not
'NoneType'` on an Ad Analytics Q1 file. Investigating turned up that
**all 18** of Ad Analytics Q1's `evaluationMetrics` files had every
`queryMetrics` and `operatorMetrics` field null (not just the sparse
`node_memory` fields from bug #19) -- a 100%, uniform failure, plus 2 of
Click Analytics Q2's 18 files in the exact same pattern.

**Root cause (two layers)**: (1) `run_sweep_risingwave.py`'s
`check_nulls()` (`def check_nulls(): ... for workload_name, *_ in
WORKLOADS:`) only ever iterates the module-level `WORKLOADS` list -- it
was never called with `WORKLOADS` reassigned to
`ADDITIONAL_QUERY_VARIANTS` the way `run_additional_variants.py`/
`rerun_bargain_index.py` reassign it for the *run* command, so every
"18/18 clean" claim made about Ad Analytics Q1, Click Analytics Q2/Q3,
Smart Grid Q2, Log Processing Q2, TPCH Q2, and Google cloud Monitoring Q2
earlier in this session was based on the sweep's own `"ok"` submission
status, never an actual null-content check. (2) The underlying data
problem this blind spot hid: all 20 affected files' timestamps cluster
in one narrow window (2026-08-02 00:55-03:57) -- consistent with
Prometheus being unreachable from the machine running the sweep during
that stretch (a local network hiccup, not a cluster-side outage: `psql`
DDL/SQL submission to RisingWave itself succeeded fine throughout, since
`run_one_combo()` only checks for a submission `ERROR` string, never
metrics completeness, so the combo was correctly marked `"ok"`).

**Fix**: re-ran the 20 affected combos (all 18 of Ad Analytics Q1, plus
Click Analytics Q2's `p2/r100000` and `p2/r500000`) after confirming both
the RisingWave cluster and Prometheus (`curl .../api/v1/query?query=up`)
were healthy -- all 20 came back with real, non-null data on retry
(Ad Analytics Q1 dropped from 288 null findings to 33, all of which are
the already-known sparse bug #19 `node_memory` pattern; Click Analytics
Q2 dropped to 0). `check_nulls()` itself was *not* changed to also cover
`ADDITIONAL_QUERY_VARIANTS` -- the ad hoc per-workload `find_nulls()`
check used here to verify the fix is a reasonable substitute for a
one-time check, but a real fix (parameterizing `check_nulls()` the same
way `run_additional_variants.py` reassigns `WORKLOADS`) is worth doing if
this project runs more RisingWave sweeps in the future.

**Files**: none changed in `run_sweep_risingwave.py` for this bug
specifically (see bug #26 immediately below for a related, permanent code
fix found via the same re-run); `check_nulls()`'s coverage gap documented
here but left as-is.

---

### 26. `geo_ip_lookup` table bloat (bug #24) recurred -- `drop_all()` still had no cleanup for auxiliary TABLEs; fixed permanently this time via `WORKLOAD_AUX_TABLES`

**Symptom**: re-running Click Analytics Q2's 2 combos (see bug #25) hit
the exact same `TimeoutExpired` on SQL submission as bug #24 originally
did. `SELECT COUNT(*) FROM geo_ip_lookup` showed 4096 rows (bug #24's
first occurrence found 512) -- the table had kept silently accumulating
duplicate rows across every Click Analytics Q2 combo run since bug #24
was "fixed" the first time, because that fix was a one-off manual `DROP
TABLE`, not a code change -- `drop_all()` genuinely never gained any
logic to clean up auxiliary tables.

**Fix (this time, a real code fix)**: added a `WORKLOAD_AUX_TABLES` dict
(`{"Click Analytics Q2": ["geo_ip_lookup"]}`) and extended `drop_all()`
to accept an optional `workload_name` parameter, issuing `DROP TABLE IF
EXISTS <table>;` for each of that workload's auxiliary tables after the
usual SINK/MATERIALIZED VIEW/SOURCE drops (still `DROP TABLE`, not
`TRUNCATE` -- RisingWave doesn't support `TRUNCATE TABLE`, per bug #24).
Both call sites in `run_one_combo()` (the pre-CREATE cleanup and the
post-run cleanup) now pass `workload_name=workload_name`. Manually
cleared the bloated table once more (`RECOVER` first, since the `DROP
TABLE` itself hit the familiar barrier-latency stall -- see bug #20's
recurring-slowdown notes), then re-ran the 2 combos successfully with the
new code in place -- future sweeps/re-runs of Click Analytics Q2 will
self-clean this table every time via `drop_all()` rather than needing
another manual intervention.

**Files**: `PStreamBench_controller/dsp_be/benchmark_sweep/run_sweep_risingwave.py`
(`WORKLOAD_AUX_TABLES`, `drop_all()`, both call sites in `run_one_combo()`)

---

### 27. Default 4-core trial license silently breaks barrier recovery on any >4-core cluster -- root cause of the recurring "every RisingWave process gone, no explanation" crash (first seen 2026-07-23, plagued this project for ~a week)

**Symptom**: meta (and eventually the whole RisingWave process tree on
every node) would die completely, with no crash-worthy log line at the
point of death -- previously worked around each time via a full manual
restart (see `risingwave-meta-start.yaml`'s own 2026-07-23 comment,
written before this bug was understood). This session, caught it live:
`risingwave-meta.log` showed a continuous, ever-more-deeply-nested
`adhoc_recovery{error=adhoc recovery triggered}:recovery_attempt:...`
chain -- every single `CREATE MATERIALIZED VIEW`/recovery cycle from
2026-08-13T14:50:30 onward failed and immediately re-triggered another
nested recovery attempt, for over 12 hours straight, until meta finally
died. The actual trigger, several nesting levels down at the very first
occurrence: `WARN ... DatabaseFailureIsolation disabled by license
error=feature DatabaseFailureIsolation is not available due to license
error: a valid license key is set, but it is currently not effective
because the CPU core in the cluster (96) exceeds the maximum allowed by
the license key (4)`.

**Root cause**: RisingWave ships with a built-in default trial license
(`rw-default-all-4-core`, `rwu_limit: Some(4)`) baked into the binary --
not something any install script or playbook configures. It auto-activates
the moment meta detects more cores in the cluster than the limit allows.
Every node in this deployment is 16 cores, so any real multi-node cluster
trips it on the very first recovery cycle (a normal, frequent event --
e.g. any new streaming job submission can trigger one). Once tripped, the
license check disables `DatabaseFailureIsolation`, and that failure itself
prevents the recovery cycle from ever completing cleanly, so it
immediately retriggers another recovery attempt -- forever, with each
cycle nesting one level deeper than the last, until the process
eventually dies (exact final failure mode not pinned down -- by the time
it's checked, the process is just gone).

**Fix**: `ALTER SYSTEM SET license_key = '';` -- confirmed live this
completely disables license enforcement (not just resetting to another
default): the exact same `CREATE SOURCE`/`CREATE MATERIALIZED VIEW`
sequence that previously triggered the endless recovery loop now
completes normally, and a forced `RECOVER;` resolves in a single clean
cycle instead of nesting. Since meta runs with `--backend mem`, this and
every other system param resets to the binary's default on every meta
restart -- a one-off `psql` fix does not stick, it must be reapplied after
every single restart. Made permanent by adding a task to
`risingwave-frontend-start.yaml` (runs on the Ansible control host via
`delegate_to: localhost`, since the RisingWave nodes themselves don't have
`psql` installed) that waits for the frontend port and clears the license
key immediately after every frontend start -- covers both the normal
cluster-create path and any future manual restart via
`cluster-complex-risingwave-master-start.yaml`.

**Files**:
`PStreamBench_controller/dsp_be/utils/playbook/tasks/risingwave/complex/risingwave-frontend-start.yaml`
(new `wait_for port 4566` + license-clear tasks, appended after frontend start)

---

### Resolution of bug #19: `node_memory_{98,95,50}` intermittent null, confirmed and fixed

The leading hypothesis in bug #19 (a scrape-gap race inside the
aggregated `sum()`) turned out not to be the mechanism -- the real cause
was `_resolve_memory_formula()`'s own metric-existence probe being pinned
to the historical `timestamp_prom` instant, like every other query in the
file. Unlike the actual data queries (where pinning is correct and
necessary), this probe only decides *which metric-naming convention* the
node_exporter uses -- a static fact about the deployment that never
changes mid-sweep. Pinning it to a specific historical second meant that
if Prometheus's own PromQL evaluator had ~any staleness at exactly that
instant (observed empirically in ~51% of real runs), the probe found no
result, silently fell back to the Darwin/local-dev metric names (which
don't exist on real CloudLab nodes), and every downstream
`node_memory_*` query came back null for that run.

**Fix**: removed the `@ timestamp_prom` pin from the probe query only
(`node_memory_MemTotal_bytes{job="node_exporter"}`, unpinned = "does this
metric exist right now", which is what the question actually is). All
other memory/CPU/network queries in the same file remain correctly pinned
to `timestamp_prom` -- only the naming-convention probe needed to change.

**Files**: `infra/reporting_scripts/getprom_risingwave.py`
(`_resolve_memory_formula`)

---

### 28. Ansible's generic `cluster-complex-kafka-start.yaml` overwrote every worker's Kafka broker.id/zookeeper.connect with the master's own values

**Symptom**: after a routine infra-level restart (`/infra/start/`) of an
already-running 6-node cluster, every worker's `server.properties` ended
up with `broker.id=0` and `zookeeper.connect=localhost:2181` -- identical
to the master's, instead of each worker's own unique id and
`zookeeper.connect=<master-host>:2181`. Only one broker (whichever last
won the race to bind) ended up actually registered in ZooKeeper.

**Root cause**: the per-host `id` Ansible variable (used by
`kafka-start.yaml`'s `--override broker.id={{ id }}` and to select
localhost vs. remote in the zookeeper.connect template) is written into
`inventory.yml` by `_inventory_manipulation()` based on each host's
*position within the list passed to that one call* -- not a globally
stable per-host identity. Any code path that calls
`_inventory_manipulation()` with a single-host list (e.g. targeting just
the master for an unrelated task) resets `id` to `0` for that host in the
inventory file, and if a later playbook run reuses the same inventory
file without regenerating it for the full 6-host group first, every host
in that stale inventory silently inherits whatever `id` was last written
for it.

**Fix**: always regenerate the full inventory (all 6 hosts, correct
group) via `_inventory_manipulation()` immediately before any Kafka-start
playbook invocation -- never assume a previous call's inventory state is
still correct. Confirmed by directly editing each worker's
`server.properties` with its correct `broker.id` (matching its persisted
`meta.properties`) and `zookeeper.connect=<master>.utah.cloudlab.us:2181`.

**Files**: `infra/kafka_files/server.properties` (per-node, not
version-controlled -- fixed live via SSH each time this recurs)

---

### 29. Editing `manifest_strategy_risingwave.json` while the sweep process is running gets silently discarded

**Symptom**: manually fixing a manifest entry (e.g. clearing a bad `"ok"`
status to force a re-run) would appear to work, then silently revert back
to the old value a few minutes later, with no error anywhere.

**Root cause**: `run_strategy_sweep_risingwave.py` loads the entire
manifest into memory once at startup and writes its own in-memory copy
back to disk after every single combo -- any edit made to the file on
disk while the process is still running gets overwritten by the next
`json.dump()`, with no merge or reload logic.

**Fix**: no code change (this is a legitimate design for
crash-safety/atomicity, not a bug in the writer) -- established as a hard
operating rule instead: always find and kill the running
`run_strategy_sweep_risingwave.py` process before editing the manifest
file directly, then relaunch (which reloads the now-correct file from
disk and resumes via its normal skip-already-done logic).

**Files**: `benchmark_sweep/run_strategy_sweep_risingwave.py` (behavior
documented, not changed)

---

### 30. `VENV_PY` hardcoded to a path that silently stopped existing after the sweep venv was relocated

**Symptom**: the outer `run_strategy_sweep_risingwave.py` process ran
fine (launched with a working venv's interpreter), but every metrics
collection silently failed -- `custom_producer.py`/`getprom_risingwave.py`
subprocess calls were dying with `ModuleNotFoundError: No module named
'click'`, even though `click` was installed and importable in the venv
actually running the outer script.

**Root cause**: `run_sweep_risingwave.py` has its own separate
`VENV_PY` constant, hardcoded to the *original* venv path
(`/private/tmp/.../scratchpad/sweep_venv/bin/python3`), used to invoke
every subprocess. When the venv was relocated to a stable in-project path
after repeated corruption (see #31), only the *outer* invocation command
changed -- the hardcoded `VENV_PY` constant inside the script itself still
pointed at the old, now-nonexistent path, so every subprocess silently
ran under whatever bare `python3` was found instead (missing the venv's
packages).

**Fix**: `VENV_PY` now derives its path relative to the script's own
location (`Path(__file__).parent / ".sweep_venv" / "bin" / "python3"`)
instead of a hardcoded absolute string, so relocating the venv can never
silently desync the two.

**Files**: `benchmark_sweep/run_sweep_risingwave.py`

---

### 31. The sweep's Python venv, kept under macOS's `/tmp` scratch directory, was silently wiped by the OS multiple times mid-sweep

**Symptom**: `run_strategy_sweep_risingwave.py` (and its subprocess
calls) started failing with `ModuleNotFoundError` for previously-installed
packages (`click`, then later `pandas`/`confluent_kafka`), with no local
change having been made -- recurred three separate times across the
multi-day sweep.

**Root cause**: the venv lived under
`/private/tmp/claude-503/.../scratchpad/`, a path subject to macOS's
periodic temp-directory cleanup, which can remove files independent of
any process still referencing them.

**Fix**: after the third recurrence, relocated the venv to a stable
in-project path (`benchmark_sweep/.sweep_venv`), never under any `/tmp`
variant, at the user's explicit direction. Combined with the fix in #30,
this made the venv location durable across the rest of the sweep with no
further corruption.

**Files**: `benchmark_sweep/.sweep_venv` (new location, not
version-controlled)

---

### 32. `subprocess.Popen`-spawned `custom_producer.py` processes survive their parent's SIGTERM

**Symptom**: after killing a hung/stale `run_strategy_sweep_risingwave.py`
process, a `custom_producer.py` process from whatever combo it was
running mid-kill kept running orphaned, still writing to Kafka topics
that a subsequent relaunch's combo would then also be writing to
concurrently, corrupting throughput metrics for both.

**Root cause**: Python's `subprocess.Popen` children are not
automatically terminated when their parent process receives SIGTERM
(unlike, e.g., a process group kill) -- the parent script exiting doesn't
propagate to its own subprocess children.

**Fix**: no code change -- added "check for and kill any orphaned
`custom_producer.py` not matching the current combo" as a mandatory step
immediately after every sweep-process kill, as part of the standing
diagnostic routine (sometimes needing a second check a few seconds later,
since a producer already in its own shutdown sequence can take a moment
to actually exit).

**Files**: none (operating procedure, not a code fix)

---

### 33. An in-flight (not-yet-committed) streaming job invisible to `drop_all()`'s catalog-based cleanup blocks the next combo

**Symptom**: when a `CREATE MATERIALIZED VIEW` genuinely took longer than
`run_one_combo()`'s own SQL-submission timeout and got abandoned, the job
kept running server-side but never became a committed catalog entry -- so
`drop_all()`'s `rw_catalog.rw_materialized_views` lookup could never see
it, and the next combo's `CREATE` for the same object name failed with a
name collision that persisted across every subsequent retry.

**Root cause**: `SHOW JOBS` is the only place an in-flight (uncommitted)
streaming job is visible in RisingWave -- the catalog views used
everywhere else in `drop_all()` only reflect committed objects.

**Fix**: added `cancel_stale_jobs(prefix)`, which queries `SHOW JOBS` for
any job whose statement text matches the current workload's object-name
prefix and issues `CANCEL JOBS <id>` for it, wired into `drop_all()`
before its existing catalog-based DROP statements.

**Files**: `benchmark_sweep/run_sweep_risingwave.py`
(`cancel_stale_jobs`, called from `drop_all`)

---

### 34. A metrics file existing was treated as success, even when its core fields were null

**Symptom**: some "ok" manifest entries had genuinely unusable data --
`getprom_risingwave.py` had written *a* file, but with
`queryMetrics.kafka_throughput`/`kafka_event_rate` or
`hardwareMetrics.node_memory_98` null inside it (root causes: bug #19's
resolution above, and the Prometheus-corruption incident in #36) -- yet
the sweep's own success criterion was just "did a new file appear."

**Fix**: added `_check_core_metrics_present(metrics_file)`, checking a
fixed set of load-bearing fields after every combo. On any null field,
the combo's status becomes `"null_core_metrics"` (not `"ok"`, so it's
automatically retried on the next relaunch), and the bad file plus its
`metaInfo` sibling are deleted rather than being moved into the strategy
output folder.

**Files**: `benchmark_sweep/run_sweep_risingwave.py`
(`_check_core_metrics_present`, called from `run_one_combo`)

---

### 35. MinIO fully down was repeatedly misdiagnosed as per-workload capacity limits

**Symptom**: a large batch of combos across several unrelated workloads
kept failing, initially attributed to those specific workloads
genuinely exceeding cluster capacity at high parallelism.

**Root cause**: MinIO (the Hummock object store backing every
RisingWave node) had actually crashed entirely -- every one of those
"capacity" failures was really every `CREATE MATERIALIZED VIEW` failing
to persist state at all, regardless of workload or parallelism.

**Fix**: restarted MinIO and recreated its bucket. The general lesson
folded into the standing diagnostic routine: before attributing a spread
of failures to workload-specific capacity, always check MinIO
(`curl :9301/minio/health/live`) first -- a single shared-infra outage can
look exactly like many unrelated per-workload failures.

**Files**: none (diagnostic-routine addition)

---

### 36. Wiping Prometheus's TSDB data directory while Prometheus is still running corrupts it silently

**Symptom**: after clearing disk space by `rm -rf`-ing several data
directories including Prometheus's own `data/` (without stopping
Prometheus first), every subsequent combo failed with
`null_core_metrics` on `queryMetrics.kafka_throughput.throughput` --
Prometheus's own `/targets` endpoint showed every scrape target healthy
("up"), and scraping `:7075/metrics` directly returned real data, but
Prometheus's own PromQL query engine silently returned empty results for
everything.

**Root cause**: Prometheus keeps its TSDB state in memory and only
periodically flushes to the on-disk WAL/blocks it was pointed at; ripping
out that directory out from under a live process desyncs its in-memory
view from what's actually on disk, without crashing the process or
failing any health check that doesn't specifically query real data.

**Fix**: `sudo kill -9` the corrupted Prometheus process (root-owned) and
restart fresh via `setsid sudo .../prometheus --storage.tsdb.path=...`.
Established as a hard rule: never clear a running service's own data
directory -- always stop the service first, clear its directory, then
restart it. Verified recovery via a real PromQL query
(`count(kafka_server_brokertopicmetrics_messagesin_total)`), not just
`/-/healthy`, since the corrupted state also returned 200 on the health
check.

**Files**: none (operating procedure)

---

### 37. Kafka broker 0 fails to start with "all log dirs have failed" after a sudo-based Ansible restart leaves root-owned partition directories behind

**Symptom**: after an Ansible-driven Kafka restart (which runs with
`become: true`/sudo), a later manual, non-sudo restart of the same broker
failed immediately with `Shutdown broker because all log dirs ...
/home/playground/kafka-logs have failed`.

**Root cause**: the earlier sudo-based start created new
topic-partition subdirectories under `kafka-logs` owned by `root`; the
non-sudo process couldn't write to them.

**Fix**: `sudo chown -R <user>:root /home/playground/kafka-logs`.
Recurred at least twice; added to the standing diagnostic playbook as a
first check whenever broker 0 is missing from ZooKeeper's `/brokers/ids`.

**Files**: none (operating procedure)

---

### 38. `nohup ... & disown` over SSH doesn't reliably survive the SSH session closing

**Symptom**: manual Kafka/Prometheus restarts launched via
`ssh host "nohup cmd > log 2>&1 & disown"` sometimes left the log file
empty and no process actually running, once the SSH connection itself
closed.

**Fix**: switched to `setsid` for every manual remote daemon start
(`ssh host "sudo setsid /bin/bash -c 'cmd > log 2>&1 < /dev/null &'"`),
which detaches the process into its own session reliably regardless of
what happens to the SSH connection afterward.

**Files**: none (operating procedure)

---

### 39. `AnsibleClusterCreate`'s free-node selection picks from the *entire* project's node history, not the caller's intended hosts -- silently built Kafka/ZooKeeper and Prometheus scrape configs against an unrelated, long-dead cluster's hostnames

**Symptom**: after provisioning 6 brand-new CloudLab nodes and calling
`POST /infra/create`, the RisingWave master (meta+frontend) came up
correctly on the intended master host, but no Kafka broker or ZooKeeper
process ever started on *any* of the 6 new nodes, and once that was
fixed manually, every single sweep combo still failed with
`null_core_metrics` on every hardware/kafka/risingwave-worker metric
field.

**Root cause, two-part**: (1) `AnsibleClusterCreate`'s view logic builds
its "free nodes" list from `Nodes.objects.all()` -- every node this
project has *ever* registered, in DB order, filtered only by whether a
*currently-existing* `Cluster` row claims it -- not from any host list the
caller actually specifies. There is no way to tell the endpoint "use
exactly these 6 hosts." When several other already-registered-but-free
`Nodes` rows from long-unrelated, physically-defunct past experiments
(reused CloudLab hostnames from a stale `Cluster` row -- see #40) sorted
earlier in that global list than the intended new workers, they were
silently selected as the Kafka/ZooKeeper ensemble members and passed to
`_prometheus_config_manipulation()`'s worker list instead of the real
hosts. (2) Because the ghost hosts are unreachable, the multi-node
ZooKeeper ensemble (one instance per Kafka node, ensemble members listed
identically in every node's `zookeeper.properties`) could never form
quorum on any node, so no Kafka broker anywhere ever opened its port; and
Prometheus's scrape config, generated from that same wrong worker list,
had no `node_exporter`/Kafka/RisingWave-compute targets pointing at any
of the real hosts at all -- every scrape for those jobs targeted
nonexistent hosts and showed "down", so every hardware/kafka-dependent
metric query came back empty, discarding every combo.

**Fix (workaround, not a code change to the view)**: bypassed the
free-node auto-selection entirely for cluster (re)creation against
specific known hosts -- called `_prometheus_config_manipulation()` and the
Kafka/RisingWave Ansible playbooks directly via a Django-shell script
with the exact intended host list, instead of going through
`POST /infra/create`. Manually rewrote `server.properties`
(broker.id/zookeeper.connect) and `zookeeper.properties` (single-ZK
topology, not a 6-node ensemble -- matching this project's proven
pattern elsewhere) on all 6 real hosts, and regenerated + repushed
Prometheus's config against the real worker list. A proper fix would let
`POST /infra/create` accept an explicit host list rather than
auto-selecting from the global `Nodes` pool.

**Files**: `infra/views.py` (`AnsibleClusterCreate`, `nodes_free`
computation -- not changed, workaround applied instead);
`infra/kafka_files/server.properties`,
`infra/prometheus_files/prometheus.yml` (regenerated/fixed live per-node)

---

### 40. Stale `Cluster` DB rows from CloudLab-terminated experiments block new cluster creation on any host that gets reused

**Symptom**: `POST /infra/create` against 6 brand-new nodes failed
immediately with `"No free nodes present based on the requirements"`,
even though all 6 nodes had just been freshly registered and were fully
reachable.

**Root cause**: a `Cluster` row from a long-abandoned experiment
(`risingwave-cluster-2026`) still listed one of the new nodes' hostnames
as its `main_node_ip` -- CloudLab hostnames get reassigned to new,
unrelated experiments over time, and this project's `Cluster` table is
never cleaned up when the underlying CloudLab experiment simply expires
(as opposed to being explicitly torn down via the app's own delete
flow) -- so the stale DB row alone was enough to make the free-node check
treat that host as occupied.

**Fix**: identified and deleted the stale `Cluster` row via a Django
shell one-off. General lesson: when a `POST /infra/create` unexpectedly
reports "no free nodes" against hosts known to be genuinely free,
check for `Cluster` rows referencing those hostnames before assuming the
nodes themselves are the problem -- this is exactly the same class of bug
as #39 (both rooted in stale/unscoped node-selection state), just
surfacing as a hard failure instead of a silent misconfiguration.

**Files**: none (DB cleanup, not a code fix)

---

### 41. Re-running a RisingWave "-create" playbook against an already-running master crashes the live frontend instead of being a no-op

**Symptom**: re-invoking
`cluster-complex-risingwave-master-create.yaml` against a master node
whose meta+frontend were already healthy and actively serving a running
benchmark sweep caused the live frontend process to crash
(`Address already in use` panic, `Aborted`) moments later, taking down
the running sweep's SQL connectivity.

**Root cause**: the "-create" playbook's meta/frontend-start tasks
unconditionally attempt to launch a new process on the same ports as
whatever is already running, with no check for an existing healthy
instance. Meta's own start task's *new* attempt failed to bind (port
already held) but didn't crash the *existing* meta process, so the
in-memory catalog and running state survived intact -- but frontend's
new attempt panicked hard enough to abort the process, killing the
already-running frontend as collateral damage.

**Fix (operating rule, not a code change)**: never re-run a "-create"
playbook against a node with services already running -- use the
matching idempotent "-start" playbook instead if a restart is actually
needed, or nothing at all if the goal is just to push a config change
that doesn't require a service restart (e.g. push the file via `scp`/
`tee` and restart only that one service directly). Recovered here by
manually killing the crashed frontend, freeing the now-orphaned port
(`fuser -k -n tcp 4566`), and restarting frontend alone -- meta, and the
catalog it held in memory, were never actually lost.

**Files**: none (operating procedure)

---

### 42. A killed HTTP client doesn't stop the Django dev server from continuing a long-running synchronous view

**Symptom**: after killing a `curl POST /infra/create` call locally
(client-side timeout) because it had been running far longer than
expected, the actual cluster-creation work (Ansible playbooks starting
Kafka, ZooKeeper, MinIO, RisingWave meta+frontend) was found, on
inspection, to have continued running and partially completed on the
server for several more minutes after the client gave up -- with no way
to observe its progress or final outcome from the client side, since the
response was never received.

**Root cause**: `manage.py runserver`'s synchronous, single-threaded
per-request handling doesn't check whether the client socket is still
open while executing a long blocking view body (the Ansible calls here
run in-process, not as a detached subprocess) -- it only notices the
client is gone when it finally tries to write the HTTP response back,
by which point all the real work has already happened (or failed) with
no way to affect it further from the client side.

**Fix**: none needed in code -- established as an operating rule: for
any endpoint known to run long Ansible-backed work synchronously, always
verify actual infrastructure state directly (`ps aux`, `psql`, `curl`
health checks) after a client-side timeout, rather than assuming the
request "failed" just because the client never got a response -- it may
have completed, partially completed, or still be running server-side.

**Files**: none (operating procedure)

---

## Methodology notes (how these were actually found, in general)

- **Never trusted "no error" as "it worked"** — RisingWave's `CREATE
  MATERIALIZED VIEW` can succeed while the query semantically produces zero
  rows forever (bugs #3, #9 both looked like "successful" DDL until the
  actual output was queried).
- **Bisection over guessing**: for every "MV creates fine but never emits
  data" symptom, built the query up stage by stage (raw HOP output → GROUP
  BY → final SELECT) as separate throwaway materialized views, checking row
  counts at each stage, rather than guessing which layer was broken.
- **Cross-checked assumptions against the real, authoritative producer
  code** (`custom_producer.py`) before running anything live — this alone
  caught bug #7 before it ever produced a silent failure.
- **Used official documentation/web search** for RisingWave-specific
  semantics (emit-on-update vs. emit-on-window-close, watermark
  requirements) when local experimentation alone wasn't conclusive, rather
  than continuing to guess against a live cluster.
- **Distinguished infrastructure flakiness from real bugs**: a laptop-sleep
  event, a CloudLab node reboot, and a transient sudo timeout all produced
  symptoms that superficially looked like SQL bugs; each was confirmed via
  process/log inspection (uptime, `ps aux`, timestamps in logs) before
  concluding it wasn't code-related.
- **Validated the actual metrics-collection path, not just SQL correctness**:
  ran the real `getprom_risingwave.py` CLI end-to-end for all 24 combos and
  scanned the written JSON for nulls, rather than assuming "the SQL runs" is
  the same as "clean metrics get collected" — this is what caught bugs #13
  and #14.
- **Didn't stop at "confirmed present" — checked whether it reproduced
  consistently**: bug #14 initially looked like a real bug (7/24 combos
  affected) until noticing it wasn't consistent across combos with identical
  window configs, which pointed at a timing race rather than a code defect.
