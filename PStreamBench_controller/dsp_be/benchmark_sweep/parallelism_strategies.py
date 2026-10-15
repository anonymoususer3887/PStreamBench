"""
Parallelism-assignment strategies for the RisingWave benchmark sweep.

Every workload has a fixed number of "controllable stages" -- one per
top-level `CREATE MATERIALIZED VIEW` statement in its SQL file (source is
always fixed at 2, sink always fixed at 1, matching the other engines'
job_pds convention -- see run_sweep_risingwave.py's module docstring for why
RisingWave's per-fragment execution model makes this the practical
equivalent of "middle stages" for the other engines' arrays). Word Count
and Ad Analytics each have exactly one controllable stage (their single
MV); Machine Outlier has two (`machine_outlier_reference` and
`machine_outlier_mv`), confirmed via `grep '^CREATE MATERIALIZED VIEW'`
against its SQL file -- any strategy here must work for both shapes.

Each strategy function takes the available parallelism levels and the
number of controllable stages for the workload being swept, and returns a
list of "assignments" -- each assignment is a list of ints, one per stage,
in the same top-to-bottom order the stages' CREATE statements appear in
the SQL file. run_sweep_risingwave.py's build_sql_with_parallelism()
consumes these positionally.

Every reviewer-facing motivation is inline on each strategy, not just in
this module docstring, since that's the point of this file existing at
all: uniform parallelism (the same value for every stage) was flagged as
unrealistic for real-world/industrial deployments, where different
operators legitimately need different degrees of parallelism depending on
their own computational cost and data volume.

MIN_PARALLELISM/MAX_PARALLELISM bound every strategy's output: 2 is the
project's existing floor (matches PARALLELISM_LEVELS[0] in
run_sweep_risingwave.py), 40 is the cluster's actual capacity (5 worker
nodes x 8 cores each) -- also already PARALLELISM_LEVELS[-1]. The
enumeration-based strategies above (uniform/exhaustive/minavgmax/
increasing/random) already only ever pick from a caller-supplied `levels`
list, so as long as that list stays within [2, 40] they need no separate
clamp. ds2_strategy and dhalion_strategy below compute an arbitrary
integer from live measurements instead of picking from `levels`, so they
clamp explicitly against these two constants.

Every strategy function in this module -- including ds2_strategy and
dhalion_strategy -- takes only plain ints/floats/lists/dicts, never a
RisingWave-specific object (no SQL, no psycopg2 connection, no
rw_catalog lookup). This is deliberate: none of this file is RisingWave
code, and every function here is meant to be reusable, unmodified, for
Flink/Storm/Spark/Kafka Streams sweeps too, not just RisingWave's. The
one place engine-specific knowledge is allowed to leak in is a loader
function that turns a given engine's own evaluationMetrics-*.json into
the plain dict shape ds2_strategy/dhalion_strategy expect --
load_stage_metrics_risingwave() below is that adapter for RisingWave;
load_stage_metrics_spark() is the Spark one, added for the Spark
enumeration-strategy sweep (2026-08-25) once getprom_spark.py grew a
busy_ratio/backpressure_ratio of its own -- derived from Spark's built-in
input_rate/processing_rate (job-wide, replicated onto every stage, since
Structured Streaming exposes no per-operator timing at all; see
getprom_spark.py's _capacity_ratio docstring for the full reasoning).
getprom_kafkastreams.py already shares the *other* two fields these
strategies and heuristic_strategy use (throughput_per_sec, selectivity --
same key names, confirmed by inspection), but has no busy/idle-time or
backpressure signal yet. load_stage_metrics_storm() (added 2026-09-02) is
the Storm adapter -- Storm has genuine per-bolt "capacity" (num executed *
avg execute latency / time window), the same "fraction of time busy"
concept Dhalion's own source system already uses, so busy_ratio/
backpressure_ratio are real per-stage signals there, not a job-wide proxy
like Spark's. Extending getprom_kafkastreams.py/getprom.py (Flink) to
carry busy_ratio/backpressure_ratio and adding their own
load_stage_metrics_<engine>() adapters here is what "common strategy, any
streaming system" means in practice -- done so far for RisingWave, Spark,
and Storm, not yet for Kafka Streams/Flink, flagged here rather than
silently assumed.

FLINK SPECIFICALLY HAS ITS OWN NATIVE AUTOSCALER -- this is NOT the same
thing as this project's ds2_strategy/dhalion_strategy and should not be
treated as a pre-built substitute for the Flink adapter above (confirmed
2026-09-11 by reading
https://nightlies.apache.org/flink/flink-kubernetes-operator-docs-main/docs/managing/autoscaler/
directly, not assumed from the name alone):
  - Algorithm: NOT based on the DS2 paper at all. It sizes parallelism off
    Required Capacity = Steady-State + Catch-Up (drain existing backlog
    within a configurable window, default 30min) + Restart Allowance
    (buffer for backlog accumulated during the rescale itself), evaluated
    against a target *utilization band* (default scale-up above 1.0,
    scale-down below 0.4, aiming for ~0.7) -- not our DS2's direct
    true_rate = throughput/busy_ratio solve for an exact parallelism value.
  - Operational model: a continuously-running closed control loop that
    keeps rescaling indefinitely (in-place via the Adaptive Scheduler, or
    via restart cycles) -- not a one-shot observe-once/redeploy-once nudge
    like every strategy in this file. This project deliberately keeps
    every strategy single-shot for fair cross-engine comparability (see
    dhalion_strategy's docstring on why the repeated control loop is out
    of scope even for engines that support it) -- Flink's native
    autoscaler doesn't fit that shape at all, it's a different kind of
    thing being compared, not an alternate implementation of the same
    thing.
  - Deployment requirement: only ships with the Flink Kubernetes Operator
    (native or its standalone-process variant polling the REST API) -- NOT
    a feature of a plain standalone/Ansible-provisioned cluster, which is
    how every engine in this project (RisingWave, Spark, Storm, and
    presumably Flink too once its own sweep is built) has actually been
    deployed. It will not be silently active during this project's own
    Flink runs unless the cluster is specifically switched to running
    under the K8s Operator.
  Conclusion: build load_stage_metrics_flink() and reuse ds2_strategy/
  dhalion_strategy for Flink exactly like every other engine here, for the
  same one-shot/comparable-across-engines reasons -- Flink's own
  autoscaler is a separate, heavier-weight feature that would need its own
  dedicated comparison if ever evaluated, not a shortcut around this
  module.
"""
import json
import math
import statistics

MIN_PARALLELISM = 2
MAX_PARALLELISM = 40  # 5 worker nodes x 8 cores/node


def _clamp(value, min_parallelism=MIN_PARALLELISM, max_parallelism=MAX_PARALLELISM):
    return max(min_parallelism, min(max_parallelism, value))


def uniform_strategy(levels, num_stages):
    """The ORIGINAL approach (still here, named honestly) -- every stage
    gets the exact same value, swept through every level. This is exactly
    what a reviewer would call unrealistic if used as the *only* strategy:
    real operators rarely have identical resource needs. Kept as a
    baseline/control to compare the other strategies against, not removed.
    """
    return [[level] * num_stages for level in levels]


def exhaustive_strategy(levels, num_stages):
    """True cartesian product -- every unique combination of parallelism
    degrees across every controllable stage, independently. For a
    single-stage workload (Word Count, Ad Analytics) this is identical to
    uniform_strategy. For a multi-stage workload (Machine Outlier, 2
    stages) this is len(levels)**num_stages assignments -- e.g. 36 for the
    6-level default, not 6 -- since each stage can independently be any
    level. Gated by the caller (see run_sweep_risingwave.py) behind a
    stage-count check, since this grows combinatorially and was the
    direct cause of this project's cluster-time problems even in the
    single-stage case.
    """
    def _product(remaining_stages):
        if remaining_stages == 0:
            return [[]]
        rest = _product(remaining_stages - 1)
        return [[level] + r for level in levels for r in rest]

    return _product(num_stages)


def minavgmax_strategy(levels, num_stages):
    """3 representative points (min/avg/max of the level set) instead of
    every level, applied uniformly across all stages -- roughly half the
    cluster-time of a full uniform sweep for the same "low vs. medium vs.
    high resource intensity" story. avg is the closest available level to
    the arithmetic mean of the full level set (not necessarily itself a
    member of `levels`), so the "avg" point is still one of the real
    values you can actually pass to streaming_parallelism.
    """
    lo, hi = min(levels), max(levels)
    mean = statistics.mean(levels)
    avg = min(levels, key=lambda l: abs(l - mean))
    picks = sorted(set([lo, avg, hi]))
    return [[level] * num_stages for level in picks]


def increasing_strategy(levels, num_stages, baseline=None):
    """One-factor-at-a-time: hold every stage at a fixed baseline (the
    minimum level, unless overridden) except one, and step that one stage
    through every level from min to max -- repeated once per stage, moving
    up the dataflow graph (stage 0 = the first CREATE MATERIALIZED VIEW in
    the SQL file, i.e. closest to the source). This isolates each
    individual stage's marginal effect on throughput/latency, which a
    uniform or exhaustive sweep can't cleanly show (in an exhaustive sweep
    every stage changes at once, so you can't attribute an effect to any
    one of them). For a single-stage workload this produces the same
    assignments as uniform_strategy (there's only one stage to increase),
    which is expected -- the strategy's value is specifically for
    multi-stage workloads like Machine Outlier.
    """
    baseline = baseline if baseline is not None else min(levels)
    assignments = []
    for stage_idx in range(num_stages):
        for level in levels:
            assignment = [baseline] * num_stages
            assignment[stage_idx] = level
            assignments.append(assignment)
    return assignments


def random_strategy(levels, num_stages, num_samples, seed=None):
    """Independently draws a random level for each stage, `num_samples`
    times. Unlike uniform/minavgmax/increasing (which are all
    deterministic and reproducible), this is meant to explore
    combinations a human wouldn't necessarily think to test by hand --
    e.g. a source-heavy stage paired with a lightly-parallelized
    downstream stage, which uniform assignment can never produce at all.
    `seed` makes a given run's random choices reproducible for
    re-analysis; omit it for genuinely fresh randomness each call.
    """
    import random
    rng = random.Random(seed)
    return [[rng.choice(levels) for _ in range(num_stages)] for _ in range(num_samples)]


def heuristic_strategy(levels, num_stages, event_rate, selectivities):
    """Load-proportional parallelism -- the direct answer to "uniform
    parallelism isn't realistic": each stage's parallelism is derived from
    how much data actually reaches it, not assigned blind. Adapts this
    project's own existing rule-based formula
    (JobRunner._get_rulebased() in infra/runner.py, already used for the
    Flink/Storm/Spark sweeps -- not a new invention) to RisingWave's
    fixed-source/fixed-sink convention: stage 0 receives `event_rate`
    scaled by `selectivities[0]`, stage 1 receives stage 0's output volume
    scaled by `selectivities[1]`, and so on, and each stage's raw
    parallelism is `0.000006 * stage_input_volume ** 1.17` (the same
    power-law shape as the existing formula), rounded to the nearest
    available level so the result is always a value streaming_parallelism
    actually accepts.

    `selectivities` must have one entry per controllable stage, in the
    same top-to-bottom order as `levels`/assignments elsewhere in this
    module -- e.g. for Word Count's single aggregation stage, a
    selectivity of ~6.0 (one input line produces ~6 output words, matching
    the real ~6x selectivity this project measured live for its
    "tokenizer" stage, see BUGS_AND_FIXES.md) would push this stage's
    heuristic parallelism *up* relative to event_rate alone, reflecting
    that it's handling more, not less, data than the raw input rate
    suggests.

    Returns a single assignment (list of ints), not a list of assignments
    -- this strategy proposes one data-driven configuration per
    (workload, event_rate) pair, not a sweep across levels; combine with
    a loop over EVENT_RATES in the caller to get a heuristic assignment
    per rate.
    """
    if len(selectivities) != num_stages:
        raise ValueError(
            f"heuristic_strategy needs exactly one selectivity per stage "
            f"({num_stages} stages, got {len(selectivities)} selectivities)"
        )

    assignment = []
    stage_input = float(event_rate)
    for selectivity in selectivities:
        stage_input = stage_input * float(selectivity)
        raw_pd = 0.000006 * (stage_input ** 1.17)
        nearest_level = min(levels, key=lambda l: abs(l - raw_pd))
        assignment.append(nearest_level)
    return [assignment]


def load_stage_metrics_risingwave(evaluation_json_path, stage_names):
    """RisingWave ADAPTER for ds2_strategy/dhalion_strategy -- reads a real
    evaluationMetrics-*.json (getprom_risingwave.py's output, either from
    a live CloudLab run or a local `risingwave playground`/`single-node`
    run) and extracts operatorMetrics into the common stage_metrics dict
    shape both strategies expect: {"throughput_per_sec", "selectivity",
    "busy_ratio", "backpressure_ratio"}.

    This is named "_risingwave" deliberately, not "load_stage_metrics" --
    see this module's docstring for why ds2_strategy/dhalion_strategy
    themselves take a plain list of dicts (no engine-specific object)
    precisely so they work for ANY streaming engine, not just RisingWave.
    This function is the RisingWave-specific half of that: it knows how
    to turn getprom_risingwave.py's particular JSON shape into the common
    dict shape. A Flink/Storm/Spark/Kafka Streams sibling adapter would
    read from that engine's own getprom_*.py output instead and produce
    the exact same four keys -- see the module docstring for which of
    those fields already exist for which engines today.

    `stage_names` must be the getprom-side stage names for exactly the
    *controllable* stages, in pipeline order -- e.g. for Word Count,
    stage_relations in getprom_risingwave.py is
    [source, tokenizer, counter, sink], but only "counter" (the real
    stateful MVIEW fragment) is swept -- "tokenizer" fuses into source's
    fixed parallelism (see this module's docstring / build_sql_with_
    parallelism's comment in run_sweep_risingwave.py) -- so the caller
    passes stage_names=["counter"] here, not all four. This module stays
    workload-agnostic on purpose (same as every other strategy above,
    which take a plain `num_stages` int with no SQL/pipeline knowledge),
    so the caller -- which already tracks this mapping to build SQL with
    build_sql_with_parallelism -- is responsible for supplying the right
    subset and order.

    A stage missing from operatorMetrics (e.g. workload not yet
    instrumented, or the run genuinely produced no data for it) gets
    neutral defaults (0 throughput/busy/backpressure, 1.0 selectivity)
    rather than raising -- ds2_strategy/dhalion_strategy will then just
    leave that stage's parallelism unchanged, which is the same "can't
    compute a rate, don't guess" fallback both functions already apply
    for a zero true-rate.
    """
    with open(evaluation_json_path) as f:
        data = json.load(f)
    operator_metrics = data.get("operatorMetrics") or {}
    result = []
    for name in stage_names:
        stage = operator_metrics.get(name) or {}
        result.append({
            "throughput_per_sec": stage.get("throughput_per_sec") or 0.0,
            "selectivity": stage.get("selectivity") if stage.get("selectivity") is not None else 1.0,
            "busy_ratio": stage.get("busy_ratio") or 0.0,
            "backpressure_ratio": stage.get("backpressure_ratio") or 0.0,
        })
    return result


def load_stage_metrics_spark(evaluation_json_path, stage_names):
    """Spark ADAPTER for ds2_strategy/dhalion_strategy -- same contract and
    same four output keys as load_stage_metrics_risingwave() above (see
    that function's docstring for the general shape), reading a Spark
    evaluationMetrics-*.json (getprom_spark.py's output) instead.

    One real difference from the RisingWave adapter: busy_ratio/
    backpressure_ratio here are the SAME value repeated across every stage
    (getprom_spark.py's _capacity_ratio is a job-wide signal, not a
    per-operator one -- Structured Streaming has no per-operator timing to
    measure in the first place, unlike RisingWave's per-fragment
    stream_actor_poll_duration). This means dhalion_strategy's per-stage
    backpressure diagnosis degenerates to "the whole job is/isn't keeping
    up" for Spark -- every controllable stage gets scaled together, or
    none does. Documented here rather than silently assumed, same as this
    module's own docstring already flags for every engine besides
    RisingWave.

    `stage_names` must be the getprom_spark.py-side operator_names for
    exactly the *controllable* stages, in pipeline order -- e.g. Word
    Count's operator_names is ["source", "tokenizer", "counter", "sink"]
    (see getWordCount in getprom_spark.py) but only the interior stages are
    swept (source pinned at 2, sink pinned at 1, matching every engine's
    job_pds convention -- see run_strategy_sweep_spark.py), so the caller
    passes stage_names=["tokenizer", "counter"] here, not all four.
    """
    with open(evaluation_json_path) as f:
        data = json.load(f)
    operator_metrics = data.get("operatorMetrics") or {}
    result = []
    for name in stage_names:
        stage = operator_metrics.get(name) or {}
        result.append({
            "throughput_per_sec": stage.get("throughput_per_sec") or 0.0,
            "selectivity": stage.get("selectivity") if stage.get("selectivity") is not None else 1.0,
            "busy_ratio": stage.get("busy_ratio") or 0.0,
            "backpressure_ratio": stage.get("backpressure_ratio") or 0.0,
        })
    return result


def load_stage_metrics_kafkastreams(evaluation_json_path, stage_names):
    """Kafka Streams ADAPTER for ds2_strategy/dhalion_strategy -- same
    contract and same four output keys as every other adapter above,
    reading a Kafka Streams evaluationMetrics-*.json (getprom_kafkastreams.py's
    output) instead. Same flat-float shape as load_stage_metrics_spark()
    (no nested-dict selectivity to unwrap, unlike Storm/Flink).

    Kafka Streams has NO per-operator parallelism control at all (confirmed
    directly in WordCountTopology.java's javadoc and consistently across
    every one of the other 13 workload Topology classes, 2026-09-19): every
    task is parallelized uniformly by input-topic partition count, and
    num.stream.threads is a single job-wide knob, not a per-stage one.
    Unlike every engine above -- where stage_names lists the *several*
    genuinely-independent controllable stages -- callers here should pass
    a SINGLE-element stage_names list representing the one real knob (see
    run_strategy_sweep_kafkastreams.py's WORKLOADS/build_full_pds, which
    place that single swept value at parallelism_degree[1], matching
    WordCountTopology.java's own `parallelism.split(",")[1]`). busy_ratio
    is Kafka Streams' own real native thread-level "process-ratio" metric
    (see getOperatorMetrics() in getprom_kafkastreams.py), replicated
    across every entry in stage_names the same way Spark's job-wide proxy
    is -- not fabricated, just genuinely job-wide rather than per-stage.
    backpressure_ratio has no honest Kafka-Streams-native analog available
    (no consumer-lag plumbing exists yet) and is always 0.0, same neutral-
    default convention used everywhere else in this module for signals
    that can't actually be computed.
    """
    with open(evaluation_json_path) as f:
        data = json.load(f)
    operator_metrics = data.get("operatorMetrics") or {}
    result = []
    for name in stage_names:
        stage = operator_metrics.get(name) or {}
        result.append({
            "throughput_per_sec": stage.get("throughput_per_sec") or 0.0,
            "selectivity": stage.get("selectivity") if stage.get("selectivity") is not None else 1.0,
            "busy_ratio": stage.get("busy_ratio") or 0.0,
            "backpressure_ratio": stage.get("backpressure_ratio") or 0.0,
        })
    return result


def load_stage_metrics_storm(evaluation_json_path, stage_names):
    """Storm ADAPTER for ds2_strategy/dhalion_strategy -- same contract and
    same four output keys as load_stage_metrics_risingwave()/
    load_stage_metrics_spark() above, reading a Storm evaluationMetrics-
    *.json (getprom_storm.py's output) instead.

    Unlike Spark, Storm genuinely has per-bolt timing (via its native
    "capacity" metric, "num executed * avg execute latency / time
    window" -- the same "fraction of time busy" concept Dhalion's paper
    itself uses), so busy_ratio/backpressure_ratio here are real
    per-stage signals, not a job-wide value replicated across every
    stage -- see getprom_storm.py's getBoltMetrics() for exactly how
    busy_ratio=min(1.0, capacity) and
    backpressure_ratio=max(0, 1-1/capacity) are derived. Storm's own
    Prometheus exporter (storm_exporter.py) had the "capacity" and
    "processLatency" gauges swapped at the source (found and fixed
    2026-09-02, live) -- confirmed fixed before this adapter was written,
    otherwise busy_ratio here would have silently been process latency
    in disguise.

    `throughput_per_sec` reads getprom_storm.py's own new field of the
    same name (bolts_executed/uptime_seconds, mirroring how this file's
    existing "selectivity" ratio is already computed). `selectivity`
    unwraps getprom_storm.py's nested {"Selectivity": value} shape --
    left as-is (not flattened at the source) since
    2_operator_build_data.py's extract_scalar_from_nested() already
    depends on that nested shape for its own Storm/Flink handling.

    `stage_names` must be the getprom_storm.py-side bolt names for
    exactly the *controllable* stages, in pipeline order -- e.g. Word
    Count's bolts are ["tokenizer", "counter"] (see main()'s dispatch in
    getprom_storm.py) with no separate source/sink bolt entries at all
    (Storm's spout IS the source, and the last bolt IS the sink -- there
    is no dedicated Storm "sink" operatorMetrics entry the way Spark/
    RisingWave have, so run_strategy_sweep_storm.py's WORKLOADS must
    reflect each workload's real bolt list, not a source=.../sink=1
    placeholder).
    """
    with open(evaluation_json_path) as f:
        data = json.load(f)
    operator_metrics = data.get("operatorMetrics") or {}
    result = []
    for name in stage_names:
        stage = operator_metrics.get(name) or {}
        selectivity = None
        selectivity_field = stage.get("selectivity")
        if isinstance(selectivity_field, dict) and selectivity_field:
            try:
                selectivity = float(list(selectivity_field.values())[0])
            except (TypeError, ValueError):
                selectivity = None
        result.append({
            "throughput_per_sec": stage.get("throughput_per_sec") or 0.0,
            "selectivity": selectivity if selectivity is not None else 1.0,
            "busy_ratio": stage.get("busy_ratio") or 0.0,
            "backpressure_ratio": stage.get("backpressure_ratio") or 0.0,
        })
    return result


def load_stage_metrics_flink(evaluation_json_path, stage_names):
    """Flink ADAPTER for ds2_strategy/dhalion_strategy -- same contract and
    same four output keys as load_stage_metrics_risingwave()/
    load_stage_metrics_spark()/load_stage_metrics_storm() above, reading a
    Flink evaluationMetrics-*.json (getprom.py's output) instead.

    Like Storm, Flink genuinely has per-operator timing (native
    busyTimeMsPerSecond/backPressuredTimeMsPerSecond gauges, confirmed live
    2026-09-13 via /api/v1/series -- see getOperatorMetrics() in getprom.py),
    so busy_ratio/backpressure_ratio here are real per-stage signals, not a
    job-wide value replicated across every stage the way Spark's are.
    These gauges are labeled by task_id rather than operator_id, but every
    job in this project calls env.disableOperatorChaining() (see e.g.
    WordCountJob.java), so each task contains exactly one operator and
    task_id/operator_id are numerically identical -- getOperatorMetrics()
    already reuses operatorId directly for both.

    `selectivity` unwraps getprom.py's nested {"...metric : Selectivity":
    value} shape, identical to load_stage_metrics_storm()'s handling (same
    reason: 2_operator_build_data.py's extract_scalar_from_nested() already
    depends on that nested shape).

    `stage_names` must be the getprom.py-side operatorMetrics key names for
    exactly the *controllable* stages, in pipeline order -- e.g. Word
    Count's are ["Tokenizer", "Counter"] (see getWordCount() in getprom.py),
    capitalized exactly as each get<Workload>() function hardcodes them --
    there is no source/sink entry in operatorMetrics at all (same as Storm:
    Flink's own source/sink operators are not instrumented via
    getOperatorMetrics(), only interior stages are).
    """
    def _safe(value):
        # getprom.py's getOperatorMetrics() already coerces NaN to 0.0 at
        # the source (fixed 2026-09-13 -- Flink's own busyTimeMsPerSecond/
        # backPressuredTimeMsPerSecond gauges can report NaN during a brief
        # idle sub-interval, e.g. job startup, which poisons PromQL's
        # avg_over_time for the whole lookback window), but this guard
        # stays here too as defense-in-depth for any evaluationMetrics-
        # *.json written before that fix -- `float('nan') or 0.0` evaluates
        # to NaN in Python (NaN is truthy), so the plain `x or 0.0` pattern
        # used by every other adapter in this file does NOT catch it.
        try:
            f = float(value)
        except (TypeError, ValueError):
            return 0.0
        return 0.0 if math.isnan(f) else f

    with open(evaluation_json_path) as f:
        data = json.load(f)
    operator_metrics = data.get("operatorMetrics") or {}
    result = []
    for name in stage_names:
        stage = operator_metrics.get(name) or {}
        selectivity = None
        selectivity_field = stage.get("selectivity")
        if isinstance(selectivity_field, dict) and selectivity_field:
            try:
                selectivity = float(list(selectivity_field.values())[0])
            except (TypeError, ValueError):
                selectivity = None
            else:
                if math.isnan(selectivity):
                    selectivity = None
        result.append({
            "throughput_per_sec": _safe(stage.get("throughput_per_sec")),
            "selectivity": selectivity if selectivity is not None else 1.0,
            "busy_ratio": _safe(stage.get("busy_ratio")),
            "backpressure_ratio": _safe(stage.get("backpressure_ratio")),
        })
    return result


def ds2_strategy(stage_metrics, event_rate, current_assignment):
    """DS2 (Kalavri et al., OSDI 2018, "Three steps is all you need") --
    single-shot, model-based parallelism estimate from one baseline run's
    *measured* rates, instead of a formula with a hand-picked constant
    (contrast with heuristic_strategy's power-law above). Unlike every
    other strategy in this file, this one needs real observations, not
    just `levels`/`num_stages` -- get them via an engine-specific adapter
    such as load_stage_metrics_risingwave() from an actual
    evaluationMetrics-*.json (see this module's docstring for why the
    adapter, not this function, is where engine-specific knowledge
    lives).

    The paper's core idea: an operator's *observed* throughput
    under-states its real capacity whenever it spends time waiting on
    input/output (backpressure, empty buffers, etc.) instead of doing
    useful work. It defines "true rate" = observed rate / (fraction of
    time actually spent doing useful work), and shows the optimal
    parallelism for every stage can be computed in one topological pass
    using each stage's true rate and its selectivity (Eq. 7/8 in the
    paper). We already collect exactly the fields this needs natively
    (getprom_risingwave.py's operatorMetrics, added 2026-07-27) with zero
    extra instrumentation:
      - `busy_ratio` (stream_actor_poll_duration-based) is this project's
        proxy for the paper's "useful time fraction" -- RisingWave
        doesn't separately expose deserialize/process/serialize splits,
        so true processing rate and true output rate collapse to one
        estimate here: true_rate = throughput_per_sec / busy_ratio.
      - `selectivity` (already output/input ratio per stage) is exactly
        the paper's oj[λo]/oj[λp] term in Eq. 8 -- no separate
        computation needed.
      - `event_rate` is the paper's source rate λ_src, the recursion's
        base case (Eq. 8, j < n) -- RisingWave's source stage is fixed
        outside this strategy (parallelism=2, see build_sql_with_
        parallelism), so it is supplied directly rather than measured.

    Recursion (linear per-workload pipeline, so Eq. 8's summation over
    upstream operators has exactly one term): target rate into stage i is
    the previous stage's target rate times stage i's own selectivity,
    chaining from event_rate at the source. Optimal parallelism (Eq. 7)
    is then ceil(target_rate_i / per-instance true rate_i), where
    per-instance true rate = true_rate_i / current_parallelism_i (our
    throughput/busy_ratio numbers are aggregated across all instances of
    the fragment, so this divides back down to a single instance's rate,
    matching oi[λp]/pi in the paper).

    `current_assignment` must have one entry per stage (the parallelism
    each stage actually ran at when stage_metrics was measured) --
    without it there is no way to recover a per-instance rate from an
    aggregated one.

    Edge case not addressed by the paper: busy_ratio == 0 means the
    stage spent no measurable time doing useful work in this window
    (idle, or truly not the bottleneck) -- true rate is undefined by
    division, so this falls back to treating observed throughput as the
    rate estimate directly (equivalent to assuming busy_ratio=1). This
    is a conservative direction: if the stage's real busy_ratio was
    actually below 1, this under-estimates its true capacity, which
    biases towards recommending *more* parallelism rather than less --
    same "don't guess towards under-provisioning" spirit as Property 1
    (No overshoot) in the paper's own model.

    Returns a single assignment (list of ints), same convention as
    heuristic_strategy -- one data-driven configuration per (workload,
    baseline run), not a sweep across levels.
    """
    if len(stage_metrics) != len(current_assignment):
        raise ValueError(
            f"ds2_strategy needs one current_assignment entry per stage "
            f"({len(stage_metrics)} stages, got {len(current_assignment)} parallelism values)"
        )

    assignment = []
    target_rate = float(event_rate)
    for stage, current_p in zip(stage_metrics, current_assignment):
        target_rate = target_rate * float(stage["selectivity"])

        busy_ratio = float(stage["busy_ratio"])
        throughput = float(stage["throughput_per_sec"])
        true_rate = throughput / busy_ratio if busy_ratio > 0 else throughput

        per_instance_true_rate = true_rate / current_p if current_p else true_rate
        if per_instance_true_rate <= 0:
            pi = current_p  # no measurable rate to scale from -- leave unchanged, don't guess
        else:
            pi = math.ceil(target_rate / per_instance_true_rate)
        assignment.append(_clamp(pi))
    return [assignment]


def dhalion_strategy(stage_metrics, current_assignment):
    """Dhalion (Floratou et al., VLDB 2017, "Self-Regulating Stream
    Processing in Heron") -- rule-based, backpressure-driven parallelism
    nudge from one baseline run's *measured* backpressure, in contrast to
    DS2's model-based true-rate estimate above. Implemented as a single
    one-shot nudge (observe once, scale once), NOT Dhalion's original
    repeated deploy-observe-redeploy control loop -- deliberately, since
    the loop is what made this project's own live sweep expensive even
    without it (see run_sweep_risingwave.py's BUGS_AND_FIXES.md history),
    and the paper's own comparison in DS2 shows Dhalion needing six
    rounds to converge on a topology this simple. One nudge per baseline
    observation keeps this at the same "pure function returning an
    assignment" shape as every other strategy in this file.

    Scale-up rule taken directly from the paper's Bolt Scale Up
    Resolver (Section 5.1): the resolver computes what fraction of the
    measurement window a stage spent suspending input consumption due to
    backpressure, and treats that fraction as the portion of the input
    load the stage could not handle -- e.g. 20% suspended implies the
    stage needs roughly 25% more parallelism (1 / (1 - 0.20) = 1.25) to
    absorb the same load without backpressure. `backpressure_ratio`
    (stream_actor_output_buffer_blocking_duration_ns-based, already
    collected by getprom_risingwave.py) is this project's direct analog
    of that suspended-time fraction.

    Two deliberate simplifications versus the full paper, both because
    our instrumentation is per-fragment (aggregated across every
    instance of a stage), not per-instance the way Heron's is:
      - Dhalion's Diagnosis phase distinguishes resource-underprovisioning
        from slow-instance and data-skew causes of backpressure (Table 1
        in the paper) by comparing individual instances' processing
        rates and queue sizes against their peers. We only have one
        throughput/backpressure number per fragment, so every
        backpressure symptom here is attributed to underprovisioning --
        the only diagnosis this data can actually support.
      - The original policy resolves ONE bottlenecked bolt per round,
        picking the most upstream one first, and only reaches downstream
        bottlenecks in later rounds as they get exposed. Since the
        repeat is explicitly out of scope here, every stage currently
        showing backpressure is scaled in this single nudge instead of
        just one -- a deliberate one-shot generalization of a normally
        sequential process, not an oversight.
      - Only the scale-up path (Bolt Scale Up Resolver) is implemented.
        The paper's Bolt Scale Down Resolver (for overprovisioned,
        backpressure-free stages) needs a "queue nearly empty" signal
        this project doesn't collect (Heron's Pending Packets Detector
        has no RisingWave equivalent here), so scale-down is left out
        rather than approximated on a signal that isn't actually
        measuring the same thing.

    A `backpressure_ratio` of 1.0 (fully blocked for the entire window)
    makes the paper's formula divide by zero -- falls back to simply
    doubling that stage's parallelism, a bounded stand-in for "this
    stage is completely saturated" rather than an undefined blow-up.

    Returns a single assignment (list of ints), one nudge from
    `current_assignment` -- same one-shot-configuration convention as
    ds2_strategy and heuristic_strategy.
    """
    if len(stage_metrics) != len(current_assignment):
        raise ValueError(
            f"dhalion_strategy needs one current_assignment entry per stage "
            f"({len(stage_metrics)} stages, got {len(current_assignment)} parallelism values)"
        )

    assignment = []
    for stage, current_p in zip(stage_metrics, current_assignment):
        bp = float(stage.get("backpressure_ratio", 0.0) or 0.0)
        if bp <= 0:
            new_p = current_p  # no backpressure observed -- no diagnosis, no resolver invoked
        elif bp >= 1:
            new_p = current_p * 2  # fully saturated; formula below is undefined at bp=1
        else:
            new_p = math.ceil(current_p / (1 - bp))
        assignment.append(_clamp(new_p))
    return [assignment]


STRATEGIES = {
    "uniform": uniform_strategy,
    "exhaustive": exhaustive_strategy,
    "minavgmax": minavgmax_strategy,
    "increasing": increasing_strategy,
    "random": random_strategy,
    "heuristic": heuristic_strategy,
    "ds2": ds2_strategy,
    "dhalion": dhalion_strategy,
}
