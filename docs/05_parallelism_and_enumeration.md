# PStreamBench — Parallelism & Enumeration Strategies

> **Status: substantially grounded**, written from
> `benchmark_sweep/parallelism_strategies.py` directly plus the
> "Contributions So Far" project doc's methodology section. The
> execution-mechanics half (how a strategy's chosen vector actually gets
> submitted) is confirmed via `run_strategy_sweep_flink.py`'s own
> docstring; still needs direct confirmation from the other engines'
> sweep scripts and from `infra/runner.py`'s separate enumeration logic
> before this file is complete.

## 1. Two separate enumeration-strategy systems exist — know which one you're in

This is the single most important fact to get right before anything else
in this file: **there are two different, non-overlapping implementations
of "enumeration strategy" in this project**, and conflating them is a
real source of confusion.

| | WUI's own logic | `parallelism_strategies.py` |
|---|---|---|
| Lives in | `infra/runner.py` (~L65-111) | `benchmark_sweep/parallelism_strategies.py` |
| Strategy names | `Random`, `MinMax`, `Exhaustive`, `Rulebased`, `Custom`, `None` | `uniform`, `random`, `ds2`, `dhalion`, plus `exhaustive`/`minavgmax`/`increasing`/`heuristic` |
| Driven by | The WUI's Submit Job form directly | The command-line `run_strategy_sweep_*.py` scripts |
| Needs prior run data? | No — every strategy generates a config from scratch | `ds2`/`dhalion` **do** — they need a previous run's real metrics (see §4) |
| How it submits | Its own code path inside `runner.py` | Reuses the **same** `/infra/jobcreate/<cluster_id>` endpoint, with `job_enumeration_strategy="Custom"` and the strategy's own computed vector |

`ds2`/`dhalion` are **not** currently selectable from the WUI (see
`docs/14_issue_log.md` WUI-002) — not because they're unsupported, but
because the loop that runs them (submit → wait → collect metrics →
compute next vector → resubmit) only exists as a standalone script today,
not as something the WUI can trigger and poll. Future integration work
should extend the *second* system to be WUI-triggerable, not reimplement
it inside `runner.py`'s first system.

## 2. The four headline strategies

- **Uniform** — every operator gets the same parallelism value. The
  *only* strategy for which averaging a run's parallelism vector into one
  number is meaningful, because the mean equals every element.
- **Random** — each interior/controllable stage independently draws a
  value from a fixed level set (e.g. `{2,4,8,16,32,40}`), seeded for
  reproducibility (seed 42 in the synthetic-workload sweeps).
- **DS2** (`ds2_strategy(stage_metrics, event_rate, current_assignment)`)
  — queueing-theory-based: computes a new assignment from a baseline
  run's real per-stage metrics plus the target event rate. Exists because
  blind search is assumed too expensive/unreliable — see
  `docs/09_validation.md` (pending) for whether that assumption is
  actually tested.
- **Dhalion** (`dhalion_strategy(stage_metrics, current_assignment)`) —
  heuristic, CPU-utilization/backpressure-driven nudge from a baseline.
  **Known limitation**: clamps every stage to a floor of 2 even with no
  observed backpressure, so a true "no-change" baseline isn't
  representable (`docs/14_issue_log.md` SYNTH-003) — accepted, not fixed,
  since it's shared code also used by real-workload sweeps.

Both DS2 and Dhalion are **adaptive adjustment** strategies, categorically
different from Uniform/Random: they take a previous run's actual
Prometheus/evaluation metrics as input, not just a level set.

## 3. Parallelism vector interpretation and source/operator/sink mapping

- **Per-operator-vector engines** (Flink, Storm, Spark): the vector's
  positions map 1:1 to pipeline stages in source-code order — e.g. Word
  Count's `2,2,2,2` = `[source, tokenizer, counter, sink]` per
  `WordCountJob.java`'s own class comment. The authoritative, verified
  mapping for every (framework, query, variant) combination is
  `PStreamBench_benchmark_data/operator_templates.csv` — 382 rows, 124
  verified directly against job source, 206 inferred (mainly Spark, whose
  job code doesn't independently expose per-operator parallelism the same
  way — its stage order is taken from `operatorMetrics` naming instead),
  50 mechanical-by-convention for the job-level engines, and 2 explicitly
  left unresolved rather than guessed.
- **Job-level-scalar engines** (Kafka Streams, RisingWave): by
  project-wide convention, `vector[1]` = stream threads / job
  parallelism, `vector[0]` = a second scalar — not read per-workload
  since the convention is fixed project-wide (see §7 of
  `operator_templates.csv`'s own methodology, summarized in the
  "Contributions So Far" doc).
- **Source/sink pinning conventions** found empirically, not assumed:
  Storm/Flink's synthetic and real sweeps both pin `source=10, sink=1` —
  confirmed against dozens of pre-existing uniform-sweep artifacts
  showing 100% consistency on this convention (Word Count's own history
  is mixed, with some batches using `source=40`, but 10/1 is the
  dominant and only cross-workload-consistent value, and was adopted
  project-wide for the strategy sweep).
- **Structural anomalies, documented rather than silently smoothed over**:
  Linear Road's sink has no independently-settable parallelism (Flink) —
  it silently inherits the source's value; some jobs have a vector slot
  that no operator actually reads (a "dead" dimension). See
  `docs/03_workloads.md` (pending) for the full list.

## 4. Why average parallelism is valid for Uniform but lossy otherwise

This is the project's core methodological finding (see
`docs/11_design_decisions.md`, pending, for the full "rejected
alternatives" writeup): averaging a run's per-operator vector into one
number is only meaningful for Uniform, where every operator gets the
same value by construction. For Random/DS2/Dhalion, **two runs with
wildly different per-operator allocations can average to the same
number** — an average that discards exactly the information those
strategies are supposed to differ on. The pipeline instead treats each
run's exact vector as its own identity (`config_id`, see §5).

## 5. Configuration identity: `config_id` and local short IDs

- **`config_id`** — a canonical string, e.g.
  `Source:10|Parser:16|RepeatVisit:16|Reduce:16|Sink:1`, unique per
  configuration. This is the ground truth; everything else is a label
  over it.
- **Local short IDs (C1…Cn)** — a compact label assigned per (framework,
  strategy, workload, hardware, rate) **cell**, used on plot axes/in
  captions. Explicitly **not** comparable across cells — C1 in Flink and
  C1 in Storm are unrelated configurations. Every figure using short IDs
  must ship a caption or CSV mapping back to the full `config_id`.

## 6. Search-space vs. selected/best, kept separate

One set of figures shows every configuration a strategy actually
evaluated (the full search space); a separate set shows the single best
configuration per (framework, strategy) under one fixed objective
(throughput — never re-optimized per displayed metric; see
`docs/08_analysis_and_plots.md`, pending, "Rigor principles"). Earlier
drafts of this analysis conflated the two, which hid how large DS2/
Dhalion's actual search space was — kept separate deliberately now.

## 7. What each strategy assumes (worth stating explicitly as threats to
validity)

- **Uniform** assumes no operator-specific bottleneck exists worth
  targeting — true by construction, not tested.
- **Random** assumes no structure in the search space worth exploiting —
  a genuine, useful baseline for "is smarter search actually smarter."
- **DS2** assumes its queueing-theory model's inputs (per-stage
  throughput/backpressure) are accurate enough to compute a *correct*
  reallocation — not yet validated against an oracle/exhaustive baseline
  (flagged as a real gap in the "New Insights" doc's Insight C: "right
  now 'best throughput found' is bounded by whatever Random/DS2/Dhalion
  happened to evaluate, not a true optimum").
- **Dhalion** assumes CPU-utilization/backpressure is the right signal to
  nudge on — see the "New Insights" doc's Insight C for why this is a
  genuinely falsifiable, not yet resolved, claim (do CPU-utilization-
  driven autoscalers actually catch coordination bottlenecks, or are
  they "blind" to exactly the regime v1's own guidelines flagged?).

**Still needed for this file:** direct confirmation of `infra/runner.py`'s
`Random`/`MinMax`/`Exhaustive`/`Rulebased` logic (currently described only
from the "two systems exist" framing, not read line-by-line), and a
worked numeric example of DS2/Dhalion's nudge computation for one real
workload.
