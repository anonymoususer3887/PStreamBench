# PStreamBench — Design Decisions and Rejected Alternatives

> **Status: substantially grounded** for the decisions below, each
> confirmed from an actual project doc or code comment, not reconstructed
> from guesswork. More exist and should be added as they're found —
> this file should grow, not be treated as complete.

## Decision: track per-run `config_id` instead of averaging the parallelism vector

- **Alternatives considered:** report a single "mean parallelism" number
  per run, matching how a Uniform-only sweep (v1) was summarized.
- **Why rejected:** the mean is only meaningful for Uniform, where every
  operator has the same value by construction. For Random/DS2/Dhalion,
  two runs with very different per-operator allocations can average to
  the same number — exactly the information those strategies are meant
  to differ on.
- **Chosen design:** a canonical `config_id` string per run
  (`Source:10|Parser:16|...`), plus scoped local short IDs (C1…Cn) for
  plot axes, explicitly non-comparable across cells.
- **Trade-offs:** more bookkeeping (every plot needs a caption/CSV
  mapping back to the full config); more honest results.
- **Consequences:** this is now the project's central methodological
  contribution for the paper, not just an implementation detail — see
  `docs/00_overview.md` §3 and `docs/05_parallelism_and_enumeration.md`
  §4.

## Decision: never fabricate a per-operator breakdown for job-level engines

- **Alternatives considered:** split Kafka Streams/RisingWave's single
  scalar across invented "operators" so every engine's plots look
  structurally the same.
- **Why rejected:** would misrepresent what those engines actually let a
  user configure — a fabricated breakdown with no real independent
  control behind it.
- **Chosen design:** represent their configuration honestly as one
  value everywhere; their *performance* metrics are still tracked per
  logical operator separately, since configuration and runtime
  performance are different axes.
- **Trade-offs:** some plot families (the per-operator parallelism
  breakdown, mode F) simply don't apply to these two engines, which must
  be stated plainly rather than papered over.

## Decision: synthetic workloads as a separate, isolated project per engine

- **Alternatives considered:** add synthetic workload support into the
  existing real-workload projects (`dsp_storm_jobs`, `dsp_flink_jobs`).
- **Why rejected:** any shared code touched while building/debugging 9
  new, unproven query shapes risks breaking the 14 real workloads that
  already have collected results depending on them.
- **Chosen design:** `dsp_storm_synthetic_jobs`/`dsp_flink_synthetic_jobs`
  as entirely separate build projects (Gradle/Maven), sharing only the
  project's external collectors (`getprom_storm.py`/`getprom.py`,
  extended, not forked) and `parallelism_strategies.py`.
- **Consequences:** confirmed zero regressions to the 14 real workloads
  throughout the synthetic-workload development and debugging process.

## Decision: fixed, deterministic synthetic query shapes, not randomized plan generation

- **Alternatives considered:** reuse `pdspbench-plan-generation`'s
  existing approach (randomly deciding which optional operators to
  include per run).
- **Why rejected:** that tool was built for training an ML performance-
  prediction model, where variety is the point. For controlled
  benchmarking, cross-run and cross-engine comparison requires every run
  of "the same shape" to have an **identical** operator graph, varying
  only parallelism and rate.
- **Chosen design:** 9 fixed shapes (Linear, Chained Filters ×2/3/4,
  N-way Join ×2–6), same `(key, value)` tuple model, same thresholds,
  same calibrated join-key-space formula across every engine port.
- **Related decision — don't block on `pdspbench-plan-generation`'s own
  Flink fork:** that tool depends on a custom-patched Flink
  (`flink.version: 1.16-PGF`) that was never successfully resolved in
  this environment (`~/.m2` held only Maven failure markers, and the
  likely source build would be multi-hour with no clear payoff). Its
  *design* (deterministic-parameter pattern, join-chaining shape) was
  ported manually into a fresh project against vanilla Flink 1.16.0
  instead of spending the time to resolve the fork.

## Decision (and correction): Flink slot-allocation model

- **Initial decision:** cap synthetic-sweep parallelism levels per shape,
  based on an assumed additive slot-consumption model
  (`total_slots = 2*sources + level*swept_stages + 1*sink`).
- **What actually happened:** this assumption was **wrong**, caught by
  user pushback, not self-discovered. Flink's real default behavior (no
  `slotSharingGroup()` override anywhere in this project's jobs) packs
  one subtask per operator into the same slot per parallel slice — slot
  need is `max(vector)`, not the sum.
- **Consequences:** the per-shape cap was removed entirely; ~36
  previously-collected strategy-sweep runs under the wrong cap were
  archived (not deleted) and the sweep re-run clean. Recorded here
  specifically as an example of **a plausible-sounding but wrong
  architectural assumption that shipped into real sweep code before
  being caught** — worth remembering before assuming any other engine's
  resource model without checking directly against its own scheduler
  documentation or live behavior.

## Decision: reuse the WUI's own `/infra/jobcreate` endpoint for sweep automation, not a parallel API

- **Alternatives considered:** build a separate submission path for
  automated sweeps (direct SSH, bypassing the controller entirely — as
  was in fact necessary for the synthetic-workload cluster, which had no
  running Django controller instance against it).
- **Why the WUI endpoint was preferred where available:** the real
  14-workload sweeps reuse the exact same `POST /infra/jobcreate/
  <cluster_id>` call the WUI itself makes, with `job_enumeration_
  strategy="Custom"` and the strategy's own computed vector — rather than
  inventing a second job-lifecycle implementation that could drift from
  the one path already proven correct in production.
- **Consequence this surfaced:** `ds2`/`dhalion` are *not* currently
  WUI-selectable, specifically because the loop that drives repeated
  Custom submissions only exists as a standalone script — see
  `docs/14_issue_log.md` WUI-002 and `docs/05_parallelism_and_
  enumeration.md` §1 for the proposed fix (make the loop itself
  WUI-triggerable, don't reimplement strategy logic inside `runner.py`).

**Still needed for this file:** decisions behind the Django app split
(`infra`/`auth`/`report`/`datagen`), the choice of Ansible over a
different orchestration tool, and the choice of CloudLab specifically
over another cloud/testbed provider.
