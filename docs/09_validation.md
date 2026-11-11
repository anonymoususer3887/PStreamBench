# PStreamBench — Validation Methodology

> **Status: PENDING full write-up.** The pieces below are confirmed from
> the "Contributions So Far" doc and this session's own testing; needs
> consolidation into a complete methodology chapter plus direct
> confirmation of what's still only partially done.

## Confirmed validation practices

- **Deterministic test inputs** — synthetic workloads use fixed
  `(key, value)` tuples with `value` uniform in `[0,1)`, chosen
  specifically so the same shape always produces the same statistical
  behavior run to run (see `docs/03_workloads.md` §synthetic, pending).
- **Expected cardinalities, checked not assumed** — the calibrated join
  key-space formula was validated live against a target match count
  (~1000/window), confirmed within ±20% across all five join widths; an
  earlier version of the formula was caught under-shooting by exactly
  the factor its own pre-filter step removes (see
  `docs/14_issue_log.md` SYNTH-002).
- **Operator-level checks** — `operator_templates.csv`'s mapping was
  built by reading job source directly (124 of 382 rows verified against
  actual `.setParallelism()`/`setBolt()`/`setSpout()` call sites), not
  inferred wholesale; the 2 rows that couldn't be resolved were left
  explicitly unresolved rather than guessed.
- **Semantic-equivalence checks, explicitly gated** — two engines' jobs
  for "the same" workload are only compared head-to-head once their
  source has been read and confirmed to compute the same logical result.
  Currently confirmed for Word Count (Kafka Streams vs. RisingWave);
  comparisons for other workloads are withheld until checked the same
  way — this is a real, current limitation, not a hypothetical one.
- **Regression/methodology checks live in the sweep scripts themselves**
  — a resumable JSONL manifest (`status=='ok'` gating) means a bug fix
  can be validated by simply re-running the previously-failed subset and
  confirming it now succeeds, without re-running everything (used
  repeatedly — see `docs/14_issue_log.md` STORM-009/010/
  INFRA-ENV-001's "no code changes needed" retry pattern).
- **Live, direct confirmation over trusting a build step** — e.g.
  Flink's bootstrap-server fix was confirmed in compiled bytecode via
  `javap`, not just a successful `mvn package` (a prebuilt jar can
  silently drift from source — a documented project-wide gotcha).

## Comparison between frameworks

- **[inferred]** No formal statistical-significance testing (error bars,
  confidence intervals) was found applied to any cross-strategy or
  cross-framework "gap"/"% improvement" claim in this pass — the "New
  Insights" doc explicitly flags this as missing ("given the existing
  run_count/metric_std already tracked per configuration, each of these
  plots should carry error bars or a significance test before being
  called a finding, not just an observation"). **This is a real,
  currently-open gap in the validation methodology**, not yet resolved.

**Still needed for this file:** a direct answer to "is there an oracle/
near-exhaustive baseline for 'best possible' framing" (the "New Insights"
doc flags this as not yet available — "'best throughput found' is bounded
by whatever Random/DS2/Dhalion happened to evaluate, not a true
optimum"), and a worked example of one full semantic-equivalence check
(what was actually read, what confirmed it was equivalent) for Word
Count, since that's the one case currently marked as done.
