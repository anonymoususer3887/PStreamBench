# PStreamBench — Analysis & Plotting Pipeline

> **Status: substantially grounded** for the newer enumeration/
> configuration plot families (confirmed via the "Contributions So Far"
> project doc and direct script listing). The original query/operator
> plot families (scripts `3_plot_pdsp_query_metrics.py`,
> `4_plot_pdsp_operator_metrics.py`, `5_plot_pdsp_low_high_parallelism.py`)
> are **not yet documented here** — needs a direct read of each.

## 1. The real numbered script sequence (confirmed from the repo, not
assumed)

```text
0_migrate_{kafkastream,risingwave,spark}_results.py   # one-time migration scripts
1_data_extractor.py                                    # raw JSON → structured data
2_operator_build_data.py                               # operator-level data assembly
3_plot_pdsp_query_metrics.py                           # [not yet documented in detail]
4_plot_pdsp_operator_metrics.py                        # [not yet documented in detail]
5_plot_pdsp_low_high_parallelism.py                    # [not yet documented in detail]
6_make_all_plots.sh / _kafkastream.sh / _risingwave.sh / _spark.sh
6_plot_pdsp_enumeration_strategies.py
7_make_enumeration_plots.sh
7_plot_pdsp_enumeration_analysis.py                    # the main enumeration-analysis script, see §2
8_make_strategy_plots.sh
9_plot_intro_lmh.py
10_run_narrow_sweeps.py
11_extract_operator_metrics.py
12_run_full_enumeration_batch.py                       # the full batch driver, see §3
```

Note for whoever continues this: the user's original documentation
request referenced "the original 01-14 plot families" — the actual
on-disk numbering is `0` through `12` as above, with no gap-filled `13`/
`14`. Either the request was describing an earlier or planned numbering
that has since shifted, or an external numbering scheme (e.g. in a paper
draft) doesn't match the repo 1:1. **Flag this mismatch to the user
rather than silently renumbering anything** — don't force the repo's
real scripts to match a number that doesn't exist on disk.

## 2. `7_plot_pdsp_enumeration_analysis.py` — the main enumeration-analysis
script

Implements every enumeration/configuration figure family as a named
`--mode`, with one consistent visual language throughout: strategy always
owns the same color, marker, line style, and bar hatch in every figure.

| Mode | What it shows |
|---|---|
| B (`config_perf_single`) | One framework's configurations (C1…Cn) vs. a metric |
| C (`config_perf_compare`) | Same, all frameworks in a group side by side |
| 03B (`config_eventrate`) | A single configuration's metric across event rates — only the *same* configuration connected across rates, never a different one silently substituted |
| E (`strategy_framework`) | Selected/best configuration, framework × strategy |
| F (`selected`) | Selected/best configuration's per-operator parallelism breakdown |
| 18 (`selected_performance`) | Selected/best configuration's metric, one bar per strategy |
| 21 / 22 / 23 | Selected/best metric across workload / hardware / event rate |
| 25 (`tp_latency`) | Throughput vs. latency, every configuration, colored by strategy |
| G (`operator_own_metrics`) | Per-operator throughput/selectivity/backpressure/busy-ratio, independent of the operator-template mapping — full workload coverage regardless of template status |

## 3. `12_run_full_enumeration_batch.py` — the full batch driver

Enumerates the full workload × hardware × event-rate × metric × strategy
× framework-group matrix in one committed script — **31,851 plot calls**,
run in parallel across 10 workers, writing PNG + PDF + SVG plus a CSV
mapping file for every figure. Reproducible from a single command rather
than hand-run one workload at a time. Completed 2026-10-09: 31,134 ok on
the first pass; the remaining 717 hit a real `matplotlib.tight_layout()`
bug (an extreme shared-axis value range forcing scientific-notation
offset text produced NaN axes positions) — root-caused, fixed with a safe
fallback, all 717 re-ran clean. **Final: 31,851/31,851, zero failures.**
This bug/fix belongs in `docs/14_issue_log.md` as its own entry — add it
there (component `PLOT`) when this file is next revised.

## 4. Framework-comparison groupings

Three groupings used throughout: `all_5` (all five engines), `flink_
spark_storm` (the operator-level, per-operator-vector engines),
`kafka_risingwave` (the job-level, single-scalar engines). This grouping
exists specifically to support RQ4 in `docs/00_overview.md` — comparing
across the architectural split, not just all five engines pooled
together.

## 5. Rigor principles enforced across every figure family

- **Missing is never zero.** A framework/strategy combination with no
  data shows an explicit "N/A" label, never a zero-height bar — a zero
  would misreport "it ran and got nothing" instead of "it never ran."
- **One fixed selection objective.** Every "selected/best configuration"
  figure always picks under throughput, never re-optimizes per displayed
  metric — looping the objective would silently change *which
  configuration counts as best* depending on what's being plotted.
  Metric-specific reporting for an already-selected configuration is
  handled separately (`selected_for()`), decoupling selection from
  display.
- **Semantic equivalence is checked, not assumed** before a cross-engine
  comparison — see `docs/03_workloads.md` (pending) and
  `docs/09_validation.md` (pending).
- **IDs are local, never global.** A short label like "C1" is scoped to
  one (framework, strategy, workload, hardware, rate) cell — see
  `docs/05_parallelism_and_enumeration.md` §5.

## 6. Output directory structure

- `00_selected_best_comparison/<group>/` — selected/best config comparisons.
- `plots_pdsp_kafkastream/`, `plots_pdsp_risingwave/`, `plots_pdsp_spark/`,
  `plots_enumeration/`, `plots_todate/` — per-engine/per-family output
  roots (confirmed present in the repo; **not yet documented per-
  subfolder** — needs a direct listing pass).

**Still needed for this file:** scripts `3`/`4`/`5`/`9`/`10`/`11`'s actual
scientific questions and axes (not yet documented at all in this pass),
and a full old-family → configuration-based-view → selected/best-view
mapping table — the "Contributions So Far" doc notes this mapping "is in
progress and will be added [there] once reviewed," so check whether it
has landed there before re-deriving it here.
