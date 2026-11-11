# Paper figure scripts (PStreamBench, ICDE 2027)

Scripts for every figure, figure variant and data check made during the paper revision.
All scripts read data only (`../../datasets_v2/`, `../../artifacts/`, `inputs/`) and write
their outputs next to themselves. Nothing in `datasets_v2/` or `artifacts/` is modified.

Requirements: Python 3, pandas, numpy, matplotlib; `pypdf` and `reportlab` for the PDF
post-processing scripts. Run each script from any directory: `python3 <folder>/<script>.py`.

Common run filter (unless a script says otherwise): `hardware_type == 'm510'`, empty
`folder_flag`, DoP = round(`parallelism_avg`), median over runs per configuration.
Missing uniform values fall back to `uniform_1` (noted in the scripts that do this).

## fig1_motivation/ — intro figure (uniform vs. DS2 vs. Dhalion)

Values are hard-coded in the scripts (taken from `datasets_v2` and the Storm reruns in
`artifacts`); each script's header lists the source of every number.

| Script | Output | Note |
|---|---|---|
| `fig_motivation_intro.py` | `fig_motivation_intro_1M.*` | **Paper version.** 3.5 x 1.75 in, 1M ev/s. Kafka Streams DS2 taken from the 100k run (same colour, no marker). Storm values are run means. |
| `fig_motivation_wide_v2.py` | `fig_motivation_wide_v2_{1M,500k,100k}.*` | Wide variant, one panel per rate. |
| `fig_motivation_vertical_tuners_v3.py` | `fig_motivation_vertical_tuners_1M_v3.*` | Earlier vertical layout. |
| `fig_motivation_vertical_tuners_rates.py` | `fig_motivation_vertical_tuners_{rate}.*` | Vertical layout per rate. |
| `fig_motivation_tuners.py` | `fig_motivation_uniform_tuners_fig9.*` | First version. |

## rq1/

| Script | Input | Output |
|---|---|---|
| `rq1_consumed.py` | `datasets_v2/real_{query,operator}_level.csv` | `rq1_consumed_vs_sink.csv` (consumed-input and sink-output ratio, DoP 40 / DoP 2, 1M) |
| `rq1_consumed_plot.py` | `rq1_consumed_vs_sink.csv` | `rq1_panel_a_current_vs_consumed.*` |
| `annotate_rq1_markers.py SRC OUT` | `inputs/fig_rq1_heatmaps_v2.pdf` | Copy of the heatmap with numbered review markers (review aid only, not a paper figure). |

## rq2/

| Script | Input | Output |
|---|---|---|
| `rq2_remove_memory_row.py SRC OUT` | `inputs/fig_rq2_detail_40.pdf` | `fig_rq2_detail_40_nomem.pdf`: memory row cut out by PDF clipping, x labels moved up, stays vector. |
| `annotate_rq2_markers.py SRC OUT` | `inputs/fig_rq2_heatmaps.pdf` | Review markers only. |

Example: `python3 rq2/rq2_remove_memory_row.py inputs/fig_rq2_detail_40.pdf rq2/fig_rq2_detail_40_nomem.pdf`

## rq3/ — scaling under load

| Script | Output | Content |
|---|---|---|
| `rq3_dop_load.py` | `fig_rq3_dop_load.*`, `rq3_dop_load_values.csv` | DoP 40 / DoP 2 at L (100k) and H (1M). |
| `rq3_dop_load_LMH.py` | `fig_rq3_dop_load_LMH.*`, `..._values.csv` | Same at L, M (500k), H. |
| `rq3_dop_load_tuners.py [LH\|LMH]` | `fig_rq3_dop_load_tuners_{MODE}.*`, values CSV | DoP-40 columns plus DS2 and Dhalion at 1M; missing cells grey "n/a", no hatching. |
| `rq3_tuners_load.py [LH\|LMH]` | `fig_rq3_tuners_load_{MODE}.*`, values CSV | DS2 and Dhalion vs. DoP 2 at each rate. |
| `rq3_dop_load_estimated.py [LH\|LMH]` | `fig_rq3_dop_load_est_{MODE}.*`, values CSV | Same heatmap with the estimated cells from `rq3_estimated_cells.csv` filled in where no measured value exists (italic value, black corner triangle). Needs `rq3_dop_load_LMH_values.csv` from `rq3_dop_load_LMH.py`. |
| `pareto_options.py` | `pareto_cells.csv`, option A/B/B'/C figures, `optC_table.csv` | Pareto design options (removed from the paper). |
| `fig_rq3_pareto_guided.py` | `fig_rq3_pareto_guided.*` | Pareto plot with reversed latency axis (needs `pareto_cells.csv`). |
| `pareto_values_export.py` | `fig_rq3_pareto_guided_values.csv` | Values behind the Pareto plot. |
| `annotate_rq3_markers.py SRC OUT` | — | Review markers. **Input uncertain:** the script expects a 515.52 x 259.2 pt page, which does not match `fig_rq3_heat_final_v1` (494.76 x 212.4 pt). |

Note: `rq3_dop_load.py` and `rq3_dop_load_LMH.py` still hatch missing/zero cells; the
`*_tuners*` scripts use grey "n/a" cells instead.

## rq4/ — Fig. 9 (tuner summary at 1M)

| Script | Input | Output |
|---|---|---|
| `ext.py [SVG]` | `inputs/fig_rq4_summary_1M_v1.svg` | `extract.json`: plotted values read back from the v1 SVG vectors (log-axis fit from the tick labels). |
| `replot.py` | `extract.json` | `fig_rq4_summary_1M_v3.*`: (a),(b) throughput and latency speed-up, (c) throughput / best uniform, (d) inverted latency (higher = better), "DS2 (converged configuration)". Prints the Flink check (15.61, 13.77). |
| `rq4_values.py` | `datasets_v2/real_query_level.csv` | `rq4_values_1M.csv`. Attempt to rebuild the values from the dataset; it does **not** reproduce the v1 numbers (DS2 latency 2–11x instead of 15.6x), which is why `ext.py` exists. |

Why the SVG route: the original Fig. 9 script was not available, so its values were
recovered from the vector file to keep the published numbers identical.

## rq4_motivation/ — DoP 2 trajectory and trade-off exploration

Pipeline: `rq4_dop2_trajectory.py` → `rq4_dop2_per_app.csv`, `rq4_dop2_medians.csv`;
`rq4_dop2_common.py` → `rq4_dop2_common_geomean.csv`; the `*_plot*.py` and
`rq4_uniform_boxes*.py` scripts plot these. `rq4_tradeoff.py` → `rq4_tradeoff_*.csv`,
plotted by `rq4_tradeoff_plot.py`.

## rq5/ — synthetic query structures

Run `rq5_metrics.py` first (reads `datasets_v2/synthetic_{query,operator}_level.csv`, writes
`rq5_config_values.csv`, `rq5_summary_per_rate.csv`, `rq5_feasibility.csv`). Then:
`fig_rq5_paper.py` (paper figure), `fig_rq5_paper_marked.py` (with markers),
`rq5_plot.py`, `rq5_bars.py`, `rq5_dop_bars.py`, `rq5_struct_bars.py`,
`rq5_dop_by_system.py`, `rq5_one_plot.py` (exploration variants).

## data_checks/

| Script | Output |
|---|---|
| `missing_inventory.py` | `missing_inventory_cells.csv` (missing cells per system/app/strategy/rate/DoP) |
| `storm_spout_check.py` | `storm_spout_check.csv` (Storm spout output vs. sink output) |
| `window_mismatch_check.py` | `window_mismatch_runs.csv`, `window_mismatch_summary.csv` (runs whose window differs from the uniform reference) |
| `storm_latest_cells_check.py` | Prints the newest artifact run for each Storm cell that needed a rerun (reads `artifacts/`). |

`missing_inventory.py` and `storm_spout_check.py` write to the current directory.

## inputs/

Source figures used by the post-processing scripts: `fig_rq1_heatmaps_v2.pdf`,
`fig_rq2_heatmaps.pdf`, `fig_rq2_detail_40.pdf`, `fig_rq4_summary_1M_v1.svg`.

## Recreated scripts

Three scripts were rewritten for this folder because the originals were run inline:
`rq2/rq2_remove_memory_row.py` (tested, produces the same crop),
`data_checks/window_mismatch_check.py` and `data_checks/storm_latest_cells_check.py`
(same logic as the original checks; not re-run against the current data).
