"""Summaries for step2_plan_flink.md P1-P9 from flink_runs.csv / flink_operator_rows.csv. No labels, no thresholds.
Failed runs (no consumed input) excluded. Operator totals: throughput_total_per_sec (= per-subtask mean x DoP)."""
import pandas as pd, numpy as np
def rho(a, b):
    a, b = pd.Series(list(a)), pd.Series(list(b)); m = a.notna() & b.notna()
    return a[m].rank().corr(b[m].rank()) if m.sum() > 3 else np.nan
Q = pd.read_csv('flink_runs.csv'); Q = Q[~Q.failed]
O = pd.read_csv('flink_operator_rows.csv'); O = O[O.run_id.isin(Q.run_id)]
U = Q[Q.strategy == 'uniform']
rows = []
for (sh, rate), g in U.groupby(['application', 'producer_rate']):
    g = g.groupby('dop')[['consumed_frac', 'sink_operator_throughput', 'endtoend_latency_ms', 'cpu_50', 'cpu_95', 'network_experiment_50', 'n_exchanges']].median()
    s, l = g.sink_operator_throughput, g.endtoend_latency_ms
    rows.append(dict(shape=sh, rate=rate, n_exchanges=g.n_exchanges.iloc[0], consumed_frac_dop2=g.consumed_frac.get(2.0), consumed_frac_dop40=g.consumed_frac.get(40.0),
        consumed_frac_min=g.consumed_frac.min(), sink_dop40_over_max=s.get(40.0, np.nan) / s.max() if s.max() > 0 else np.nan, sink_max_dop=s.idxmax() if s.notna().any() else np.nan,
        lat_dop2=l.get(2.0), lat_dop40=l.get(40.0), lat_40_over_2=l.get(40.0, np.nan) / l.get(2.0, np.nan), lat_rho_dop=rho(l.index, l.values),
        cpu50_dop2=g.cpu_50.get(2.0), cpu50_dop40=g.cpu_50.get(40.0), net_40_over_2=g.network_experiment_50.get(40.0, np.nan) / g.network_experiment_50.get(2.0, np.nan)))
P = pd.DataFrame(rows); P.to_csv('flink_uniform_summary.csv', index=False)
pd.set_option('display.width', 250); print(P.round(3).to_string())
# P5: random configurations, latency vs channels and vs mean DoP, per shape and rate
R = Q[Q.strategy == 'random']; res = []
for (sh, rate), g in R.groupby(['application', 'producer_rate']):
    res.append(dict(shape=sh, rate=rate, n=int(g.endtoend_latency_ms.notna().sum()), rho_lat_channels=rho(g.channels, g.endtoend_latency_ms), rho_lat_dop=rho(g.dop, g.endtoend_latency_ms)))
P5 = pd.DataFrame(res); P5.to_csv('flink_p5_random_latency_rank.csv', index=False)
print(P5.round(3).to_string()); print('median rho channels', round(P5.rho_lat_channels.median(), 3), '| median rho dop', round(P5.rho_lat_dop.median(), 3),
      '| channels stronger in', int((P5.rho_lat_channels.abs() > P5.rho_lat_dop.abs()).sum()), 'of', int(P5[['rho_lat_channels', 'rho_lat_dop']].notna().all(axis=1).sum()))
# P6: marker latency per operator (relative to source) along the pipeline, uniform
OU = O.merge(U[['run_id', 'application', 'producer_rate', 'dop']], on='run_id', suffixes=('', '_r'))
OU = OU[OU.edge_kind != 'src']
M = OU.groupby(['application', 'producer_rate', 'dop', 'stage_pos', 'operator_name'])['processing_latency_50th'].median().reset_index()
M.to_csv('flink_p6_marker_latency_per_operator.csv', index=False)
last = M.sort_values('stage_pos').groupby(['application', 'producer_rate', 'dop']).tail(1)
print(last[last.producer_rate == 100000].pivot_table(index='application', columns='dop', values='processing_latency_50th').round(1).to_string())
# P2/P7: backpressure: source operators and maximum over non-source operators, busy ratio maximum
OB = O.merge(Q[['run_id', 'strategy', 'application', 'producer_rate', 'dop', 'consumed_frac']], on='run_id', suffixes=('', '_r'))
B = OB[OB.strategy_r == 'uniform'].groupby(['application', 'producer_rate', 'dop']).apply(lambda g: pd.Series(dict(
    src_backpressure=g[g.edge_kind == 'src'].backpressure_ratio.mean(), max_nonsrc_backpressure=g[g.edge_kind.isin(['nk', 'key'])].backpressure_ratio.max(),
    max_busy=g[g.edge_kind != 'src'].busy_ratio.max(), busiest_op=g[g.edge_kind != 'src'].sort_values('busy_ratio').operator_name.iloc[-1] if (g.edge_kind != 'src').any() else '',
    consumed_frac=g.consumed_frac.iloc[0]))).reset_index()
B.to_csv('flink_p2_p7_backpressure.csv', index=False)
print(B[B.producer_rate == 1000000].round(3).to_string())
