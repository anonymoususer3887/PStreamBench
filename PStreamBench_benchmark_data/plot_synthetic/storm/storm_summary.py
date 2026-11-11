"""Summaries for P1-P4, P7 of step2_plan_storm.md. Reads storm_syn_configs.csv, storm_real_configs.csv. No labels/thresholds."""
import pandas as pd, numpy as np
def spearmanr(a, b):
    class R: pass
    r = R(); r.correlation = pd.Series(list(a)).rank().corr(pd.Series(list(b)).rank()); return r
C = pd.read_csv('storm_syn_configs.csv'); U = C[C.strategy == 'uniform']
out = []
for (sh, rate), g in U.groupby(['shape', 'rate']):
    g = g.set_index('dop').sort_index()
    def pk(col):
        s = g[col]; p = s.idxmax() if s.notna().any() else np.nan
        return p, s.max(), (s.get(40.0, np.nan) / s.max() if s.max() > 0 else np.nan)
    p1, m1, r1 = pk('stage1_input'); p2, m2, r2 = pk('throughput')
    out.append(dict(shape=sh, rate=rate, stage1_peak_dop=p1, stage1_peak=m1, stage1_dop40_over_peak=r1,
        stage1_dop2=g.stage1_input.get(2.0), sink_peak_dop=p2, sink_dop40_over_peak=r2,
        cpu50_dop2=g.cpu_50.get(2.0), cpu50_dop40=g.cpu_50.get(40.0), cpu95_dop40=g.cpu_95.get(40.0),
        cpuW_dop40=g.cpu_workers_max50.get(40.0), busy_max_at_dop40=g.busy_max.get(40.0), busy_max_overall=g.busy_max.max(),
        net_dop40_over_dop2=g.net_exp_50.get(40.0) / g.net_exp_50.get(2.0), input_dop40_over_dop2=g.stage1_input.get(40.0) / g.stage1_input.get(2.0),
        mem_dop2=g.memory_50.get(2.0), mem_dop40=g.memory_50.get(40.0)))
P = pd.DataFrame(out); P.to_csv('storm_uniform_summary.csv', index=False)
pd.set_option('display.width', 260); print(P.round(3).to_string())
# P3: all strategies, normalised stage-1 input vs executors / dop
A = C.copy(); A = A[A.stage1_input.notna()]
A['norm'] = A.stage1_input / A.groupby(['shape', 'rate']).stage1_input.transform('max')
res = []
for rate, g in A.groupby('rate'):
    for sub, gg in [('all', g), ('random', g[g.strategy == 'random'])]:
        res.append(dict(rate=rate, subset=sub, n=len(gg), rho_executors=spearmanr(gg.norm, gg.executors).correlation, rho_dop=spearmanr(gg.norm, gg.dop).correlation))
print(pd.DataFrame(res).round(3).to_string()); pd.DataFrame(res).to_csv('storm_p3_rank_correlation.csv', index=False)
# P4: real, peak DoP of sink throughput per app, rate, hardware
R = pd.read_csv('storm_real_configs.csv')
pk = R.loc[R.groupby(['app', 'hardware_type', 'slots', 'producer_rate']).throughput_50.idxmax().dropna()]
pk = pk[['app', 'hardware_type', 'slots', 'producer_rate', 'parallelism_avg', 'throughput_50']]
pk.to_csv('storm_p4_peak_dop_real.csv', index=False)
print(pk.groupby(['hardware_type', 'slots', 'producer_rate']).parallelism_avg.describe()[['count', '25%', '50%', '75%']])
print(R.groupby(['hardware_type','slots']).parallelism_avg.agg(lambda s: sorted(s.unique())))
