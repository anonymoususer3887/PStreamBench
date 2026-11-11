"""Summaries for step2_plan_kafkastreams.md (P1-P7). Reads ks_syn_configs.csv, ks_real_configs.csv. No labels/thresholds."""
import pandas as pd, numpy as np
C = pd.read_csv('ks_syn_configs.csv'); C['cons_frac'] = C.consumed / C.offered_total
U = C[C.strategy == 'uniform']
rows = []
for (sh, rate), g in U.groupby(['shape', 'rate']):
    g = g.set_index('threads').sort_index()
    c = g.consumed; t = g.throughput
    rows.append(dict(shape=sh, rate=rate,
        cons_frac_t2=g.cons_frac.get(2.0), cons_frac_t8=g.cons_frac.get(8.0), cons_frac_t40=g.cons_frac.get(40.0),
        cons_t40_over_max=c.get(40.0) / c.max(), cons_max_threads=c.idxmax(),
        sink_t2=t.get(2.0), sink_t8=t.get(8.0), sink_t40=t.get(40.0),
        lat_s_t2=g.latency_avg.get(2.0) / 1000, lat_s_t40=g.latency_avg.get(40.0) / 1000, lat_s_min=g.latency_avg.min() / 1000,
        cpu50_t8=g.cpu_50.get(8.0), cpu50_t40=g.cpu_50.get(40.0),
        cpuW_t8=g.cpu_workers_max50.get(8.0), cpuW_t40=g.cpu_workers_max50.get(40.0), cpu_master_t40=g.cpu_master50.get(40.0),
        net_MBps_t2=g.net_exp_50.get(2.0) / 1e6, net_MBps_t40=g.net_exp_50.get(40.0) / 1e6))
P = pd.DataFrame(rows); P.to_csv('ks_uniform_summary.csv', index=False)
pd.set_option('display.width', 280); print(P.round(3).to_string())
# consistency: random vs uniform at the same thread count (consumed input)
R = C[C.strategy == 'random'].merge(U[['shape', 'rate', 'threads', 'consumed']], on=['shape', 'rate', 'threads'], how='left', suffixes=('', '_uniform'))
R['ratio_to_uniform'] = R.consumed / R.consumed_uniform
R.to_csv('ks_random_vs_uniform.csv', index=False)
print('random configs with matching uniform thread level:', R.consumed_uniform.notna().sum(), 'of', len(R))
print('ratio random/uniform consumed, quantiles:', R.ratio_to_uniform.quantile([.1, .25, .5, .75, .9]).round(3).to_dict())
# real applications, uniform
M = pd.read_csv('ks_real_configs.csv'); M = M[M.enumeration_strategy == 'uniform']
rr = []
for (a, rate), g in M.groupby(['app', 'producer_rate']):
    g = g.set_index('threads').sort_index(); t = g.throughput_50
    if t.max() > 0:
        rr.append(dict(app=a, rate=rate, levels=len(g), max_threads=t.idxmax(), t40_over_max=t.get(40.0, np.nan) / t.max(),
                       t2=t.get(2.0), t40=t.get(40.0), cpu50_t2=g.cpu_50.get(2.0), cpu50_t40=g.cpu_50.get(40.0),
                       lat_s_t2=g.endtoend_latency.get(2.0, np.nan) / 1000, lat_s_t40=g.endtoend_latency.get(40.0, np.nan) / 1000))
RR = pd.DataFrame(rr); RR.to_csv('ks_real_uniform_summary.csv', index=False)
print(RR.round(3).to_string())
