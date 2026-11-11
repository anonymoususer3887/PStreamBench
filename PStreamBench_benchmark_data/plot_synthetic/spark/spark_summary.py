"""Summaries for step2_plan_spark.md. Reads spark_runs.csv / spark_configs.csv and datasets_v2 real rows. No labels/thresholds."""
import os, pandas as pd, numpy as np
def rho(a, b):
    a, b = pd.Series(list(a)), pd.Series(list(b)); m = a.notna() & b.notna()
    return a[m].rank().corr(b[m].rank()) if m.sum() > 3 else np.nan
Q = pd.read_csv('spark_runs.csv'); C = pd.read_csv('spark_configs.csv')
U = C[(C.strategy_a == 'uniform') & (C.state_store == 'default')]
rows = []
for (sh, rate), g in U.groupby(['application', 'producer_rate']):
    g = g.set_index('dop').sort_index(); t = g.thr; b = g.spark_processing_latency_ms_50; l = g.endtoend_latency_ms
    rows.append(dict(shape=sh, rate=rate, configs=len(g), configs_ok=int((g.successes > 0).sum()),
        thr_dop2=t.get(2.0), thr_max=t.max(), thr_max_dop=t.idxmax() if t.notna().any() else np.nan, thr_dop40_over_max=t.get(40.0, np.nan) / t.max() if t.max() > 0 else np.nan,
        thr_dop40_over_offered=t.get(40.0, np.nan) / g.offered if 'offered' in g else np.nan,
        batch_ms_dop2=b.get(2.0), batch_ms_dop40=b.get(40.0), batch_40_over_2=b.get(40.0, np.nan) / b.get(2.0, np.nan),
        lat_ms_dop2=l.get(2.0), lat_ms_dop40=l.get(40.0), lat_40_over_2=l.get(40.0, np.nan) / l.get(2.0, np.nan),
        cpu50_dop2=g.cpu_50.get(2.0), cpu50_dop40=g.cpu_50.get(40.0), net_40_over_2=g.network_experiment_50.get(40.0, np.nan) / g.network_experiment_50.get(2.0, np.nan),
        mem_dop2=g.memory_50.get(2.0), mem_dop40=g.memory_50.get(40.0)))
P = pd.DataFrame(rows); P.to_csv('spark_uniform_summary.csv', index=False)
pd.set_option('display.width', 260); print(P.drop(columns=['thr_dop40_over_offered']).round(3).to_string())
ok = U[U.successes > 0]
print('P3 rank corr latency vs batch duration (all successful uniform configs):', round(rho(ok.spark_processing_latency_ms_50, ok.endtoend_latency_ms), 3), 'n =', int(ok[['spark_processing_latency_ms_50', 'endtoend_latency_ms']].notna().all(axis=1).sum()))
# failures: CPU of failed attempts (busy = failed while running; idle = never processed)
F = Q[~Q.success & (Q.state_store == 'default')]
F = F.assign(cpu_state=np.where(F.cpu_busiest_node_50 >= 15, 'busy (>=15% on busiest node)', 'idle (<15%)'))
FT = F.groupby(['strategy_a', 'application', 'producer_rate', 'cpu_state']).size().unstack(fill_value=0); FT.to_csv('spark_failures_by_cpu.csv')
print(FT.to_string())
R = Q[Q.state_store == 'rocksdb']
print('RocksDB runs', len(R), 'successful', int(R.success.sum()), '| busiest-node CPU median', round(R.cpu_busiest_node_50.median(), 2), '| max', round(R.cpu_busiest_node_50.max(), 2))
# real applications (m510, uniform): processing rate and latency DoP 40 vs DoP 2
D = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'datasets_v2')
r = pd.read_csv(os.path.join(D, 'real_query_level.csv'), low_memory=False)
r = r[(r.system == 'spark') & (r.strategy == 'uniform') & (r.hardware_type == 'm510') & (r.folder_flag.fillna('') == '')]
g = r.groupby(['application', 'variant', 'producer_rate', 'parallelism_avg'], dropna=False)[['throughput_50', 'throughput_avg', 'spark_processing_latency_ms_50', 'endtoend_latency_ms', 'cpu_50']].median().reset_index()
out = []
for (a, v, rate), h in g.groupby(['application', 'variant', 'producer_rate'], dropna=False):
    h = h.set_index('parallelism_avg')
    if 2.0 in h.index and 40.0 in h.index:
        out.append(dict(app=a, variant=v, rate=rate, thr_40_over_2=h.throughput_avg[40.0] / h.throughput_avg[2.0] if h.throughput_avg[2.0] > 0 else np.nan,
                        batch_40_over_2=h.spark_processing_latency_ms_50[40.0] / h.spark_processing_latency_ms_50[2.0] if h.spark_processing_latency_ms_50[2.0] > 0 else np.nan,
                        lat_40_over_2=h.endtoend_latency_ms[40.0] / h.endtoend_latency_ms[2.0] if h.endtoend_latency_ms[2.0] > 0 else np.nan,
                        cpu_dop2=h.cpu_50[2.0], cpu_dop40=h.cpu_50[40.0]))
RR = pd.DataFrame(out); RR.to_csv('spark_real_uniform_summary.csv', index=False)
print(RR.describe().round(3).to_string())
print('real: latency 40/2 > 1 in', int((RR.lat_40_over_2 > 1).sum()), 'of', int(RR.lat_40_over_2.notna().sum()), '| batch 40/2 > 1 in', int((RR.batch_40_over_2 > 1).sum()), 'of', int(RR.batch_40_over_2.notna().sum()))
