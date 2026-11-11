"""Spark step 2 (step2_plan_spark.md; approved 2026-10-27: D1-D6 as recommended).
Input: datasets_v2/synthetic_query_level.csv (Spark rows, folder_flag empty). Read-only; writes only into this folder.
Job code (dsp_spark_synthetic_jobs/SyntheticTopology.java): source = Spark 'rate' format (rowsPerSecond = offered rate,
no Kafka, no rampUpTime), noop sink, append mode, no trigger; narrow stages repartition(n); all shuffles (joins and
window-agg) share ONE spark.sql.shuffle.partitions value. The rate source emits rows for the elapsed time, so a slow
batch makes the next one larger (no per-trigger cap).
Configurations: uniform = uniform_e2e_rerun (primary; original 'uniform' kept separately as check, never mixed);
random/ds2/dhalion = original folders; RocksDB = *_rocksdb_retry (separate configuration).
Throughput rule: processing rate p50 (throughput_50); where it is 0/missing and p95 > 0, p95 is used and flagged.
The average processing rate (throughput_avg) is kept alongside.
Success: processing rate (p50, p95 or avg) > 0 or sink output > 0. Failures are kept and counted.
Repeated attempts: performance = median over successful attempts; attempts and failures counted per configuration."""
import os, re, numpy as np, pandas as pd
D = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'datasets_v2')
Q = pd.read_csv(os.path.join(D, 'synthetic_query_level.csv'), low_memory=False)
Q = Q[(Q.system == 'spark') & (Q.folder_flag.fillna('') == '')].copy()
CFG = {'uniform_e2e_rerun': ('uniform', 'default'), 'uniform': ('uniform_original', 'default'), 'random': ('random', 'default'),
       'ds2': ('ds2', 'default'), 'dhalion': ('dhalion', 'default'), 'uniform_rocksdb_retry': ('uniform', 'rocksdb'),
       'baseline_rocksdb_retry': ('baseline', 'rocksdb'), 'baseline': ('baseline', 'default'), 'baseline_e2e_rerun': ('baseline', 'default'),
       'ds2_e2e_rerun': ('ds2', 'default'), 'dhalion_e2e_rerun': ('dhalion', 'default')}
Q = Q[Q.strategy_folder.isin(CFG)].copy()
Q['strategy_a'] = Q.strategy_folder.map(lambda s: CFG[s][0]); Q['state_store'] = Q.strategy_folder.map(lambda s: CFG[s][1])
NJ = {'TwoWayJoin': 2, 'ThreeWayJoin': 3, 'FourWayJoin': 4, 'FiveWayJoin': 5, 'SixWayJoin': 6}
def dop(r):
    m = re.match(r'level(\d+)', str(r.config_folder))
    if m: return float(m.group(1))
    p = [float(x) for x in str(r.parallelism).split(',')]; k = NJ.get(r.application, 1)
    return float(np.mean(p[k:-1])) if len(p) > k + 1 else np.nan
Q['dop'] = Q.apply(dop, axis=1)
Q['thr'] = Q.throughput_50; Q['thr_flag'] = ''
inv = ~(Q.thr > 0) & (Q.throughput_95 > 0)
Q.loc[inv, 'thr'] = Q.loc[inv, 'throughput_95']; Q.loc[inv, 'thr_flag'] = 'p50=0 -> p95'
Q['success'] = (Q[['throughput_50', 'throughput_95', 'throughput_avg', 'sink_operator_throughput']].fillna(0) > 0).any(axis=1)
Q['n_sources'] = Q.application.map(lambda s: NJ.get(s, 1)); Q['offered_total'] = Q.producer_rate * Q.n_sources
Q.to_csv('spark_runs.csv', index=False)
key = ['strategy_a', 'state_store', 'application', 'producer_rate', 'dop']
S = Q[Q.success].groupby(key)[['thr', 'throughput_avg', 'sink_operator_throughput', 'spark_input_rate_50', 'spark_processing_latency_ms_50',
     'spark_processing_latency_ms_avg', 'endtoend_latency_ms', 'cpu_50', 'cpu_95', 'cpu_busiest_node_50', 'network_experiment_50', 'memory_50']].median()
A = Q.groupby(key).agg(attempts=('run_id', 'size'), successes=('success', 'sum'), flagged=('thr_flag', lambda s: int((s != '').sum())))
C = A.join(S, how='left').reset_index(); C.to_csv('spark_configs.csv', index=False)
print('runs', len(Q), '| configs', len(C)); print(Q.groupby(['strategy_a', 'state_store']).agg(runs=('run_id', 'size'), ok=('success', 'sum')).to_string())
print('p50 -> p95 flagged runs:', int(inv.sum()))
