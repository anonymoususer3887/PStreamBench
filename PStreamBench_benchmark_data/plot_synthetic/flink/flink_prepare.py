"""Flink step 2 (step2_plan_flink.md; approved 2026-10-26: D1 all strategies, D2 new datasets_v2 operator data,
D3 job code read for edge types, failed runs skipped). Read-only; writes only into this folder.
Input: datasets_v2/synthetic_query_level.csv, synthetic_operator_level.csv (Flink rows, folder_flag empty,
strategies uniform/random/ds2/dhalion).
Failed run = consumed input (source_operator_throughput) missing or 0 -> listed in flink_failed_runs.csv, not analysed.
Edge types from dsp_flink_synthetic_jobs (SyntheticTopology/LinearQuery/ChainedFilters/JoinTopology):
  window-agg and join inputs are keyBy (all-to-all, p_u*p_v channels); other edges: forward (p_u) if p_u == p_v,
  rebalance (p_u*p_v) otherwise; into the sink: p_u. Chaining disabled, so every edge is an exchange.
Pipeline order (vector order): Linear: spout, filter1, window-agg, filter2, sink; ChainedFilters: spout, window-agg,
  filter1..N, sink; n-way join: spout-0..n-1, [filter-0..n-1 if n<=3], join-1..n-1, window-agg, sink.
Sources are in-job DataGeneratorSource (no Kafka), rate-limited to the offered rate per source."""
import os, numpy as np, pandas as pd
D = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'datasets_v2')
Q = pd.read_csv(os.path.join(D, 'synthetic_query_level.csv'), low_memory=False)
O = pd.read_csv(os.path.join(D, 'synthetic_operator_level.csv'), low_memory=False)
Q = Q[(Q.system == 'flink') & (Q.folder_flag.fillna('') == '') & Q.strategy.isin(['uniform', 'random', 'ds2', 'dhalion'])].copy()
O = O[O.run_id.isin(Q.run_id)].copy()
NJ = {'TwoWayJoin': 2, 'ThreeWayJoin': 3, 'FourWayJoin': 4, 'FiveWayJoin': 5, 'SixWayJoin': 6}
NF = {'TwoFilters': 2, 'ThreeFilters': 3, 'FourFilters': 4}
def stages(shape):
    if shape == 'LinearQuery': return [('spout', 'src'), ('filter1', 'nk'), ('window-agg', 'key'), ('filter2', 'nk'), ('sink', 'sink')]
    if shape in NF: return [('spout', 'src'), ('window-agg', 'key')] + [(f'filter{i}', 'nk') for i in range(1, NF[shape] + 1)] + [('sink', 'sink')]
    n = NJ[shape]; s = [(f'spout-{i}', 'src') for i in range(n)]
    if n <= 3: s += [(f'filter-{i}', 'nk') for i in range(n)]
    return s + [(f'join-{i}', 'key') for i in range(1, n)] + [('window-agg', 'key'), ('sink', 'sink')]
def edges(shape, p):
    st = stages(shape); P = dict(zip([s for s, _ in st], p)); E = []
    if shape in NJ:
        n = NJ[shape]; pre = n <= 3
        up = [f'filter-{i}' if pre else f'spout-{i}' for i in range(n)]
        if pre: E += [(f'spout-{i}', f'filter-{i}', 'nk') for i in range(n)]
        left = up[0]
        for i in range(1, n): E += [(left, f'join-{i}', 'key'), (up[i], f'join-{i}', 'key')]; left = f'join-{i}'
        E += [(left, 'window-agg', 'key'), ('window-agg', 'sink', 'sink')]
    else:
        names = [s for s, _ in st]; kinds = [k for _, k in st]
        E = [(names[i], names[i + 1], kinds[i + 1]) for i in range(len(names) - 1)]
    ch = 0
    for u, v, k in E:
        pu, pv = P[u], P[v]
        ch += pu if k == 'sink' else (pu * pv if (k == 'key' or pu != pv) else pu)
    return ch, len(E)
rows = []
for _, r in Q.iterrows():
    p = [int(float(x)) for x in str(r.parallelism).split(',')]
    st = stages(r.application)
    if len(st) != len(p): ch, ne = np.nan, np.nan
    else: ch, ne = edges(r.application, p)
    rows.append(dict(run_id=r.run_id, channels=ch, n_exchanges=ne))
Q = Q.merge(pd.DataFrame(rows), on='run_id')
Q['n_sources'] = Q.application.map(lambda s: NJ.get(s, 1)); Q['offered_total'] = Q.producer_rate * Q.n_sources
Q['consumed_frac'] = Q.source_operator_throughput / Q.offered_total
def opdop(r):
    p = [float(x) for x in str(r.parallelism).split(',')]; st = stages(r.application)
    return float(np.mean([pp for pp, (_, k) in zip(p, st) if k not in ('src', 'sink')])) if len(p) == len(st) else np.nan
Q['dop'] = Q.apply(opdop, axis=1)   # mean operator DoP without sources and sink (= level for uniform)
Q['failed'] = ~(Q.source_operator_throughput > 0)
Q[Q.failed][['run_id', 'strategy', 'application', 'producer_rate', 'parallelism', 'source_operator_throughput', 'cpu_busiest_node_50']].to_csv('flink_failed_runs.csv', index=False)
# operator data with pipeline position
pos = []
for _, r in Q.iterrows():
    for i, (s, k) in enumerate(stages(r.application)): pos.append(dict(run_id=r.run_id, operator_name=s, stage_pos=i, edge_kind=k))
O = O.merge(pd.DataFrame(pos), on=['run_id', 'operator_name'], how='left')
O.to_csv('flink_operator_rows.csv', index=False); Q.to_csv('flink_runs.csv', index=False)
print('runs', len(Q), 'failed', int(Q.failed.sum()), '| operator rows', len(O), 'unmapped operator names', int(O.stage_pos.isna().sum()))
print('channel count missing (vector length mismatch):', int(Q.channels.isna().sum()))
