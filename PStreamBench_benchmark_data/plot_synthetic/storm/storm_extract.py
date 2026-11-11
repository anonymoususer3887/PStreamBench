"""Storm, step 2 (plan step2_plan_storm.md, recommendations accepted 2026-10-25; spout metric dropped as unreliable).
Read-only on data; writes only into this folder. Full precision; every run kept; medians per configuration with n_runs.
Synthetic (uniform, random, ds2, dhalion):
  throughput   = queryMetrics.throughput_50 (sink bolt), _95/_98 as reference
  stage1_input = rows/s executed by the first-stage bolts: n-way join -> sum of 'filter-<i>' bolts; otherwise the
                 highest bolt throughput_per_sec; 4-6-way joins (no filter bolts): the 'join' bolt (the first stage executes every input row; dict order is not pipeline order)
  busy_max     = highest bolt busy_ratio (Storm 'capacity': share of time executing); bolt = its name
  cpu_50/95    = hardwareMetrics.node_cpu_50/95 (avg over 6 nodes of per-node quantile over time)
  cpu_workers_max50 = max over worker nodes (10.10.1.2-6) of per-node median CPU; cpu_master50 = node 10.10.1.1
  net_exp_50/95 = hardwareMetrics.experiment_network_out_50/95 (experiment LAN, eno1d1), bytes/s
  net_ctrl_50 = node_network_out_50 (control interface eno1); memory_50/95 = node_memory_50/95
  dop = mean of operator entries of job_parallelization (without n spouts and the sink); executors = sum of the vector
Real (query_performance_data.csv, storm, uniform, all hardware): throughput_50/95/98, cpu_50/95, memory_50/95,
  network_50/95 (= node_network_out, i.e. control interface; see 1_data_extractor.py), parallelism_avg, nodes, slots."""
import os, json, re, glob, statistics as st
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
BD = os.path.dirname(os.path.dirname(HERE))
runs = pd.read_csv(os.path.join(os.path.dirname(HERE), 'runs.csv'))
S = runs[(runs.system == 'storm') & (runs.config != 'UNPLACED') & (runs.bad_json == 0)]
NSRC = {'TwoWayJoin':2,'ThreeWayJoin':3,'FourWayJoin':4,'FiveWayJoin':5,'SixWayJoin':6}
def num(v):
    if v is None: return np.nan
    if isinstance(v, dict): v = next(iter(v.values()), None)
    try: return float(v)
    except Exception:
        m = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", str(v)); return float(m[0]) if m else np.nan
rows = []
for _, r in S.iterrows():
    d = json.load(open(r.file)); q = d.get('queryMetrics') or {}; o = d.get('operatorMetrics') or {}; h = d.get('hardwareMetrics') or {}
    vec = [float(x) for x in str(r.vector).split(',')]
    k = NSRC.get(r['shape'], 1)
    bolts = {n: m for n, m in o.items() if isinstance(m, dict)}
    thr = {n: num(m.get('throughput_per_sec')) for n, m in bolts.items()}
    f1 = [v for n, v in thr.items() if re.fullmatch(r'filter-\d+', n)]
    if r['shape'] in NSRC: stage1 = np.nansum(f1) if f1 else thr.get('join', np.nan)   # 4-6-way joins have no per-input filter bolts
    else: stage1 = np.nanmax(list(thr.values())) if thr else np.nan
    busy = {n: num(m.get('busy_ratio')) for n, m in bolts.items()}
    bmax_name = max(busy, key=lambda n: busy[n] if busy[n] == busy[n] else -1) if busy else ''
    wk, ms = [], np.nan
    for s in h.get('node_cpu') or []:
        vals = [float(x[1]) for x in s['values']]
        if not vals: continue
        if s['metric'].get('instance', '').startswith('10.10.1.1:'): ms = st.median(vals)
        else: wk.append(st.median(vals))
    rows.append(dict(strategy=r.strategy, shape=r['shape'], rate=r.rate, config=r.config, vector=r.vector,
        dop=float(np.mean(vec[k:-1])), executors=sum(vec),
        throughput=num(q.get('throughput_50')), throughput_95=num(q.get('throughput_95')), throughput_98=num(q.get('throughput_98')),
        stage1_input=stage1, busy_max=busy.get(bmax_name, np.nan), busy_max_bolt=bmax_name,
        cpu_50=num(h.get('node_cpu_50')), cpu_95=num(h.get('node_cpu_95')),
        cpu_workers_max50=max(wk) if wk else np.nan, cpu_master50=ms,
        net_exp_50=num(h.get('experiment_network_out_50')), net_exp_95=num(h.get('experiment_network_out_95')),
        net_ctrl_50=num(h.get('node_network_out_50')),
        memory_50=num(h.get('node_memory_50')), memory_95=num(h.get('node_memory_95')), file=r.file))
R = pd.DataFrame(rows).sort_values(['strategy','shape','rate','config'])
R.to_csv(os.path.join(HERE, 'storm_syn_runs.csv'), index=False)
num_cols = [c for c in R.columns if c not in ('strategy','shape','rate','config','vector','busy_max_bolt','file')]
G = R.groupby(['strategy','shape','rate','config'])
C = G[num_cols].median(); C['n_runs'] = G.size(); C['vector'] = G['vector'].first()
C.reset_index().to_csv(os.path.join(HERE, 'storm_syn_configs.csv'), index=False)
# real applications
d = pd.read_csv(os.path.join(BD, 'query_performance_data.csv'), low_memory=False)
r = d[(d.framework == 'storm') & (d.enumeration_strategy == 'uniform')].copy()
r['app'] = r['query'].astype(str) + '|' + r['query_variant'].fillna('').astype(str)
cols = ['throughput_50','throughput_95','throughput_98','event_rate_50','cpu_50','cpu_95','memory_50','memory_95','network_50','network_95']
r[['app','hardware_type','nodes','slots_per_node','slots','producer_rate','parallelism','parallelism_avg']+cols+['path']].to_csv(os.path.join(HERE,'storm_real_runs.csv'), index=False)
g = r.groupby(['app','hardware_type','slots','producer_rate','parallelism_avg'])
M = g[cols].median(); M['n_runs'] = g.size(); M.reset_index().to_csv(os.path.join(HERE,'storm_real_configs.csv'), index=False)
print('synthetic runs', len(R), 'configs', len(C), '| real runs', len(r), 'configs', len(M))
