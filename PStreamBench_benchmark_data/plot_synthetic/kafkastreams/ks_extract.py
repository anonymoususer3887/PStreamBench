"""Kafka Streams, step 2 (step2_plan_kafkastreams.md; approved 2026-10-26: all strategies, producer-rate column not used,
6 instances = master + 5 workers (playbook cluster-simple-kafkastreams-job.yaml, hosts alpha:betas), commit interval 30 s).
Read-only on data; writes only into this folder. Full precision; every run kept; medians per configuration with n_runs.
Synthetic: throughput = sink operator throughput_per_sec; consumed = sum of source-* throughput_per_sec;
  out_topic_50 = queryMetrics.kafka_throughput_50th; latency_avg/max = streamsMetrics.end_to_end_latency_avg/_max (ms);
  cpu_50/95; cpu_workers_max50 / cpu_master50 = per-node median CPU (max over workers / master);
  net_exp_50/95 = experiment_network_out (bytes/s); memory_50/95; threads = entry 1 of job_parallelization;
  n_inputs = number of source topics (n for n-way joins, else 1); offered_total = rate * n_inputs.
Real: query_performance_data.csv, kafkastream, m510: throughput_50/95/98, endtoend_latency, cpu_50/95, memory_50/95;
  threads = entry 1 of 'parallelism'. event_rate_* and network_* not used (producer rate; control interface)."""
import os, json, re, ast, statistics as st
import numpy as np, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); BD = os.path.dirname(os.path.dirname(HERE))
runs = pd.read_csv(os.path.join(os.path.dirname(HERE), 'runs.csv'))
S = runs[(runs.system == 'kafkastreams') & (runs.bad_json == 0)]
NSRC = {'TwoWayJoin':2,'ThreeWayJoin':3,'FourWayJoin':4,'FiveWayJoin':5,'SixWayJoin':6}
def num(v):
    if v is None: return np.nan
    if isinstance(v, dict): v = next(iter(v.values()), None)
    if isinstance(v, (int, float)): return float(v)
    m = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", str(v).split(':', 1)[-1] if str(v).startswith('{') else str(v))
    return float(m[0]) if m else np.nan
rows = []
for _, r in S.iterrows():
    d = json.load(open(r.file)); q = d.get('queryMetrics') or {}; sm = d.get('streamsMetrics') or {}; h = d.get('hardwareMetrics') or {}
    o = {}
    for k, m in (d.get('operatorMetrics') or {}).items():
        if isinstance(m, str):
            try: m = ast.literal_eval(m)
            except Exception: m = {}
        o[k] = m
    vec = [float(x) for x in str(r.vector).split(',')]
    wk, ms = [], np.nan
    for s in h.get('node_cpu') or []:
        vals = [float(x[1]) for x in s['values']]
        if not vals: continue
        if s['metric'].get('instance', '').startswith(('node0:', '10.10.1.1:')): ms = st.median(vals)
        else: wk.append(st.median(vals))
    nin = NSRC.get(r['shape'], 1)
    rows.append(dict(strategy=r.strategy, shape=r['shape'], rate=r.rate, config=r.config, vector=r.vector,
        threads=vec[1], total_threads=6 * vec[1], n_inputs=nin, offered_total=r.rate * nin,
        throughput=num((o.get('sink') or {}).get('throughput_per_sec')),
        consumed=float(np.nansum([num(m.get('throughput_per_sec')) for k, m in o.items() if k.startswith('source')])),
        out_topic_50=num(q.get('kafka_throughput_50th')), out_topic_95=num(q.get('kafka_throughput_95th')),
        latency_avg=num(sm.get('end_to_end_latency_avg')), latency_max=num(sm.get('end_to_end_latency_max')),
        cpu_50=num(h.get('node_cpu_50')), cpu_95=num(h.get('node_cpu_95')),
        cpu_workers_max50=max(wk) if wk else np.nan, cpu_master50=ms,
        net_exp_50=num(h.get('experiment_network_out_50')), net_exp_95=num(h.get('experiment_network_out_95')),
        memory_50=num(h.get('node_memory_50')), memory_95=num(h.get('node_memory_95')), file=r.file))
R = pd.DataFrame(rows).sort_values(['strategy','shape','rate','config'])
R.to_csv(os.path.join(HERE, 'ks_syn_runs.csv'), index=False)
nc = [c for c in R.columns if c not in ('strategy','shape','rate','config','vector','file')]
G = R.groupby(['strategy','shape','rate','config']); C = G[nc].median(); C['n_runs'] = G.size(); C['vector'] = G.vector.first()
C.reset_index().to_csv(os.path.join(HERE, 'ks_syn_configs.csv'), index=False)
d = pd.read_csv(os.path.join(BD, 'query_performance_data.csv'), low_memory=False)
k = d[d.framework == 'kafkastream'].copy()
k['app'] = k['query'].astype(str) + '|' + k['query_variant'].fillna('').astype(str)
k['threads'] = k['parallelism'].astype(str).str.split(',').str[1].astype(float)
cols = ['throughput_50','throughput_95','throughput_98','endtoend_latency','cpu_50','cpu_95','memory_50','memory_95']
k[['app','hardware_type','enumeration_strategy','producer_rate','parallelism','threads']+cols+['path']].to_csv(os.path.join(HERE,'ks_real_runs.csv'), index=False)
g = k.groupby(['app','enumeration_strategy','producer_rate','threads']); M = g[cols].median(); M['n_runs'] = g.size()
M.reset_index().to_csv(os.path.join(HERE,'ks_real_configs.csv'), index=False)
print('synthetic runs', len(R), 'configs', len(C), '| real runs', len(k), 'configs', len(M))
print('master instance found:', R.cpu_master50.notna().sum(), 'of', len(R))
