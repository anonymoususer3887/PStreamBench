"""Extract synthetic-shape metrics for Flink, Storm, Spark, Kafka Streams (uniform, random, ds2, dhalion).
Read-only on the data; writes only into this folder. Approved choices (2026-10-24):
- (2026-10-24, user decision) Throughput = SINK output rate: see sink(). The consumed-input metric below is kept as column consumed_input.
- Consumed input = rows the system consumes: sum of source-operator throughput_per_sec
  (Flink, Kafka Streams; Flink fallback recPerSecOutSource_50), Storm: sum of recPerSecOutSource*_50
  (fallback throughput_50), Spark: sparkStreamingMetrics.processing_rate (average). NOT kafka event rate (= producer).
- Latency = p50 end-to-end: Flink queryMetrics.endtoend_latency_50, Spark endToEndLatency.end_to_end_latency_seconds_50th
  (x1000 -> ms), Kafka Streams streamsMetrics.end_to_end_latency_avg (average; only value reported). Storm: not collected.
- CPU = hardwareMetrics.node_cpu_50 (avg over 6 nodes of per-node median); node_cpu_95 as reference.
- DoP (x axis): operator DoP = mean of the parallelism vector without source entries and sink
  (n sources for an n-way join, otherwise 1); Kafka Streams: entry 1 (num.stream.threads, the only one applied).
- Skipped folders: archive, local, diagnostics. Spark uniform: uniform_e2e_rerun only.
- Every run is written (runs.csv); configurations with repeats get the median (configs.csv, with n_runs).
- Throughput p95 (Flink fallback, Storm, Spark only) is extracted as reference for configurations whose p50 is 0/missing.
- Zero/missing: kept as is (0 or NaN), flagged in plots, nothing dropped. Full precision."""
import os, re, glob, json, ast
import numpy as np, pandas as pd
ROOT = os.path.expanduser('~/mnt/PStreamBench/PStreamBench_workload_generator')
OUT = os.path.dirname(os.path.abspath(__file__))
NSRC = {'TwoWayJoin':2,'ThreeWayJoin':3,'FourWayJoin':4,'FiveWayJoin':5,'SixWayJoin':6}
def num(v):
    if v is None: return np.nan
    if isinstance(v, dict): v = next(iter(v.values()), None)
    if isinstance(v, (int, float)): return float(v)
    m = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", str(v).split(':', 1)[-1] if str(v).startswith('{') else str(v))
    return float(m[0]) if m else np.nan
def ops(d):
    out = {}
    for k, m in (d.get('operatorMetrics') or {}).items():
        if isinstance(m, str):
            try: m = ast.literal_eval(m)
            except Exception: m = {}
        out[k] = m
    return out
def thr(e, d):
    q = d.get('queryMetrics') or {}; o = ops(d)
    src = [num(m.get('throughput_per_sec')) for k, m in o.items() if k.startswith(('source', 'spout'))]
    src = [x for x in src if x == x]
    if e == 'flink': return sum(src) if src else num(q.get('recPerSecOutSource_50'))
    if e == 'kafkastreams': return sum(src) if src else np.nan
    if e == 'storm':
        v = [num(x) for k, x in q.items() if k.startswith('recPerSecOutSource') and k.endswith('_50')]
        v = [x for x in v if x == x]
        return sum(v) if v else num(q.get('throughput_50'))
    if e == 'spark': return num((d.get('sparkStreamingMetrics') or {}).get('processing_rate'))
def thr95(e, d):
    """p95 counterpart, only where the system reports percentiles (Flink fallback, Storm, Spark). Used only
    where the p50 value is 0 or missing (rule of 2026-10-24: p50 primary, p95 as reference)."""
    q = d.get('queryMetrics') or {}
    if e == 'flink': return num(q.get('recPerSecOutSource_95'))
    if e == 'storm':
        v = [num(x) for k, x in q.items() if k.startswith('recPerSecOutSource') and k.endswith('_95')]
        v = [x for x in v if x == x]
        return sum(v) if v else num(q.get('throughput_95'))
    if e == 'spark': return num((d.get('sparkStreamingMetrics') or {}).get('processing_rate_95th'))
    return np.nan
def sink(e, d):
    """Throughput = sink output rate (as in RQ1). Flink, Spark, Kafka Streams: sink operator throughput_per_sec;
    Storm: queryMetrics.throughput_50 (sink bolt). Returns (p50-or-rate, p95 where reported)."""
    q = d.get('queryMetrics') or {}; o = ops(d)
    if e == 'storm': return num(q.get('throughput_50')), num(q.get('throughput_95'))
    if e == 'flink':
        # Flink's sink operator reports records OUT, which is 0 for a sink. The sink's input rate equals the
        # output rate of the operator directly before it (operatorMetrics is stored in pipeline order).
        keys = list(o.keys())
        if 'sink' in keys and keys.index('sink') > 0:
            return num(o[keys[keys.index('sink') - 1]].get('throughput_per_sec')), np.nan
        return np.nan, np.nan
    v = [num(m.get('throughput_per_sec')) for k, m in o.items() if k.lower().startswith('sink') and isinstance(m, dict)]
    v = [x for x in v if x == x]
    return (sum(v) if v else np.nan), np.nan
def lat(e, d):
    if e == 'flink': return num((d.get('queryMetrics') or {}).get('endtoend_latency_50'))
    if e == 'spark':
        v = num((d.get('endToEndLatency') or {}).get('end_to_end_latency_seconds_50th')); return v * 1000
    if e == 'kafkastreams': return num((d.get('streamsMetrics') or {}).get('end_to_end_latency_avg'))
    return np.nan
def dop(e, shape, vec):
    v = [float(x) for x in str(vec).split(',') if x.strip()]
    if e == 'kafkastreams': return v[1]
    k = NSRC.get(shape, 1)
    mid = v[k:-1]
    return float(np.mean(mid)) if mid else np.nan
rows = []
for e in ['flink', 'storm', 'spark', 'kafkastreams']:
    for f in glob.glob(f'{ROOT}/{e}/dsp_{e}_synthetic_jobs/data/**/evaluationMetrics*.json', recursive=True):
        if any(x in f for x in ('/archive/', '/local/', '/diagnostics/')): continue
        p = f.split('/data/')[1].split('/'); st = p[3]
        if e == 'spark' and st == 'uniform': continue
        if st == 'uniform_e2e_rerun': st = 'uniform'
        if st not in ('uniform', 'random', 'ds2', 'dhalion'): continue
        if len(p) < 6 or not p[4].isdigit():   # file not inside a <rate>/<config> folder: cannot be placed, listed only
            rows.append(dict(system=e, strategy=st, shape=p[0], rate=np.nan, config='UNPLACED', bad_json=1, file=f)); continue
        shape, rate, cfg = p[0], int(p[4]), (p[5] if len(p) > 6 else '')
        mf = f.replace('evaluationMetrics', 'metaInfo')
        try: vec = json.load(open(mf)).get('job_parallelization')
        except Exception: vec = None
        try: d = json.load(open(f))
        except Exception:
            rows.append(dict(system=e, strategy=st, shape=shape, rate=rate, config=cfg, vector=vec, bad_json=1, file=f)); continue
        h = d.get('hardwareMetrics') or {}
        rows.append(dict(system=e, strategy=st, shape=shape, rate=rate, config=cfg, vector=vec,
                         dop=dop(e, shape, vec) if vec else np.nan,
                         throughput=sink(e, d)[0], throughput_95=sink(e, d)[1], consumed_input=thr(e, d), consumed_input_95=thr95(e, d), latency_ms=lat(e, d),
                         cpu_50=num(h.get('node_cpu_50')), cpu_95=num(h.get('node_cpu_95')), cpu_98=num(h.get('node_cpu_98')),
                         bad_json=0, file=f))
R = pd.DataFrame(rows).sort_values(['system', 'strategy', 'shape', 'rate', 'config'])
R.to_csv(f'{OUT}/runs.csv', index=False)
G = R[R.bad_json == 0].groupby(['system', 'strategy', 'shape', 'rate', 'config'])
C = G[['dop', 'throughput', 'throughput_95', 'consumed_input', 'consumed_input_95', 'latency_ms', 'cpu_50', 'cpu_95', 'cpu_98']].median()
C['n_runs'] = G.size(); C['vector'] = G['vector'].first()
C.reset_index().to_csv(f'{OUT}/configs.csv', index=False)
print(len(R), 'runs,', len(C), 'configurations')
print(R.groupby(['system','strategy']).size().to_string())
print('missing vector:', R.vector.isna().sum(), 'bad json:', R.bad_json.sum())
