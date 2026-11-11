"""Build query-level and operator-level datasets for real and synthetic workloads (PStreamBench).

Read-only on all inputs; writes only into this folder (datasets_v2/). Run:
    python3 build_datasets.py part real:SYSTEM        (SYSTEM = flink|storm|spark|kafkastream|risingwave)
    python3 build_datasets.py part synthetic:SYSTEM
    python3 build_datasets.py merge
Rules (agreed 2026-10-26):
- Real: only artifacts/<application>/... (artifacts/workloads_hardwares_additional/ and
  artifacts/query_and_operator_metrics/ are NOT used).
  Synthetic: PStreamBench_workload_generator/<system>/dsp_<system>_synthetic_jobs/data/**.
- One row per run (query level) and per run x operator (operator level); run_id = path of the
  evaluationMetrics file. Failed or unreadable runs are kept and flagged, never dropped.
  Folder names are kept as found (strategy_folder, folder_flag).
- Full precision; nothing rounded or thresholded. Missing = empty (NaN), never replaced by 0.
- End-to-end latency: ONE column family, endtoend_latency_ms (+ _95/_98/_max where reported), in ms.
  latency_stat says which statistic it is, latency_metric says what the system measures.
- Real 'uniform': a metric missing in a uniform run is taken from uniform_1 for the same system,
  application, variant, hardware, rate and parallelism vector; fill_note says which metrics and
  which run. A configuration present only in uniform_1 gets strategy='uniform' and a fill_note.
- Flink operator throughput_per_sec is a per-subtask mean; operator totals multiply by operator_dop.
- throughput_* keeps the per-system definition of query_performance_data.csv (see README).
  sink_operator_throughput is given separately for every system.
"""
import os, sys, re, ast, json, glob, pickle, importlib.util, statistics
import numpy as np
import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
BD = os.path.dirname(HERE)                                      # PStreamBench_benchmark_data
WG = os.path.join(os.path.dirname(BD), 'PStreamBench_workload_generator')
_spec = importlib.util.spec_from_file_location('dx', os.path.join(BD, '1_data_extractor.py'))
dx = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(dx)   # helpers only; its main() is not run

FW = {'Apache Flink': 'flink', 'Apache Storm': 'storm', 'Apache Spark': 'spark',
      'Apache Kafka Streams': 'kafkastream', 'RisingWave': 'risingwave'}
SYN_DIR = {'flink': 'flink', 'storm': 'storm', 'spark': 'spark', 'kafkastream': 'kafkastreams', 'risingwave': 'risingwave'}
STRATS = {'uniform', 'uniform_1', 'uniform_2', 'random', 'ds2', 'dhalion', 'extra', 'uniform_e2e_rerun'}
LAT = {
    'flink': ('latency markers: source to sink; exclude waiting in Kafka, operator processing and window waits', 'p50'),
    'storm': ('Storm-reported end-to-end latency (queryMetrics.endtoend_latency_storm)', 'reported value'),
    'spark': ('ingestion-to-processing lag (Dataset.observe), stops before the sink; max over input streams', 'p50'),
    'kafkastream': ('native record end-to-end latency (record-e2e-latency), includes waiting in Kafka', 'avg'),
    'risingwave': ('barrier in-flight latency (p99 barrier duration, internal); excludes waiting in Kafka', 'avg'),
}
THR_DEF = {
    'flink': 'queryMetrics.throughput series: summed per timestamp, leading/trailing zeros trimmed, percentiles',
    'storm': 'queryMetrics.throughput series: summed per timestamp, leading/trailing zeros trimmed, percentiles',
    'spark': 'sparkStreamingMetrics.processing_rate_*th (records processed per second)',
    'kafkastream': 'sink operator throughput_per_sec (single value in all three columns)',
    'risingwave': 'sink operator throughput_per_sec (single value in all three columns)',
}


def num(v):
    """Scalar from the raw formats used in the result files (float, str, dict-as-str, nested dict)."""
    if v is None:
        return np.nan
    if isinstance(v, dict):
        return num(next(iter(v.values()), None))
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v)
    if s.startswith('['):
        return np.nan
    if s.startswith('{'):
        try:
            return num(ast.literal_eval(s))
        except Exception:
            s = s.split(':', 1)[-1]
    try:
        return float(s)
    except ValueError:
        m = re.findall(r"[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?", s)
        return float(m[0]) if m else np.nan


def todict(m):
    if isinstance(m, dict):
        return m
    try:
        return ast.literal_eval(m)
    except Exception:
        return {}


def ops_of(d):
    return {k: todict(m) for k, m in (d.get('operatorMetrics') or {}).items()}


def series_pct(obj):
    ts, vals = dx.extract_prom_series(obj, agg='sum')
    _, v, lead, trail = dx.trim_leading_trailing_zeros(ts, vals)
    p = dx.compute_percentiles_sorted(v, qs=(0.50, 0.95, 0.98))
    f = lambda x: np.nan if x is pd.NA else x
    return f(p[0.50]), f(p[0.95]), f(p[0.98]), len(v), lead, trail


def busiest_node(h):
    med = []
    for s in (h.get('node_cpu') or []):
        try:
            vals = [float(x[1]) for x in s['values']]
        except Exception:
            continue
        if vals:
            med.append(statistics.median(vals))
    return max(med) if med else np.nan


def op_dop(m):
    hits = [int(x) for x in re.findall(r'parallelization\s*:\s*(\d+)', str(m)) if int(x) > 0]
    return max(set(hits), key=hits.count) if hits else np.nan


def metrics(fw, d):
    q = d.get('queryMetrics') or {}
    h = d.get('hardwareMetrics') or {}
    o = ops_of(d)
    r = {'throughput_definition': THR_DEF[fw]}
    # --- throughput as in query_performance_data.csv
    if fw in ('flink', 'storm') and isinstance(q.get('throughput'), list):
        (r['throughput_50'], r['throughput_95'], r['throughput_98'], r['throughput_n'],
         r['throughput_trim_leading'], r['throughput_trim_trailing']) = series_pct(q['throughput'])
    elif fw in ('flink', 'storm'):
        r['throughput_50'] = num(q.get('throughput_50'))
        r['throughput_95'] = num(q.get('throughput_95'))
        r['throughput_98'] = num(q.get('throughput_98'))
        r['throughput_definition'] = 'queryMetrics.throughput_50/95/98 (no series stored)'
    elif fw == 'spark':
        s = d.get('sparkStreamingMetrics') or {}
        r['throughput_50'] = num(s.get('processing_rate_50th'))
        r['throughput_95'] = num(s.get('processing_rate_95th'))
        r['throughput_98'] = num(s.get('processing_rate_98th'))
        r['throughput_avg'] = num(s.get('processing_rate'))
    else:
        v = num((o.get('sink') or {}).get('throughput_per_sec'))
        r['throughput_50'] = r['throughput_95'] = r['throughput_98'] = v
    if 'throughput_avg' not in r:
        r['throughput_avg'] = num(q.get('throughput_avg'))
    # --- sink operator throughput, every system
    keys = list(o.keys())
    if fw == 'flink' and 'sink' in keys and keys.index('sink') > 0:
        prev = keys[keys.index('sink') - 1]
        r['sink_operator_throughput'] = num(o[prev].get('throughput_per_sec')) * op_dop(o[prev])
        r['sink_operator_throughput_note'] = (f'operator before the sink ({prev}) x its DoP; Flink reports the per-subtask '
                                              f'mean (avg over subtasks in getprom.py) and its sink reports records out = 0')
    else:
        r['sink_operator_throughput'] = num((o.get('sink') or {}).get('throughput_per_sec'))
    # --- consumed input
    # Flink's operator throughput_per_sec is the MEAN over subtasks (getprom.py: avg(avg_over_time(...))), so the
    # operator total is throughput_per_sec x operator DoP; the other systems already report the operator total.
    mult = (lambda m: op_dop(m)) if fw == 'flink' else (lambda m: 1.0)
    src = [num(m.get('throughput_per_sec')) * mult(m) for k, m in o.items() if k.lower().startswith(('source', 'spout'))]
    src = [x for x in src if x == x]
    r['source_operator_throughput'] = float(np.sum(src)) if src else np.nan
    for p in ('50', '95', '98', 'avg', 'max'):
        vs = [num(v) for k, v in q.items() if k.startswith('recPerSecOutSource') and k.endswith('_' + p)]
        vs = [x for x in vs if x == x]
        r[f'source_output_{p}'] = float(np.sum(vs)) if vs else np.nan
    if fw == 'spark':
        s = d.get('sparkStreamingMetrics') or {}
        for a, b in (('spark_input_rate', 'input_rate'), ('spark_processing_latency_ms', 'processing_latency_ms')):
            r[a + '_avg'] = num(s.get(b))
            for p in ('50', '95', '98'):
                r[f'{a}_{p}'] = num(s.get(f'{b}_{p}th'))
    # --- rates measured at the Kafka broker
    for a, b in (('broker_input_rate', 'kafka_event_rate'), ('broker_output_rate', 'kafka_throughput')):
        r[a + '_avg'] = num(q.get(b))
        for p in ('50', '95', '98'):
            r[f'{a}_{p}'] = num(q.get(f'{b}_{p}th'))
    # --- end-to-end latency, one column family, ms
    r['latency_metric'], r['latency_stat'] = LAT[fw]
    if fw == 'flink':
        r['endtoend_latency_ms'] = num(q.get('endtoend_latency_50'))
        r['endtoend_latency_95_ms'] = num(q.get('endtoend_latency_95'))
        r['endtoend_latency_98_ms'] = num(q.get('endtoend_latency_98'))
    elif fw == 'storm':
        r['endtoend_latency_ms'] = num(q.get('endtoend_latency_storm'))
    elif fw == 'spark':
        e = d.get('endToEndLatency') or {}
        for p, col in (('50', 'endtoend_latency_ms'), ('95', 'endtoend_latency_95_ms'), ('98', 'endtoend_latency_98_ms')):
            vs = [num(v) for k, v in e.items() if 'end_to_end_latency_seconds' in k and k.endswith(f'_{p}th')]
            vs = [x for x in vs if x == x]
            r[col] = max(vs) * 1000.0 if vs else np.nan
        r['spark_latency_inputs'] = len([k for k in e if k.endswith('_50th')])
    elif fw == 'kafkastream':
        sm = d.get('streamsMetrics') or {}
        r['endtoend_latency_ms'] = num(sm.get('end_to_end_latency_avg'))
        r['endtoend_latency_max_ms'] = num(sm.get('end_to_end_latency_max'))
    else:
        am = d.get('actorMetrics') or {}
        r['endtoend_latency_ms'] = num(am.get('barrier_inflight_latency_avg')) * 1000.0
        r['endtoend_latency_max_ms'] = num(am.get('barrier_inflight_latency_max')) * 1000.0
    # --- hardware (node exporter)
    for a, b in (('cpu', 'node_cpu'), ('memory', 'node_memory'),
                 ('network_control', 'node_network_out'), ('network_experiment', 'experiment_network_out')):
        for p in ('50', '95', '98'):
            r[f'{a}_{p}'] = num(h.get(f'{b}_{p}'))
    r['cpu_busiest_node_50'] = busiest_node(h)
    if fw == 'flink':
        for a, b in (('flink_tm_cpu', 'cpu'), ('flink_tm_network', 'NW'), ('flink_tm_heap', 'Memory')):
            for p in ('50', '95', '98'):
                r[f'{a}_{p}'] = num(q.get(f'{b}_{p}'))
    return r, o


def operator_rows(run_id, fw, o):
    rows = []
    for i, (name, m) in enumerate(o.items()):
        row = dict(run_id=run_id, operator_index=i, operator_name=name,
                   operator_dop=op_dop(m) if fw == 'flink' else np.nan)
        for k, v in m.items():
            row[k] = num(v)
        # operator total rate: Flink reports the per-subtask mean, the other systems the total
        row['throughput_total_per_sec'] = row.get('throughput_per_sec', np.nan) * (row['operator_dop'] if fw == 'flink' else 1.0)
        rows.append(row)
    return rows


def real_files(system):
    # engine folder matched case-insensitively and kept with its on-disk spelling (e.g. smart-grid/m510/Flink)
    engines = [e for e in glob.glob(os.path.join(BD, 'artifacts', '*', '*', '*')) if os.path.basename(e).lower() == system]
    files = [f for e in engines for f in glob.glob(os.path.join(e, '**', 'evaluationMetrics*.json'), recursive=True)]
    if system == 'flink':   # 'others' folders: engine taken from metaInfo, files attached to this part
        files += glob.glob(os.path.join(BD, 'artifacts', '*', '*', 'others', '**', 'evaluationMetrics*.json'), recursive=True)
    skip = ('workloads_hardwares_additional', 'query_and_operator_metrics')
    return [f for f in files if not any(s in f for s in skip)]


def describe_real(f):
    rel = os.path.relpath(f, os.path.join(BD, 'artifacts'))
    p = rel.split(os.sep)
    slug, hw, eng, rest = p[0], p[1], p[2], p[3:-1]
    sf = next((x for x in rest if x in STRATS), '')
    vf = rest[0] if rest and rest[0] not in STRATS and not rest[0].isdigit() else ''
    rate = next((x for x in rest if x.isdigit()), '')
    flag = 'others' if eng == 'others' else ('extra' if sf == 'extra' else '')
    try:
        app = dx.get_query_from_directory(slug)
    except Exception:
        app = slug
    return dict(data_kind='real', application_slug=slug, application=app, hardware_type=hw, engine_folder=eng,
                variant_folder=vf, strategy_folder=sf or '(none)', rate_folder=rate, config_folder='',
                folder_flag=flag, run_id='artifacts/' + rel)


def describe_syn(f, system):
    rel = os.path.relpath(f, WG)
    p = rel.split(os.sep)
    q = p[p.index('data') + 1:]
    flag = next((x for x in q if x in ('archive', 'local', 'diagnostics')), '')
    sf = q[3] if len(q) > 4 else '(none)'
    rate = q[4] if len(q) > 5 and q[4].isdigit() else ''
    cfg = q[5] if rate and len(q) > 6 else ''
    if not rate:
        flag = flag or 'outside_rate_folder'
    return dict(data_kind='synthetic', application_slug=q[0], application=q[0], hardware_type='',
                engine_folder=system, variant_folder='', strategy_folder=sf, rate_folder=rate,
                config_folder=cfg, folder_flag=flag, run_id='workload_generator/' + rel)


def build_part(kind, system):
    if kind == 'real':
        files = real_files(system)
    else:
        sd = SYN_DIR[system]
        files = glob.glob(os.path.join(WG, sd, f'dsp_{sd}_synthetic_jobs', 'data', '**', 'evaluationMetrics*.json'), recursive=True)
    Q, O = [], []
    for f in files:
        row = describe_real(f) if kind == 'real' else describe_syn(f, system)
        row['file_name'] = os.path.basename(f)
        try:
            info = json.load(open(f.replace('evaluationMetrics', 'metaInfo')))
            row['missing_meta'] = 0
        except Exception:
            info, row['missing_meta'] = {}, 1
        fw = FW.get(info.get('job_framework'), system if (kind == 'synthetic' or row['engine_folder'] != 'others') else '')
        row['system'] = fw
        vec = info.get('job_parallelization')
        if vec is not None:
            vec = str(vec).replace('-', ',')   # some newer runs write the vector as 2-2-2-2-2
        row.update(dict(
            producer_rate=num(info.get('producer_event_per_second')), parallelism=vec,
            parallelism_avg=dx.compute_parallelism_avg(vec) if vec else np.nan,
            parallelism_source=num(str(vec).split(',')[0]) if vec else np.nan,
            parallelism_sink=num(str(vec).split(',')[-1]) if vec else np.nan,
            nodes=num(info.get('cluster_nodes')), slots_per_node=num(info.get('cluster_slots_per_node')),
            run_time_min=num(info.get('job_run_time')), window_size=num(info.get('job_window_size')),
            window_slide=num(info.get('job_window_slide_size')), job_query_number=info.get('job_query_number'),
            iteration=info.get('job_iteration'), sample=info.get('job_sample'),
            meta_enumeration_strategy=info.get('enumeration_strategy'), meta_hardware=info.get('cluster_hardware_type')))
        if kind == 'synthetic':
            row['hardware_type'] = info.get('cluster_hardware_type', '')
            row['variant'] = ''
        else:
            qn = str(info.get('job_query_number', ''))
            row['variant'] = dx.QUERY_VARIANT.get(row['application_slug'], {}).get(qn) or row['variant_folder'] or ''
        try:
            d = json.load(open(f))
            row['bad_json'] = 0
        except Exception:
            d, row['bad_json'] = None, 1
        if d is not None and fw in LAT:
            row['job_name'] = d.get('job_name')
            m, o = metrics(fw, d)
            row.update(m)
            row['n_operators'] = len(o)
            O.extend(operator_rows(row['run_id'], fw, o))
        Q.append(row)
    os.makedirs(os.path.join(HERE, '_parts'), exist_ok=True)
    pickle.dump((Q, O), open(os.path.join(HERE, '_parts', f'{kind}_{system}.pkl'), 'wb'))
    print(kind, system, 'runs', len(Q), 'operator rows', len(O))


FILL = ['throughput_50', 'throughput_95', 'throughput_98', 'sink_operator_throughput', 'source_operator_throughput',
        'endtoend_latency_ms', 'endtoend_latency_95_ms', 'endtoend_latency_98_ms',
        'cpu_50', 'cpu_95', 'cpu_98', 'memory_50', 'memory_95', 'memory_98', 'network_experiment_50', 'network_experiment_95']
KEY = ['system', 'application_slug', 'variant', 'hardware_type', 'producer_rate', 'parallelism']


def fill_uniform_1(Q):
    Q = Q.copy()
    Q['strategy'] = Q.strategy_folder
    Q['fill_note'] = ''
    real = Q[Q.data_kind == 'real']
    u1 = real[real.strategy_folder == 'uniform_1']
    u = real[real.strategy_folder == 'uniform']
    k = lambda df: df[KEY].astype(str).agg('|'.join, axis=1)
    ukeys = set(k(u)) if len(u) else set()
    if len(u1):
        u1k = k(u1)
        for idx in u1.index[~u1k.isin(ukeys)]:
            Q.at[idx, 'strategy'] = 'uniform'
            Q.at[idx, 'fill_note'] = 'configuration absent in uniform; run taken from uniform_1'
        first = {kk: idx for idx, kk in u1k.items() if kk not in {}}
        for idx, kk in k(u).items():
            if kk not in first:
                continue
            src = Q.loc[first[kk]]
            got = []
            for c in FILL:
                if c in Q.columns and not (Q.at[idx, c] == Q.at[idx, c]) and src[c] == src[c]:
                    Q.at[idx, c] = src[c]
                    got.append(c)
            if got:
                Q.at[idx, 'fill_note'] = f"{','.join(got)} taken from uniform_1 run {src.run_id}"
    return Q


def merge():
    Q, O = [], []
    for f in sorted(glob.glob(os.path.join(HERE, '_parts', '*.pkl'))):
        q, o = pickle.load(open(f, 'rb'))
        Q += q; O += o
    Q = fill_uniform_1(pd.DataFrame(Q))
    O = pd.DataFrame(O)
    front = ['run_id', 'data_kind', 'system', 'application', 'application_slug', 'variant', 'hardware_type', 'strategy',
             'strategy_folder', 'folder_flag', 'fill_note', 'producer_rate', 'parallelism', 'parallelism_avg', 'bad_json', 'missing_meta']
    Q = Q[[c for c in front if c in Q.columns] + [c for c in Q.columns if c not in front]]
    keycols = ['run_id', 'data_kind', 'system', 'application', 'variant', 'hardware_type', 'strategy', 'strategy_folder', 'producer_rate', 'parallelism']
    O = Q[keycols].merge(O, on='run_id', how='right')
    for kind in ('real', 'synthetic'):
        Q[Q.data_kind == kind].to_csv(os.path.join(HERE, f'{kind}_query_level.csv'), index=False)
        O[O.data_kind == kind].to_csv(os.path.join(HERE, f'{kind}_operator_level.csv'), index=False)
    print(Q.groupby(['data_kind', 'system', 'strategy_folder', 'folder_flag']).size().to_string())
    print('operator rows:', O.groupby('data_kind').size().to_dict())
    print('fill notes:', int((Q.fill_note != '').sum()), '| bad_json:', int(Q.bad_json.sum()), '| missing_meta:', int(Q.missing_meta.sum()))


if __name__ == '__main__':
    if sys.argv[1] == 'part':
        kind, system = sys.argv[2].split(':')
        build_part(kind, system)
    else:
        merge()
