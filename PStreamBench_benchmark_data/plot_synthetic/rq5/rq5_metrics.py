"""RQ5 metrics: how query structure shapes scaling. Read-only on datasets_v2; writes only into this folder.
Folders: Flink/Kafka Streams/RisingWave 'uniform'; Spark 'uniform_e2e_rerun'; Storm 'distributed_fixed_exporter_fix'.
Rows with a folder_flag are excluded. One value per (system, shape, rate, DoP) = median over runs
(Spark: median over attempts that consumed input; attempts and successes are counted).
consumed   : Flink/KS/RW = query-level source_operator_throughput (source operator totals); Spark = processing rate
             (throughput_50); Storm = first-stage bolts from the operator file (sum of filter-i for 2/3-way joins,
             'join' bolt for 4-6-way joins, highest bolt otherwise).
offered    : rate x number of inputs for joins (each source/topic gets the full rate) except Storm, whose topology
             splits the total rate over the N spouts (JoinTopology.java), so offered = rate.
capacity   = consumed / offered.   runs = consumed > 0.   output = sink_operator_throughput (or throughput_50) > 0.
plateau DoP = lowest DoP with capacity >= 0.9 x max over DoP (threshold stated, full series kept).
gain = capacity at the DoP of the maximum / at DoP 2;  loss = capacity at DoP 40 / maximum.
latency: endtoend_latency_ms (p50), per-system definition; only ratios within a system are compared.
lat_ratio = latency at DoP 40 / at DoP 2;  lat_floor_rel = min latency over DoP / TwoWayJoin's min at the same rate."""
import os, re, warnings, numpy as np, pandas as pd
warnings.filterwarnings("ignore")
HERE = os.path.dirname(os.path.abspath(__file__)); D = os.path.join(os.path.dirname(os.path.dirname(HERE)), 'datasets_v2')
FOLDER = {'flink':'uniform','kafkastream':'uniform','risingwave':'uniform','spark':'uniform_e2e_rerun','storm':'distributed_fixed_exporter_fix'}
NJ = {'TwoWayJoin':2,'ThreeWayJoin':3,'FourWayJoin':4,'FiveWayJoin':5,'SixWayJoin':6}
DOPS = [2,4,8,16,32,40]
Q = pd.read_csv(os.path.join(D,'synthetic_query_level.csv'), low_memory=False)
Q = Q[(Q.folder_flag.fillna('')=='') & Q.apply(lambda r: FOLDER.get(r.system)==r.strategy_folder, axis=1)].copy()
# Storm first-stage input from operator rows
O = pd.read_csv(os.path.join(D,'synthetic_operator_level.csv'), low_memory=False,
                usecols=['run_id','system','strategy_folder','application','operator_name','throughput_per_sec'])
O = O[(O.system=='storm') & (O.strategy_folder==FOLDER['storm'])]
def stage1(g):
    a = g.application.iloc[0]; t = dict(zip(g.operator_name, g.throughput_per_sec))
    f = [v for k,v in t.items() if re.fullmatch(r'filter-\d+', str(k))]
    if a in NJ: return np.nansum(f) if f else t.get('join', np.nan)
    v = [x for x in t.values() if x==x]; return max(v) if v else np.nan
st1 = O.groupby('run_id').apply(stage1).rename('storm_stage1')
Q = Q.merge(st1, left_on='run_id', right_index=True, how='left')
Q['dop'] = Q.config_folder.astype(str).str.extract(r'level(\d+)')[0].astype(int)  # uniform level = DoP of the swept operators (sources stay at 2 in Flink/KS/Spark, 1 in RisingWave)
Q['n_in'] = Q.application_slug.map(NJ).fillna(1)
Q['offered'] = np.where(Q.system=='storm', Q.producer_rate, Q.producer_rate*Q.n_in)
Q['consumed'] = np.select([Q.system=='spark', Q.system=='storm'], [Q.throughput_50, Q.storm_stage1], Q.source_operator_throughput)
Q['consumed'] = Q.consumed.fillna(0)
Q['out'] = Q[['sink_operator_throughput','throughput_50']].max(axis=1).fillna(0)
Q.loc[Q.system=='spark','out'] = Q.loc[Q.system=='spark','sink_operator_throughput'].fillna(0)  # Spark throughput_50 is the processing rate
Q['runs'] = Q.consumed > 0; Q['has_out'] = Q.out > 0
Q['lat'] = Q.endtoend_latency_ms.where(Q.endtoend_latency_ms > 0)
print('DoP values per system:', Q.groupby('system').dop.unique().apply(sorted).to_dict())
key = ['system','application_slug','producer_rate','dop']
def agg(g):
    ok = g[g.runs]; h = ok if len(ok) else g
    return pd.Series(dict(n_runs=len(g), n_consumed=int(g.runs.sum()), n_output=int(g.has_out.sum()),
        consumed=h.consumed.median(), offered=g.offered.iloc[0], out=h.out.median(), lat=h.lat.median(),
        cpu_50=h.cpu_50.median()))
C = Q.groupby(key).apply(agg).reset_index()
C['capacity'] = C.consumed / C.offered
C['runs'] = C.n_consumed > 0; C['has_out'] = C.n_output > 0
C.to_csv(os.path.join(HERE,'rq5_config_values.csv'), index=False)
rows = []
for (s,a,r), g in C.groupby(['system','application_slug','producer_rate']):
    g = g.set_index('dop').sort_index(); cap = g.capacity.where(g.runs)
    mx = cap.max(); d = {'system':s,'shape':a,'rate':r,'n_dop':len(g),'dops_missing':','.join(str(x) for x in DOPS if x not in g.index),
         'share_runs':g.runs.mean(),'share_output':g.has_out.mean(),'cap_max':mx}
    if mx and mx==mx and mx>0:
        d['dop_max'] = int(cap.idxmax()); d['plateau_dop'] = int(cap[cap >= 0.9*mx].index.min())
        d['gain'] = mx/cap.get(2) if cap.get(2,np.nan)>0 else np.nan
        d['loss'] = cap.get(40)/mx if 40 in cap.index else np.nan
    l = g.lat
    d['lat_2'] = l.get(2,np.nan); d['lat_40'] = l.get(40,np.nan); d['lat_min'] = l.min()
    d['lat_ratio'] = d['lat_40']/d['lat_2'] if d['lat_2']==d['lat_2'] and d['lat_40']==d['lat_40'] else np.nan
    rows.append(d)
T = pd.DataFrame(rows)
ref = T[T['shape']=='TwoWayJoin'].set_index(['system','rate']).lat_min
T['lat_floor_rel'] = T.apply(lambda r: r.lat_min/ref.get((r.system,r.rate),np.nan), axis=1)
# feasibility over all 18 configurations of a shape
F = C.groupby(['system','application_slug']).agg(n_cfg=('runs','size'), runs=('runs','sum'), output=('has_out','sum')).reset_index()
T.to_csv(os.path.join(HERE,'rq5_summary_per_rate.csv'), index=False); F.to_csv(os.path.join(HERE,'rq5_feasibility.csv'), index=False)
pd.set_option('display.width',250); pd.set_option('display.max_rows',200)
print(F.to_string())
print(T[T['shape'].isin(['TwoWayJoin','FourWayJoin','SixWayJoin','TwoFilters','LinearQuery'])][['system','shape','rate','dops_missing','share_runs','share_output','cap_max','dop_max','plateau_dop','gain','loss','lat_ratio','lat_floor_rel']].round(2).to_string())
