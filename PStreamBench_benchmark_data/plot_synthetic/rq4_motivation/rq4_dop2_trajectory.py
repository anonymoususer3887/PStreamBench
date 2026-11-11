"""Trajectory data: uniform DoP 2..40, DS2 and Dhalion, each normalized to uniform DoP 2 of the same application.
Same runs and application/variant selection as rq4_tradeoff.py (m510, 1M events/s, real workloads, 14 applications).
thr_rel = throughput / throughput at uniform DoP 2; inv_lat_rel = latency at uniform DoP 2 / latency.
Per system: median over applications (applications without a DoP-2 value or without the configuration are skipped)."""
import os, numpy as np, pandas as pd, warnings; warnings.filterwarnings('ignore')
HERE=os.path.dirname(os.path.abspath(__file__)); D=os.path.join(HERE,'..','..','datasets_v2')
Q=pd.read_csv(os.path.join(D,'real_query_level.csv'),low_memory=False)
Q=Q[(Q.hardware_type=='m510')&(Q.producer_rate==1e6)&(Q.folder_flag.fillna('')=='')&Q.strategy.isin(['uniform','ds2','dhalion'])].copy()
Q['variant']=Q.variant.fillna('')
cnt=Q[Q.strategy=='uniform'].groupby(['system','application','variant']).parallelism.nunique().rename('nu').reset_index()
has=Q[Q.strategy.isin(['ds2','dhalion'])][['system','application','variant']].drop_duplicates().assign(h=1)
cnt=cnt.merge(has,how='left').fillna({'h':0}).sort_values(['system','application','h','nu','variant'],ascending=[True,True,False,False,True])
pick=cnt.groupby(['system','application']).head(1)[['system','application','variant']]
Q=Q.merge(pick,on=['system','application','variant'])
Q['thr']=Q.throughput_50.where(Q.throughput_50>0); Q['lat']=Q.endtoend_latency_ms.where(Q.endtoend_latency_ms>0)
Q['cfg']=np.where(Q.strategy=='uniform','u'+Q.parallelism_avg.round().astype('Int64').astype(str),Q.strategy)
C=Q.groupby(['system','application','cfg']).agg(thr=('thr','median'),lat=('lat','median')).reset_index()
rows=[]
for (s,a),g in C.groupby(['system','application']):
    g=g.set_index('cfg')
    if 'u2' not in g.index: continue
    t2,l2=g.thr['u2'],g.lat['u2']
    for c in g.index:
        rows.append(dict(system=s,application=a,cfg=c,thr_rel=g.thr[c]/t2,inv_lat_rel=l2/g.lat[c]))
R=pd.DataFrame(rows); R.to_csv(os.path.join(HERE,'rq4_dop2_per_app.csv'),index=False,float_format='%.6g')
M=R.groupby(['system','cfg']).agg(thr_rel=('thr_rel','median'),inv_lat_rel=('inv_lat_rel','median'),
     n_thr=('thr_rel','count'),n_lat=('inv_lat_rel','count')).reset_index()
M.to_csv(os.path.join(HERE,'rq4_dop2_medians.csv'),index=False,float_format='%.6g')
order=['u2','u4','u8','u16','u32','u40','ds2','dhalion']
M['o']=M.cfg.map({c:i for i,c in enumerate(order)}); M=M.dropna(subset=['o']).sort_values(['system','o'])
pd.set_option('display.width',200); print(M.drop(columns='o').round(2).to_string())
