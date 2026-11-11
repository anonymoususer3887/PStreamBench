"""Motivation figure data: throughput and latency of four strategies relative to the best uniform DoP, per system.
Real workloads, m510, 1M events/s, datasets_v2/real_query_level.csv, rows without folder flag.
Per application (application + variant) and configuration (parallelism vector): median over runs.
best uniform = max throughput / min latency over the uniform configurations of that application.
Uniform, Random: median over their configurations (blind choice). DS2, Dhalion: their configuration.
Runs without output (throughput <= 0) or without latency are omitted (as in fig_rq4_summary).
Per system: median over applications of throughput ratio and of latency ratio (strategy / best uniform)."""
import os, numpy as np, pandas as pd, warnings; warnings.filterwarnings('ignore')
HERE=os.path.dirname(os.path.abspath(__file__)); D=os.path.join(HERE,'..','..','datasets_v2')
Q=pd.read_csv(os.path.join(D,'real_query_level.csv'),low_memory=False)
Q=Q[(Q.hardware_type=='m510')&(Q.producer_rate==1e6)&(Q.folder_flag.fillna('')=='')&Q.strategy.isin(['uniform','random','ds2','dhalion'])].copy()
Q['variant']=Q.variant.fillna('')
# one variant per application (14 applications, as in fig_rq4_summary): the variant with the most uniform
# configurations among those that also have DS2/Dhalion runs; ties -> alphabetical (AD: with_join, as in RQ1)
cnt=Q[Q.strategy=='uniform'].groupby(['system','application','variant']).parallelism.nunique().rename('nu').reset_index()
has=Q[Q.strategy.isin(['ds2','dhalion'])][['system','application','variant']].drop_duplicates().assign(h=1)
cnt=cnt.merge(has,how='left').fillna({'h':0}).sort_values(['system','application','h','nu','variant'],ascending=[True,True,False,False,True])
pick=cnt.groupby(['system','application']).head(1)[['system','application','variant']]
Q=Q.merge(pick,on=['system','application','variant'])
Q['app']=Q.application+'|'+Q.variant
Q['thr']=Q.throughput_50.where(Q.throughput_50>0); Q['lat']=Q.endtoend_latency_ms.where(Q.endtoend_latency_ms>0)
C=Q.groupby(['system','app','strategy','parallelism']).agg(thr=('thr','median'),lat=('lat','median'),pavg=('parallelism_avg','median')).reset_index()
# fixed uniform settings: uniform configuration with average DoP 2 or 4
for p in (2,4,40):
    x=C[(C.strategy=='uniform')&(C.pavg.round()==p)].copy(); x['strategy']=f'uniform_p{p}'; C=pd.concat([C,x])
rows=[]
for (s,a),g in C.groupby(['system','app']):
    u=g[g.strategy=='uniform']  # best uniform over all uniform DoPs (2..40)
    bt=u.thr.max(); bl=u.lat.min()
    for st in ['uniform','random','ds2','dhalion','uniform_p2','uniform_p4','uniform_p40']:
        x=g[g.strategy==st]
        if x.empty: continue
        rows.append(dict(system=s,app=a,strategy=st,n_cfg=len(x),thr_ratio=x.thr.median()/bt if bt==bt else np.nan,
                         lat_ratio=x.lat.median()/bl if bl==bl else np.nan))
R=pd.DataFrame(rows); R.to_csv(os.path.join(HERE,'rq4_tradeoff_per_app.csv'),index=False,float_format='%.6g')
M=R.groupby(['system','strategy']).agg(thr_ratio=('thr_ratio','median'),lat_ratio=('lat_ratio','median'),
     n_apps_thr=('thr_ratio','count'),n_apps_lat=('lat_ratio','count')).reset_index()
M['inv_lat']=1/M.lat_ratio
M.to_csv(os.path.join(HERE,'rq4_tradeoff_medians.csv'),index=False,float_format='%.6g')
pd.set_option('display.width',200); print(M.round(3).to_string())
