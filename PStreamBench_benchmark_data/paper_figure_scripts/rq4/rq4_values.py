"""RQ4 values (m510, 1M events/s). Per application x system:
Uniform = strategy_folder 'uniform', DoP in {2,4,8,16,32,40} (median per DoP over runs); Random = strategy_folder 'random' (each run one config);
DS2/Dhalion = latest run in 'ds2'/'dhalion'. Variant per application: Ad Analytics = with_join, else variant with most uniform runs.
Runs with throughput <= 0 or latency <= 0 are excluded."""
import os,re,numpy as np,pandas as pd
HERE=os.path.dirname(os.path.abspath(__file__)); D=os.path.join(HERE,'..','..','datasets_v2')
R=pd.read_csv(os.path.join(D,'real_query_level.csv'),low_memory=False)
R=R[(R.hardware_type=='m510')&(R.folder_flag.fillna('')=='')&(R.producer_rate==1e6)&R.strategy_folder.isin(['uniform','random','ds2','dhalion'])].copy()
R['variant']=R.variant.fillna(''); R['dop']=R.parallelism_avg.round()
pick={}
for a,g in R[R.strategy_folder=='uniform'].groupby('application'):
    c=g.groupby('variant').size().sort_index(); pick[a]='with_join' if a=='Ad Analytics' else c.idxmax()
R=R[R.apply(lambda r:r.variant==pick.get(r.application),axis=1)]
R=R[(R.throughput_50>0)&(R.endtoend_latency_ms>0)]
def ts(f):
    n=[int(x) for x in re.findall(r'\d{9,}',str(f).replace('T',''))]; return max(n) if n else -1
rows=[]
for (a,s),g in R.groupby(['application','system']):
    u=g[(g.strategy_folder=='uniform')&g.dop.isin([2,4,8,16,32,40])].groupby('dop')[['throughput_50','endtoend_latency_ms']].median()
    rnd=g[g.strategy_folder=='random'][['throughput_50','endtoend_latency_ms']]
    d=dict(application=a,system=s,n_uniform=len(u),n_random=len(rnd))
    if len(u):
        bt,bl=u.throughput_50.max(),u.endtoend_latency_ms.min()
        d.update(u_thr_best=bt,u_thr_worst=u.throughput_50.min(),u_thr_med=u.throughput_50.median(),u_lat_best=bl,u_lat_worst=u.endtoend_latency_ms.max(),u_lat_med=u.endtoend_latency_ms.median())
    if len(rnd):
        d.update(r_thr_best=rnd.throughput_50.max(),r_thr_worst=rnd.throughput_50.min(),r_thr_med=rnd.throughput_50.median(),r_lat_best=rnd.endtoend_latency_ms.min(),r_lat_worst=rnd.endtoend_latency_ms.max(),r_lat_med=rnd.endtoend_latency_ms.median())
    for st in ['ds2','dhalion']:
        t=g[g.strategy_folder==st]
        if len(t): x=t.loc[t.file_name.map(ts).idxmax()]; d[f'{st}_thr']=x.throughput_50; d[f'{st}_lat']=x.endtoend_latency_ms
    rows.append(d)
V=pd.DataFrame(rows); V['variant']=V.application.map(pick)
V.to_csv(os.path.join(HERE,'rq4_values_1M.csv'),index=False,float_format='%.6g')
# quick check against paper (Flink): medians over apps
f=V[V.system=='flink']
print('Flink DS2 thr/bestU', (f.ds2_thr/f.u_thr_best).median(), 'Dh', (f.dhalion_thr/f.u_thr_best).median())
print('Flink DS2 lat/bestU', (f.ds2_lat/f.u_lat_best).median(), 'Dh', (f.dhalion_lat/f.u_lat_best).median())
print('Flink random blind thr', (f.r_thr_med/f.u_thr_best).median(), 'lat', (f.r_lat_med/f.u_lat_best).median(), 'uniform blind lat',(f.u_lat_med/f.u_lat_best).median())
print(V.groupby('system')[['n_uniform','n_random']].median())
