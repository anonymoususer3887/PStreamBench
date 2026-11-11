"""Missing-data inventory (read-only on datasets_v2).
Expected grid per system: real (m510, 14 applications, any variant): rates 100k/500k/1M x {uniform DoP 2,4,8,16,32,40, DS2, Dhalion};
synthetic (9 shapes; folders: uniform as used in RQ5, ds2, dhalion): same rates x same configurations.
A cell is: ok (throughput>0 and latency>0), thr_only, lat_only, no_output (run exists, neither metric), missing (no run)."""
import os, numpy as np, pandas as pd, warnings; warnings.filterwarnings('ignore')
D=os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','..','datasets_v2')
CFG=['u2','u4','u8','u16','u32','u40','ds2','dhalion']; RATES=[100000,500000,1000000]; SYS=['flink','storm','spark','kafkastream','risingwave']
def status(g):
    if g.empty: return 'missing'
    t=(g.thr>0).any(); l=(g.lat>0).any()
    return 'ok' if t and l else ('thr_only' if t else ('lat_only' if l else 'no_output'))
def grid(Q,units,unitcol):
    rows=[]
    for s in SYS:
        for u in units:
            for r in RATES:
                for c in CFG:
                    g=Q[(Q.system==s)&(Q[unitcol]==u)&(Q.producer_rate==r)&(Q.cfg==c)]
                    rows.append(dict(system=s,unit=u,rate=r,cfg=c,status=status(g),runs=len(g)))
    return pd.DataFrame(rows)
def cfgcol(Q,uniform_folders):
    Q=Q.copy(); u=Q.strategy_folder.isin(uniform_folders)
    Q['cfg']=np.where(u,'u'+Q.parallelism_avg.round().astype('Int64').astype(str),Q.strategy_folder)
    return Q
# real
R=pd.read_csv(os.path.join(D,'real_query_level.csv'),low_memory=False)
R=R[(R.hardware_type=='m510')&(R.folder_flag.fillna('')=='')&R.strategy_folder.isin(['uniform','uniform_1','uniform_2','ds2','dhalion'])]
R['thr']=R.throughput_50; R['lat']=R.endtoend_latency_ms
R=cfgcol(R,['uniform','uniform_1','uniform_2'])
GR=grid(R,sorted(R.application.unique()),'application'); GR['kind']='real'
# synthetic
S=pd.read_csv(os.path.join(D,'synthetic_query_level.csv'),low_memory=False)
UF={'flink':'uniform','kafkastream':'uniform','risingwave':'uniform','spark':'uniform_e2e_rerun','storm':'distributed_fixed_exporter_fix'}
S=S[(S.folder_flag.fillna('')=='')]
S=S[S.apply(lambda r: r.strategy_folder in (UF[r.system],'ds2','dhalion'),axis=1)].copy()
S['strategy_folder']=np.where(S.strategy_folder.isin(['ds2','dhalion']),S.strategy_folder,'uniformX')
S['cfg']=np.where(S.strategy_folder=='uniformX','u'+S.config_folder.astype(str).str.extract(r'level(\d+)')[0],S.strategy_folder)
S['thr']=S[['source_operator_throughput','throughput_50','sink_operator_throughput']].max(axis=1); S['lat']=S.endtoend_latency_ms
GS=grid(S,sorted(S.application_slug.unique()),'application_slug'); GS['kind']='synthetic'
G=pd.concat([GR,GS]); G.to_csv('missing_inventory_cells.csv',index=False)
pd.set_option('display.width',220)
for k in ['real','synthetic']:
    x=G[G.kind==k]; print('#####',k)
    print(pd.crosstab(x.system,x.status).to_string())
    bad=x[x.status!='ok']
    print(pd.crosstab([bad.system,bad.cfg],bad.rate).to_string())
