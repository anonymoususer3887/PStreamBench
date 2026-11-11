"""Smoother trajectories without changing the data: (1) per system, keep only applications that have throughput and
latency at every uniform DoP (2,4,8,16,32,40), so every point is computed from the same applications;
(2) aggregate with the geometric mean of the ratios (the natural mean for ratios on log axes) and, for comparison, the median.
DS2/Dhalion: same applications where available. Input: rq4_dop2_per_app.csv (from rq4_dop2_trajectory.py)."""
import os, numpy as np, pandas as pd
HERE=os.path.dirname(os.path.abspath(__file__))
R=pd.read_csv(os.path.join(HERE,'rq4_dop2_per_app.csv'))
U=['u2','u4','u8','u16','u32','u40']
ok=R[R.cfg.isin(U)].dropna(subset=['thr_rel','inv_lat_rel']).groupby(['system','application']).cfg.nunique()
keep=ok[ok==len(U)].reset_index()[['system','application']]
X=R.merge(keep,on=['system','application'])
gm=lambda v: float(np.exp(np.log(v.dropna()).mean())) if v.notna().any() else np.nan
M=X.groupby(['system','cfg']).agg(thr_rel=('thr_rel',gm),inv_lat_rel=('inv_lat_rel',gm),thr_med=('thr_rel','median'),lat_med=('inv_lat_rel','median'),
                                   n=('thr_rel','count')).reset_index()
M.to_csv(os.path.join(HERE,'rq4_dop2_common_geomean.csv'),index=False,float_format='%.6g')
print(keep.groupby('system').size().to_dict())
order={c:i for i,c in enumerate(U+['ds2','dhalion'])}; M['o']=M.cfg.map(order)
pd.set_option('display.width',200); print(M.sort_values(['system','o']).drop(columns='o').round(2).to_string())
