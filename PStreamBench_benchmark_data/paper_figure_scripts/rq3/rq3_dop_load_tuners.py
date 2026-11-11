"""RQ3 heatmap with tuners. Cells: change of a configuration relative to uniform DoP 2 at the same input rate.
Columns per system: DoP 40 at the chosen rates, plus DS2 and Dhalion at 1M events/s.
Data: datasets_v2/real_query_level.csv, m510, no folder_flag. Uniform: strategy_folder 'uniform' (fallback 'uniform_1',
marked with a dot), DoP = round(parallelism_avg) in {2,40}, median over runs. DS2/Dhalion: strategy_folder 'ds2'/'dhalion';
if several runs exist, the latest one (largest timestamp in the file name).
Variant per application: Ad Analytics = with_join, otherwise the variant with most uniform runs.
Missing or zero values are shown as grey 'n/a' cells (no hatching, no thresholds).
Usage: python3 rq3_dop_load_tuners.py LH   or   python3 rq3_dop_load_tuners.py LMH"""
import os, re, sys, numpy as np, pandas as pd, matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap
MODE=sys.argv[1] if len(sys.argv)>1 else 'LH'
HERE=os.path.dirname(os.path.abspath(__file__)); D=os.path.join(HERE,'..','..','datasets_v2')
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':6.5,'axes.linewidth':0.5,'pdf.fonttype':42,'svg.fonttype':'none'})
R=pd.read_csv(os.path.join(D,'real_query_level.csv'),low_memory=False)
R=R[(R.hardware_type=='m510')&(R.folder_flag.fillna('')=='')&R.strategy_folder.isin(['uniform','uniform_1','ds2','dhalion'])].copy()
R['variant']=R.variant.fillna(''); R['dop']=R.parallelism_avg.round()
pick={}
for a,g in R[R.strategy_folder=='uniform'].groupby('application'):
    c=g.groupby('variant').size().sort_index(); pick[a]='with_join' if a=='Ad Analytics' else c.idxmax()
R=R[R.apply(lambda r:r.variant==pick.get(r.application),axis=1)]
def ts(f):
    n=[int(x) for x in re.findall(r'\d{9,}',str(f).replace('T',''))]; return max(n) if n else -1
def uni(a,s,r,d):
    g=R[(R.application==a)&(R.system==s)&(R.producer_rate==r)&(R.dop==d)]
    u=g[g.strategy_folder=='uniform']; fb=False
    if u.empty: u=g[g.strategy_folder=='uniform_1']; fb=not u.empty
    if u.empty: return None
    return dict(thr=u.throughput_50.median(),lat=u.endtoend_latency_ms.median(),cpu=u.cpu_50.median(),fb=fb,n=len(u))
def tun(a,s,r,st):
    g=R[(R.application==a)&(R.system==s)&(R.producer_rate==r)&(R.strategy_folder==st)]
    if g.empty: return None
    x=g.loc[g.file_name.map(ts).idxmax()]
    return dict(thr=x.throughput_50,lat=x.endtoend_latency_ms,cpu=x.cpu_50,fb=False,n=len(g))
COLS=([('DoP 40',1e5,'L'),('DoP 40',1e6,'H')] if MODE=='LH' else [('DoP 40',1e5,'L'),('DoP 40',5e5,'M'),('DoP 40',1e6,'H')])+[('ds2',1e6,'DS2'),('dhalion',1e6,'Dh')]
APPS=[('Sentiment Analysis','SA'),('Traffic Monitoring','TM'),('Trending Topics','TT'),('Machine Outlier','MO'),('Ad Analytics','AD'),
      ('Click Analytics','CA'),('Linear Road','LR'),('Log Processing','LA'),('Bargain Index','BI'),('Smart Grid','SG'),
      ('Google Cloud Monitoring','GCM'),('Spike Detection','SD'),('TPCH','TPCH'),('Word Count','WC')]
CATS={'CPU':(0,4),'S+N':(4,5),'State':(5,7),'State+UDF':(7,12),'Stateless':(12,14)}
LONG={'Log Processing':'Log Analyzer','TPCH':'TPC-H'}
SYS=['flink','storm','spark','kafkastream','risingwave']; SN=['Flink','Storm','Spark','Kafka S.','RisingW.']
rows=[]
for a,ab in APPS:
    for s in SYS:
        for st,r,lab in COLS:
            b=uni(a,s,r,2); c=uni(a,s,r,40) if st=='DoP 40' else tun(a,s,r,st)
            def ratio(k):
                if b is None or c is None or not (b[k]>0) or not (c[k]>0): return np.nan
                return c[k]/b[k]
            cpu=(c['cpu']-b['cpu']) if (b is not None and c is not None and pd.notna(b['cpu']) and pd.notna(c['cpu'])) else np.nan
            rows.append(dict(application=a,system=s,column=lab,strategy=st,rate=r,
                thr_base=None if b is None else b['thr'],thr_cfg=None if c is None else c['thr'],
                lat_base_ms=None if b is None else b['lat'],lat_cfg_ms=None if c is None else c['lat'],
                thr_ratio=ratio('thr'),lat_ratio=ratio('lat'),cpu_diff_pp=cpu,
                fallback=bool((b and b['fb']) or (c and c['fb'])),runs_cfg=None if c is None else c['n'],variant=pick.get(a)))
O=pd.DataFrame(rows); O.to_csv(os.path.join(HERE,f'rq3_dop_load_tuners_{MODE}_values.csv'),index=False,float_format='%.6g')
cm=LinearSegmentedColormap.from_list('pg',['#8E0152','#DE77AE','#FDE0EF','#F7F7F7','#E6F5D0','#7FBC41','#276419'])
cm_cpu=LinearSegmentedColormap.from_list('cpu',['#276419','#B8E186','#F7F7F7','#F1B6DA','#C51B7D'])
k=len(COLS); nR=len(APPS); nC=len(SYS)*k
fig,axs=plt.subplots(3,1,figsize=(7.16,8.2)); fig.subplots_adjust(left=0.185,right=0.995,top=0.93,bottom=0.045,hspace=0.32)
panels=[('thr_ratio','(a) Throughput [$\\times$]','green = increases',True),('lat_ratio','(b) E2E latency [$\\times$]','green = decreases',False),('cpu_diff_pp','(c) CPU utilization [pp]','%-points',None)]
fs=5.2 if k==4 else 4.8
for p,(col,title,sub,better_high) in enumerate(panels):
    ax=axs[p]; ax.set_xlim(0,nC); ax.set_ylim(nR,0)
    for i,(a,ab) in enumerate(APPS):
        for j,s in enumerate(SYS):
            for q,(st,r,lab) in enumerate(COLS):
                x=j*k+q; row=O[(O.application==a)&(O.system==s)&(O.column==lab)].iloc[0]; v=row[col]
                if pd.isna(v):
                    ax.add_patch(plt.Rectangle((x,i),1,1,facecolor='#E6E6E6',edgecolor='white',lw=0.4))
                    ax.text(x+0.5,i+0.55,'n/a',ha='center',va='center',fontsize=fs-0.6,color='0.45'); continue
                if col=='cpu_diff_pp': c=cm_cpu(np.clip((v+60)/120,0,1)); txt=f'{v:+.0f}'
                else:
                    t=np.log10(v) if better_high else -np.log10(v); c=cm(np.clip(0.5+t/4,0,1))
                    txt=(f'{v:.2g}' if v<10 else f'{v:.0f}') if v>=0.01 else f'{v:.3f}'
                ax.add_patch(plt.Rectangle((x,i),1,1,facecolor=c,edgecolor='white',lw=0.4))
                lum=np.dot(c[:3],[0.299,0.587,0.114])
                ax.text(x+0.5,i+0.55,txt,ha='center',va='center',fontsize=fs,color='white' if lum<0.5 else 'black')
                if row.fallback: ax.plot(x+0.9,i+0.18,'o',ms=1.1,color='0.2')
    for j in range(len(SYS)+1): ax.axvline(j*k,color='0.25',lw=0.6)
    for j in range(len(SYS)): ax.axvline(j*k+k-2,color='0.25',lw=0.3,ls=(0,(2,1.5)))   # separates DoP-40 columns from tuners
    for b in [4,5,7,12]: ax.axhline(b,color='0.25',lw=0.6)
    ax.set_xticks([x+0.5 for x in range(nC)]); ax.set_xticklabels([c[2] for c in COLS]*len(SYS),fontsize=5.2)
    ax.xaxis.tick_top(); ax.tick_params(length=0,pad=1)
    for j,n in enumerate(SN): ax.text(j*k+k/2,-1.0,n,ha='center',va='bottom',fontsize=7,fontweight='bold')
    ax.set_yticks([i+0.5 for i in range(nR)])
    if True:
        ax.set_yticklabels([f'{LONG.get(a,a)} ({ab})' for a,ab in APPS],fontsize=5.6)
        for cat,(y0,y1) in CATS.items(): ax.text(-0.175,1-(y0+y1)/2/nR,cat,rotation=90,ha='center',va='center',fontsize=5.6,style='italic',transform=ax.transAxes)
    else: ax.set_yticklabels([])
    ax.text(0.5,-0.025,title+'   ('+sub+')',ha='center',va='top',fontsize=7,fontweight='bold',transform=ax.transAxes)
rates='L = 100k, H = 1M' if MODE=='LH' else 'L = 100k, M = 500k, H = 1M'
fig.text(0.58,0.985,f'Change vs. uniform DoP 2 at the same rate: DoP 40 at {rates} events/s; DS2 and Dhalion (Dh) at 1M events/s; m510',ha='center',fontsize=6.1)
for e in ['pdf','svg','png']: fig.savefig(os.path.join(HERE,f'fig_rq3_dop_load_tuners_{MODE}.'+e),dpi=300)
print(MODE,'n/a thr cells:',int(O.thr_ratio.isna().sum()),'of',len(O))
