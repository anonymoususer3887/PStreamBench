"""RQ3 heatmap, option A: effect of raising uniform DoP 2 -> 40, at low load (100k) and at high load (1M).
Data: datasets_v2/real_query_level.csv, m510, no folder_flag, strategy_folder 'uniform' (fallback 'uniform_1' if a
configuration has no 'uniform' run; such cells are marked with a dot), DoP = round(parallelism_avg) in {2,40}.
Variant per application: Ad Analytics = with_join, otherwise the variant with most uniform runs (ties alphabetical).
Per configuration: median over runs of throughput_50, endtoend_latency_ms, cpu_50.
Cells: throughput and latency ratio DoP40/DoP2, CPU difference DoP40-DoP2 [pp].
Hatched: throughput < 100 events/s at either DoP (as in Fig. RQ1) for throughput/latency, or value missing."""
import os, numpy as np, pandas as pd, matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, TwoSlopeNorm
HERE=os.path.dirname(os.path.abspath(__file__)); D=os.path.join(HERE,'..','..','datasets_v2')
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':6.5,'axes.linewidth':0.5,'pdf.fonttype':42,'svg.fonttype':'none','hatch.linewidth':0.4})
R=pd.read_csv(os.path.join(D,'real_query_level.csv'),low_memory=False)
R=R[(R.hardware_type=='m510')&(R.folder_flag.fillna('')=='')&R.strategy_folder.isin(['uniform','uniform_1'])&R.producer_rate.isin([1e5,5e5,1e6])].copy()
R['variant']=R.variant.fillna(''); R['dop']=R.parallelism_avg.round(); R=R[R.dop.isin([2,40])]
pick={}
for a,g in R[R.strategy_folder=='uniform'].groupby('application'):
    c=g.groupby('variant').size().sort_index(); pick[a]='with_join' if a=='Ad Analytics' else c.idxmax()
R=R[R.apply(lambda r:r.variant==pick.get(r.application),axis=1)]
rows=[]
for (a,s,r,d),g in R.groupby(['application','system','producer_rate','dop']):
    u=g[g.strategy_folder=='uniform']; src='uniform'
    if u.empty: u=g; src='uniform_1'
    rows.append(dict(application=a,system=s,rate=r,dop=d,src=src,n=len(u),thr=u.throughput_50.median(),lat=u.endtoend_latency_ms.median(),cpu=u.cpu_50.median()))
M=pd.DataFrame(rows)
P=M.pivot_table(index=['application','system','rate'],columns='dop',values=['thr','lat','cpu'],aggfunc='first')
S=M.pivot_table(index=['application','system','rate'],columns='dop',values='src',aggfunc='first')
out=pd.DataFrame({'thr_dop2':P[('thr',2.0)],'thr_dop40':P[('thr',40.0)],'lat_dop2_ms':P[('lat',2.0)],'lat_dop40_ms':P[('lat',40.0)],
                  'cpu_dop2':P[('cpu',2.0)],'cpu_dop40':P[('cpu',40.0)],'src_dop2':S[2.0],'src_dop40':S[40.0]}).reset_index()
out['thr_ratio']=out.thr_dop40/out.thr_dop2; out['lat_ratio']=out.lat_dop40_ms/out.lat_dop2_ms; out['cpu_diff_pp']=out.cpu_dop40-out.cpu_dop2
out['low_output']=(out.thr_dop2.fillna(0)<100)|(out.thr_dop40.fillna(0)<100)
out['variant']=out.application.map(pick)
out.to_csv(os.path.join(HERE,'rq3_dop_load_LMH_values.csv'),index=False,float_format='%.6g')
APPS=[('Sentiment Analysis','SA','CPU'),('Traffic Monitoring','TM','CPU'),('Trending Topics','TT','CPU'),('Machine Outlier','MO','CPU'),
      ('Ad Analytics','AD','S+N'),('Click Analytics','CA','State'),('Linear Road','LR','State'),
      ('Log Processing','LA','State+UDF'),('Bargain Index','BI','State+UDF'),('Smart Grid','SG','State+UDF'),('Google Cloud Monitoring','GCM','State+UDF'),('Spike Detection','SD','State+UDF'),
      ('TPCH','TPCH','Stateless'),('Word Count','WC','Stateless')]
LONG={'Log Processing':'Log Analyzer','TPCH':'TPC-H'}
SYS=['flink','storm','spark','kafkastream','risingwave']; SN=['Flink','Storm','Spark','Kafka S.','RisingW.']
RATES=[1e5,5e5,1e6]; RL=['L','M','H']
cm=LinearSegmentedColormap.from_list('pg',['#8E0152','#DE77AE','#FDE0EF','#F7F7F7','#E6F5D0','#7FBC41','#276419'])
cm_cpu=LinearSegmentedColormap.from_list('cpu',['#276419','#B8E186','#F7F7F7','#F1B6DA','#C51B7D'])
def val(a,s,r,col):
    x=out[(out.application==a)&(out.system==s)&(out.rate==r)]
    return (np.nan,True,False) if x.empty else (x[col].iloc[0],bool(x.low_output.iloc[0]),(x.src_dop2.iloc[0]=='uniform_1') or (x.src_dop40.iloc[0]=='uniform_1'))
nR=len(APPS); nC=len(SYS)*3
fig,axs=plt.subplots(1,3,figsize=(7.16,3.3)); fig.subplots_adjust(left=0.175,right=0.995,top=0.86,bottom=0.12,wspace=0.06)
panels=[('thr_ratio','(a) Throughput [$\\times$]','green = increases',True),('lat_ratio','(b) E2E latency [$\\times$]','green = decreases',False),('cpu_diff_pp','(c) CPU utilization [pp]','%-points',None)]
for k,(col,title,sub,better_high) in enumerate(panels):
    ax=axs[k]; ax.set_xlim(0,nC); ax.set_ylim(nR,0)
    for i,(a,ab,cat) in enumerate(APPS):
        for j,s in enumerate(SYS):
            for q,r in enumerate(RATES):
                x=j*3+q; v,low,fb=val(a,s,r,col)
                bad=np.isnan(v) or (col!='cpu_diff_pp' and v<=0)
                if bad:
                    ax.add_patch(plt.Rectangle((x,i),1,1,facecolor='white',edgecolor='0.6',hatch='////',lw=0.2)); continue
                if col=='cpu_diff_pp':
                    c=cm_cpu(np.clip((v+60)/120,0,1)); txt=f'{v:+.0f}'
                else:
                    lv=np.log10(v); t=lv if better_high else -lv
                    c=cm(np.clip(0.5+t/4,0,1))   # +-2 decades saturate
                    txt=(f'{v:.2g}' if v<10 else f'{v:.0f}') if v>=0.01 else f'{v:.3f}'
                ax.add_patch(plt.Rectangle((x,i),1,1,facecolor=c,edgecolor='white',lw=0.4))
                lum=np.dot(c[:3],[0.299,0.587,0.114])
                ax.text(x+0.5,i+0.55,txt,ha='center',va='center',fontsize=3.9,color='white' if lum<0.5 else 'black')
                if fb: ax.plot(x+0.88,i+0.15,'o',ms=1.2,color='0.2')
    for j in range(len(SYS)+1): ax.axvline(j*3,color='0.25',lw=0.6)
    for b in [4,5,7,12]: ax.axhline(b,color='0.25',lw=0.6)
    ax.set_xticks([x+0.5 for x in range(nC)]); ax.set_xticklabels(RL*len(SYS),fontsize=4.6); ax.xaxis.tick_top(); ax.tick_params(length=0,pad=1)
    for j,n in enumerate(SN): ax.text(j*3+1.5,-1.05,n,ha='center',va='bottom',fontsize=6.5,fontweight='bold')
    ax.set_yticks([i+0.5 for i in range(nR)])
    if k==0:
        ax.set_yticklabels([f'{LONG.get(a,a)} ({ab})' for a,ab,_ in APPS],fontsize=5.6)
        for cat,(y0,y1) in {'CPU':(0,4),'S+N':(4,5),'State':(5,7),'State+UDF':(7,12),'Stateless':(12,14)}.items():
            ax.text(-0.62,1-(y0+y1)/2/nR,cat,rotation=90,ha='center',va='center',fontsize=5.6,style='italic',transform=ax.transAxes)
    else: ax.set_yticklabels([])
    for sp in ax.spines.values(): sp.set_linewidth(0.6)
    ax.text(0.5,-0.03,title,ha='center',va='top',fontsize=7,fontweight='bold',transform=ax.transAxes)
    ax.text(0.5,-0.085,sub,ha='center',va='top',fontsize=5.4,style='italic',transform=ax.transAxes)
fig.text(0.56,0.965,'Change from DoP 2 to DoP 40 at L = 100k, M = 500k and H = 1M events/s, m510',ha='center',fontsize=6.3)
for e in ['pdf','svg','png']: fig.savefig(os.path.join(HERE,'fig_rq3_dop_load_LMH.'+e),dpi=300)
print(pick)
print(out[(out.rate==1e6)&(out.application.isin(['Sentiment Analysis','Ad Analytics']))][['application','system','thr_ratio','lat_ratio','cpu_diff_pp','src_dop2','src_dop40']].round(2).to_string(index=False))
print('fallback cells:',int(((out.src_dop2=='uniform_1')|(out.src_dop40=='uniform_1')).sum()))
