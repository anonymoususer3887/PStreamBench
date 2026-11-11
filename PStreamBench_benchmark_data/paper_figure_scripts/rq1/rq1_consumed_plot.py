"""Side-by-side preview: current panel (a) sink-output ratio vs proposed consumed-input ratio (DoP 40 / DoP 2)."""
import os, numpy as np, pandas as pd, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.colors import LinearSegmentedColormap, LogNorm
from matplotlib.patches import Rectangle
HERE=os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'font.size':7,'pdf.fonttype':42,'svg.fonttype':'none'})
R=pd.read_csv(os.path.join(HERE,'rq1_consumed_vs_sink.csv'))
ROWS=[('Sentiment Analysis','SA'),('Traffic Monitoring','TM'),('Trending Topics','TT'),('Machine Outlier','MO'),('Ad Analytics','AD'),
      ('Click Analytics','CA'),('Linear Road','LR'),('Log Processing','LA'),('Bargain Index','BI'),('Smart Grid','SG'),
      ('Google Cloud Monitoring','GCM'),('Spike Detection','SD'),('TPCH','TPCH'),('Word Count','WC')]
GROUPS=[(0,4,'CPU'),(4,5,'S+N'),(5,7,'State'),(7,12,'State+UDF'),(12,14,'Stateless')]
SYS=['flink','storm','spark','kafkastream','risingwave']; SN=['Flink','Storm','Spark','Kafka','RisingWave']
cmap=LinearSegmentedColormap.from_list('pg',['#8e0152','#de77ae','#fde0ef','#f7f7f7','#e6f5d0','#7fbc41','#276419'])
norm=LogNorm(0.1,10)
def panel(ax,val,hatch,title,sub):
    for i,(app,ab) in enumerate(ROWS):
        for j,s in enumerate(SYS):
            r=R[(R.application==app)&(R.system==s)]
            v=r[val].iloc[0] if len(r) else np.nan
            h=hatch(r) if len(r) else True
            if h or not np.isfinite(v):
                ax.add_patch(Rectangle((j,i),1,1,facecolor='#f2f2f2',edgecolor='#bdbdbd',hatch='////',lw=0))
                if np.isfinite(v) and h: pass
                continue
            vv=max(v,0.1001)
            ax.add_patch(Rectangle((j,i),1,1,facecolor=cmap(norm(min(vv,9.99))),lw=0))
            txt='0' if v<0.005 else (f'{v:.0f}×' if v>=9.5 else f'{v:.1f}×')
            ax.text(j+.5,i+.5,txt,ha='center',va='center',fontsize=6.3,color='white' if (vv>4 or vv<0.25) else 'black')
    for a,b,_ in GROUPS: ax.axhline(a,color='black',lw=0.9)
    ax.axhline(len(ROWS),color='black',lw=0.9)
    ax.set_xlim(0,5); ax.set_ylim(len(ROWS),0); ax.set_xticks(np.arange(5)+.5); ax.set_xticklabels(SN,fontsize=6.8); ax.xaxis.tick_top()
    ax.tick_params(length=0); ax.set_yticks([])
    for sp in ax.spines.values(): sp.set_linewidth(0.8)
    ax.set_xlabel(title,fontsize=7.5,fontweight='bold',labelpad=4); ax.text(2.5,len(ROWS)+1.55,sub,ha='center',fontsize=6.3,style='italic',color='0.35')
fig,axs=plt.subplots(1,2,figsize=(6.6,3.3),gridspec_kw=dict(wspace=0.06,left=0.235,right=0.99,top=0.92,bottom=0.12))
low=lambda r: (min(r.sink_dop2.iloc[0],r.sink_dop40.iloc[0])<100) if np.isfinite(r.sink_dop2.iloc[0]) and np.isfinite(r.sink_dop40.iloc[0]) else True
panel(axs[0],'sink_ratio',low,'(a) current: sink-output throughput','hatched: sink < 100 events/s or missing')
panel(axs[1],'consumed_ratio',lambda r: False,'(a\') proposed: consumed input','hatched: input metric not reported')
for i,(app,ab) in enumerate(ROWS): axs[0].text(-0.1,i+.5,f'{app} ({ab})',ha='right',va='center',fontsize=6.6)
for a,b,g in GROUPS: axs[0].text(-4.9,(a+b)/2,g,rotation=90,ha='center',va='center',fontsize=6.6,style='italic')
for e in ['pdf','svg','png']: fig.savefig(os.path.join(HERE,f'rq1_panel_a_current_vs_consumed.{e}'),dpi=220)
print('ok')
