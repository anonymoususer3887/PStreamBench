"""RQ5 paper figure. x = structure (Lin, 2F-4F | 2J-6J); five bars = systems.
Bar = value at DoP 16; whisker = min-max over DoP 2-40 (DoP levels without a value are skipped).
Rows: consumed / offered input (capacity), end-to-end latency p50 [s] (definition differs per system), CPU cpu_50 [%].
'x' = query consumed no input at DoP 16; a whisker without bar = value missing at DoP 16 only.
Source: rq5_config_values.csv (median over runs per configuration)."""
import os, sys, numpy as np, pandas as pd, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter, MultipleLocator
from matplotlib.patches import Patch
HERE=os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':7,'axes.labelsize':7,'xtick.labelsize':7,'ytick.labelsize':6.3,'axes.linewidth':0.6,'hatch.linewidth':0.45,'hatch.color':'0.35',
 'pdf.fonttype':42,'svg.fonttype':'none','xtick.direction':'in','ytick.direction':'in','xtick.major.size':0,'ytick.major.size':2})
C=pd.read_csv(os.path.join(HERE,'rq5_config_values.csv'))
SYS=['flink','storm','spark','kafkastream','risingwave']; NAME={'flink':'Flink','storm':'Storm','spark':'Spark','kafkastream':'Kafka Streams','risingwave':'RisingWave'}
SCOL={'flink':'#9EC5E8','storm':'#F6B092','spark':'#F7DD8F','kafkastream':'#A8DDBF','risingwave':'#D8BCE8'}  # pastel
XCOL={'flink':'#3A78B0','storm':'#C8592B','spark':'#B8890F','kafkastream':'#2E8A5A','risingwave':'#8A55A8'}  # darker tone for x marks
HATCH={'flink':None,'storm':'////','spark':'....','kafkastream':'xxxx','risingwave':'\\\\\\\\'}
SHAPES=['LinearQuery','TwoFilters','ThreeFilters','FourFilters','TwoWayJoin','ThreeWayJoin','FourWayJoin','FiveWayJoin','SixWayJoin']
LAB=['Lin','2F','3F','4F','2J','3J','4J','5J','6J']
X=np.arange(len(SHAPES)); bw=0.84/len(SYS)
MET=[('capacity','Consumed /\noffered input','log',(1e-4,2.5),1),('lat','E2E latency [s]','log',(0.05,1000),1000),('cpu_50','CPU [%]','lin',(0,70),1)]
def fig(rate, REF=16, outdir=HERE, suffix='', whiskers=True):
    R=C[C.producer_rate==rate]; out=[]
    f,axs=plt.subplots(3,1,figsize=(7.16,3.7),sharex=True,gridspec_kw=dict(hspace=0.09,height_ratios=[1.15,1.15,0.9],left=0.075,right=0.995,top=0.85,bottom=0.075))
    for ax,(col,lab,sc,lim,div) in zip(axs,MET):
        for i,k in enumerate(SYS):
            for j,s in enumerate(SHAPES):
                g=R[(R.system==k)&(R.application_slug==s)].set_index('dop')
                ok=g[g.runs] if col!='cpu_50' else g
                v=(ok[col]/div).dropna(); v=v[v>0] if sc=='log' else v
                x=X[j]+(i-(len(SYS)-1)/2)*bw
                ran16 = REF in g.index and bool(g.runs.get(REF,False))
                b=v.get(REF,np.nan) if ran16 or col=='cpu_50' else np.nan
                out.append(dict(rate=rate,bar_dop=REF,metric=col,system=k,structure=LAB[j],value_dop16=b,min_over_dop=v.min() if len(v) else np.nan,
                                max_over_dop=v.max() if len(v) else np.nan,n_dop=len(v),ran_at_dop16=ran16))
                if b==b:
                    ax.bar(x,b,bw,color=SCOL[k],edgecolor='0.25',lw=0.4,zorder=2,hatch=HATCH[k])
                if whiskers and len(v)>1:
                    ax.plot([x,x],[v.min(),v.max()],color='black',lw=0.6,zorder=3)
                    for y in (v.min(),v.max()): ax.plot([x-bw*0.28,x+bw*0.28],[y,y],color='black',lw=0.6,zorder=3)
                if not ran16:
                    ax.text(x,lim[0]*1.12 if sc=='log' else 1.5,'×',ha='center',va='bottom',fontsize=6.5,color=XCOL[k],fontweight='bold',zorder=4)
        if sc=='log':
            ax.set_yscale('log'); ax.yaxis.set_major_locator(LogLocator(10,numticks=8)); ax.yaxis.set_minor_formatter(NullFormatter())
            ax.yaxis.set_major_formatter(FuncFormatter(lambda v,p: f'{v:g}'))
        else: ax.yaxis.set_major_locator(MultipleLocator(20))
        if col=='capacity': ax.axhline(1,color='0.35',lw=0.6,ls=(0,(4,2)),zorder=1)
        ax.set_ylim(*lim); ax.set_xlim(-0.5,len(SHAPES)-0.5); ax.set_ylabel(lab,fontsize=6.8,labelpad=2)
        ax.grid(axis='y',ls=(0,(2,2)),lw=0.35,color='0.8',zorder=0)
        for b in X[:-1]+0.5: ax.axvline(b,color='0.88',lw=0.5,zorder=0)
        ax.axvline(3.5,color='0.3',lw=0.9,zorder=1)
    axs[-1].set_xticks(X); axs[-1].set_xticklabels(LAB)
    axs[0].text(1.5,1.02,'pipeline depth',transform=axs[0].get_xaxis_transform(),ha='center',va='bottom',fontsize=6.5,style='italic',color='0.3')
    axs[0].text(6.0,1.02,'join width',transform=axs[0].get_xaxis_transform(),ha='center',va='bottom',fontsize=6.5,style='italic',color='0.3')
    H=[Patch(facecolor=SCOL[k],edgecolor='black',hatch=HATCH[k],lw=0.5) for k in SYS]
    f.legend(H,[NAME[k] for k in SYS],loc='upper center',ncol=5,frameon=False,bbox_to_anchor=(0.535,1.0),handlelength=2.4,handleheight=1.0,columnspacing=1.8,fontsize=7)
    k=f'{int(rate/1000)}k'+suffix
    f.text(0.535,0.925,f'{int(rate/1000)}k events/s  ·  bars: DoP {REF}'+('  ·  whiskers: min–max over DoP 2–40' if whiskers else ''),ha='center',va='center',fontsize=6.3,color='0.3')
    for ext in ['pdf','svg','png']: f.savefig(os.path.join(outdir,f'fig_rq5_paper_{k}.{ext}'),dpi=250)
    plt.close(f); return out
rows=[]
for r in [100000.0,500000.0,1000000.0]: rows+=fig(r)
PD=os.path.join(HERE,'per_dop'); os.makedirs(PD,exist_ok=True)
for r in [100000.0,500000.0,1000000.0]:
    for d in [2,4,8,16,32,40]: rows+=fig(r,d,PD,f'_dop{d}',whiskers=False)
pd.DataFrame(rows).drop_duplicates().to_csv(os.path.join(HERE,'fig_rq5_paper_values.csv'),index=False,float_format='%.6g'); print('ok')
