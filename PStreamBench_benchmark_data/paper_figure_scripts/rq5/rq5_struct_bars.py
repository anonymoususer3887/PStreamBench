"""All query structures in one plot per metric, one figure per event rate.
x = structure (Lin, 2F, 3F, 4F, 2J..6J); five bars = systems; bar = median over the six DoP levels (2..40),
whisker = min..max over DoP (how much DoP changes the value). Same data and definitions as rq5_bars.py:
throughput = sink output, latency = endtoend_latency_ms p50 [s] (definition differs per system), CPU = cpu_50.
'x' = no value at any DoP. A DoP level without a value is left out of median/whisker (count printed to the CSV)."""
import os, numpy as np, pandas as pd, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.colors import to_rgb, to_hex
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter, MultipleLocator
from matplotlib.patches import Patch
HERE = os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':7,'axes.labelsize':7,'xtick.labelsize':6.5,'ytick.labelsize':6,'axes.linewidth':0.6,'hatch.linewidth':0.5,
 'pdf.fonttype':42,'svg.fonttype':'none','xtick.direction':'in','ytick.direction':'in','xtick.major.size':2,'ytick.major.size':2})
C = pd.read_csv(os.path.join(HERE,'rq5_config_values.csv'))
def darken(c,f=0.6): r,g,b=to_rgb(c); return to_hex((r*f,g*f,b*f))
def lighten(c,f=0.5): r,g,b=to_rgb(c); return to_hex((r+(1-r)*f,g+(1-g)*f,b+(1-b)*f))
SHADE={'flink':lambda c: darken(c,0.62),'storm':lambda c: c,'spark':lambda c: lighten(c,0.62),'kafkastream':lambda c: darken(c,0.62),'risingwave':lambda c: lighten(c,0.55)}
HATCH={'flink':None,'storm':'///','spark':'..','kafkastream':'xx','risingwave':'\\\\\\'}
SYS=['flink','storm','spark','kafkastream','risingwave']; NAME={'flink':'Flink','storm':'Storm','spark':'Spark','kafkastream':'Kafka Streams','risingwave':'RisingWave'}
SHAPES=['LinearQuery','TwoFilters','ThreeFilters','FourFilters','TwoWayJoin','ThreeWayJoin','FourWayJoin','FiveWayJoin','SixWayJoin']
LAB=['Lin','2F','3F','4F','2J','3J','4J','5J','6J']
COL={'TwoFilters':'#1b9e77','ThreeFilters':'#66a61e','FourFilters':'#a6761d','LinearQuery':'#e6ab02','TwoWayJoin':'#7570b3',
     'ThreeWayJoin':'#e7298a','FourWayJoin':'#d95f02','FiveWayJoin':'#1f78b4','SixWayJoin':'#525252'}
MET=[('out','Throughput [events/s]','log',(0.01,1e7),1),('lat','E2E latency [s]','log',(0.05,1000),1000),('cpu_50','CPU [%]','lin',(0,100),1)]
def kfmt(v,p): return f'{v/1e6:g}M' if v>=1e6 else (f'{v/1e3:g}k' if v>=1e3 else f'{v:g}')
X=np.arange(len(SHAPES)); bw=0.8/len(SYS); rows=[]
def figure(rate):
    fig, axs = plt.subplots(3,1, figsize=(7.16,4.3), sharex=True, gridspec_kw=dict(hspace=0.08, height_ratios=[1.2,1.2,0.85], left=0.075, right=0.995, top=0.90, bottom=0.07))
    for ax,(col,lab,sc,lim,div) in zip(axs,MET):
        for i,k in enumerate(SYS):
            xs = X+(i-(len(SYS)-1)/2)*bw
            for x,s in zip(xs,SHAPES):
                v = C[(C.system==k)&(C.application_slug==s)&(C.producer_rate==rate)][col]/div
                v = v[v>0] if sc=='log' else v.dropna()
                rows.append(dict(rate=rate,metric=col,system=k,shape=s,n_dop=len(v),median=v.median() if len(v) else np.nan,
                                 min=v.min() if len(v) else np.nan,max=v.max() if len(v) else np.nan))
                base=COL[s]
                if len(v)==0:
                    ax.text(x, lim[0]*1.15 if sc=='log' else 1, '×', ha='center', va='bottom', fontsize=5, color='0.35'); continue
                m=v.median(); fc=SHADE[k](base)
                ax.bar(x,m,bw,color=fc,edgecolor='black',lw=0.35,zorder=2)
                if HATCH[k]:
                    ax.bar(x,m,bw,color='none',edgecolor=('white' if k in ('storm','kafkastream') else darken(base,0.55)),lw=0,hatch=HATCH[k],zorder=2.1)
                    ax.bar(x,m,bw,color='none',edgecolor='black',lw=0.35,zorder=2.2)
                ax.plot([x,x],[v.min(),v.max()],color='black',lw=0.55,zorder=3)
                ax.plot([x-bw*0.25,x+bw*0.25],[v.min()]*2,color='black',lw=0.55,zorder=3); ax.plot([x-bw*0.25,x+bw*0.25],[v.max()]*2,color='black',lw=0.55,zorder=3)
        if sc=='log':
            ax.set_yscale('log'); ax.yaxis.set_major_locator(LogLocator(10,numticks=8)); ax.yaxis.set_minor_formatter(NullFormatter())
            ax.yaxis.set_major_formatter(FuncFormatter(kfmt if col=='out' else (lambda v,p: f'{v:g}')))
        else: ax.yaxis.set_major_locator(MultipleLocator(25))
        ax.set_ylim(*lim); ax.set_xlim(-0.5,len(SHAPES)-0.5); ax.set_ylabel(lab, fontsize=6.8)
        ax.grid(axis='y',ls=(0,(2,2)),lw=0.35,color='0.8',zorder=0)
        for b in (0.5,3.5): ax.axvline(b,color='0.55',lw=0.6,zorder=1)
        ax.axvline(5.5,color='0.55',lw=0.6,ls='--',zorder=1)
    axs[-1].set_xticks(X); axs[-1].set_xticklabels(LAB)
    for t,lbl in zip(axs[-1].get_xticklabels(),SHAPES): t.set_color(darken(COL[lbl],0.85)); t.set_fontweight('bold')
    axs[0].text(2,5e6,'pipeline depth',ha='center',fontsize=6.5,style='italic',color='0.35')
    axs[0].text(6,5e6,'join width',ha='center',fontsize=6.5,style='italic',color='0.35')
    GREY='#9e9e9e'
    H=[Patch(facecolor=SHADE[k](GREY),edgecolor=('white' if k in ('storm','kafkastream') else darken(GREY,0.55)),hatch=HATCH[k],lw=0.6) for k in SYS]
    fig.legend(H,[NAME[k] for k in SYS],loc='upper center',ncol=5,frameon=False,bbox_to_anchor=(0.53,1.0),handlelength=2.6,handleheight=1.1,columnspacing=1.6,
               title=f'{int(rate/1000)}k events/s · bar = median over DoP 2–40, whisker = min–max over DoP · × = no value',title_fontsize=6.5,fontsize=6.8)
    k=f'{int(rate/1000)}k'
    for ext in ['pdf','svg','png']: fig.savefig(os.path.join(HERE,f'fig_syn_structures_{k}.{ext}'),dpi=220)
    plt.close(fig)
for r in [100000.0,500000.0,1000000.0]: figure(r)
pd.DataFrame(rows).to_csv(os.path.join(HERE,'fig_syn_structures_values.csv'),index=False); print('ok')
