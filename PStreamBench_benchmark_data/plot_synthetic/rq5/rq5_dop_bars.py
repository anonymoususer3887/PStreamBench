"""DoP on the x-axis, all structures in one panel. One figure per event rate: rows = systems, columns = throughput,
latency, CPU; at each DoP nine bars = structures (Lin, 2F..4F, 2J..6J). Values from rq5_config_values.csv (median over runs).
throughput = sink output (variant '_consumed': consumed input instead); latency = endtoend_latency_ms p50 [s],
definition differs per system; CPU = cpu_50. 'x' = no value for that structure at that DoP."""
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
SYS=['flink','storm','spark','kafkastream','risingwave']; NAME={'flink':'Flink','storm':'Storm','spark':'Spark','kafkastream':'Kafka Streams','risingwave':'RisingWave'}
SHAPES=['LinearQuery','TwoFilters','ThreeFilters','FourFilters','TwoWayJoin','ThreeWayJoin','FourWayJoin','FiveWayJoin','SixWayJoin']
LAB=['Lin','2F','3F','4F','2J','3J','4J','5J','6J']
COL={'LinearQuery':'#e6ab02','TwoFilters':'#1b9e77','ThreeFilters':'#66a61e','FourFilters':'#a6761d','TwoWayJoin':'#7570b3',
     'ThreeWayJoin':'#e7298a','FourWayJoin':'#d95f02','FiveWayJoin':'#1f78b4','SixWayJoin':'#525252'}
HATCH={s:None for s in SHAPES}; HATCH.update({'TwoWayJoin':'///','ThreeWayJoin':'///','FourWayJoin':'///','FiveWayJoin':'///','SixWayJoin':'///'})
L=[2,4,8,16,32,40]; X=np.arange(len(L)); bw=0.86/len(SHAPES)
def kfmt(v,p): return f'{v/1e6:g}M' if v>=1e6 else (f'{v/1e3:g}k' if v>=1e3 else (f'{v:g}' if v>=1 else f'{v:.2g}'))
def figure(rate, thr_col):
    MET=[(thr_col,'Throughput [ev/s]' if thr_col=='out' else 'Consumed input [ev/s]','log',(0.01,1e7) if thr_col=='out' else (100,1e7),1),
         ('lat','E2E latency [s]','log',(0.05,1000),1000),('cpu_50','CPU [%]','lin',(0,100),1)]
    fig, axs = plt.subplots(len(SYS),3, figsize=(7.16,7.4), sharex=True, gridspec_kw=dict(hspace=0.12, wspace=0.22, left=0.085, right=0.995, top=0.93, bottom=0.05))
    for r,k in enumerate(SYS):
        for c,(col,lab,sc,lim,div) in enumerate(MET):
            ax=axs[r,c]
            for i,s in enumerate(SHAPES):
                g=C[(C.system==k)&(C.application_slug==s)&(C.producer_rate==rate)].set_index('dop')
                v=np.array([g[col].get(l,np.nan) if l in g.index else np.nan for l in L],float)/div
                if sc=='log': v[v<=0]=np.nan
                xs=X+(i-(len(SHAPES)-1)/2)*bw
                ax.bar(xs,v,bw,color=COL[s],edgecolor='black',lw=0.3,zorder=2)
                if HATCH[s]: ax.bar(xs,v,bw,color='none',edgecolor='white',lw=0,hatch=HATCH[s],zorder=2.1); ax.bar(xs,v,bw,color='none',edgecolor='black',lw=0.3,zorder=2.2)
                for x,val in zip(xs,v):
                    if np.isnan(val): ax.text(x,lim[0]*1.12 if sc=='log' else 0.5,'×',ha='center',va='bottom',fontsize=3.8,color='0.4')
            if sc=='log':
                ax.set_yscale('log'); ax.yaxis.set_major_locator(LogLocator(10,numticks=6 if c==0 else 5)); ax.yaxis.set_minor_formatter(NullFormatter())
                ax.yaxis.set_major_formatter(FuncFormatter(kfmt if c==0 else (lambda v,p: f'{v:g}')))
            else: ax.yaxis.set_major_locator(MultipleLocator(50)); ax.yaxis.set_minor_locator(MultipleLocator(25))
            ax.set_ylim(*lim); ax.set_xlim(-0.5,len(L)-0.5); ax.grid(axis='y',ls=(0,(2,2)),lw=0.35,color='0.8',zorder=0)
            for b in X[:-1]+0.5: ax.axvline(b,color='0.85',lw=0.5,zorder=1)
            if r==0: ax.set_title(lab,fontsize=7,fontweight='bold',pad=3)
        axs[r,0].set_ylabel(NAME[k],fontsize=7.5,fontweight='bold')
    for ax in axs[-1]: ax.set_xticks(X); ax.set_xticklabels([str(l) for l in L])
    H=[Patch(facecolor=COL[s],edgecolor='white' if HATCH[s] else 'black',hatch=HATCH[s],lw=0.5) for s in SHAPES]
    fig.legend(H,LAB,loc='upper center',ncol=9,frameon=False,bbox_to_anchor=(0.54,1.0),handlelength=1.8,handleheight=1.0,columnspacing=1.1,
               title=f'{int(rate/1000)}k events/s · bars = query structure (plain = pipeline depth, hatched = join width) · × = no value',title_fontsize=6.5,fontsize=6.8)
    fig.text(0.54,0.01,'Degree of parallelism',ha='center',fontsize=7)
    k=f'{int(rate/1000)}k'+('' if thr_col=='out' else '_consumed')
    for ext in ['pdf','svg','png']: fig.savefig(os.path.join(HERE,f'fig_syn_dop_{k}.{ext}'),dpi=220)
    plt.close(fig)
for r in [100000.0,500000.0,1000000.0]:
    figure(r,'out'); figure(r,'consumed')
print('ok')
