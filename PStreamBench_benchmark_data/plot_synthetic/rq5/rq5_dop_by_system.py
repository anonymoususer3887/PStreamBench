"""Large exploration figure (not paper size). One figure per event rate: rows = query structures (Lin, 2F..4F, 2J..6J),
columns = throughput, latency, CPU. x = DoP groups 2, 4, 8, 16, 32, 40; inside each group one bar per system.
Values = rq5_config_values.csv (median over runs). throughput = sink output (variant '_consumed': consumed input);
latency = endtoend_latency_ms p50 [s] (definition differs per system); CPU = cpu_50. 'x' = no value."""
import os, numpy as np, pandas as pd, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter, MultipleLocator
from matplotlib.patches import Patch
HERE = os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({'font.family':'serif','font.size':11,'axes.labelsize':11,'xtick.labelsize':10,'ytick.labelsize':9.5,'axes.linewidth':0.8,
                     'hatch.linewidth':0.6,'pdf.fonttype':42,'svg.fonttype':'none'})
C = pd.read_csv(os.path.join(HERE,'rq5_config_values.csv'))
SYS=['flink','storm','spark','kafkastream','risingwave']; NAME={'flink':'Flink','storm':'Storm','spark':'Spark','kafkastream':'Kafka Streams','risingwave':'RisingWave'}
SCOL={'flink':'#0072B2','storm':'#D55E00','spark':'#E69F00','kafkastream':'#009E73','risingwave':'#CC79A7'}
HATCH={'flink':None,'storm':'///','spark':'..','kafkastream':'xx','risingwave':'\\\\'}
SHAPES=['LinearQuery','TwoFilters','ThreeFilters','FourFilters','TwoWayJoin','ThreeWayJoin','FourWayJoin','FiveWayJoin','SixWayJoin']
TITLE={'LinearQuery':'Linear (filter–window–filter)','TwoFilters':'Two filters (2F)','ThreeFilters':'Three filters (3F)','FourFilters':'Four filters (4F)',
       'TwoWayJoin':'2-way join (2J)','ThreeWayJoin':'3-way join (3J)','FourWayJoin':'4-way join (4J)','FiveWayJoin':'5-way join (5J)','SixWayJoin':'6-way join (6J)'}
L=[2,4,8,16,32,40]; X=np.arange(len(L)); bw=0.8/len(SYS)
def kfmt(v,p): return f'{v/1e6:g}M' if v>=1e6 else (f'{v/1e3:g}k' if v>=1e3 else (f'{v:g}' if v>=1 else f'{v:.2g}'))
def figure(rate, thr):
    MET=[(thr,'Throughput (sink output) [events/s]' if thr=='out' else 'Consumed input [events/s]','log',(0.01,1e7) if thr=='out' else (100,1e7),1),
         ('lat','End-to-end latency [s]','log',(0.05,1000),1000),('cpu_50','CPU (cpu_50) [%]','lin',(0,100),1)]
    fig, axs = plt.subplots(len(SHAPES),3, figsize=(20,30), sharex=True, gridspec_kw=dict(hspace=0.22, wspace=0.16, left=0.07, right=0.99, top=0.955, bottom=0.03))
    for r,s in enumerate(SHAPES):
        for c,(col,lab,sc,lim,div) in enumerate(MET):
            ax=axs[r,c]
            for i,k in enumerate(SYS):
                g=C[(C.system==k)&(C.application_slug==s)&(C.producer_rate==rate)].set_index('dop')
                v=np.array([g[col].get(l,np.nan) if l in g.index else np.nan for l in L],float)/div
                if sc=='log': v[v<=0]=np.nan
                xs=X+(i-(len(SYS)-1)/2)*bw
                ax.bar(xs,v,bw,color=SCOL[k],edgecolor='black',lw=0.5,hatch=HATCH[k],zorder=2)
                for x,val in zip(xs,v):
                    if np.isnan(val): ax.text(x,lim[0]*1.15 if sc=='log' else 1,'×',ha='center',va='bottom',fontsize=10,color=SCOL[k],fontweight='bold')
            if sc=='log':
                ax.set_yscale('log'); ax.yaxis.set_major_locator(LogLocator(10,numticks=10)); ax.yaxis.set_minor_formatter(NullFormatter())
                ax.yaxis.set_major_formatter(FuncFormatter(kfmt if c==0 else (lambda v,p: f'{v:g}')))
            else: ax.yaxis.set_major_locator(MultipleLocator(25))
            ax.set_ylim(*lim); ax.set_xlim(-0.5,len(L)-0.5); ax.grid(axis='y',ls=':',lw=0.6,color='0.75',zorder=0)
            for b in X[:-1]+0.5: ax.axvline(b,color='0.6',lw=0.8,zorder=1)
            ax.set_title(f'{TITLE[s]} — {lab}',fontsize=11,loc='left')
            ax.set_xticks(X); ax.set_xticklabels([f'DoP {l}' for l in L]); ax.tick_params(labelbottom=True)
    H=[Patch(facecolor=SCOL[k],edgecolor='black',hatch=HATCH[k]) for k in SYS]
    fig.legend(H,[NAME[k] for k in SYS],loc='upper center',ncol=5,frameon=False,bbox_to_anchor=(0.53,0.995),fontsize=14,handlelength=3,handleheight=1.5,
               title=f'Event rate {int(rate/1000)}k events/s   ·   at each DoP: one bar per system   ·   × = no value',title_fontsize=13)
    k=f'{int(rate/1000)}k'+('' if thr=='out' else '_consumed')
    for ext in ['pdf','png']: fig.savefig(os.path.join(HERE,f'fig_explore_{k}.{ext}'),dpi=110)
    plt.close(fig)
for r in [100000.0,500000.0,1000000.0]:
    figure(r,'out'); figure(r,'consumed')
print('ok')
