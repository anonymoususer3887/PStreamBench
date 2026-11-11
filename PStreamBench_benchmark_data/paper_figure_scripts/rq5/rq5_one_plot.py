"""One plot per metric, one figure per event rate (exploration size). x-axis, nested: DoP 2 | 4 | 8 | 16 | 32 | 40;
inside each DoP the structures Lin, 2F, 3F, 4F, 2J..6J; inside each structure one bar per system.
Values = rq5_config_values.csv (median over runs). throughput = sink output (variant '_consumed': consumed input);
latency = endtoend_latency_ms p50 [s] (definition differs per system); CPU = cpu_50. 'x' = no value."""
import os, numpy as np, pandas as pd, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter, MultipleLocator
from matplotlib.patches import Patch
HERE = os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({'font.family':'serif','font.size':12,'xtick.labelsize':10,'ytick.labelsize':11,'hatch.linewidth':0.6,'pdf.fonttype':42})
C = pd.read_csv(os.path.join(HERE,'rq5_config_values.csv'))
SYS=['flink','storm','spark','kafkastream','risingwave']; NAME={'flink':'Flink','storm':'Storm','spark':'Spark','kafkastream':'Kafka Streams','risingwave':'RisingWave'}
SCOL={'flink':'#0072B2','storm':'#D55E00','spark':'#E69F00','kafkastream':'#009E73','risingwave':'#CC79A7'}
HATCH={'flink':None,'storm':'///','spark':'..','kafkastream':'xx','risingwave':'\\\\'}
SHAPES=['LinearQuery','TwoFilters','ThreeFilters','FourFilters','TwoWayJoin','ThreeWayJoin','FourWayJoin','FiveWayJoin','SixWayJoin']
LAB=['Lin','2F','3F','4F','2J','3J','4J','5J','6J']
L=[2,4,8,16,32,40]; NS=len(SHAPES); GAP=1.6   # gap between DoP groups (in structure slots)
bw=0.82/len(SYS)
pos={(d,s):gi*(NS+GAP)+si for gi,d in enumerate(L) for si,s in enumerate(SHAPES)}
def kfmt(v,p): return f'{v/1e6:g}M' if v>=1e6 else (f'{v/1e3:g}k' if v>=1e3 else (f'{v:g}' if v>=1 else f'{v:.2g}'))
def figure(rate, thr):
    MET=[(thr,'Throughput (sink output)\n[events/s]' if thr=='out' else 'Consumed input\n[events/s]','log',(0.01,1e7) if thr=='out' else (100,1e7),1),
         ('lat','End-to-end latency\n[s]','log',(0.05,1000),1000),('cpu_50','CPU (cpu_50)\n[%]','lin',(0,100),1)]
    fig, axs = plt.subplots(3,1, figsize=(34,16), sharex=True, gridspec_kw=dict(hspace=0.06, left=0.045, right=0.997, top=0.92, bottom=0.09))
    R=C[C.producer_rate==rate].set_index(['system','application_slug','dop'])
    for ax,(col,lab,sc,lim,div) in zip(axs,MET):
        for i,k in enumerate(SYS):
            xs=[];vs=[]
            for (d,s),p in pos.items():
                v=R[col].get((k,s,d),np.nan)/div
                if sc=='log' and not v>0: v=np.nan
                xs.append(p+(i-(len(SYS)-1)/2)*bw); vs.append(v)
            xs=np.array(xs); vs=np.array(vs,float)
            ax.bar(xs,vs,bw,color=SCOL[k],edgecolor='black',lw=0.4,hatch=HATCH[k],zorder=2)
            for x,v in zip(xs,vs):
                if np.isnan(v): ax.text(x,lim[0]*1.15 if sc=='log' else 0.8,'×',ha='center',va='bottom',fontsize=8,color=SCOL[k],fontweight='bold')
        if sc=='log':
            ax.set_yscale('log'); ax.yaxis.set_major_locator(LogLocator(10,numticks=10)); ax.yaxis.set_minor_formatter(NullFormatter())
            ax.yaxis.set_major_formatter(FuncFormatter(kfmt if col!='lat' else (lambda v,p: f'{v:g}')))
        else: ax.yaxis.set_major_locator(MultipleLocator(25))
        ax.set_ylim(*lim); ax.set_ylabel(lab,fontsize=13); ax.grid(axis='y',ls=':',lw=0.6,color='0.7',zorder=0)
        for gi in range(len(L)):
            x0=gi*(NS+GAP)
            ax.axvspan(x0-0.5,x0+NS-0.5,color='0.96' if gi%2==0 else 'white',zorder=0)
            ax.axvline(x0+3.5,color='0.55',lw=0.8,ls='--',zorder=1)   # filters | joins
            if gi: ax.axvline(x0-GAP/2-0.5,color='black',lw=1.2,zorder=1)
        ax.set_xlim(-0.8,(len(L)-1)*(NS+GAP)+NS-0.2)
    axs[-1].set_xticks([pos[(d,s)] for d in L for s in SHAPES]); axs[-1].set_xticklabels(LAB*len(L))
    for gi,d in enumerate(L):
        axs[-1].text(gi*(NS+GAP)+(NS-1)/2,-0.22,f'DoP {d}',transform=axs[-1].get_xaxis_transform(),ha='center',va='top',fontsize=15,fontweight='bold')
    H=[Patch(facecolor=SCOL[k],edgecolor='black',hatch=HATCH[k]) for k in SYS]
    fig.legend(H,[NAME[k] for k in SYS],loc='upper center',ncol=5,frameon=False,bbox_to_anchor=(0.52,0.995),fontsize=15,handlelength=3,handleheight=1.4,
               title=f'Event rate {int(rate/1000)}k events/s  ·  x: DoP group → structure (Lin, 2F–4F | 2J–6J) → one bar per system  ·  × = no value',title_fontsize=14)
    k=f'{int(rate/1000)}k'+('' if thr=='out' else '_consumed')
    for ext in ['pdf','png']: fig.savefig(os.path.join(HERE,f'fig_oneplot_{k}.{ext}'),dpi=100)
    plt.close(fig)
for r in [100000.0,500000.0,1000000.0]:
    figure(r,'out'); figure(r,'consumed')
print('ok')
