"""Ad Analytics (with_join) at 1M events/s on m510: uniform DoP 2, uniform DoP 40, DS2 and Dhalion per system.
Values from datasets_v2/real_query_level.csv (uniform runs from the 'uniform' folder, single run each):
throughput = throughput_50 [events/s], latency = endtoend_latency_ms p50 [s]. None = no run / no output."""
import os
HERE=os.path.dirname(os.path.abspath(__file__))
# Storm (all four bars): throughput_avg (mean of the 1-min sink samples), because its windowed sink emits in bursts and the median of 5 samples can be 0 (Dhalion 1M: 14459,0,0,0,54184). Other systems: throughput_50. DS2/Dhalion = latest rerun2.
import numpy as np, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.patches import Patch
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':7,'axes.labelsize':7,'xtick.labelsize':6.3,'ytick.labelsize':6.3,'axes.linewidth':0.6,'pdf.fonttype':42,'svg.fonttype':'none',
 'xtick.direction':'in','ytick.direction':'in','hatch.linewidth':0.5})
SYS=['Flink','Storm','Spark','Kafka\nStreams','Rising-\nWave']
# (throughput, latency_s) for DoP 2, DoP 40, DS2, Dhalion
D={'Flink':[(210273,3.48),(464557,27.90),(463232,620.28),(384204,92.05)],
   'Storm':[(56060,0.45),(121575,1.05),(107338,0.878),(13729,0.458)],
   'Spark':[(1263717,23.79),(1853144,3.65),(145975,4.18),(612287,19.95)],
   'Kafka\nStreams':[(5985,17.23),(1151,2.78),None,(403388,157.69)],
   'Rising-\nWave':[(17732,1.37),(9575,1.37),(21046,1.69),(20554,1.69)]}
CFG=['DoP 2','DoP 40','DS2','Dhalion']
COL=['#9EC5E8','#F6C08A','#B8B8B8','#D8BCE8']; HAT=[None,None,'////','\\\\\\\\']; ECOL=['#3A78B0','#C8592B','#555555','#8A55A8']
UP,DN='#1B7F3B','#B2182B'
fig,axs=plt.subplots(1,2,figsize=(3.5,2.5)); fig.subplots_adjust(left=0.115,right=0.995,top=0.83,bottom=0.14,wspace=0.42)
x=np.arange(len(SYS)); bw=0.2
def kfmt(v): return f'{v/1e6:.1f}M' if v>=1e6 else (f'{v/1e3:.0f}k' if v>=1e3 else f'{v:.0f}')
def lfmt(v): return f'{v:.0f}' if v>=10 else f'{v:.1f}'
for ax,(k,lab,better_high,lim,fmt) in zip(axs,[(0,'(a) Throughput [events/s]',True,(500,4e7),kfmt),(1,'(b) End-to-end latency [s]',False,(0.2,1e4),lfmt)]):
    ax.set_yscale('log'); ax.set_ylim(*lim)
    for i,s in enumerate(SYS):
        base=D[s][0][k]
        for j in range(4):
            xx=x[i]+(j-1.5)*bw; v=D[s][j]
            if v is None or v[k] is None or not v[k]>0:
                ax.text(xx,lim[0]*1.25,'×',ha='center',va='bottom',fontsize=6,color='0.35'); continue
            ax.bar(xx,v[k],bw,color=COL[j],edgecolor=ECOL[j],lw=0.5,hatch=HAT[j],zorder=2,bottom=lim[0])
            if j>0:
                r=v[k]/base; good=(r>1)==better_high
                if abs(np.log(r))<np.log(1.15): t,c='≈1×','0.4'
                else: t,c=(r'$\uparrow$'+f'{r:.1f}×' if r>1 else r'$\downarrow$'+f'{1/r:.1f}×'),(UP if good else DN)
                ax.text(xx,v[k]*1.18,t,rotation=90,ha='center',va='bottom',fontsize=5,color=c,fontweight='bold',zorder=3)
    ax.set_xticks(x); ax.set_xticklabels(SYS,fontsize=6.1); ax.tick_params(axis='x',length=0)
    ax.set_ylabel(lab,fontsize=6.8,labelpad=1); ax.grid(axis='y',lw=0.3,color='0.88',zorder=0)
    for b in x[:-1]+0.5: ax.axvline(b,color='0.9',lw=0.5,zorder=0)
H=[Patch(facecolor=COL[j],edgecolor=ECOL[j],hatch=HAT[j],lw=0.5,label=CFG[j]) for j in range(4)]
H+=[Patch(facecolor='white',edgecolor='white',label='')]
fig.legend(handles=H[:4],loc='upper center',ncol=4,frameon=True,bbox_to_anchor=(0.55,1.0),fontsize=6.3,handlelength=1.4,columnspacing=0.9,borderpad=0.35)
fig.text(0.55,0.875,'Ad Analytics · m510 · 1M events/s · change vs. DoP 2 · ×: no run · Storm throughput: run mean',
         ha='center',fontsize=5,style='italic',color='0.3')
for e in ['pdf','svg','png']: fig.savefig(os.path.join(HERE,f'fig_motivation_vertical_tuners_1M_v3.{e}'),dpi=300)
