"""Motivation figure: where each strategy lands relative to the best uniform DoP (1M events/s, m510).
x = throughput / best uniform throughput, y = best uniform latency / latency (both: higher is better, 1 = best uniform).
Medians over applications from rq4_tradeoff_medians.csv. Kafka Streams excluded (uniform baseline not comparable, see RQ4)."""
import os, numpy as np, pandas as pd, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
HERE=os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':7,'axes.labelsize':7,'xtick.labelsize':6.5,'ytick.labelsize':6.5,'axes.linewidth':0.6,'pdf.fonttype':42,'svg.fonttype':'none',
 'xtick.direction':'in','ytick.direction':'in'})
M=pd.read_csv(os.path.join(HERE,'rq4_tradeoff_medians.csv'))
SYS=['flink','storm','spark','kafkastream','risingwave']; NAME={'flink':'Flink','storm':'Storm','spark':'Spark','kafkastream':'Kafka Streams','risingwave':'RisingWave'}
FILL={'kafkastream':'#A8DDBF','flink':'#9EC5E8','storm':'#F6B092','spark':'#F7DD8F','risingwave':'#D8BCE8'}
EDGE={'kafkastream':'#2E8A5A','flink':'#3A78B0','storm':'#C8592B','spark':'#B8890F','risingwave':'#8A55A8'}
STR=[('uniform_p2','v','Uniform $p{=}2$'),('uniform_p40','^','Uniform $p{=}40$'),('ds2','o','DS2'),('dhalion','s','Dhalion')]
fig,ax=plt.subplots(figsize=(3.45,2.95)); fig.subplots_adjust(left=0.15,right=0.98,top=0.86,bottom=0.15)
XL,YL=(0.015,1.6),(0.03,1.6)
ax.set_xscale('log'); ax.set_yscale('log'); ax.set_xlim(*XL); ax.set_ylim(*YL)
YL=(0.011,1.9); ax.set_ylim(*YL)
ax.axhline(1,color='0.45',lw=0.6,ls=(0,(3,2)),zorder=1); ax.axvline(1,color='0.45',lw=0.6,ls=(0,(3,2)),zorder=1)
ax.plot(1,1,marker='*',ms=10,color='black',zorder=6,clip_on=False)
ax.annotate('best uniform DoP',xy=(1,1),xytext=(1.25,1.32),fontsize=6,ha='right',va='bottom')
for s in SYS:
    g=M[M.system==s].set_index('strategy')
    pts=[(g.thr_ratio.get(k,np.nan),g.inv_lat.get(k,np.nan)) for k,_,_ in STR]
    xs=[p[0] for p in pts if np.isfinite(p[0]) and np.isfinite(p[1])]; ys=[p[1] for p in pts if np.isfinite(p[0]) and np.isfinite(p[1])]
    for k,mk,_ in STR:
        if k in g.index and np.isfinite(g.thr_ratio[k]) and np.isfinite(g.inv_lat[k]):
            ax.plot(g.thr_ratio[k],g.inv_lat[k],marker=mk,ms=5.2 if mk not in ('D',) else 4.4,mfc=FILL[s],mec=EDGE[s],mew=1.0,ls='none',zorder=5)
for s_ in SYS:
    g=M[M.system==s_].set_index('strategy')
    for a_,b_,ls_ in (('uniform_p2','uniform_p40',(0,(2,1.5))),('ds2','dhalion','-')):
        if a_ in g.index and b_ in g.index:
            ax.plot([g.thr_ratio[a_],g.thr_ratio[b_]],[g.inv_lat[a_],g.inv_lat[b_]],color=EDGE[s_],lw=0.8,ls=ls_,alpha=0.7,zorder=4)
ANN={'flink':('Flink: tuners reach $p{=}40$ throughput\nat 6$\\times$ its latency',(0.9,0.071),(0.0165,0.06)),
     'storm':('Storm: tuners lose 4–38$\\times$\nthroughput vs. $p{=}40$',(0.028,0.33),(0.0165,0.14)),
     'spark':('Spark: tuners lower latency,\nlose up to 2.7$\\times$ throughput',(0.34,0.98),(0.0165,1.4)),
     'kafkastream':('Kafka Streams*: tuners keep throughput,\n44–56$\\times$ higher latency',(0.97,0.02),(0.06,0.0165)),
     'risingwave':('RisingWave: all\nconfigurations close',(0.88,0.78),(0.075,0.66))}
for s_,(t,xy,xyt) in ANN.items():
    if xy is None: ax.text(*xyt,t,color=EDGE[s_],fontsize=6.3,fontweight='bold',linespacing=0.95)
    else: ax.annotate(t,xy=xy,xytext=xyt,color=EDGE[s_],fontsize=6.3,fontweight='bold',linespacing=0.95,va='center',
                      arrowprops=dict(arrowstyle='-',lw=0.6,color=EDGE[s_],shrinkB=4))
ax.set_xlabel('throughput / best uniform throughput  (→ better)')
ax.set_ylabel('best uniform latency / latency  (↑ better)')
ax.set_xticks([0.02,0.05,0.1,0.2,0.5,1]); ax.set_xticklabels(['0.02','0.05','0.1','0.2','0.5','1'])
ax.set_yticks([0.02,0.05,0.1,0.2,0.5,1]); ax.set_yticklabels(['0.02','0.05','0.1','0.2','0.5','1'])
ax.minorticks_off(); ax.grid(lw=0.3,color='0.88',zorder=0)
H=[Line2D([],[],marker=mk,ls='none',mfc='white',mec='0.25',mew=0.9,ms=5,label=l) for _,mk,l in STR]
fig.legend(handles=H,loc='upper center',ncol=4,frameon=False,bbox_to_anchor=(0.56,0.985),columnspacing=0.9,handletextpad=0.3,fontsize=6.6)
for e in ['pdf','svg','png']: fig.savefig(os.path.join(HERE,f'fig_motivation_uniform_tuners_1M.{e}'),dpi=300)
print('ok')
