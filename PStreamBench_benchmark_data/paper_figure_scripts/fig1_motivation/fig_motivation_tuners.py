"""Motivation figure: DS2 and Dhalion relative to the best uniform configuration, five DSPS, 1M events/s.
Values are the medians over applications from Fig. 9 (fig_rq4_summary_1M.pdf), panels (c) and (d),
read from the vector median lines of the PDF (axis-calibrated, exact to the drawn values):
x = DS2/Dhalion throughput / best uniform throughput, y = best uniform latency / DS2/Dhalion latency."""
import os
HERE=os.path.dirname(os.path.abspath(__file__))
import matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':7,'axes.labelsize':7,'xtick.labelsize':6.5,'ytick.labelsize':6.5,'axes.linewidth':0.6,'pdf.fonttype':42,'svg.fonttype':'none',
 'xtick.direction':'in','ytick.direction':'in'})
# (throughput ratio, latency ratio) from Fig. 9(c),(d)
V={'flink':{'ds2':(0.969,15.6),'dhalion':(0.897,13.8)},
   'spark':{'ds2':(0.513,1.15),'dhalion':(0.664,1.17)},
   'storm':{'ds2':(0.026,3.02),'dhalion':(0.226,3.84)},
   'kafkastream':{'ds2':(1.32,55.9),'dhalion':(59.7,44.4)},
   'risingwave':{'ds2':(0.976,1.24),'dhalion':(0.972,1.52)}}
# Uniform p=2 and p=40 (not in Fig. 9): medians over applications from datasets_v2 (rq4_tradeoff_medians.csv,
# same runs, best-uniform normalization and application selection as plot_synthetic/rq4_motivation/rq4_tradeoff.py)
U={'flink':{'p2':(0.718,1.226),'p40':(0.938,2.497)},
   'spark':{'p2':(0.779,1.254),'p40':(0.903,1.309)},
   'storm':{'p2':(0.486,1.641),'p40':(0.995,2.197)},
   'kafkastream':{'p2':(0.502,28.0),'p40':(0.091,1.071)},
   'risingwave':{'p2':(0.954,1.059),'p40':(0.899,1.009)}}
NAME={'flink':'Flink','spark':'Spark','storm':'Storm','kafkastream':'Kafka Streams*','risingwave':'RisingWave'}
FILL={'flink':'#9EC5E8','storm':'#F6B092','spark':'#F7DD8F','kafkastream':'#A8DDBF','risingwave':'#D8BCE8'}
EDGE={'flink':'#3A78B0','storm':'#C8592B','spark':'#B8890F','kafkastream':'#2E8A5A','risingwave':'#8A55A8'}
fig,ax=plt.subplots(figsize=(3.45,2.8)); fig.subplots_adjust(left=0.15,right=0.98,top=0.90,bottom=0.15)
ax.set_xscale('log'); ax.set_yscale('log'); ax.set_xlim(0.015,150); ax.set_ylim(0.012,2.2)
ax.axvline(1,color='0.4',lw=0.7,ls=(0,(4,2)),zorder=1); ax.axhline(1,color='0.4',lw=0.7,ls=(0,(4,2)),zorder=1)
ax.grid(lw=0.3,color='0.9',which='major',zorder=0)
for s,d in V.items():
    (x1,l1),(x2,l2)=d['ds2'],d['dhalion']
    ax.plot([x1,x2],[1/l1,1/l2],color=EDGE[s],lw=1.0,alpha=0.8,zorder=3)
    ax.plot(x1,1/l1,'o',ms=6,mfc=FILL[s],mec=EDGE[s],mew=1.1,zorder=4)
    ax.plot(x2,1/l2,'s',ms=5.6,mfc=FILL[s],mec=EDGE[s],mew=1.1,zorder=4)
for s,d in U.items():
    (x1,l1),(x2,l2)=d['p2'],d['p40']
    ax.annotate('',xy=(x2,1/l2),xytext=(x1,1/l1),arrowprops=dict(arrowstyle='-|>,head_length=0.35,head_width=0.18',lw=0.8,ls=(0,(2,1.5)),
                color=EDGE[s],alpha=0.85,shrinkA=3,shrinkB=3),zorder=3)
    ax.plot(x1,1/l1,'v',ms=5,mfc='white',mec=EDGE[s],mew=1.0,zorder=4)
    ax.plot(x2,1/l2,'^',ms=5,mfc='white',mec=EDGE[s],mew=1.0,zorder=4)
def ann(s,t,xy,xyt,ha='left'):
    ax.annotate(t,xy=xy,xytext=xyt,ha=ha,va='center',fontsize=6.2,color=EDGE[s],fontweight='bold',linespacing=0.95,
                arrowprops=dict(arrowstyle='-',lw=0.6,color=EDGE[s],shrinkB=4))
ann('flink','Flink: near-best throughput\n(0.90–0.97×), but 14–16×\nhigher latency',(0.98,0.068),(1.9,0.105))
ann('spark','Spark: only 1.15–1.17× latency,\nbut 0.51–0.66× throughput',(0.58,0.86),(0.018,1.55))
ann('storm','Storm: throughput\ncollapses (0.03–0.23×)',(0.03,0.33),(0.018,0.16))
ann('risingwave','RisingWave: close\nto best uniform',(0.98,0.72),(2.2,1.45))
ann('kafkastream','Kafka Streams*',(15,0.02),(4,0.036))
ax.set_xlabel('throughput / best uniform throughput  (higher is better)')
ax.set_ylabel('best uniform latency / latency  (higher is better)')
ax.set_xticks([0.02,0.1,1,10,100]); ax.set_xticklabels(['0.02','0.1','1','10','100'])
ax.set_yticks([0.02,0.05,0.1,0.2,0.5,1]); ax.set_yticklabels(['0.02','0.05','0.1','0.2','0.5','1']); ax.minorticks_off()
H=[Line2D([],[],marker='v',ls='none',mfc='white',mec='0.25',mew=1,ms=5,label='Uniform $p{=}2$'),
   Line2D([],[],marker='^',ls='none',mfc='white',mec='0.25',mew=1,ms=5,label='Uniform $p{=}40$'),
   Line2D([],[],marker='o',ls='none',mfc='0.75',mec='0.25',mew=1,ms=5.5,label='DS2'),
   Line2D([],[],marker='s',ls='none',mfc='0.75',mec='0.25',mew=1,ms=5,label='Dhalion')]
fig.legend(handles=H,loc='upper center',ncol=4,frameon=False,bbox_to_anchor=(0.56,1.0),fontsize=6.5,columnspacing=0.9,handletextpad=0.3)
for e in ['pdf','svg','png']: fig.savefig(os.path.join(HERE,f'fig_motivation_uniform_tuners_fig9.{e}'),dpi=300)
