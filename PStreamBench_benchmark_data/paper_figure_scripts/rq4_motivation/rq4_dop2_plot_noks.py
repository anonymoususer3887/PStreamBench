"""Motivation figure (data, not illustrative): trajectory of uniform DoP 2->40 plus DS2 and Dhalion, normalized to uniform DoP 2."""
import os, numpy as np, pandas as pd, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
HERE=os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':7,'axes.labelsize':7,'xtick.labelsize':6.5,'ytick.labelsize':6.5,'axes.linewidth':0.6,'pdf.fonttype':42,'svg.fonttype':'none',
 'xtick.direction':'in','ytick.direction':'in'})
M=pd.read_csv(os.path.join(HERE,'rq4_dop2_medians.csv')).set_index(['system','cfg'])
SYS=['flink','spark','storm','risingwave']   # Kafka Streams in a separate panel version (its range dwarfs the others)
NAME={'flink':'Flink','spark':'Spark','storm':'Storm','kafkastream':'Kafka Streams*','risingwave':'RisingWave'}
EDGE={'flink':'#3A78B0','storm':'#C8592B','spark':'#B8890F','kafkastream':'#2E8A5A','risingwave':'#8A55A8'}
FILL={'flink':'#9EC5E8','storm':'#F6B092','spark':'#F7DD8F','kafkastream':'#A8DDBF','risingwave':'#D8BCE8'}
U=['u2','u4','u8','u16','u32','u40']
fig,ax=plt.subplots(figsize=(3.45,3.0)); fig.subplots_adjust(left=0.14,right=0.98,top=0.84,bottom=0.13)
ax.set_xlim(0.55,2.0); ax.set_ylim(0.2,1.45)
ax.axvline(1,color='0.5',lw=0.6,ls=(0,(4,2)),zorder=1); ax.axhline(1,color='0.5',lw=0.6,ls=(0,(4,2)),zorder=1)
ax.grid(lw=0.3,color='0.9',zorder=0)
for s in SYS:
    xs=[M.loc[(s,c),'thr_rel'] for c in U]; ys=[M.loc[(s,c),'inv_lat_rel'] for c in U]
    ax.plot(xs,ys,'-',color=EDGE[s],lw=1.1,zorder=3)
    ax.plot(xs[1:],ys[1:],'o',ms=3.6,mfc=FILL[s],mec=EDGE[s],mew=0.8,zorder=4)
    ax.annotate('40',(xs[-1],ys[-1]),xytext=(2,-6),textcoords='offset points',fontsize=5,color=EDGE[s])
    for c,mk,ms in (('ds2','D',4.8),('dhalion','*',8)):
        if (s,c) in M.index: ax.plot(M.loc[(s,c),'thr_rel'],M.loc[(s,c),'inv_lat_rel'],mk,ms=ms,mfc=EDGE[s],mec='white',mew=0.6,zorder=5)
ax.plot(1,1,'o',ms=5.5,mfc='white',mec='black',mew=1.1,zorder=6)
def ann(s,t,xy,xyt):
    ax.annotate(t,xy=xy,xytext=xyt,fontsize=6,color=EDGE[s],fontweight='bold',linespacing=0.95,va='center',
                arrowprops=dict(arrowstyle='-',lw=0.6,color=EDGE[s],shrinkB=3))
g=lambda s,c,k: M.loc[(s,c),k]
ann('flink','Flink: up to 1.5× throughput,\nbut up to 1.8× latency;\ntuners: 2.2–3.7× latency',(g('flink','dhalion','thr_rel'),g('flink','dhalion','inv_lat_rel')),(0.58,0.33))
ann('storm','Storm: 1.8× throughput\nat 1.2× latency',(g('storm','u40','thr_rel'),g('storm','u40','inv_lat_rel')),(1.45,0.5))
ann('spark','Spark: small gains (≤1.26×);\ntuners trade throughput\nfor lower latency',(g('spark','ds2','thr_rel'),g('spark','ds2','inv_lat_rel')),(0.58,1.33))
ann('risingwave','RisingWave: flat',(g('risingwave','u40','thr_rel'),g('risingwave','u40','inv_lat_rel')),(1.15,1.36))
ax.set_xlabel('throughput (normalized to uniform DoP 2)'); ax.set_ylabel('inverse latency (normalized to uniform DoP 2)')


H=[Line2D([],[],marker='o',color='0.3',ms=3.6,mfc='white',lw=1,label='Uniform DoP 2→40'),
   Line2D([],[],marker='D',ls='none',mfc='0.3',mec='white',ms=4.8,label='DS2'),
   Line2D([],[],marker='*',ls='none',mfc='0.3',mec='white',ms=8,label='Dhalion')]
fig.legend(handles=H,loc='upper center',ncol=3,frameon=False,bbox_to_anchor=(0.56,1.0),fontsize=6.5,columnspacing=1.0,handletextpad=0.3)
H2=[Line2D([],[],color=EDGE[s],lw=1.5,label=NAME[s]) for s in SYS]
fig.legend(handles=H2,loc='upper center',ncol=4,frameon=False,bbox_to_anchor=(0.56,0.945),fontsize=6,columnspacing=0.7,handlelength=1.2,handletextpad=0.3)
for e in ['pdf','svg','png']: fig.savefig(os.path.join(HERE,f'fig_motivation_trajectory_noKS_1M.{e}'),dpi=300)
print('ok')
