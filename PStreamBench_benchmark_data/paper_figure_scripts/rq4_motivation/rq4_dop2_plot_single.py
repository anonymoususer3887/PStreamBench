"""Single-panel motivation figure: uniform DoP 2..40, DS2, Dhalion for all five systems, normalized to uniform DoP 2.
Values: rq4_dop2_common_geomean.csv (geometric mean over applications that ran at every uniform DoP).
Axes: y log; x log with the range below 1 compressed (factor C) so that the few points there take less space."""
import os, numpy as np, pandas as pd, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
HERE=os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':7,'axes.labelsize':7,'xtick.labelsize':6.5,'ytick.labelsize':6.5,'axes.linewidth':0.6,'pdf.fonttype':42,'svg.fonttype':'none',
 'xtick.direction':'in','ytick.direction':'in'})
M=pd.read_csv(os.path.join(HERE,'rq4_dop2_common_geomean.csv')).set_index(['system','cfg'])
SYS=['flink','spark','storm','risingwave','kafkastream']
NAME={'flink':'Flink','spark':'Spark','storm':'Storm','kafkastream':'Kafka Streams*','risingwave':'RisingWave'}
EDGE={'flink':'#3A78B0','storm':'#C8592B','spark':'#B8890F','kafkastream':'#2E8A5A','risingwave':'#8A55A8'}
FILL={'flink':'#9EC5E8','storm':'#F6B092','spark':'#F7DD8F','kafkastream':'#A8DDBF','risingwave':'#D8BCE8'}
U=[('u2','2'),('u4','4'),('u8','8'),('u16','16'),('u32','32'),('u40','40')]
C=0.45   # compression of the range below 1 on the throughput axis
fwd=lambda x: np.where(np.asarray(x)>=1, np.log10(np.maximum(x,1e-9)), C*np.log10(np.maximum(x,1e-9)))
inv=lambda y: np.where(np.asarray(y)>=0, 10**np.asarray(y), 10**(np.asarray(y)/C))
OFF={'flink':(3,-7),'spark':(-3,5),'storm':(2,4),'risingwave':(0,0),'kafkastream':(4,1)}
fig,ax=plt.subplots(figsize=(3.45,3.1)); fig.subplots_adjust(left=0.13,right=0.98,top=0.83,bottom=0.13)
ax.set_xscale('function',functions=(fwd,inv)); ax.set_yscale('log')
ax.set_xlim(0.15,3.6); ax.set_ylim(0.12,30)
ax.axvspan(0.15,1,color='0.96',zorder=0,lw=0)
ax.text(0.155,0.13,'compressed scale',fontsize=5.5,color='0.5',style='italic',va='bottom')
ax.axvline(1,color='0.5',lw=0.6,ls=(0,(4,2)),zorder=1); ax.axhline(1,color='0.5',lw=0.6,ls=(0,(4,2)),zorder=1)
ax.grid(lw=0.3,color='0.88',zorder=0)
for s in SYS:
    xs=[M.loc[(s,c),'thr_rel'] for c,_ in U]; ys=[M.loc[(s,c),'inv_lat_rel'] for c,_ in U]
    ax.plot(xs,ys,'-',color=EDGE[s],lw=1.1,zorder=3)
    ax.plot(xs[1:],ys[1:],'o',ms=3.5,mfc=FILL[s],mec=EDGE[s],mew=0.8,zorder=4)
    if s!='risingwave':
        for (c,l),x,y in zip(U[1:],xs[1:],ys[1:]):
            ax.annotate(l,(x,y),xytext=OFF[s],textcoords='offset points',fontsize=4.8,color=EDGE[s],zorder=7)
    for c,mk,ms in (('ds2','D',4.6),('dhalion','*',8)):
        if (s,c) in M.index: ax.plot(M.loc[(s,c),'thr_rel'],M.loc[(s,c),'inv_lat_rel'],mk,ms=ms,mfc=EDGE[s],mec='white',mew=0.6,zorder=5)
ax.plot(1,1,'o',ms=5.2,mfc='white',mec='black',mew=1.1,zorder=6)
g=lambda s,c,k: M.loc[(s,c),k]
def ann(s,t,xy,xyt):
    ax.annotate(t,xy=xy,xytext=xyt,fontsize=5.8,color=EDGE[s],fontweight='bold',linespacing=0.95,va='center',
                arrowprops=dict(arrowstyle='-',lw=0.55,color=EDGE[s],shrinkB=3))
ann('flink','Flink: 1.5× throughput at 1.9× latency;\ntuners 1.9–2.4× throughput\nat 5.5–6.7× latency',(g('flink','u40','thr_rel'),g('flink','u40','inv_lat_rel')),(0.2,0.24))
ann('storm','Storm: up to 3× throughput\nat ≤1.5× latency;\ntuners 0.3–0.9×\nthroughput',(g('storm','u40','thr_rel'),g('storm','u40','inv_lat_rel')),(1.75,0.33))
ann('spark','Spark: up to 1.6× throughput;\ntuners trade throughput\nfor latency',(g('spark','ds2','thr_rel'),g('spark','ds2','inv_lat_rel')),(0.42,4.0))
ann('risingwave','RisingWave: near (1, 1)',(g('risingwave','u40','thr_rel'),g('risingwave','u40','inv_lat_rel')),(1.35,2.2))
ann('kafkastream','Kafka Streams*: output 0.17×,\nlatency of emitted records 20× lower',(g('kafkastream','u40','thr_rel'),g('kafkastream','u40','inv_lat_rel')),(0.42,22))
ax.set_xticks([0.15,0.3,0.5,1,1.5,2,3]); ax.set_xticklabels(['0.15','0.3','0.5','1','1.5','2','3'])
ax.set_yticks([0.15,0.25,0.5,1,2,5,10,20]); ax.set_yticklabels(['0.15','0.25','0.5','1','2','5','10','20']); ax.minorticks_off()
ax.set_xlabel('throughput (normalized to uniform DoP 2)'); ax.set_ylabel('inverse latency (normalized to uniform DoP 2)')
H=[Line2D([],[],marker='o',color='0.3',ms=3.5,mfc='white',lw=1,label='Uniform DoP 2→40'),
   Line2D([],[],marker='D',ls='none',mfc='0.3',mec='white',ms=4.6,label='DS2'),
   Line2D([],[],marker='*',ls='none',mfc='0.3',mec='white',ms=8,label='Dhalion')]
fig.legend(handles=H,loc='upper center',ncol=3,frameon=False,bbox_to_anchor=(0.55,1.0),fontsize=6.3,columnspacing=1.0,handletextpad=0.3)
H2=[Line2D([],[],color=EDGE[s],lw=1.5,label=NAME[s]) for s in SYS]
fig.legend(handles=H2,loc='upper center',ncol=5,frameon=False,bbox_to_anchor=(0.55,0.945),fontsize=5.8,columnspacing=0.6,handlelength=1.1,handletextpad=0.25)
for e in ['pdf','svg','png']: fig.savefig(os.path.join(HERE,f'fig_motivation_trajectory_single_1M.{e}'),dpi=300)
print('ok')
