"""Motivation figure: every uniform DoP (2,4,8,16,32,40) labelled, plus DS2 and Dhalion, all normalized to uniform DoP 2.
Left: Flink, Spark, Storm, RisingWave (log axes). Right: Kafka Streams (log axes; its range is two orders larger).
Values: rq4_dop2_medians.csv (median over applications, m510, 1M events/s)."""
import os, pandas as pd, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
HERE=os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':7,'axes.labelsize':7,'xtick.labelsize':6.5,'ytick.labelsize':6.5,'axes.linewidth':0.6,'pdf.fonttype':42,'svg.fonttype':'none',
 'xtick.direction':'in','ytick.direction':'in'})
M=pd.read_csv(os.path.join(HERE,'rq4_dop2_medians.csv')).set_index(['system','cfg'])
NAME={'flink':'Flink','spark':'Spark','storm':'Storm','kafkastream':'Kafka Streams*','risingwave':'RisingWave'}
EDGE={'flink':'#3A78B0','storm':'#C8592B','spark':'#B8890F','kafkastream':'#2E8A5A','risingwave':'#8A55A8'}
FILL={'flink':'#9EC5E8','storm':'#F6B092','spark':'#F7DD8F','kafkastream':'#A8DDBF','risingwave':'#D8BCE8'}
U=[('u2','2'),('u4','4'),('u8','8'),('u16','16'),('u32','32'),('u40','40')]
# label offsets (points) per system to keep DoP labels readable
OFF={'flink':(4,-6),'spark':(-9,4),'storm':(3,4),'risingwave':(-3,-8),'kafkastream':(4,2)}
def draw(ax,systems):
    for s in systems:
        xs=[M.loc[(s,c),'thr_rel'] for c,_ in U]; ys=[M.loc[(s,c),'inv_lat_rel'] for c,_ in U]
        ax.plot(xs,ys,'-',color=EDGE[s],lw=1.1,zorder=3)
        ax.plot(xs[1:],ys[1:],'o',ms=3.8,mfc=FILL[s],mec=EDGE[s],mew=0.8,zorder=4)
        for (c,l),x,y in ([] if s=='risingwave' else zip(U[1:],xs[1:],ys[1:])):  # RisingWave points coincide; label omitted
            ax.annotate(l,(x,y),xytext=OFF[s],textcoords='offset points',fontsize=5.2,color=EDGE[s],zorder=7)
        for c,mk,ms in (('ds2','D',5),('dhalion','*',8.5)):
            if (s,c) in M.index: ax.plot(M.loc[(s,c),'thr_rel'],M.loc[(s,c),'inv_lat_rel'],mk,ms=ms,mfc=EDGE[s],mec='white',mew=0.6,zorder=5)
    ax.plot(1,1,'o',ms=5.5,mfc='white',mec='black',mew=1.1,zorder=6)
    ax.axvline(1,color='0.5',lw=0.6,ls=(0,(4,2)),zorder=1); ax.axhline(1,color='0.5',lw=0.6,ls=(0,(4,2)),zorder=1)
    ax.grid(lw=0.3,color='0.9',zorder=0)
fig,(a1,a2)=plt.subplots(1,2,figsize=(7.16,2.9),gridspec_kw=dict(width_ratios=[2.2,1],wspace=0.22,left=0.065,right=0.99,top=0.84,bottom=0.14))
draw(a1,['flink','spark','storm','risingwave']); a1.set_xscale('log'); a1.set_yscale('log'); a1.set_xlim(0.58,2.1); a1.set_ylim(0.22,1.45)
a1.set_xticks([0.6,0.8,1,1.25,1.5,2]); a1.set_xticklabels(['0.6','0.8','1','1.25','1.5','2'])
a1.set_yticks([0.25,0.33,0.5,0.67,1,1.25]); a1.set_yticklabels(['0.25','0.33','0.5','0.67','1','1.25']); a1.minorticks_off()
draw(a2,['kafkastream']); a2.set_xscale('log'); a2.set_yscale('log'); a2.set_xlim(0.07,3); a2.set_ylim(0.4,45)
a2.set_xticks([0.1,0.3,1,3]); a2.set_xticklabels(['0.1','0.3','1','3']); a2.set_yticks([0.5,1,2,5,10,20]); a2.set_yticklabels(['0.5','1','2','5','10','20']); a2.minorticks_off()
g=lambda s,c,k: M.loc[(s,c),k]
def ann(ax,s,t,xy,xyt):
    ax.annotate(t,xy=xy,xytext=xyt,fontsize=6.2,color=EDGE[s],fontweight='bold',linespacing=0.95,va='center',
                arrowprops=dict(arrowstyle='-',lw=0.6,color=EDGE[s],shrinkB=4))
ann(a1,'flink','Flink: up to 1.5× throughput, 1.8× latency;\nDS2 1.8× throughput at 2.2× latency,\nDhalion 3.7× latency',(g('flink','dhalion','thr_rel'),g('flink','dhalion','inv_lat_rel')),(1.24,0.27))
ann(a1,'storm','Storm: 1.8× throughput at 1.2×\nlatency; tuners only 0.89–1.12×\nthroughput at 1.3–1.7× latency',(g('storm','u40','thr_rel'),g('storm','u40','inv_lat_rel')),(1.38,0.55))
ann(a1,'spark','Spark: ≤1.26× throughput;\ntuners trade throughput for latency',(g('spark','ds2','thr_rel'),g('spark','ds2','inv_lat_rel')),(0.6,1.36))
ann(a1,'risingwave','RisingWave: all DoPs at (1, 1)',(g('risingwave','u32','thr_rel'),g('risingwave','u32','inv_lat_rel')),(1.15,1.33))
ann(a2,'kafkastream','output falls to 0.1×;\nlatency of the few emitted\nrecords 27× lower',(g('kafkastream','u40','thr_rel'),g('kafkastream','u40','inv_lat_rel')),(0.085,4))
a2.set_title('Kafka Streams* (log scale)',fontsize=7,color=EDGE['kafkastream'],pad=3)
for a in (a1,a2): a.set_xlabel('throughput (normalized to uniform DoP 2)')
a1.set_ylabel('inverse latency (normalized to uniform DoP 2)')
H=[Line2D([],[],marker='o',color='0.3',ms=3.8,mfc='white',lw=1,label='Uniform DoP 2→40 (labels: DoP)'),
   Line2D([],[],marker='D',ls='none',mfc='0.3',mec='white',ms=5,label='DS2'),
   Line2D([],[],marker='*',ls='none',mfc='0.3',mec='white',ms=8.5,label='Dhalion')]
H+= [Line2D([],[],color=EDGE[s],lw=1.6,label=NAME[s]) for s in ['flink','spark','storm','risingwave','kafkastream']]
fig.legend(handles=H,loc='upper center',ncol=8,frameon=False,bbox_to_anchor=(0.52,1.0),fontsize=6.3,columnspacing=0.9,handletextpad=0.3,handlelength=1.4)
for e in ['pdf','svg','png']: fig.savefig(os.path.join(HERE,f'fig_motivation_trajectory_all_log_1M.{e}'),dpi=300)
print('ok')
