"""Motivation figure with callout boxes for ONE application (Ad Analytics): uniform DoP 2->40 per system, normalized to uniform DoP 2.
Values: rq4_dop2_per_app.csv (raw per-configuration medians over runs; same variant selection as RQ4; m510, 1M events/s).
x: log with the range below 1 compressed (C); y: log."""
import os, numpy as np, pandas as pd, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
HERE=os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':8,'axes.labelsize':8,'xtick.labelsize':7.5,'ytick.labelsize':7.5,'axes.linewidth':0.7,'pdf.fonttype':42,'svg.fonttype':'none',
 'xtick.direction':'in','ytick.direction':'in'})
R=pd.read_csv(os.path.join(HERE,'rq4_dop2_per_app.csv')); M=R[R.application=='Ad Analytics'].set_index(['system','cfg'])
SYS=['flink','spark','storm','kafkastream','risingwave']
NAME={'flink':'Flink','spark':'Spark','storm':'Storm','kafkastream':'Kafka Streams*','risingwave':'RisingWave'}
EDGE={'flink':'#3A78B0','storm':'#C8592B','spark':'#B8890F','kafkastream':'#2E8A5A','risingwave':'#8A55A8'}
FILL={'flink':'#9EC5E8','storm':'#F6B092','spark':'#F7DD8F','kafkastream':'#A8DDBF','risingwave':'#D8BCE8'}
BOX={'flink':'#EAF2FB','storm':'#FDEDE6','spark':'#FDF6DF','kafkastream':'#E8F6EE','risingwave':'#F4ECF9'}
U=[('u2','2'),('u4','4'),('u8','8'),('u16','16'),('u32','32'),('u40','40')]
C=0.4
fwd=lambda x: np.where(np.asarray(x)>=1, np.log10(np.maximum(x,1e-9)), C*np.log10(np.maximum(x,1e-9)))
inv=lambda y: np.where(np.asarray(y)>=0, 10**np.asarray(y), 10**(np.asarray(y)/C))
OFF={'flink':(4,-3),'spark':(4,2),'storm':(3,4),'kafkastream':(5,1),'risingwave':(4,-7)}
fig,ax=plt.subplots(figsize=(7.16,3.35)); fig.subplots_adjust(left=0.07,right=0.79,top=0.97,bottom=0.13)
ax.set_xscale('function',functions=(fwd,inv)); ax.set_yscale('log'); ax.set_xlim(0.0042,9); ax.set_ylim(0.09,50)
ax.axvspan(0.0042,1,color='0.965',zorder=0,lw=0); ax.text(0.0045,0.095,'compressed scale',fontsize=6,color='0.5',style='italic')
ax.axvline(1,color='0.55',lw=0.6,ls=(0,(4,2)),zorder=1); ax.axhline(1,color='0.55',lw=0.6,ls=(0,(4,2)),zorder=1)
ax.grid(lw=0.3,color='0.88',zorder=0)
for s in SYS:
    xs=[M.loc[(s,c),'thr_rel'] for c,_ in U]; ys=[M.loc[(s,c),'inv_lat_rel'] for c,_ in U]
    ax.plot(xs,ys,'-',color=EDGE[s],lw=1.5,zorder=3)
    ax.plot(xs[1:],ys[1:],'o',ms=4.5,mfc=FILL[s],mec=EDGE[s],mew=0.9,zorder=4)
    if s in OFF and s!='risingwave':
        for (c,l),x,y in zip(U[1:],xs[1:],ys[1:]):
            ax.annotate(l,(x,y),xytext=OFF[s],textcoords='offset points',fontsize=5.8,color=EDGE[s],zorder=7)
ax.plot(1,1,'o',ms=6.5,mfc='white',mec='black',mew=1.3,zorder=6)
ax.annotate('DoP 2',(1,1),xytext=(-24,-11),textcoords='offset points',fontsize=6.3,color='black')
g=lambda s,c,k: M.loc[(s,c),k]
def box(s,title,body,xy,xyt):
    ax.annotate(r'$\mathbf{'+title+r'}$'+body,xy=xy,xytext=xyt,fontsize=7,va='center',ha='left',color='0.1',linespacing=1.1,zorder=8,
                bbox=dict(boxstyle='round,pad=0.45,rounding_size=0.3',fc=BOX[s],ec=EDGE[s],lw=0.9),
                arrowprops=dict(arrowstyle='-|>,head_length=0.4,head_width=0.2',lw=0.9,color=EDGE[s],shrinkA=2,shrinkB=4))
box('kafkastream',r'Kafka\ Streams^{*}:',' output collapses to 0.006×;\nstream threads exceed tasks and cores;\nlower latency because few records arrive.',
    (g('kafkastream','u32','thr_rel'),g('kafkastream','u32','inv_lat_rel')),(0.016,6.5))
box('spark','Spark:',' improves both: up to 1.7× throughput,\n6.5× lower latency; more tasks shorten\nthe heavy micro-batches.',(g('spark','u40','thr_rel'),g('spark','u40','inv_lat_rel')),(1.9,18))
box('risingwave','RisingWave:',' output falls to 0.5–0.7×;\nonly the stateful fragment\nscales, the source does not.',(g('risingwave','u32','thr_rel'),g('risingwave','u32','inv_lat_rel')),(0.012,0.55))
box('storm','Storm:',' up to 6.8× throughput at DoP 16,\nthen falls (3.8× at 40) at 2.3× latency\nas executor threads contend for cores.',(g('storm','u8','thr_rel'),g('storm','u8','inv_lat_rel')),(2.65,0.15))
box('flink','Flink:',' 2.2× throughput already at DoP 4,\nbut 8× higher latency at DoP 40 as\nkeyed all-to-all exchanges grow.',(g('flink','u40','thr_rel'),g('flink','u40','inv_lat_rel')),(0.012,0.17))
ax.set_xticks([0.005,0.01,0.03,0.1,0.3,1,2,4,8]); ax.set_xticklabels(['0.005','0.01','0.03','0.1','0.3','1','2','4','8'])
ax.set_yticks([0.1,0.2,0.5,1,2,5,10,20]); ax.set_yticklabels(['0.1','0.2','0.5','1','2','5','10','20']); ax.minorticks_off()
ax.set_xlabel('throughput (normalized to uniform DoP 2)'); ax.set_ylabel('inverse latency (normalized to uniform DoP 2)')
H=[Line2D([],[],color=EDGE[s],lw=1.5,marker='o',ms=4.5,mfc=FILL[s],mec=EDGE[s],label=NAME[s]) for s in SYS]
l1=fig.legend(handles=H,loc='upper left',bbox_to_anchor=(0.80,0.97),frameon=True,fontsize=7.5,title='System',title_fontsize=7.5,borderpad=0.6)
l1.get_frame().set_edgecolor('0.4'); l1.get_frame().set_linewidth(0.6)
H2=[Line2D([],[],marker='o',color='0.3',ms=6,mfc='white',mec='black',ls='none',label='uniform DoP 2\n(reference)'),
    Line2D([],[],marker='o',color='0.3',ms=4.5,mfc='0.85',lw=1,label='uniform DoP\n4, 8, 16, 32, 40')]
l2=fig.legend(handles=H2,loc='upper left',bbox_to_anchor=(0.80,0.55),frameon=True,fontsize=7.2,title='Configuration',title_fontsize=7.5,borderpad=0.6)
l2.get_frame().set_edgecolor('0.4'); l2.get_frame().set_linewidth(0.6)
for e in ['pdf','svg','png']: fig.savefig(os.path.join(HERE,f'fig_motivation_uniform_boxes_AD_1M.{e}'),dpi=300)
print('ok')
