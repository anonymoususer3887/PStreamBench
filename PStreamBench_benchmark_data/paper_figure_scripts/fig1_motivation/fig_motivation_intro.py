"""Intro figure (Ad Analytics with_join, m510, 1M events/s): DoP 2, DoP 40, DS2, Dhalion per system, column width.
Other systems: datasets_v2 throughput_50 / endtoend latency p50. Storm: throughput_avg (run mean) for all four bars,
DS2/Dhalion from rerun2. Kafka Streams DS2: NO valid 1M run -> value of the 100k run (199127 ev/s, 3.23 s), drawn hollow and marked with a dagger."""
import os
HERE=os.path.dirname(os.path.abspath(__file__))
import numpy as np, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.patches import Patch
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':6,'axes.linewidth':0.5,'pdf.fonttype':42,'svg.fonttype':'none','xtick.direction':'in','ytick.direction':'in','hatch.linewidth':0.4})
SYS=['Flink','Storm','Spark','Kafka\nStreams','Rising-\nWave']
D={'Flink':[(210273,3.48),(464557,27.90),(463232,620.28),(384204,92.05)],
   'Storm':[(56060,0.449),(121575,1.045),(107338,0.878),(13729,0.458)],
   'Spark':[(1263717,23.79),(1853144,3.65),(145975,4.18),(612287,19.95)],
   'Kafka\nStreams':[(5985,17.23),(1151,2.78),(199127,3.23),(403388,157.69)],
   'Rising-\nWave':[(17732,1.37),(9575,1.37),(21046,1.69),(20554,1.69)]}
SUBST=set()   # KS DS2 value taken from the 100k run; disclosed in the caption only
CFG=['DoP 2','DoP 40','DS2','Dhalion']
COL=['#9EC5E8','#F6C08A','#B8B8B8','#D8BCE8']; HAT=[None,None,'////','\\\\\\\\']; ECOL=['#3A78B0','#C8592B','#555555','#8A55A8']
UP,DN='#1B7F3B','#B2182B'
fig,axs=plt.subplots(1,2,figsize=(3.5,1.75)); fig.subplots_adjust(left=0.105,right=0.895,top=0.80,bottom=0.15,wspace=0.04)
x=np.arange(len(SYS)); bw=0.2
for ax,(k,lab,better_high,lim) in zip(axs,[(0,'Throughput [events/s]',True,(500,6e7)),(1,'E2E latency [s]',False,(0.2,2e4))]):
    ax.set_yscale('log'); ax.set_ylim(*lim)
    for i,s in enumerate(SYS):
        base=D[s][0][k]
        for j in range(4):
            xx=x[i]+(j-1.5)*bw; v=D[s][j]
            if v is None:
                ax.text(xx,lim[0]*1.2,'×',ha='center',va='bottom',fontsize=5,color='0.35'); continue
            sub=(s,j) in SUBST
            ax.bar(xx,v[k],bw,color=COL[j],edgecolor=ECOL[j],lw=0.45,hatch=HAT[j],zorder=2,bottom=lim[0])
            if sub:
                ax.text(xx,v[k]*1.15,'†',ha='center',va='bottom',fontsize=5.5,color='0.25',zorder=3); continue
            if j>0:
                r=v[k]/base; good=(r>1)==better_high
                if abs(np.log(r))<np.log(1.15): t,c='≈1×','0.45'
                else: t,c=(r'$\uparrow$'+f'{r:.1f}×' if r>1 else r'$\downarrow$'+f'{1/r:.1f}×'),(UP if good else DN)
                ax.text(xx,v[k]*1.15,t,rotation=90,ha='center',va='bottom',fontsize=3.9,color=c,fontweight='bold',zorder=3)
    ax.set_xticks(x); ax.set_xticklabels(SYS,fontsize=5.2,linespacing=0.9); ax.tick_params(axis='x',length=0,pad=1.5)
    ax.tick_params(axis='y',labelsize=5,length=2,pad=1); ax.grid(axis='y',lw=0.25,color='0.88',zorder=0)
    for b in x[:-1]+0.5: ax.axvline(b,color='0.9',lw=0.4,zorder=0)
    ax.set_ylabel(lab,fontsize=5.6,labelpad=1)
    if k==1: ax.yaxis.tick_right(); ax.yaxis.set_label_position('right')
    ax.text(0.02 if k==0 else 0.98,0.96,'(a)' if k==0 else '(b)',transform=ax.transAxes,ha='left' if k==0 else 'right',va='top',fontsize=6,fontweight='bold')
H=[Patch(facecolor=COL[j],edgecolor=ECOL[j],hatch=HAT[j],lw=0.45,label=CFG[j]) for j in range(4)]
fig.legend(handles=H,loc='upper center',ncol=4,frameon=False,bbox_to_anchor=(0.5,1.01),fontsize=5.6,handlelength=1.3,columnspacing=0.9,handletextpad=0.4)
fig.text(0.5,0.865,'Labels: change vs. DoP 2 (green better, red worse)',ha='center',fontsize=4.6,style='italic',color='0.3')
for e in ['pdf','svg','png']: fig.savefig(os.path.join(HERE,f'fig_motivation_intro_1M.{e}'),dpi=300)
