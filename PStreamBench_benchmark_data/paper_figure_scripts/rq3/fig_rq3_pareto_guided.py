"""RQ3 Pareto figure, reader-guided version.
Data: pareto_cells.csv from pareto_options.py (m510, uniform runs, DoP in {2,4,8,16,32,40}, median per DoP,
variant: Ad Analytics with_join, otherwise variant with most uniform runs).
Design: x = E2E latency on a reversed log axis (low latency on the right), y = throughput (log), so the upper-right
corner is better on both metrics; a faint background gradient marks the better (green) and worse (red) corner.
Pareto front: filled markers joined by a line, labelled with the DoP. Dominated DoPs: small grey crosses."""
import os, numpy as np, pandas as pd, matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.ticker import LogLocator, FuncFormatter, NullFormatter
HERE=os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':6,'axes.linewidth':0.5,'pdf.fonttype':42,'svg.fonttype':'none','xtick.direction':'in','ytick.direction':'in'})
M=pd.read_csv(os.path.join(HERE,'pareto_cells.csv'))
SYS=['flink','storm','spark','kafkastream','risingwave']; SN=['Flink','Storm','Spark','Kafka Streams','RisingWave']
APPS=[('Ad Analytics','AD\n(State+Net.)'),('Click Analytics','CA\n(State)'),('Smart Grid','SG\n(State+UDF)'),('Word Count','WC\n(Stateless)')]
RATES=[1e5,5e5,1e6]; RC={1e5:'#2E9C7A',5e5:'#E09A1F',1e6:'#C8413F'}; RL={1e5:'LOW (100k ev/s)',5e5:'MED (500k ev/s)',1e6:'HIGH (1M ev/s)'}
cmap=LinearSegmentedColormap.from_list('z',['#F6D5D3','#FFFFFF','#D5EEDB'])
g=np.add.outer(np.linspace(0,1,60),np.linspace(0,1,60))/2   # 0 bottom-left .. 1 top-right
fig,axs=plt.subplots(4,5,figsize=(7.16,4.6))
fig.subplots_adjust(left=0.085,right=0.995,top=0.90,bottom=0.075,wspace=0.28,hspace=0.30)
for i,(app,lab) in enumerate(APPS):
    for j,s in enumerate(SYS):
        ax=axs[i,j]; d=M[(M.application==app)&(M.system==s)]
        ax.set_xscale('log'); ax.set_yscale('log')
        if d.empty: ax.text(0.5,0.5,'no data',transform=ax.transAxes,ha='center',color='0.5'); continue
        lat=d.lat/1e3; thr=d.thr
        x0,x1=lat.min()/1.6,lat.max()*1.6; y0,y1=thr.min()/1.6,thr.max()*1.6
        ax.set_xlim(x1,x0); ax.set_ylim(y0,y1)          # reversed latency axis: low latency on the right
        ax.imshow(g,extent=(0,1,0,1),transform=ax.transAxes,cmap=cmap,aspect='auto',zorder=0,alpha=0.9,origin='lower')
        for r in RATES:
            h=d[d.producer_rate==r]
            dm=h[~h.eff]; ax.scatter(dm.lat/1e3,dm.thr,marker='x',s=9,color='0.55',lw=0.6,zorder=2)
            e=h[h.eff].sort_values('lat')
            ax.plot(e.lat/1e3,e.thr,'-o',color=RC[r],ms=2.8,lw=0.9,zorder=3,mec='white',mew=0.3)
            for _,p in e.iterrows():
                ax.annotate(str(int(p.dop)),(p.lat/1e3,p.thr),xytext=(1.5,1.8),textcoords='offset points',fontsize=4.6,color=RC[r],fontweight='bold',zorder=4)
        def f(v,_):
            if v>=1e6: return f'{v/1e6:g}M'
            if v>=1e3: return f'{v/1e3:g}k'
            return f'{v:g}'
        for a_ in (ax.xaxis,ax.yaxis):
            a_.set_major_locator(LogLocator(base=10,subs=(1,2,5))); a_.set_major_formatter(FuncFormatter(f)); a_.set_minor_formatter(NullFormatter())
        ax.tick_params(labelsize=4.6,length=1.8,pad=1,which='both')
        ax.grid(alpha=0.25,lw=0.3,which='major')
        if i==0: ax.set_title(SN[j],fontsize=7,fontweight='bold',pad=3)
        if j==0: ax.set_ylabel(lab,fontsize=6,labelpad=2)
        ax.annotate('',xy=(0.985,0.975),xytext=(0.87,0.84),xycoords='axes fraction',textcoords='axes fraction',fontsize=4.3,color='#1B7F3B',
                    fontweight='bold',ha='center',va='top',arrowprops=dict(arrowstyle='->',color='#1B7F3B',lw=0.6,shrinkA=0,shrinkB=0),zorder=1.5)
fig.text(0.54,0.012,r'E2E latency [s] (log scale, reversed: lower latency to the right $\rightarrow$)',ha='center',fontsize=6.5)
fig.text(0.008,0.49,r'Throughput [events/s] (log scale, higher is better $\uparrow$)',rotation=90,va='center',fontsize=6.5)
H=[Line2D([],[],color=RC[r],marker='o',ms=3,lw=0.9,label=RL[r]) for r in RATES]
H+=[Line2D([],[],color='0.55',marker='x',ls='none',ms=3.5,label='dominated DoP'),
    Line2D([],[],color='0.3',marker='o',ls='-',ms=3,lw=0.9,label='Pareto front (numbers = DoP)')]
fig.legend(handles=H,loc='upper center',ncol=5,frameon=False,fontsize=5.8,bbox_to_anchor=(0.54,1.0),handlelength=1.6,columnspacing=1.2)
fig.text(0.54,0.935,r'Green arrow ($\nearrow$) in each panel points to better: higher throughput and lower latency (green corner); lower left is worse on both (red corner).',ha='center',fontsize=5.2,style='italic',color='0.3')
for e in ['pdf','svg','png']: fig.savefig(os.path.join(HERE,'fig_rq3_pareto_guided.'+e),dpi=300)
