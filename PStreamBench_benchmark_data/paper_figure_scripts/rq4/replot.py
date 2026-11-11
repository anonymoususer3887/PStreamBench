"""Rebuild of fig_rq4_summary_1M from the plotted data of fig_rq4_summary_1M_v1.svg (values extracted from the vector
coordinates; no new data). Changes: 'one suggested configuration' -> 'converged configuration'; panels (a),(b) named as
speed-ups; panel (d) inverted (best Uniform latency / latency) so that higher is better in every panel."""
import json,os,numpy as np,matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.lines import Line2D
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':7,'axes.linewidth':0.6,'pdf.fonttype':42,'svg.fonttype':'none','xtick.direction':'in','ytick.direction':'in'})
HERE=os.path.dirname(os.path.abspath(__file__))
O=json.load(open(os.path.join(HERE,'extract.json')))
SYS=['Flink','Storm','Spark','Kafka\nStr.','Rising\nWave']; XS=np.array([0,1,2,3.45,4.55])
def clusters(xs):   # sort x positions into 5 systems
    xs=sorted(xs); return xs
def series_dots(A,color):
    d=[q for q in A['dots'] if q['fill']==color]
    xs=sorted(set(round(q['x'],0) for q in d))
    # group by gaps
    groups=[];cur=[]
    for q in sorted(d,key=lambda q:q['x']):
        if cur and q['x']-cur[-1]['x']>6: groups.append(cur); cur=[]
        cur.append(q)
    groups.append(cur); return [[q['v'] for q in g] for g in groups]
def bars(A):
    out={}
    for col in ('#222222','#0b7a55'):
        L=[l for l in A['lines'] if l['stroke']==col and l['lw']=='1.6']
        xs=sorted(set(round(np.mean(l['x']),1) for l in L))
        rng=[]
        for x in xs:
            vs=[v for l in L if round(np.mean(l['x']),1)==x for v in l['v']]; rng.append((min(vs),max(vs)))
        di=sorted([d for d in A['dots'] if d['fill']=='#ffffff' and d['stroke']==col],key=lambda d:d['x'])
        out[col]=(rng,[d['v'] for d in di])
    return out
def fmt(v,_):
    if v>=1000: return f'{v/1000:g}k×'
    if v<0.001: return f'$10^{{{int(round(np.log10(v)))}}}$×'
    return f'{v:g}×'
def box(ax,x,vals,color,w=0.2,alpha=0.35):
    v=np.array(vals); 
    if len(v)==0: return
    q1,med,q3=np.percentile(v,[25,50,75]); iqr=q3-q1
    lo=v[v>=q1-1.5*iqr].min(); hi=v[v<=q3+1.5*iqr].max()
    ax.add_patch(plt.Rectangle((x-w/2,q1),w,q3-q1,facecolor=color,alpha=alpha,edgecolor='none',zorder=2))
    ax.plot([x,x],[lo,q1],color=color,lw=0.7,zorder=2); ax.plot([x,x],[q3,hi],color=color,lw=0.7,zorder=2)
    ax.plot([x-w/2,x+w/2],[med,med],color=color,lw=1.5,zorder=3)
    jit=np.random.default_rng(len(v)+abs(int(x*100))).uniform(-w*0.35,w*0.35,len(v))
    ax.scatter(x+np.array(jit),v,s=5,color=color,alpha=0.85,lw=0,zorder=4)
def frame(ax,title,ylab):
    ax.set_yscale('log'); ax.axvspan(2.8,5.15,color='#f2f2f2',zorder=0)
    ax.set_xlim(-0.6,5.15); ax.set_xticks(XS); ax.set_xticklabels(SYS,fontsize=6.0,linespacing=0.9)
    ax.yaxis.set_major_locator(LogLocator(base=10,numticks=20)); ax.yaxis.set_major_formatter(FuncFormatter(fmt)); ax.yaxis.set_minor_formatter(NullFormatter())
    ax.tick_params(axis='y',labelsize=6.2); ax.grid(axis='y',lw=0.3,color='0.85',ls='--',zorder=0)
    ax.set_title(title,fontsize=7.4,fontweight='bold',pad=11); ax.set_ylabel(ylab,fontsize=6.6,labelpad=1)
    ax.text(1.0,1.012,'per-operator',transform=ax.get_xaxis_transform(),ha='center',va='bottom',fontsize=5.6,style='italic',color='0.35')
    ax.text(4.0,1.012,'job-level',transform=ax.get_xaxis_transform(),ha='center',va='bottom',fontsize=5.6,style='italic',color='0.35')
fig,axs=plt.subplots(1,4,figsize=(7.16,2.75)); fig.subplots_adjust(left=0.055,right=0.995,top=0.655,bottom=0.17,wspace=0.42)
# (a),(b): Uniform grey, Random green  (values unchanged; already >= 1, higher = larger gap = larger speed-up)
for k,(title,ylab) in enumerate([('(a) Throughput speed-up:\nbest vs. worst configuration','throughput speed-up [×]'),
                                 ('(b) Latency speed-up:\nbest vs. worst configuration','latency speed-up [×]')]):
    A=O[str(k)]; ax=axs[k]; frame(ax,title,ylab)
    for col,off in (('#7a7a7a',-0.17),('#4e9c84',0.17)):
        for i,vals in enumerate(series_dots(A,col)): box(ax,XS[i]+off,vals,col,w=0.28)
    ax.set_ylim(0.7,6e4)
# (c) throughput relative to best Uniform (unchanged); (d) inverted latency
for k,inv in ((2,False),(3,True)):
    A=O[str(k)]; ax=axs[k]
    if inv: frame(ax,'(d) Latency speed-up vs.\nthe best Uniform run','latency speed-up [×]')
    else:   frame(ax,'(c) Throughput speed-up vs.\nthe best Uniform run','throughput speed-up [×]')
    f=(lambda v:1/np.array(v)) if inv else (lambda v:np.array(v))
    ax.axhline(1,color='0.4',lw=0.6,ls=':',zorder=1)
    for col,off in (('#6a7db0',-0.12),('#d9714a',0.12)):
        for i,vals in enumerate(series_dots(A,col)): box(ax,XS[i]+off,f(vals),col,w=0.2)
    B=bars(A)
    for col,off in (('#222222',-0.36),('#0b7a55',0.36)):
        rng,dia=B[col]
        for i,((lo,hi),dm) in enumerate(zip(rng,dia)):
            lo2,hi2=sorted(f([lo,hi])); x=XS[i]+off
            ax.plot([x,x],[lo2,hi2],color=col,lw=1.6,zorder=3,solid_capstyle='butt')
            for yy in (lo2,hi2): ax.plot([x-0.07,x+0.07],[yy,yy],color=col,lw=1.0,zorder=3)
            ax.scatter([x],f([dm]),marker='D',s=16,facecolor='white',edgecolor=col,lw=1.1,zorder=5)
    ax.set_ylim(*( (5e-6,3e3) if inv else (5e-4,3e3)))
H1=[Patch(facecolor='#7a7a7a',alpha=0.35,label='Uniform (DoP 2–40)'),Patch(facecolor='#4e9c84',alpha=0.35,label='Random (per-operator DoP)')]
fig.legend(handles=H1,loc='upper left',bbox_to_anchor=(0.05,1.0),ncol=2,frameon=False,fontsize=6.6,title='(a), (b):',title_fontsize=6.4,alignment='left',handlelength=1.6)
H2=[Patch(facecolor='#6a7db0',alpha=0.35,label='DS2 (converged configuration)'),Patch(facecolor='#d9714a',alpha=0.35,label='Dhalion (converged configuration)'),
    Line2D([],[],color='#222222',lw=1.6,marker='D',mfc='white',ms=4,label=r'Uniform, 6 runs: best–worst, $\diamond$ median run'),
    Line2D([],[],color='#0b7a55',lw=1.6,marker='D',mfc='white',ms=4,label=r'Random, 5–10 runs: best–worst, $\diamond$ median run')]
fig.legend(handles=H2,loc='upper left',bbox_to_anchor=(0.47,1.0),ncol=2,frameon=False,fontsize=6.6,title='(c), (d):',title_fontsize=6.4,alignment='left',handlelength=1.6,columnspacing=1.2)
fig.text(0.5,0.008,'m510, 1M events/s, 14 applications; each dot is one application; higher is better in all panels. Speed-up: (a) best ÷ worst throughput, (b) worst ÷ best latency,\n(c) throughput ÷ best Uniform run, (d) best Uniform latency ÷ latency. In (c), (d), 1× = best Uniform run of that application; bars and $\\diamond$ are medians over applications.',
         ha='center',fontsize=5.8,style='italic',color='0.35')
for e in ['pdf','svg','png']: fig.savefig(os.path.join(HERE,'fig_rq4_summary_1M_v3.'+e),dpi=300)
# check medians
A=O['3']; print('Flink DS2/Dh latency medians (orig ratio):',[round(np.median(v),2) for v in [series_dots(A,'#6a7db0')[0],series_dots(A,'#d9714a')[0]]])
