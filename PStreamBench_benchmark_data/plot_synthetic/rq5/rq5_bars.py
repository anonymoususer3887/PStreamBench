"""Bar plots per event rate: sink throughput, end-to-end latency, CPU (cpu_50) for all five systems, per synthetic shape.
Input: rq5_config_values.csv (rq5_metrics.py). Same folders as RQ5 (Flink/KS/RW uniform, Spark uniform_e2e_rerun,
Storm distributed_fixed_exporter_fix); one value per system x shape x rate x DoP = median over runs.
throughput = sink output (Flink: operator before the sink x its DoP; Spark: noop sink; others: sink operator).
latency = endtoend_latency_ms p50 in seconds; its definition differs per system (see datasets_v2 README).
'x' = no value (no output / no latency). Style = RQ1 v3: colour = shape, shade + hatch = system."""
import os, numpy as np, pandas as pd, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.colors import to_rgb, to_hex
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter, MultipleLocator
from matplotlib.patches import Patch
HERE = os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({'font.family':'serif','font.serif':['Times New Roman','Times','STIXGeneral','DejaVu Serif'],'mathtext.fontset':'stix',
 'font.size':7,'axes.labelsize':7,'axes.titlesize':7,'xtick.labelsize':6,'ytick.labelsize':6,'axes.linewidth':0.6,'hatch.linewidth':0.5,
 'pdf.fonttype':42,'svg.fonttype':'none','xtick.direction':'in','ytick.direction':'in','xtick.major.size':2,'ytick.major.size':2})
C = pd.read_csv(os.path.join(HERE, 'rq5_config_values.csv'))
def darken(c,f=0.6): r,g,b=to_rgb(c); return to_hex((r*f,g*f,b*f))
def lighten(c,f=0.5): r,g,b=to_rgb(c); return to_hex((r+(1-r)*f,g+(1-g)*f,b+(1-b)*f))
SHADE={'flink':lambda c: darken(c,0.62),'storm':lambda c: c,'spark':lambda c: lighten(c,0.62),'kafkastream':lambda c: darken(c,0.62),'risingwave':lambda c: lighten(c,0.55)}
HATCH={'flink':None,'storm':'///','spark':'..','kafkastream':'xx','risingwave':'\\\\\\'}
SYS=['flink','storm','spark','kafkastream','risingwave']; NAME={'flink':'Flink','storm':'Storm','spark':'Spark','kafkastream':'Kafka Streams','risingwave':'RisingWave'}
ROWS=[('Pipeline depth',['TwoFilters','ThreeFilters','FourFilters','LinearQuery']),('Join width',['TwoWayJoin','ThreeWayJoin','FourWayJoin','FiveWayJoin','SixWayJoin'])]
TITLE={'TwoFilters':'Two filters','ThreeFilters':'Three filters','FourFilters':'Four filters','LinearQuery':'Linear (filter–window–filter)',
       'TwoWayJoin':'2-way join','ThreeWayJoin':'3-way join','FourWayJoin':'4-way join','FiveWayJoin':'5-way join','SixWayJoin':'6-way join'}
COL={'TwoFilters':'#1b9e77','ThreeFilters':'#66a61e','FourFilters':'#a6761d','LinearQuery':'#e6ab02','TwoWayJoin':'#7570b3',
     'ThreeWayJoin':'#e7298a','FourWayJoin':'#d95f02','FiveWayJoin':'#1f78b4','SixWayJoin':'#525252'}
L=[2,4,8,16,32,40]; X=np.arange(len(L)); bw=0.8/len(SYS)
def kfmt(v,p): return f'{v/1e6:g}M' if v>=1e6 else (f'{v/1e3:g}k' if v>=1e3 else f'{v:g}')
METRICS=[('thr','Throughput\n[events/s]','log',(1,1e7)),('lat','E2E latency [s]','log',(0.05,1000)),('cpu','CPU [%]','lin',(0,100))]
def draw(ax, shape, rate, m, base):
    g = C[(C.application_slug==shape)&(C.producer_rate==rate)]
    for i,k in enumerate(SYS):
        r = g[g.system==k].set_index('dop'); xs = X+(i-(len(SYS)-1)/2)*bw
        col = {'thr':'out','lat':'lat','cpu':'cpu_50'}[m]
        v = np.array([r[col].get(l,np.nan) if l in r.index else np.nan for l in L], float)
        if m=='lat': v = v/1000
        if m!='cpu': v[v<=0] = np.nan
        fc = SHADE[k](base)
        ax.bar(xs, v, bw, color=fc, edgecolor='black', lw=0.35, zorder=2)
        if HATCH[k]:
            ax.bar(xs, v, bw, color='none', edgecolor=('white' if k in ('storm','kafkastream') else darken(base,0.55)), lw=0, hatch=HATCH[k], zorder=2.1)
            ax.bar(xs, v, bw, color='none', edgecolor='black', lw=0.35, zorder=2.2)
        lo = METRICS[[x[0] for x in METRICS].index(m)][3][0]
        for x,val in zip(xs,v):
            if np.isnan(val): ax.text(x, lo*1.15 if m!='cpu' else 1, '×', ha='center', va='bottom', fontsize=4.5, color='0.35')
def figure(rate):
    fig = plt.figure(figsize=(7.16, 6.4))
    outer = fig.add_gridspec(2, 1, hspace=0.22, left=0.075, right=0.995, top=0.955, bottom=0.06)
    for ri,(rname,shapes) in enumerate(ROWS):
        sg = outer[ri].subgridspec(3, 5, hspace=0.08, wspace=0.07, height_ratios=[1.2,1.2,0.8])
        for ci,shape in enumerate(shapes):
            base = COL[shape]; first=None
            for mi,(m,lab,sc,lim) in enumerate(METRICS):
                ax = fig.add_subplot(sg[mi,ci], sharex=first); first = first or ax
                draw(ax, shape, rate, m, base)
                if sc=='log':
                    ax.set_yscale('log'); ax.yaxis.set_major_locator(LogLocator(10, numticks=8)); ax.yaxis.set_minor_formatter(NullFormatter())
                    ax.yaxis.set_major_formatter(FuncFormatter(kfmt if m=='thr' else (lambda v,p: f'{v:g}')))
                else: ax.yaxis.set_major_locator(MultipleLocator(50)); ax.yaxis.set_minor_locator(MultipleLocator(25))
                ax.set_ylim(*lim); ax.set_xlim(-0.5,len(L)-0.5); ax.grid(axis='y', ls=(0,(2,2)), lw=0.35, color='0.8', zorder=0)
                ax.set_xticks(X); ax.set_xticklabels([str(l) for l in L])
                if mi<2: ax.tick_params(axis='x', labelbottom=False)
                if ci==0: ax.set_ylabel(lab, fontsize=6.5, labelpad=2)
                else: ax.set_yticklabels([])
                if mi==0: ax.set_title(TITLE[shape], fontsize=7, fontweight='bold', color=darken(base,0.8), pad=2)
        fig.text(0.006, (outer[ri].get_position(fig).y0+outer[ri].get_position(fig).y1)/2, rname, rotation=90, va='center', fontsize=7.5, style='italic')
    # legend in the free slot of the first row
    lax = fig.add_subplot(outer[0].subgridspec(3,5, height_ratios=[1.2,1.2,0.8])[:,4]); lax.axis('off')
    GREY='#9e9e9e'
    H=[Patch(facecolor=SHADE[k](GREY), edgecolor=('white' if k in ('storm','kafkastream') else darken(GREY,0.55)), hatch=HATCH[k], lw=0.6) for k in SYS]
    lax.legend(H, [NAME[k] for k in SYS], loc='center', frameon=False, fontsize=7, handlelength=2.6, handleheight=1.3,
               title=f'Event rate {int(rate/1000)}k events/s\n\nshade + pattern = system\ncolour = query shape\n× = no value', title_fontsize=6.5)
    fig.text(0.535, 0.012, 'Degree of parallelism', ha='center', fontsize=7)
    k = f'{int(rate/1000)}k'
    for ext in ['pdf','svg','png']: fig.savefig(os.path.join(HERE, f'fig_syn_bars_{k}.{ext}'), dpi=220)
    plt.close(fig)
for r in [100000.0, 500000.0, 1000000.0]: figure(r)
print('ok')
