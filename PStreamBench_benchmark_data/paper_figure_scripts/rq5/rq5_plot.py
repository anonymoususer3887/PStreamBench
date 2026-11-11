"""RQ5 figures from rq5_config_values.csv / rq5_summary_per_rate.csv / rq5_feasibility.csv (run rq5_metrics.py first).
Fig 1: capacity (consumed / offered, log y) vs DoP, one panel per system, one line per shape, at one rate.
       filled marker = output > 0; hollow = consumes input but no sink output; 'x' at the bottom = no input consumed.
Fig 2: (a) share of the 18 configurations that consume input (filled) / produce output (hollow);
       (b) plateau DoP (>= 0.9 x max) at the rate; (c) latency DoP 40 / DoP 2; (d) min latency / TwoWayJoin's min.
Nothing is dropped; caveated series are drawn with a dotted line and named in the caption notes."""
import os, sys, numpy as np, pandas as pd, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
HERE = os.path.dirname(os.path.abspath(__file__))
plt.rcParams.update({'font.size':7,'axes.labelsize':7,'xtick.labelsize':6.5,'ytick.labelsize':6.5,'legend.fontsize':6.5,
                     'font.family':'serif','axes.linewidth':0.6,'pdf.fonttype':42,'svg.fonttype':'none'})
C = pd.read_csv(os.path.join(HERE,'rq5_config_values.csv')); T = pd.read_csv(os.path.join(HERE,'rq5_summary_per_rate.csv'))
F = pd.read_csv(os.path.join(HERE,'rq5_feasibility.csv'))
SYS = ['flink','storm','spark','kafkastream','risingwave']
NAME = {'flink':'Flink','storm':'Storm','spark':'Spark','kafkastream':'Kafka Streams','risingwave':'RisingWave'}
SCOL = {'flink':'#0072B2','storm':'#D55E00','spark':'#E69F00','kafkastream':'#009E73','risingwave':'#CC79A7'}
SMRK = {'flink':'o','storm':'s','spark':'^','kafkastream':'D','risingwave':'v'}
SHAPES = ['TwoFilters','ThreeFilters','FourFilters','LinearQuery','TwoWayJoin','ThreeWayJoin','FourWayJoin','FiveWayJoin','SixWayJoin']
LAB = {'TwoFilters':'2F','ThreeFilters':'3F','FourFilters':'4F','LinearQuery':'Lin','TwoWayJoin':'2J','ThreeWayJoin':'3J',
       'FourWayJoin':'4J','FiveWayJoin':'5J','SixWayJoin':'6J'}
COL = dict(zip(SHAPES, ['#fdae6b','#f16913','#a63603','#7f2704','#c6dbef','#9ecae1','#4292c6','#2171b5','#08306b']))
LS = {s:'-' for s in SHAPES}; LS['LinearQuery'] = '--'
DOPS = [2,4,8,16,32,40]
# caveats: (system, shape) drawn dotted
CAVEAT = {('storm',s) for s in ['TwoFilters','ThreeFilters','FourFilters']}   # first bolt is the window; its metric counts emitted aggregates
FLOOR = 3e-3

def fig1(rate):
    fig, axs = plt.subplots(1, 5, figsize=(7.16, 2.05), sharey=True, gridspec_kw=dict(wspace=0.08, left=0.065, right=0.995, top=0.80, bottom=0.17))
    for ax, s in zip(axs, SYS):
        for a in SHAPES:
            g = C[(C.system==s)&(C.application_slug==a)&(C.producer_rate==rate)].sort_values('dop')
            if g.empty: continue
            ok = g[g.runs]; ls = ':' if (s,a) in CAVEAT else LS[a]
            ax.plot(ok.dop, ok.capacity.clip(lower=FLOOR), ls=ls, color=COL[a], lw=1.0, zorder=2)
            o1 = ok[ok.has_out]; o0 = ok[~ok.has_out]; nr = g[~g.runs]
            ax.scatter(o1.dop, o1.capacity.clip(lower=FLOOR), s=9, color=COL[a], zorder=3, lw=0)
            ax.scatter(o0.dop, o0.capacity.clip(lower=FLOOR), s=11, facecolor='white', edgecolor=COL[a], lw=0.8, zorder=3)
            ax.scatter(nr.dop, np.full(len(nr), FLOOR*1.15), s=12, marker='x', color=COL[a], lw=0.8, zorder=3)
        ax.axhline(1, color='0.5', lw=0.5, ls='-', zorder=1)
        ax.set_xscale('log', base=2); ax.set_xticks(DOPS); ax.set_xticklabels(['2','4','8','16','','40']); ax.minorticks_off()
        ax.set_yscale('log'); ax.set_ylim(FLOOR, 2.5); ax.set_title(NAME[s], fontsize=7, pad=2)
        ax.grid(True, which='major', lw=0.3, color='0.88'); ax.tick_params(length=2, pad=1.5)
    axs[0].set_ylabel('consumed / offered input')
    fig.supxlabel('degree of parallelism', fontsize=7, y=0.015)
    h = [Line2D([], [], color=COL[a], ls=LS[a], lw=1.2, label=LAB[a]) for a in SHAPES]
    h += [Line2D([], [], color='0.3', marker='o', ls='', ms=3, label='output > 0'),
          Line2D([], [], color='0.3', marker='o', mfc='white', ls='', ms=3.3, label='no output'),
          Line2D([], [], color='0.3', marker='x', ls='', ms=3.3, label='no input')]
    fig.legend(handles=h, loc='upper center', ncol=12, frameon=False, bbox_to_anchor=(0.53, 1.0), handlelength=1.6, columnspacing=0.9)
    k = f'{int(rate/1000)}k'
    for ext in ['pdf','svg','png']: fig.savefig(os.path.join(HERE, f'fig_rq5_capacity_{k}.{ext}'), dpi=220)
    plt.close(fig)

def fig2(rate):
    fig, axs = plt.subplots(1, 4, figsize=(7.16, 2.05), gridspec_kw=dict(wspace=0.32, left=0.06, right=0.995, top=0.80, bottom=0.17))
    x = np.arange(len(SHAPES)); off = dict(zip(SYS, np.linspace(-0.28, 0.28, 5)))
    t = T[T.rate==rate].set_index(['system','shape'])
    for s in SYS:
        f = F[F.system==s].set_index('application_slug')
        xs = x + off[s]
        sh_r = [f.runs.get(a,np.nan)/f.n_cfg.get(a,np.nan) for a in SHAPES]
        sh_o = [f.output.get(a,np.nan)/f.n_cfg.get(a,np.nan) for a in SHAPES]
        axs[0].scatter(xs, sh_r, s=10, marker=SMRK[s], color=SCOL[s], lw=0, zorder=3)
        axs[0].scatter(xs, sh_o, s=12, marker=SMRK[s], facecolor='white', edgecolor=SCOL[s], lw=0.7, zorder=3)
        for i,a in enumerate(SHAPES):
            cav = (s,a) in CAVEAT
            for j,col in [(1,'plateau_dop'),(2,'lat_ratio'),(3,'lat_floor_rel')]:
                v = t[col].get((s,a), np.nan) if (s,a) in t.index else np.nan
                if v==v and v>0:
                    axs[j].scatter(xs[i], v, s=11, marker=SMRK[s], lw=0.7, zorder=3,
                                   facecolor='white' if cav else SCOL[s], edgecolor=SCOL[s])
    titles = ['(a) configurations that run', f'(b) plateau DoP ({int(rate/1000)}k)', f'(c) latency DoP 40 / DoP 2 ({int(rate/1000)}k)', f'(d) min latency / 2J ({int(rate/1000)}k)']
    for j, ax in enumerate(axs):
        ax.set_xticks(x); ax.set_xticklabels([LAB[a] for a in SHAPES]); ax.tick_params(length=2, pad=1.5)
        ax.axvline(3.5, color='0.6', lw=0.5); ax.axvline(5.5, color='0.6', lw=0.5, ls='--')
        ax.set_title(titles[j], fontsize=7, pad=2); ax.grid(True, axis='y', lw=0.3, color='0.88')
    axs[0].set_ylim(-0.05, 1.08)
    axs[1].set_yscale('log', base=2); axs[1].set_yticks(DOPS); axs[1].set_yticklabels(DOPS); axs[1].set_ylim(1.6, 52); axs[1].minorticks_off()
    for j in (2,3): axs[j].set_yscale('log'); axs[j].axhline(1, color='0.5', lw=0.5)
    h = [Line2D([], [], color=SCOL[s], marker=SMRK[s], ls='', ms=3.5, label=NAME[s]) for s in SYS]
    h += [Line2D([], [], color='0.3', marker='o', ls='', ms=3.3, label='(a) consumes input'),
          Line2D([], [], color='0.3', marker='o', mfc='white', ls='', ms=3.3, label='hollow: (a) has output; (b–d) metric caveat')]
    fig.legend(handles=h, loc='upper center', ncol=7, frameon=False, bbox_to_anchor=(0.53, 1.0), columnspacing=1.0, handletextpad=0.3)
    k = f'{int(rate/1000)}k'
    for ext in ['pdf','svg','png']: fig.savefig(os.path.join(HERE, f'fig_rq5_summary_{k}.{ext}'), dpi=220)
    plt.close(fig)

for r in [500000.0, 1000000.0]:
    fig1(r); fig2(r)
print('ok')
