"""Figures: DoP vs throughput / end-to-end latency / CPU, per system and input rate, one column per strategy.
Input: configs.csv from extract_synthetic.py (medians over repeated runs). Nothing dropped or thresholded:
zero throughput is drawn as an 'x' on the bottom axis (log scale cannot show 0); missing values are absent
and counted in the panel corner."""
import numpy as np, pandas as pd, matplotlib
matplotlib.use('Agg'); import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
plt.rcParams.update({'font.family':'serif','font.size':8,'axes.linewidth':0.6,'pdf.fonttype':42})
C = pd.read_csv('configs.csv')
SH = ['LinearQuery','TwoFilters','ThreeFilters','FourFilters','TwoWayJoin','ThreeWayJoin','FourWayJoin','FiveWayJoin','SixWayJoin']
blues = plt.get_cmap('Blues')(np.linspace(0.45,0.95,4)); oranges = plt.get_cmap('Oranges')(np.linspace(0.4,0.95,5))
COL = dict(zip(SH, list(blues)+list(oranges)))
ST = ['uniform','random','ds2','dhalion']; STN = {'uniform':'Uniform','random':'Random','ds2':'DS2','dhalion':'Dhalion'}
SYS = {'flink':'Flink','storm':'Storm','spark':'Spark','kafkastreams':'Kafka Streams'}
MET = [('throughput','throughput (sink output) [rows/s]'),('latency_ms','end-to-end latency [ms]'),('cpu_50','CPU p50 [%]  (dashed/hollow: p95)')]
for sysk, sysn in SYS.items():
    for rate in [100000, 500000, 1000000]:
        D = C[(C.system==sysk)&(C.rate==rate)]
        fig, axs = plt.subplots(3, 4, figsize=(11, 7.2), sharex=True, sharey='row')
        for j, st in enumerate(ST):
            S = D[D.strategy==st]
            for i, (m, lab) in enumerate(MET):
                ax = axs[i, j]
                if m=='latency_ms' and sysk=='storm':
                    ax.text(0.5,0.5,'not collected\nfor Storm',ha='center',va='center',transform=ax.transAxes,color='0.4'); 
                nmiss = 0; nzero = 0; ninv_tot = [0]
                for sh in SH:
                    s = S[S['shape']==sh].sort_values('dop')
                    if s.empty: continue
                    y = s[m].values; x = s.dop.values
                    ok = np.isfinite(y) & (y>0) if m!='cpu_50' else np.isfinite(y)
                    nmiss += int(np.sum(~np.isfinite(y))) if not (m=='latency_ms' and sysk=='storm') else 0
                    z = np.isfinite(y) & (y==0) & (m!='cpu_50')
                    nzero += int(z.sum())
                    if st=='uniform':
                        ax.plot(x[ok], y[ok], '-o', ms=2.8, lw=1, color=COL[sh])
                        if m=='cpu_50': ax.plot(s.dop, s.cpu_95, '--', lw=0.8, color=COL[sh], alpha=0.7)
                    else:
                        ax.scatter(x[ok], y[ok], s=12, color=COL[sh], edgecolor='none')
                        if m=='cpu_50': ax.scatter(s.dop, s.cpu_95, s=12, facecolor='none', edgecolor=COL[sh], lw=0.6)
                    if m=='throughput':
                        inv = ~(np.isfinite(y) & (y>0)) & np.isfinite(s.throughput_95.values) & (s.throughput_95.values>0)
                        if inv.any():
                            ax.scatter(x[inv], s.throughput_95.values[inv], s=22, marker='^', facecolor='none', edgecolor=COL[sh], lw=0.8)
                            ninv_tot[0] += int(inv.sum())
                    if z.any():
                        ax.scatter(x[z], np.full(z.sum(), 0.02), s=18, marker='x', color=COL[sh], transform=ax.get_xaxis_transform(), clip_on=False)
                note = []
                if nzero: note.append(f'{nzero}× zero (x)')
                if nmiss: note.append(f'{nmiss} missing')
                if ninv_tot[0]: note.append(f'{ninv_tot[0]}× p95 (△)')
                if note: ax.text(0.98,0.90,', '.join(note),ha='right',va='top',transform=ax.transAxes,fontsize=6.5,color='0.3',bbox=dict(fc='white',ec='none',alpha=0.85,pad=1))
                if m in ('throughput','latency_ms'): ax.set_yscale('log')
                if j==0: ax.set_ylabel(lab)
                if i==0: ax.set_title(STN[st], fontweight='bold')
                if i==2: ax.set_xlabel('operator DoP' + (' (stream threads)' if sysk=='kafkastreams' else ''))
                ax.grid(alpha=0.25, lw=0.4)
        axs[2,0].set_xticks([2,8,16,24,32,40])
        h = [Line2D([],[],color=COL[s],marker='o',ms=3,lw=1,label=s) for s in SH]
        h.append(Line2D([],[],color='0.3',marker='x',ls='none',label='p50 = 0 (drawn on axis)'))
        h.append(Line2D([],[],color='0.3',marker='^',mfc='none',ls='none',label='p95 where p50 = 0/missing'))
        fig.legend(handles=h, loc='lower center', ncol=6, frameon=False, fontsize=7, bbox_to_anchor=(0.5,-0.005))
        fig.suptitle(f'{sysn}: synthetic shapes at {rate//1000}k events/s  (lines/points = median per configuration)', fontsize=10)
        fig.subplots_adjust(left=0.07, right=0.99, top=0.92, bottom=0.14, hspace=0.12, wspace=0.06)
        for ext in ('png','pdf'): fig.savefig(f'syn_{sysk}_{rate//1000}k.{ext}', dpi=170)
        plt.close(fig)
print('done')
