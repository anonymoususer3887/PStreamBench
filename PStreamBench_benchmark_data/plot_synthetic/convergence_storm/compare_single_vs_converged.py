"""Storm on m510: DS2/Dhalion as used in the paper (one controller decision, then a fixed run) vs. the converged
configuration from the convergence experiments (controller loop, ~5 min, then final measurement).
Single change: newest run per cell in artifacts/<app>/m510/storm/[with_join/]{ds2,dhalion}/<rate>/ (largest timestamp in
the file name, as in the paper; Ad Analytics = with_join). Converged: conv_<app>_final.csv written by conv_plot.py /
wc_convergence_plot.py. Metrics for both: sink throughput = mean of the one-minute sink samples (= throughput_avg),
spout output = sum over spouts of the mean spout rate, latency = endtoend_latency_storm. Values of 0 are shown as 0.0
and labelled, never dropped. Output: fig_single_vs_converged.pdf/svg, single_vs_converged_values.csv"""
import os, re, glob, json, numpy as np, pandas as pd, matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt
HERE = os.path.dirname(os.path.abspath(__file__)); ART = os.path.join(HERE, '..', '..', 'artifacts')
plt.rcParams.update({'font.family': 'serif', 'font.serif': ['Times New Roman', 'Times', 'STIXGeneral', 'DejaVu Serif'], 'mathtext.fontset': 'stix',
                     'font.size': 7, 'axes.linewidth': 0.5, 'pdf.fonttype': 42, 'svg.fonttype': 'none'})
APPS = [('Word Count', 'word-count', ''), ('Ad Analytics', 'ad-analytics', 'with_join/')]
CELLS = [('DS2', 'ds2', '100k', 100000), ('DS2', 'ds2', '1M', 1000000), ('Dhalion', 'dhalion', '100k', 100000), ('Dhalion', 'dhalion', '1M', 1000000)]
def fmt(v):
    if v is None or np.isnan(v): return 'n/a'
    a = abs(v); return '0.0' if a == 0 else (f'{v:.1f}' if a >= 10 else (f'{v:.2f}' if a >= 1 else f'{v:.2g}'))
rows = []
for app, folder, sub in APPS:
    conv = pd.read_csv(os.path.join(HERE, f"conv_{app.replace(' ', '_')}_final.csv"))
    for sl, s, rl, r in CELLS:
        fs = sorted(glob.glob(f'{ART}/{folder}/m510/storm/{sub}{s}/{r}/evaluationMetrics*.json'), key=lambda f: int(re.search(r'-(\d{10})', os.path.basename(f)).group(1)))
        f = fs[-1]; q = json.load(open(f))['queryMetrics']; m = json.load(open(f.replace('evaluationMetrics', 'metaInfo')))
        sp = sum(np.mean([float(v) for _, v in q[k][0]['values']]) for k in q if k.startswith('recPerSecOutSource_') and isinstance(q[k], list))
        sink = [float(v) for _, v in q['throughput'][0]['values']]
        rows.append(dict(app=app, strategy=sl, rate=rl, mode='single change', config=m['job_parallelization'], run=os.path.basename(f),
                         spout_k=sp / 1e3, sink_k=np.mean(sink) / 1e3, sink_p50_k=np.median(sink) / 1e3, latency_s=float(q['endtoend_latency_storm']) / 1e3,
                         window=f"{m['job_window_size']}/{m['job_window_slide_size']}", note='run produced no output' if max(sink) == 0 and sp == 0 else ''))
        c = conv[(conv.strategy == sl) & (conv.rate == rl)]
        if c.empty or not str(c.iloc[0].final_measurement if 'final_measurement' in c else 'evaluationMetrics').startswith('evaluationMetrics'):
            rows.append(dict(app=app, strategy=sl, rate=rl, mode='converged', config=c.iloc[0].final_assignment if not c.empty else '', spout_k=np.nan, sink_k=np.nan,
                             sink_p50_k=np.nan, latency_s=np.nan, note='no final measurement')); continue
        c = c.iloc[0]
        rows.append(dict(app=app, strategy=sl, rate=rl, mode='converged', config=c.final_assignment, run='convergence experiment', spout_k=c.spout_k, sink_k=c.sink_k,
                         sink_p50_k=c.get('sink_p50_k', np.nan), latency_s=c.latency_s, window=c.window, note=''))
V = pd.DataFrame(rows); V.to_csv(os.path.join(HERE, 'single_vs_converged_values.csv'), index=False)
M = [('sink_k', 'Sink throughput [k ev/s]'), ('spout_k', 'Spout output (consumed) [k ev/s]'), ('latency_s', 'E2E latency [s]')]
COL = {'single change': '#b8b8b8', 'converged': '#0b7a55'}
fig, axs = plt.subplots(2, 3, figsize=(7.16, 4.2)); fig.subplots_adjust(left=0.07, right=0.975, top=0.88, bottom=0.12, hspace=0.62, wspace=0.28)
for i, (app, _, _) in enumerate(APPS):
    for k, (m, lab) in enumerate(M):
        ax = axs[i, k]; x = np.arange(len(CELLS)); w = 0.38
        top = np.nanmax(V[V.app == app][m]) or 1
        for j, mode in enumerate(['single change', 'converged']):
            for xi, (sl, _, rl, _) in enumerate(CELLS):
                row = V[(V.app == app) & (V.strategy == sl) & (V.rate == rl) & (V['mode'] == mode)].iloc[0]; v = row[m]; xx = xi + (j - 0.5) * w
                if np.isnan(v): ax.text(xx, top * 0.03, 'n/a', ha='center', va='bottom', fontsize=5.4, rotation=90, color='0.35'); continue
                ax.bar(xx, v, w, color=COL[mode], edgecolor='0.3' if mode == 'single change' else COL[mode], lw=0.3, label=mode if (xi == 0 and i == 0 and k == 0) else None)
                ax.text(xx, v + top * 0.02, fmt(v) + (' *' if row.note == 'run produced no output' else ''), ha='center', va='bottom', fontsize=5.0, rotation=90)
        ax.set_xticks(x); ax.set_xticklabels([f'{sl}\n{rl}' for sl, _, rl, _ in CELLS], fontsize=6); ax.set_ylim(0, top * 1.45)
        ax.set_title(f'{app}: {lab}', fontsize=7, fontweight='bold'); ax.grid(axis='y', lw=0.3, color='0.88'); ax.set_axisbelow(True)
        for sp_ in ['top', 'right']: ax.spines[sp_].set_visible(False)
fig.legend(loc='upper center', ncol=2, frameon=False, fontsize=7, bbox_to_anchor=(0.5, 0.995))
fig.text(0.5, 0.01, 'single change = one DS2/Dhalion decision as in the paper (newest run per cell); converged = configuration after the ~5-min controller loop. '
         '* = run produced no output. n/a = no final measurement.', ha='center', fontsize=5.6, style='italic')
for e in ['pdf', 'svg']: fig.savefig(os.path.join(HERE, 'fig_single_vs_converged.' + e), dpi=300)
pd.set_option('display.width', 260); print(V.drop(columns=['run']).to_string(index=False))
