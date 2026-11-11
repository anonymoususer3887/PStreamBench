"""Storm convergence of DS2 and Dhalion on m510 at 100k and 1M events/s, any number of stages.
Input (read-only): PStreamBench_controller/dsp_be/<app>/storm/convergence_{ds2,dhalion}/
  timeline_<rate>.jsonl  one line per controller check (elapsed_seconds, current/proposed assignment, changed,
                          per-stage throughput_per_sec, busy_ratio, backpressure_ratio as seen by the controller)
  summary_<rate>.json    final_metrics_file names the final measurement (evaluationMetrics-*.json)
Rebalances, final assignment and convergence are taken from the timeline. The summary is used only for the final
metrics file, and only if it was written after the timeline started (otherwise it belongs to an older run: 'stale').
Final performance = mean of each time series in the final measurement: spout output summed over all spouts,
sink throughput (mean of the one-minute samples; median reported in the CSV), endtoend_latency_storm, CPU = mean of
node_cpu over all reporting nodes, failed spout tuples summed over spouts. No value is rounded to a whole number.
Usage: python3 conv_plot.py "<app>"   -> fig_conv_<app>.pdf/svg, conv_<app>_timeline.csv, conv_<app>_final.csv"""
import os, sys, json, numpy as np, pandas as pd, matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt
HERE = os.path.dirname(os.path.abspath(__file__)); APP = sys.argv[1] if len(sys.argv) > 1 else 'Ad Analytics'
BASE = os.path.join(HERE, '..', '..', '..', 'PStreamBench_controller', 'dsp_be', APP, 'storm')
plt.rcParams.update({'font.family': 'serif', 'font.serif': ['Times New Roman', 'Times', 'STIXGeneral', 'DejaVu Serif'], 'mathtext.fontset': 'stix',
                     'font.size': 7, 'axes.linewidth': 0.5, 'pdf.fonttype': 42, 'svg.fonttype': 'none'})
STRATS = [('ds2', 'DS2', '#0b7a55'), ('dhalion', 'Dhalion', '#d9714a')]; RATES = [(100000, '100k'), (1000000, '1M')]
SC = ['#1f4e9c', '#6aa5d8', '#8c2d7a', '#d58ac8', '#333333', '#999999']
def fmt(v):
    if v is None or (isinstance(v, float) and np.isnan(v)): return 'n/a'
    a = abs(v); return '0.0' if a == 0 else (f'{v:.1f}' if a >= 10 else (f'{v:.2f}' if a >= 1 else f'{v:.2g}'))
T, F = [], []
for s, sl, _ in STRATS:
    for r, rl in RATES:
        d = os.path.join(BASE, f'convergence_{s}'); tf = os.path.join(d, f'timeline_{r}.jsonl')
        if not os.path.exists(tf): continue
        tl = [json.loads(l) for l in open(tf) if l.strip()]; stages = [m['stage'] for m in tl[0]['stage_metrics']]
        for i, c in enumerate(tl, 1):
            for j, st in enumerate(stages):
                m = c['stage_metrics'][j]
                T.append(dict(strategy=sl, rate=rl, check=i, elapsed_s=c['elapsed_seconds'], stage=st, dop_before=c['current_assignment'][j],
                              dop_after=c['proposed_assignment'][j], changed=c['changed'], thr=m['throughput_per_sec'], busy=m['busy_ratio'], bp=m['backpressure_ratio']))
        nreb = sum(c['changed'] for c in tl); last = max([i for i, c in enumerate(tl, 1) if c['changed']] or [0])
        conv = np.nan if tl[-1]['changed'] else last + 1
        row = dict(strategy=sl, rate=rl, final_assignment=tl[-1]['proposed_assignment'], num_rebalances=nreb, num_checks=len(tl), converged_at_check=conv,
                   duration_s=tl[-1]['elapsed_seconds'], final_measurement='none')
        sf = os.path.join(d, f'summary_{r}.json')
        if os.path.exists(sf):
            S = json.load(open(sf))
            if os.path.getmtime(sf) < tl[0]['wall_time']: row['final_measurement'] = 'stale summary (older run)'
            elif S.get('final_metrics_file'):
                ef = os.path.join(d, os.path.basename(S['final_metrics_file'])); E = json.load(open(ef)); q = E['queryMetrics']
                spouts = [k for k in q if k.startswith('recPerSecOutSource_') and isinstance(q[k], list)]
                sink = [float(v) for _, v in q['throughput'][0]['values']]
                cpu = [float(v) for n in E['hardwareMetrics']['node_cpu'] for _, v in n['values']]
                meta = json.load(open(ef.replace('evaluationMetrics', 'metaInfo')))
                row.update(final_measurement=os.path.basename(ef), spout_k=sum(np.mean([float(v) for _, v in q[k][0]['values']]) for k in spouts) / 1e3,
                           sink_k=np.mean(sink) / 1e3, sink_p50_k=np.median(sink) / 1e3, latency_s=float(q['endtoend_latency_storm']) / 1e3, cpu=np.mean(cpu),
                           cpu_nodes=len(E['hardwareMetrics']['node_cpu']), failed_k=sum(float(v) for k, v in q.items() if k.startswith('spout_failed_')) / 1e3,
                           acked_k=sum(float(v) for k, v in q.items() if k.startswith('spout_acked_')) / 1e3,
                           window=f"{meta['job_window_size']}/{meta['job_window_slide_size']}")
            else: row['final_measurement'] = 'none (final_metrics_file null)'
        F.append(row)
T = pd.DataFrame(T); F = pd.DataFrame(F); tag = APP.replace(' ', '_')
T.to_csv(os.path.join(HERE, f'conv_{tag}_timeline.csv'), index=False); F.to_csv(os.path.join(HERE, f'conv_{tag}_final.csv'), index=False)
stages = list(dict.fromkeys(T.stage))
fig, axs = plt.subplots(4, 2, figsize=(7.16, 7.6)); fig.subplots_adjust(left=0.085, right=0.99, top=0.945, bottom=0.05, hspace=0.75, wspace=0.18)
for k, (r, rl) in enumerate(RATES):
    for row, (s, sl, c) in enumerate(STRATS):
        ax = axs[row, k]; g0 = T[(T.strategy == sl) & (T.rate == rl)]
        for j, st in enumerate(stages):
            g = g0[g0.stage == st]; off = 1 + 0.035 * (j - (len(stages) - 1) / 2)   # small offset so equal DoPs stay visible
            ax.step([0] + list(g.check), np.array([g.dop_before.iloc[0]] + list(g.dop_after)) * off, where='post', color=SC[j], lw=1.1, label=st)
            ch = g[g.changed]; ax.plot(ch.check, ch.dop_after * off, 'o', ms=2.6, color=SC[j])
        f = F[(F.strategy == sl) & (F.rate == rl)].iloc[0]
        txt = f"{f.num_rebalances} rebalance{'' if f.num_rebalances == 1 else 's'}, " + (f"stable from check {int(f.converged_at_check)}" if pd.notna(f.converged_at_check) else f"not converged in {f.num_checks} checks") + f", final {f.final_assignment}"
        ax.text(0.99, 0.05, txt, transform=ax.transAxes, ha='right', fontsize=6, color=c)
        ax.set_yscale('log', base=2); ax.set_yticks([1, 2, 4, 8, 16, 40]); ax.set_yticklabels(['1', '2', '4', '8', '16', '40']); ax.set_ylim(0.8, 60)
        ax.set_xlabel('controller check'); ax.set_ylabel('DoP'); ax.grid(axis='y', lw=0.3, color='0.85')
        ax.set_title(f'({"abcd"[row * 2 + k]}) {sl} configuration, {rl} events/s', fontsize=7.5, fontweight='bold', color=c)
        if k == 0 and row == 0: ax.legend(fontsize=5.4, ncol=1, frameon=False, loc='center left', bbox_to_anchor=(0.0, 0.62), handlelength=1.6)
    ax = axs[2, k]
    for s, sl, c in STRATS:
        for j, st in enumerate(stages):
            g = T[(T.strategy == sl) & (T.rate == rl) & (T.stage == st)]
            ax.plot(g.check, g.thr / 1e3, color=SC[j], ls='-' if s == 'ds2' else '--', lw=0.9, marker='.', ms=2.5)
    ax.set_xlabel('controller check'); ax.set_ylabel('observed throughput [k ev/s]'); ax.grid(axis='y', lw=0.3, color='0.85')
    ax.set_title(f'({"ef"[k]}) Observed stage throughput, {rl} (solid DS2, dashed Dhalion)', fontsize=7.5, fontweight='bold')
    ax.text(0.01, 0.95, f'max backpressure ratio over all checks: {fmt(T[T.rate == rl].bp.max())}', transform=ax.transAxes, fontsize=6, va='top')
    ax = axs[3, k]; sub = F[F.rate == rl]
    ax.set_title(f'({"gh"[k]}) Performance of the final configuration, {rl}', fontsize=7.5, fontweight='bold')
    for sp_ in ['top', 'right', 'left']: ax.spines[sp_].set_visible(False)
    ok = sub[sub.final_measurement.str.startswith('evaluationMetrics')] if 'spout_k' in sub else sub.iloc[0:0]
    if ok.empty:
        ax.set_xticks([]); ax.set_yticks([]); ax.spines['bottom'].set_visible(False)
        ax.text(0.5, 0.5, 'no final measurement:\n' + '\n'.join(f'{x.strategy}: {x.final_measurement}' for x in sub.itertuples()), ha='center', va='center', fontsize=6.5, transform=ax.transAxes)
        continue
    M = [('spout_k', 'spout out\n[k ev/s]'), ('sink_k', 'sink out, mean\n[k ev/s]'), ('latency_s', 'E2E latency\n[s]'), ('cpu', 'CPU\n[%]'), ('failed_k', 'failed tuples\n[k]')]
    x = np.arange(len(M)); w = 0.36; mx = [max(ok[m]) or 1 for m, _ in M]
    for i, (s, sl, c) in enumerate(STRATS):
        f = sub[sub.strategy == sl]
        if f.empty or not str(f.iloc[0].final_measurement).startswith('evaluationMetrics'):
            ax.text(0.5, 1.05 - 0.1 * i, f'{sl}: {f.iloc[0].final_measurement if not f.empty else "no run"}', transform=ax.transAxes, fontsize=6, color=c, ha='center'); continue
        f = f.iloc[0]; vals = [f[m] for m, _ in M]
        ax.bar(x + (i - 0.5) * w, [v / m for v, m in zip(vals, mx)], w, color=c, label=sl)
        for xi, v, m in zip(x, vals, mx): ax.text(xi + (i - 0.5) * w, v / m + 0.02, fmt(v), ha='center', va='bottom', fontsize=5.6)
    ax.set_xticks(x); ax.set_xticklabels([l for _, l in M], fontsize=6); ax.set_ylim(0, 1.25); ax.set_yticks([])
    ax.legend(fontsize=6, frameon=False, loc='upper right', ncol=2, bbox_to_anchor=(1.0, 1.12))
fig.suptitle(f'Storm {APP} on m510: DS2 vs Dhalion convergence (bars normalised per metric, labels = measured values)', fontsize=8)
for e in ['pdf', 'svg']: fig.savefig(os.path.join(HERE, f'fig_conv_{tag}.' + e), dpi=300)
pd.set_option('display.width', 250); print(F.to_string(index=False))
