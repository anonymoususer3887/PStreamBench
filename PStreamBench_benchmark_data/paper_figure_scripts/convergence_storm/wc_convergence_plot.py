"""Storm Word Count on m510: DS2 vs Dhalion convergence at 100k and 1M events/s.
Input (read-only): PStreamBench_controller/dsp_be/<app>/storm/convergence_{ds2,dhalion}/
  timeline_<rate>.jsonl  one line per controller check: elapsed_seconds, current/proposed assignment [tokenizer, counter],
                          changed, per-stage throughput_per_sec / busy_ratio / backpressure_ratio as seen by the controller
  summary_<rate>.json    initial/final assignment, num_rebalances, num_checks, final_metrics_file
  evaluationMetrics-*.json  final measurement after the controller stopped (file named in summary)
Rows: (1) DoP per stage after each check (step plot, filled marker = rebalance issued)
      (2) per-stage throughput the controller observed at each check
      (3) final performance: spout output (consumed input) and sink throughput [k events/s] = mean of the time series,
          end-to-end latency endtoend_latency_storm [s], CPU = mean of node_cpu over all reporting nodes and samples [%].
'Converged' = first check after which the assignment no longer changes; a run whose last check still changed is marked
'not converged'. No values are rounded to whole numbers.
Usage: python3 wc_convergence_plot.py [app]   (default 'Word Count'); writes fig_conv_<app>.pdf/svg and values CSVs."""
import os, sys, json, numpy as np, pandas as pd, matplotlib; matplotlib.use('Agg'); import matplotlib.pyplot as plt
HERE = os.path.dirname(os.path.abspath(__file__))
APP = sys.argv[1] if len(sys.argv) > 1 else 'Word Count'
BASE = os.path.join(HERE, '..', '..', '..', 'PStreamBench_controller', 'dsp_be', APP, 'storm')
plt.rcParams.update({'font.family': 'serif', 'font.serif': ['Times New Roman', 'Times', 'STIXGeneral', 'DejaVu Serif'], 'mathtext.fontset': 'stix',
                     'font.size': 7, 'axes.linewidth': 0.5, 'pdf.fonttype': 42, 'svg.fonttype': 'none'})
STRATS = [('ds2', 'DS2', '#0b7a55'), ('dhalion', 'Dhalion', '#d9714a')]; RATES = [(100000, '100k'), (1000000, '1M')]
def fmt(v): a = abs(v); return '0.0' if a == 0 else (f'{v:.1f}' if a >= 10 else (f'{v:.2f}' if a >= 1 else f'{v:.2g}'))
T, F = [], []
for s, sl, _ in STRATS:
    for r, rl in RATES:
        d = os.path.join(BASE, f'convergence_{s}')
        S = json.load(open(os.path.join(d, f'summary_{r}.json')))
        tl = [json.loads(l) for l in open(os.path.join(d, f'timeline_{r}.jsonl')) if l.strip()]
        stages = [m['stage'] for m in tl[0]['stage_metrics']]
        for i, c in enumerate(tl, 1):
            for j, st in enumerate(stages):
                m = c['stage_metrics'][j]
                T.append(dict(strategy=sl, rate=rl, check=i, elapsed_s=c['elapsed_seconds'], stage=st, dop_before=c['current_assignment'][j],
                              dop_after=c['proposed_assignment'][j], changed=c['changed'], thr=m['throughput_per_sec'], busy=m['busy_ratio'], bp=m['backpressure_ratio']))
        last_change = max([i for i, c in enumerate(tl, 1) if c['changed']] or [0])
        conv = None if tl[-1]['changed'] else last_change + 1
        E = json.load(open(os.path.join(d, os.path.basename(S['final_metrics_file'])))); q = E['queryMetrics']
        sp = np.mean([float(v) for _, v in q['recPerSecOutSource_spout'][0]['values']])
        sk = np.mean([float(v) for _, v in q['throughput'][0]['values']])
        cpu = [float(v) for n in E['hardwareMetrics']['node_cpu'] for _, v in n['values']]
        meta = json.load(open(os.path.join(d, os.path.basename(S['final_metrics_file']).replace('evaluationMetrics', 'metaInfo'))))
        F.append(dict(strategy=sl, rate=rl, final_assignment=S['final_assignment'], num_rebalances=S['num_rebalances'], num_checks=S['num_checks'],
                      converged_at_check=conv, spout_k=sp / 1e3, sink_k=sk / 1e3, latency_s=float(q['endtoend_latency_storm']) / 1e3,
                      cpu=np.mean(cpu), cpu_nodes=len(E['hardwareMetrics']['node_cpu']), failed=q['spout_failed_spout'],
                      window=f"{meta['job_window_size']}/{meta['job_window_slide_size']}"))
T = pd.DataFrame(T); F = pd.DataFrame(F); tag = APP.replace(' ', '_')
T.to_csv(os.path.join(HERE, f'conv_{tag}_timeline.csv'), index=False); F.to_csv(os.path.join(HERE, f'conv_{tag}_final.csv'), index=False)
stages = list(dict.fromkeys(T.stage)); LS = ['-', '--', ':', '-.']
fig, axs = plt.subplots(3, 2, figsize=(7.16, 5.6)); fig.subplots_adjust(left=0.085, right=0.99, top=0.93, bottom=0.07, hspace=0.62, wspace=0.18)
for k, (r, rl) in enumerate(RATES):
    ax = axs[0, k]
    for s, sl, c in STRATS:
        for j, st in enumerate(stages):
            g = T[(T.strategy == sl) & (T.rate == rl) & (T.stage == st)]
            x = [0] + list(g.check); y = [g.dop_before.iloc[0]] + list(g.dop_after)
            ax.step(x, y, where='post', color=c, ls=LS[j], lw=1.1, label=f'{sl} {st}')
            ch = g[g.changed]; ax.plot(ch.check, ch.dop_after, 'o', ms=3, color=c, ls='none')
        f = F[(F.strategy == sl) & (F.rate == rl)].iloc[0]
        txt = f"{sl}: {f.num_rebalances} rebalance{'' if f.num_rebalances == 1 else 's'}, " + (f"stable from check {int(f.converged_at_check)}" if pd.notna(f.converged_at_check) else f"not converged in {f.num_checks} checks")
        ax.text(0.99, 0.30 if s == 'ds2' else 0.14, txt + f", final {f.final_assignment}", transform=ax.transAxes, ha='right', fontsize=6, color=c)
    ax.set_yscale('log', base=2); ax.set_yticks([1, 2, 4, 8, 16, 40]); ax.set_yticklabels(['1', '2', '4', '8', '16', '40']); ax.set_ylim(0.8, 56)
    ax.set_xlabel('controller check'); ax.set_ylabel('DoP'); ax.set_title(f'({"ab"[k]}) Configuration, {rl} events/s', fontsize=7.5, fontweight='bold')
    ax.grid(axis='y', lw=0.3, color='0.85')
    if k == 0: ax.legend(fontsize=5.6, ncol=1, frameon=False, loc='center left', bbox_to_anchor=(0.0, 0.6), handlelength=2.2)
    ax = axs[1, k]
    for s, sl, c in STRATS:
        for j, st in enumerate(stages):
            g = T[(T.strategy == sl) & (T.rate == rl) & (T.stage == st)]
            ax.plot(g.check, g.thr / 1e3, color=c, ls=LS[j], lw=1.0, marker='.', ms=3)
    ax.set_xlabel('controller check'); ax.set_ylabel('observed throughput [k ev/s]')
    ax.set_title(f'({"cd"[k]}) Stage throughput seen by the controller, {rl}', fontsize=7.5, fontweight='bold'); ax.grid(axis='y', lw=0.3, color='0.85')
    bp = T[(T.rate == rl)].bp.max(); ax.text(0.01, 0.95, f'max backpressure ratio over all checks: {fmt(bp)}', transform=ax.transAxes, fontsize=6, va='top')
    ax = axs[2, k]
    M = [('spout_k', 'spout out\n[k ev/s]'), ('sink_k', 'sink out\n[k ev/s]'), ('latency_s', 'E2E latency\n[s]'), ('cpu', 'CPU\n[%]')]
    x = np.arange(len(M)); w = 0.36
    sub = F[F.rate == rl]
    for i, (s, sl, c) in enumerate(STRATS):
        f = sub[sub.strategy == sl].iloc[0]; vals = [f[m] for m, _ in M]
        # each metric on its own scale: normalise to the larger of the two strategies, print the raw value
        mx = [max(sub[m]) for m, _ in M]
        b = ax.bar(x + (i - 0.5) * w, [v / m for v, m in zip(vals, mx)], w, color=c, label=sl)
        for xi, v, m in zip(x, vals, mx): ax.text(xi + (i - 0.5) * w, v / m + 0.02, fmt(v), ha='center', va='bottom', fontsize=5.6)
    ax.set_xticks(x); ax.set_xticklabels([l for _, l in M], fontsize=6); ax.set_ylim(0, 1.25); ax.set_yticks([])
    ax.set_title(f'({"ef"[k]}) Performance of the final configuration, {rl}', fontsize=7.5, fontweight='bold')
    if k == 0: ax.legend(fontsize=6, frameon=False, loc='upper right', ncol=2)
    for sp_ in ['top', 'right', 'left']: ax.spines[sp_].set_visible(False)
fig.suptitle(f'Storm {APP} on m510: DS2 vs Dhalion convergence (bars normalised per metric, labels = measured values)', fontsize=8)
for e in ['pdf', 'svg']: fig.savefig(os.path.join(HERE, f'fig_conv_{tag}.' + e), dpi=300)
print(F.to_string(index=False))
