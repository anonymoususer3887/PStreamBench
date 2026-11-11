"""For selected Storm cells, report the newest run in artifacts/<app>/m510/storm (including reruns):
throughput_avg, throughput_50 and endtoend_latency_storm from evaluationMetrics*.json.
Used to decide which Storm cells still need a rerun and which only need a dataset rebuild."""
import os, glob, json, time
HERE = os.path.dirname(os.path.abspath(__file__)); A = os.path.join(HERE, '..', '..', 'artifacts')
def newest(app, strat, rate, dop=None):
    out = []
    for f in glob.glob(f'{A}/{app}/m510/storm/**/{strat}/{rate}/evaluationMetrics*.json', recursive=True):
        meta = f.replace('evaluationMetrics', 'metaInfo')
        m = json.load(open(meta)) if os.path.exists(meta) else {}
        p = m.get('job_parallelization', '')
        ps = [int(x) for x in p.split(',')[1:-1]] if p else []
        if dop and not (ps and all(x == dop for x in ps)): continue
        q = json.load(open(f)).get('queryMetrics', {})
        out.append((os.path.getmtime(f), os.path.basename(f), q.get('throughput_avg'), q.get('throughput_50'), q.get('endtoend_latency_storm'),
                    m.get('job_window_size'), m.get('job_window_slide_size')))
    return sorted(out)[-1] if out else None
CELLS  = [('bargain-index','uniform',r,d) for r,ds in [(100000,[4,8,40]),(500000,[8,16,32,40]),(1000000,[16])] for d in ds]
CELLS += [('trending-topics','uniform',1000000,d) for d in [16,32]]
CELLS += [('spike-detection',s,r,None) for s in ['ds2','dhalion'] for r in [100000,500000,1000000]]
CELLS += [('spike-detection','uniform',100000,8), ('spike-detection','uniform',1000000,32)]
CELLS += [(a,s,1000000,None) for a in ['click-analytics','word-count'] for s in ['ds2','dhalion']]
CELLS += [('smart-grid','ds2',500000,None)] + [('google-cloud-monitoring','uniform',r,16) for r in [100000,500000,1000000]]
for c in CELLS:
    r = newest(*c)
    print(c, 'NONE' if r is None else dict(date=time.strftime('%Y-%m-%d', time.localtime(r[0])), file=r[1], thr_avg=r[2], thr_p50=r[3], lat_ms=r[4], window=r[5], slide=r[6]))
