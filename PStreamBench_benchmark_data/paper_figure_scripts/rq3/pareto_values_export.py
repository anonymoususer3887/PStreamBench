"""Export the values behind fig_rq3_pareto_guided (throughput, latency, inverted latency, front/dominated)
from pareto_cells.csv, which pareto_options.py writes. Output: fig_rq3_pareto_guided_values.csv"""
import os, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__))
M = pd.read_csv(os.path.join(HERE, 'pareto_cells.csv'))
M = M[M.application.isin(['Ad Analytics','Click Analytics','Smart Grid','Word Count'])].copy()
M['rate'] = M.producer_rate.map({1e5:'100k', 5e5:'500k', 1e6:'1M'})
M['throughput_ev_s'] = M.thr.round(1); M['latency_s'] = (M.lat/1e3).round(4); M['inverted_latency_1_per_s'] = (1e3/M.lat).round(4)
M['pareto'] = M.eff.map({True:'front', False:'dominated'}); M['dop'] = M.dop.astype(int)
SO = {'flink':0,'storm':1,'spark':2,'kafkastream':3,'risingwave':4}
M = M.assign(so=M.system.map(SO)).sort_values(['application','so','producer_rate','dop'])
M[['application','system','rate','dop','throughput_ev_s','latency_s','inverted_latency_1_per_s','pareto']].to_csv(os.path.join(HERE,'fig_rq3_pareto_guided_values.csv'), index=False)
