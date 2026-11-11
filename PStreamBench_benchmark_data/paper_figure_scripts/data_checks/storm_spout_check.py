"""Check whether incomplete Storm real-world cells (BI, LR, SA, GCM) have spout/operator metrics that recover throughput.
Read-only on datasets_v2. Same run filter as missing_inventory.py."""
import os, numpy as np, pandas as pd, warnings; warnings.filterwarnings('ignore')
D=os.path.join(os.path.dirname(os.path.abspath(__file__)),'..','..','datasets_v2')
APPS=['Bargain Index','Linear Road','Sentiment Analysis','Google Cloud Monitoring']
R=pd.read_csv(os.path.join(D,'real_query_level.csv'),low_memory=False)
R=R[(R.hardware_type=='m510')&(R.system=='storm')&(R.folder_flag.fillna('')=='')&R.strategy_folder.isin(['uniform','uniform_1','uniform_2','ds2','dhalion'])&R.application.isin(APPS)].copy()
R['cfg']=np.where(R.strategy_folder.str.startswith('uniform'),'u'+R.parallelism_avg.round().astype('Int64').astype(str),R.strategy_folder)
cols=[c for c in ['throughput_50','source_output_50','source_operator_throughput','sink_operator_throughput','endtoend_latency_ms'] if c in R.columns]
print('query cols:',cols)
O=pd.read_csv(os.path.join(D,'real_operator_level.csv'),low_memory=False)
print('op cols:',[c for c in O.columns if 'through' in c or 'name' in c or 'index' in c or 'output' in c or 'input' in c][:30])
O=O[O.run_id.isin(R.run_id)]
agg=O.groupby(['run_id','operator_index']).throughput_per_sec.sum(min_count=1).unstack()
agg.columns=['op%d'%c for c in agg.columns]
X=R[['application','producer_rate','cfg','run_id']+cols].merge(agg,left_on='run_id',right_index=True,how='left')
pd.set_option('display.width',250); pd.set_option('display.max_rows',500); pd.set_option('display.max_columns',30)
X=X.sort_values(['application','producer_rate','cfg'])
print(X.drop(columns='run_id').round(1).to_string(index=False))
X.to_csv('storm_spout_check.csv',index=False)
