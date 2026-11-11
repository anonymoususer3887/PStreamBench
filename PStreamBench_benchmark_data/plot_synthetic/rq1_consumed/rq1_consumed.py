"""RQ1 alternative to heatmap panel (a): consumed-input ratio DoP 40 / DoP 2 (real workloads, m510, uniform, 1M).
consumed input per run:
  Kafka Streams, RisingWave: source_operator_throughput (query level)
  Spark: source_operator_throughput, else throughput_50 (processing rate)
  Flink, Storm: source_output_50 (source operator / spout output summed over instances, median over time);
  a value of 0 is treated as missing (spout metric not reported), not as zero input.
Offered rate is identical at both DoPs and cancels in the ratio. Per configuration: median over runs.
Also writes the sink-output ratio (throughput_50) from the same runs for comparison."""
import os, numpy as np, pandas as pd, warnings; warnings.filterwarnings('ignore')
HERE=os.path.dirname(os.path.abspath(__file__)); D=os.path.join(HERE,'..','..','datasets_v2')
Q=pd.read_csv(os.path.join(D,'real_query_level.csv'),low_memory=False)
Q=Q[(Q.hardware_type=='m510')&(Q.strategy=='uniform')&(Q.producer_rate==1e6)&(Q.folder_flag.fillna('')=='')].copy()
Q['dop']=Q.parallelism_avg.round(); Q=Q[Q.dop.isin([2,40])]
O=pd.read_csv(os.path.join(D,'real_operator_level.csv'),low_memory=False,
              usecols=['run_id','system','application','operator_index','throughput_per_sec','throughput_total_per_sec'])
O=O[O.run_id.isin(Q.run_id)&O.system.isin(['flink','storm'])]
first=O[(O.operator_index==0)|((O.application=='Ad Analytics')&(O.operator_index==1))].copy()
first['v']=np.where(first.system=='flink',first.throughput_total_per_sec,first.throughput_per_sec)
fc=first.groupby('run_id').v.sum(min_count=1)
Q['consumed']=Q.source_operator_throughput
Q.loc[Q.system=='spark','consumed']=Q.loc[Q.system=='spark','source_operator_throughput'].fillna(Q.loc[Q.system=='spark','throughput_50'])
m=Q.system.isin(['flink','storm']); Q.loc[m,'consumed']=Q.loc[m,'source_output_50'].where(Q.loc[m,'source_output_50']>0)
g=Q.groupby(['application','system','dop']).agg(consumed=('consumed','median'),sink=('throughput_50','median'),n=('run_id','size')).reset_index()
p=g.pivot_table(index=['application','system'],columns='dop',values=['consumed','sink','n'])
R=pd.DataFrame({'consumed_dop2':p[('consumed',2.0)],'consumed_dop40':p[('consumed',40.0)],'sink_dop2':p[('sink',2.0)],'sink_dop40':p[('sink',40.0)],
                'runs_dop2':p[('n',2.0)],'runs_dop40':p[('n',40.0)]}).reset_index()
R['consumed_ratio']=R.consumed_dop40/R.consumed_dop2; R['sink_ratio']=R.sink_dop40/R.sink_dop2
R.to_csv(os.path.join(HERE,'rq1_consumed_vs_sink.csv'),index=False,float_format='%.6g')
pd.set_option('display.width',200)
print(R.pivot(index='application',columns='system',values='consumed_ratio').round(2).to_string())
print(R.pivot(index='application',columns='system',values='sink_ratio').round(2).to_string())
