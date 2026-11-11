"""List runs whose window size or slide differs from the uniform runs of the same application and system (m510).
Reference window per (system, application) = most frequent window/slide in strategy_folder 'uniform'.
Writes window_mismatch_runs.csv (all differing runs) and window_mismatch_summary.csv (counts per system and strategy)."""
import os, pandas as pd
HERE = os.path.dirname(os.path.abspath(__file__)); D = os.path.join(HERE, '..', '..', 'datasets_v2')
R = pd.read_csv(os.path.join(D, 'real_query_level.csv'), low_memory=False,
                usecols=['run_id','system','application','variant','strategy_folder','hardware_type','producer_rate',
                         'parallelism_avg','window_size','window_slide','meta_enumeration_strategy','folder_flag','file_name'])
R = R[(R.hardware_type == 'm510') & (R.folder_flag.fillna('') == '')]
ref = R[R.strategy_folder == 'uniform'].groupby(['system','application'])[['window_size','window_slide']].agg(lambda s: s.mode().iloc[0])
ref.columns = ['ref_window','ref_slide']
R = R.join(ref, on=['system','application'])
R['window_differs'] = (R.window_size != R.ref_window) | (R.window_slide != R.ref_slide)
R[R.window_differs].sort_values(['system','application','strategy_folder','producer_rate']).to_csv(os.path.join(HERE,'window_mismatch_runs.csv'), index=False)
R.groupby(['system','strategy_folder']).window_differs.agg(['sum','size']).reset_index().to_csv(os.path.join(HERE,'window_mismatch_summary.csv'), index=False)
print(int(R.window_differs.sum()), 'of', len(R), 'm510 runs differ from the uniform reference window')
