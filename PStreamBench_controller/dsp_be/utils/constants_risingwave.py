# Mirrors constants_flink.py / constants_storm.py / constants_spark.py /
# constants_kafkastreams.py so the shared _config_manipulation_ansible()
# dispatcher in infra/views.py works unmodified for the "RisingWave"
# cluster_type. INPUT/OUTPUT topic names are identical to constants_flink.py
# so all 14 workloads reuse the existing Kafka producer/datasets without any
# changes there.
#
# *_PROGRAM is a path to a .sql file (relative to
# PStreamBench_workload_generator/risingwave/dsp_risingwave_jobs/), not a
# class name -- RisingWave jobs are CREATE SOURCE / CREATE MATERIALIZED VIEW /
# CREATE SINK statements run via psql, not JVM programs. Only Word Count is
# actually translated so far (see KAFKA_STREAMS_RISINGWAVE_PLAN.md Part B.2,
# Phase 1); the rest are scaffolded ahead of their SQL translation landing,
# same as constants_kafkastreams.py was before all 14 workloads' Java ports
# landed.

WORD_COUNT = "Word Count"
WORD_COUNT_PROGRAM = "wordcount/word_count.sql"
WORD_COUNT_INPUT = "WordCountIn"
WORD_COUNT_OUTPUT = "WordCountOut"

SMART_GRID = "Smart Grid"
SMART_GRID_PROGRAM = "smartgrid/smart_grid.sql"
SMART_GRID_INPUT = "SmartGridIn"
SMART_GRID_OUTPUT = "SmartGridOut"

AD_ANALYTICS = "Ad Analytics"
AD_ANALYTICS_PROGRAM = "AdsAnalytics/ads_analytics.sql"
AD_ANALYTICS_INPUT = "AdAnalyticsIn"
AD_ANALYTICS_OUTPUT = "AdAnalyticsOut"

GOOGLE_CLOUD_MONITORING = "Google cloud Monitoring"
GOOGLE_CLOUD_MONITORING_PROGRAM = "GoogleCloudMonitoring/google_cloud_monitoring.sql"
GOOGLE_CLOUD_MONITORING_INPUT = "GoogleIn"
GOOGLE_CLOUD_MONITORING_OUTPUT = "GoogleOut"

SENTIMENT_ANALYSIS = "Sentiment Analysis"
SENTIMENT_ANALYSIS_PROGRAM = "SentimentAnalysis/sentiment_analysis.sql"
SENTIMENT_ANALYSIS_INPUT = "SentimentIn"
SENTIMENT_ANALYSIS_OUTPUT = "SentimentOut"

SPIKE_DETECTION = "Spike Detection"
SPIKE_DETECTION_PROGRAM = "SpikeDetection/spike_detection.sql"
SPIKE_DETECTION_INPUT = "SpikeIn"
SPIKE_DETECTION_OUTPUT = "SpikeOut"

LOG_ANALYZER = 'Log Processing'
LOG_ANALYZER_PROGRAM = "LogsAnalyzer/logs_analyzer.sql"
LOG_ANALYZER_INPUT = "LogIn"
LOG_ANALYZER_OUTPUT = "LogOut"

TRENDING_TOPICS = 'Trending Topics'
TRENDING_TOPICS_PROGRAM = "TrendingTopics/trending_topics.sql"
TRENDING_TOPICS_INPUT = "TTIn"
TRENDING_TOPICS_OUTPUT = "TTOut"

BARGAIN_INDEX = 'Bargain Index'
BARGAIN_INDEX_PROGRAM = "BargainIndex/bargain_index.sql"
BARGAIN_INDEX_INPUT = "BIIn"
BARGAIN_INDEX_OUTPUT = "BIOut"

CLICK_ANALYTICS = 'Click Analytics'
CLICK_ANALYTICS_PROGRAM = "ClickAnalytics/click_analytics.sql"
CLICK_ANALYTICS_INPUT = "CAIn"
CLICK_ANALYTICS_OUTPUT = "CAOut"

LINEAR_ROAD = 'Linear Road'
LINEAR_ROAD_PROGRAM = "LinearRoad/linear_road.sql"
LINEAR_ROAD_INPUT = "LRIn"
LINEAR_ROAD_OUTPUT = "LROut"

TPCH = 'TPCH'
TPCH_PROGRAM = "TPCH/tpch.sql"
TPCH_INPUT = "TPIn"
TPCH_OUTPUT = "TPOut"

MACHINE_OUTLIER = 'Machine Outlier'
MACHINE_OUTLIER_PROGRAM = "MachineOutlier/machine_outlier.sql"
MACHINE_OUTLIER_INPUT = "MOIn"
MACHINE_OUTLIER_OUTPUT = "MOOut"

TRAFFIC_MONITORING = 'Traffic Monitoring'
TRAFFIC_MONITORING_PROGRAM = "TrafficMonitoring/traffic_monitoring.sql"
TRAFFIC_MONITORING_INPUT = "TMIn"
TRAFFIC_MONITORING_OUTPUT = "TMOut"


KAFKA = "Kafka"
