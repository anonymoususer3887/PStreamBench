# Mirrors constants_flink.py / constants_storm.py / constants_spark.py so
# that the shared _config_manipulation_ansible() dispatcher in
# infra/views.py works unmodified for the "Apache Kafka Streams"
# cluster_type. INPUT/OUTPUT topic names are identical to constants_flink.py
# so all 14 workloads reuse the existing Kafka producer/datasets without any
# changes there.
#
# *_PROGRAM follows the <Workload>Topology.java naming convention (see
# KAFKA_STREAMS_RISINGWAVE_PLAN.md Part A.2) rather than Flink/Spark's bare
# <Workload>Job -- distinguishes a Kafka Streams Topology-builder class from
# a Flink/Spark job entry point at a glance. Only WordCountTopology is
# actually implemented so far (see PStreamBench_workload_generator/
# kafkastreams/dsp_kafkastreams_jobs); the rest are scaffolded ahead of the
# Java port landing, same as constants_spark.py was before all 14 Spark
# workloads were validated.

WORD_COUNT = "Word Count"
WORD_COUNT_PROGRAM = "com.kom.dsp.wordcount.WordCountTopology"
WORD_COUNT_INPUT = "WordCountIn"
WORD_COUNT_OUTPUT = "WordCountOut"

SMART_GRID = "Smart Grid"
SMART_GRID_PROGRAM = "com.kom.dsp.smartgrid.SmartGridTopology"
SMART_GRID_INPUT = "SmartGridIn"
SMART_GRID_OUTPUT = "SmartGridOut"

AD_ANALYTICS = "Ad Analytics"
AD_ANALYTICS_PROGRAM = "com.kom.dsp.AdsAnalytics.AdsAnalyticsTopology"
AD_ANALYTICS_INPUT = "AdAnalyticsIn"
AD_ANALYTICS_OUTPUT = "AdAnalyticsOut"

GOOGLE_CLOUD_MONITORING = "Google cloud Monitoring"
GOOGLE_CLOUD_MONITORING_PROGRAM = "com.kom.dsp.GoogleCloudMonitoring.GoogleCloudMonitoringTopology"
GOOGLE_CLOUD_MONITORING_INPUT = "GoogleIn"
GOOGLE_CLOUD_MONITORING_OUTPUT = "GoogleOut"

SENTIMENT_ANALYSIS = "Sentiment Analysis"
SENTIMENT_ANALYSIS_PROGRAM = "com.kom.dsp.SentimentAnalysis.SentimentAnalysisTopology"
SENTIMENT_ANALYSIS_INPUT = "SentimentIn"
SENTIMENT_ANALYSIS_OUTPUT = "SentimentOut"

SPIKE_DETECTION = "Spike Detection"
SPIKE_DETECTION_PROGRAM = "com.kom.dsp.SpikeDetection.SpikeDetectionTopology"
SPIKE_DETECTION_INPUT = "SpikeIn"
SPIKE_DETECTION_OUTPUT = "SpikeOut"

LOG_ANALYZER = 'Log Processing'
LOG_ANALYZER_PROGRAM = "com.kom.dsp.LogsAnalyzer.LogsAnalyzerTopology"
LOG_ANALYZER_INPUT = "LogIn"
LOG_ANALYZER_OUTPUT = "LogOut"

TRENDING_TOPICS = 'Trending Topics'
TRENDING_TOPICS_PROGRAM = "com.kom.dsp.TrendingTopics.TrendingTopicsTopology"
TRENDING_TOPICS_INPUT = "TTIn"
TRENDING_TOPICS_OUTPUT = "TTOut"

BARGAIN_INDEX = 'Bargain Index'
BARGAIN_INDEX_PROGRAM = "com.kom.dsp.BargainIndex.BargainIndexTopology"
BARGAIN_INDEX_INPUT = "BIIn"
BARGAIN_INDEX_OUTPUT = "BIOut"

CLICK_ANALYTICS = 'Click Analytics'
CLICK_ANALYTICS_PROGRAM = "com.kom.dsp.ClickAnalytics.ClickAnalyticsTopology"
CLICK_ANALYTICS_INPUT = "CAIn"
CLICK_ANALYTICS_OUTPUT = "CAOut"

LINEAR_ROAD = 'Linear Road'
LINEAR_ROAD_PROGRAM = "com.kom.dsp.LinearRoad.LinearRoadTopology"
LINEAR_ROAD_INPUT = "LRIn"
LINEAR_ROAD_OUTPUT = "LROut"

TPCH = 'TPCH'
TPCH_PROGRAM = "com.kom.dsp.TPCH.TPCHTopology"
TPCH_INPUT = "TPIn"
TPCH_OUTPUT = "TPOut"

MACHINE_OUTLIER = 'Machine Outlier'
MACHINE_OUTLIER_PROGRAM = "com.kom.dsp.MachineOutlier.MachineOutlierTopology"
MACHINE_OUTLIER_INPUT = "MOIn"
MACHINE_OUTLIER_OUTPUT = "MOOut"

TRAFFIC_MONITORING = 'Traffic Monitoring'
TRAFFIC_MONITORING_PROGRAM = "com.kom.dsp.TrafficMonitoring.TrafficMonitoringTopology"
TRAFFIC_MONITORING_INPUT = "TMIn"
TRAFFIC_MONITORING_OUTPUT = "TMOut"


KAFKA = "Kafka"
