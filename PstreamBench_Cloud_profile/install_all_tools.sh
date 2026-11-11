# Copy custom_producer.py
cp /local/repository/custom_producer.py /home/playground/

# Copy smartgrid data file
#cp /local/repository/smart-grid.xlsx /home/playground/
#copy adAnalytics data file
#cp /local/repository/adAnalytics.dat /home/playground/
#copy googleCloudMonitoring.csv file

#cp /local/repository/googleCloudMonitoring.csv /home/playground
#
##copy sentiment analysis  file
#
#cp /local/repository/sentimentAnalysis_tweetstream.jsonl /home/playground
#
##copy spike detection file
#
#cp /local/repository/spikeDetection_sensors.dat /home/playground
##wget -P /home/playground/  https://code.jquery.com/jquery-3.6.0.min.js
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/click-stream.json?inline=false -O click-stream.json
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/http.log?inline=false -O http.log
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/outlier.csv?inline=false -O outlier.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/stocks.csv?inline=false -O stocks.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/ttopic.csv?inline=false -O ttopic.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/lrb.csv?inline=false -O lrb.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/taxi-traces.csv?inline=false -O taxi-traces.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/tpc_h.csv?inline=false -O tpc_h.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/adAnalytics.dat?inline=false -O adAnalytics.dat
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/googleCloudMonitoring.csv?inline=false -O googleCloudMonitoring.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/sentimentAnalysis_tweetstream.json?inline=false -O sentimentAnalysis_tweetstream.json
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/smart-grid.csv?inline=false -O smart-grid.csv
#wget -P /home/playground/ https://gitlab.com/sumalya/dsp_data/-/raw/master/spikeDetection_sensors.dat?inline=false -O spikeDetection_sensors.dat
#
#cp /click-stream.json /home/playground
#cp /http.log /home/playground
#cp /outlier.csv /home/playground
#cp /stocks.csv /home/playground
#cp /ttopic.csv /home/playground
#cp /lrb.csv /home/playground
#cp /taxi-traces.csv /home/playground
#cp /tpc_h.csv /home/playground
#cp /adAnalytics.dat /home/playground
#cp /googleCloudMonitoring.csv /home/playground
#cp /sentimentAnalysis_tweetstream.json /home/playground
#cp /smart-grid.csv /home/playground
#cp /spikeDetection_sensors.dat /home/playground
# cp /local/repository/click-stream.json /home/playground
# cp /local/repository/http.log /home/playground
# cp /local/repository/outlier.csv /home/playground
# cp /local/repository/stocks.csv /home/playground
# cp /local/repository/ttopic.csv /home/playground


# Downloading flink 
#curl -L https://dlcdn.apache.org/flink/flink-1.16.2/flink-1.16.2-bin-scala_2.12.tgz > /home/playground/zip/flink.tgz
curl -0 https://archive.apache.org/dist/flink/flink-1.16.2/flink-1.16.2-bin-scala_2.12.tgz --output /home/playground/zip/flink.tgz
# Unzip flink to playgrounds directory
tar zxf /home/playground/zip/flink.tgz -C /home/playground/

# Downloading storm
curl -0 https://archive.apache.org/dist/storm/apache-storm-2.6.2/apache-storm-2.6.2.tar.gz --output /home/playground/zip/storm.tar.gz
# Unzip flink to playgrounds directory
tar zxf /home/playground/zip/storm.tar.gz -C /home/playground/

# Downloading spark (Standalone mode -- no Hadoop/YARN cluster required, same as
# how Flink/Storm run standalone here; -bin-hadoop3 build still bundles the
# client libraries the Kafka connector needs)
curl -0 https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz --output /home/playground/zip/spark.tgz
# Unzip spark to playgrounds directory
tar zxf /home/playground/zip/spark.tgz -C /home/playground/

# Unlike Flink's dsp_jobs jar, dsp_spark_jobs is not baked in here (same as
# Storm's dsp_storm_jobs) -- it is built locally and copied to the cluster by
# the controller's spark-setup-job.yaml at job-submission time instead. See
# PStreamBench_workload_generator/spark/dsp_spark_jobs.

# Copy oshi,jna and jna_platform
cp /local/repository/jna-platform-5.10.0.jar /home/playground/flink-1.16.2/lib
cp /local/repository/jna-5.10.0.jar /home/playground/flink-1.16.2/lib
cp /local/repository/oshi-core-6.1.5.jar /home/playground/flink-1.16.2/lib

# Copy dsp_jobs files
cp /local/repository/dsp_jobs-1.0-SNAPSHOT.jar /home/playground/flink-1.16.2/bin

# Copy roads.geojson file to flink/bin

cp /local/repository/roads.geojson /home/playground/flink-1.16.2/bin 

# Downloading prometheus 
curl -L https://github.com/prometheus/prometheus/releases/download/v2.42.0/prometheus-2.42.0.linux-amd64.tar.gz > /home/playground/zip/prometheus.tar.gz
# Unzip prometheus to playgrounds directory
tar zxf /home/playground/zip/prometheus.tar.gz -C /home/playground/

# Setup storm exporter
sudo pip install prometheus_client
cp /local/repository/storm_exporter.py /home/playground/apache-storm-2.6.2/bin

# Download grafana
curl -L https://dl.grafana.com/enterprise/release/grafana-enterprise-9.3.6.linux-amd64.tar.gz > /home/playground/zip/grafana.tar.gz
# Unzip grafana to playgrounds directory
tar zxf /home/playground/zip/grafana.tar.gz -C /home/playground/


# Download kafka
curl -L https://archive.apache.org/dist/kafka/3.8.0/kafka_2.12-3.8.0.tgz > /home/playground/zip/kafka.tgz

# Unzip kafka to playgrounds directory
tar zxf /home/playground/zip/kafka.tgz -C /home/playground/

mv /home/playground/kafka_2.12-3.8.0 /home/playground/kafka_2.12

# removing default kafka property
rm /home/playground/kafka_2.12/config/server.properties

# Copying the server.properties to /home/playground
cp /local/repository/server.properties /home/playground/kafka_2.12/config

# Copy prometheus exporter library
cp /local/repository/jmx_prometheus_javaagent.jar /home/playground/kafka_2.12/libs/jmx_prometheus_javaagent.jar

# Copy prometheus exporter config (broker exporter, port 7075). Per-job
# Kafka Streams instances (port 7076) use a separate minimal config -- see
# jmx_exporter_kafkastreams_app.yml and install_kafkastreams_only.sh.
cp /local/repository/jmx_exporter.yml /home/playground/kafka_2.12/config/jmx_exporter.yml
cp /local/repository/jmx_exporter_kafkastreams_app.yml /home/playground/kafka_2.12/config/jmx_exporter_kafkastreams_app.yml

# Enable Spark's own PrometheusServlet (spark.metrics.conf on the shared
# spark-submit command points here) -- powers both the built-in
# spark_streaming_* query metrics and the custom OperatorMetricsSource
# gauges, scraped by the "spark_driver" Prometheus target.
cp /local/repository/spark-metrics.properties /home/playground/spark-3.5.1-bin-hadoop3/conf/metrics.properties

# libpython3.12 -- RisingWave's binary dynamically links against it (Python
# UDF support), confirmed live even for the "plain" release tarball, on a
# stock Ubuntu 22.04 node (ships Python 3.10, not 3.12). See
# install_risingwave_only.sh for the full explanation.
sudo add-apt-repository -y ppa:deadsnakes/ppa
sudo apt-get update -qq
sudo apt-get install -y -qq libpython3.12

# Download RisingWave (single self-contained binary, no JVM/Maven toolchain
# needed, unlike Flink/Storm/Spark -- see
# KAFKA_STREAMS_RISINGWAVE_PLAN.md Part B.1).
curl -L https://github.com/risingwavelabs/risingwave/releases/download/v3.0.1/risingwave-v3.0.1-x86_64-unknown-linux.tar.gz > /home/playground/zip/risingwave.tar.gz
mkdir -p /home/playground/risingwave
tar zxf /home/playground/zip/risingwave.tar.gz -C /home/playground/risingwave

# MinIO + mc -- shared object store for Hummock across a real multi-node
# RisingWave cluster (local-filesystem storage only works single-node, see
# risingwave-minio-start.yaml and install_risingwave_only.sh for the full
# explanation).
curl -L https://dl.min.io/server/minio/release/linux-amd64/minio -o /home/playground/minio
chmod +x /home/playground/minio
curl -L https://dl.min.io/client/mc/release/linux-amd64/mc -o /home/playground/mc
chmod +x /home/playground/mc

#rm -r /local/repository

#sudo rm -r /home/playground/zip

