# Spark-only subset of install_all_tools.sh -- skips Flink/Storm/Grafana
# downloads (their upstream mirrors have been extremely slow), for when only
# Spark benchmarking is needed. Installs Kafka + Spark + Prometheus + the
# Kafka JMX exporter, same paths/config install_all_tools.sh uses, so the
# controller's Ansible playbooks work identically either way.

# Copy custom_producer.py
cp /local/repository/custom_producer.py /home/playground/

# Downloading spark (Standalone mode -- no Hadoop/YARN cluster required)
curl -0 https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz --output /home/playground/zip/spark.tgz
tar zxf /home/playground/zip/spark.tgz -C /home/playground/

# Downloading prometheus
curl -L https://github.com/prometheus/prometheus/releases/download/v2.42.0/prometheus-2.42.0.linux-amd64.tar.gz > /home/playground/zip/prometheus.tar.gz
tar zxf /home/playground/zip/prometheus.tar.gz -C /home/playground/

# Download kafka
curl -L https://archive.apache.org/dist/kafka/3.8.0/kafka_2.12-3.8.0.tgz > /home/playground/zip/kafka.tgz
tar zxf /home/playground/zip/kafka.tgz -C /home/playground/
mv /home/playground/kafka_2.12-3.8.0 /home/playground/kafka_2.12

# removing default kafka property
rm /home/playground/kafka_2.12/config/server.properties

# Copying the server.properties to /home/playground
cp /local/repository/server.properties /home/playground/kafka_2.12/config

# Copy prometheus exporter library
cp /local/repository/jmx_prometheus_javaagent.jar /home/playground/kafka_2.12/libs/jmx_prometheus_javaagent.jar

# Copy prometheus exporter config
cp /local/repository/jmx_exporter.yml /home/playground/kafka_2.12/config/jmx_exporter.yml

# Enable Spark's own PrometheusServlet (spark.metrics.conf on the shared
# spark-submit command points here) -- powers both the built-in
# spark_streaming_* query metrics and the custom OperatorMetricsSource
# gauges, scraped by the "spark_driver" Prometheus target.
cp /local/repository/spark-metrics.properties /home/playground/spark-3.5.1-bin-hadoop3/conf/metrics.properties
