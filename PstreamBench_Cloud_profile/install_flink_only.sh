# Flink-only subset of install_all_tools.sh -- skips Storm/Spark/RisingWave
# downloads (their upstream mirrors have been extremely slow, same reasoning
# as install_spark_only.sh), for when only Flink benchmarking is needed.
# Installs Kafka + Flink + Prometheus + Grafana + the Kafka JMX exporter,
# same paths/config install_all_tools.sh uses, so the controller's Ansible
# playbooks work identically either way. Grafana is included here (unlike
# the other _only scripts) because Flink is one of the two engines with a
# working live Grafana dashboard today (the other being Storm -- see
# install_storm_only.sh).

# Copy custom_producer.py
cp /local/repository/custom_producer.py /home/playground/

# Downloading flink
curl -0 https://archive.apache.org/dist/flink/flink-1.16.2/flink-1.16.2-bin-scala_2.12.tgz --output /home/playground/zip/flink.tgz
tar zxf /home/playground/zip/flink.tgz -C /home/playground/

# Copy oshi, jna and jna_platform (Flink's own system-metrics libraries)
cp /local/repository/jna-platform-5.10.0.jar /home/playground/flink-1.16.2/lib
cp /local/repository/jna-5.10.0.jar /home/playground/flink-1.16.2/lib
cp /local/repository/oshi-core-6.1.5.jar /home/playground/flink-1.16.2/lib

# Copy dsp_jobs jar
cp /local/repository/dsp_jobs-1.0-SNAPSHOT.jar /home/playground/flink-1.16.2/bin

# Copy roads.geojson (Linear Road job)
cp /local/repository/roads.geojson /home/playground/flink-1.16.2/bin

# Downloading prometheus
curl -L https://github.com/prometheus/prometheus/releases/download/v2.42.0/prometheus-2.42.0.linux-amd64.tar.gz > /home/playground/zip/prometheus.tar.gz
tar zxf /home/playground/zip/prometheus.tar.gz -C /home/playground/

# Download grafana
curl -L https://dl.grafana.com/enterprise/release/grafana-enterprise-9.3.6.linux-amd64.tar.gz > /home/playground/zip/grafana.tar.gz
tar zxf /home/playground/zip/grafana.tar.gz -C /home/playground/

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
