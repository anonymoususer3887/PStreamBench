# Storm-only subset of install_all_tools.sh -- skips Flink/Spark/RisingWave
# downloads (their upstream mirrors have been extremely slow, same reasoning
# as install_spark_only.sh), for when only Storm benchmarking is needed.
# Installs Kafka + Storm + Prometheus + Grafana + the Kafka JMX exporter,
# same paths/config install_all_tools.sh uses, so the controller's Ansible
# playbooks work identically either way. Grafana is included here (unlike
# the other _only scripts) because Storm is one of the two engines with a
# working live Grafana dashboard today (the other being Flink -- see
# install_flink_only.sh).

# Copy custom_producer.py
cp /local/repository/custom_producer.py /home/playground/

# Downloading storm
curl -0 https://archive.apache.org/dist/storm/apache-storm-2.6.2/apache-storm-2.6.2.tar.gz --output /home/playground/zip/storm.tar.gz
tar zxf /home/playground/zip/storm.tar.gz -C /home/playground/

# Setup storm exporter
sudo pip install prometheus_client
cp /local/repository/storm_exporter.py /home/playground/apache-storm-2.6.2/bin

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
