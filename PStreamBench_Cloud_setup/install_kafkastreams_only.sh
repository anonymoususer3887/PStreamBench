# Kafka Streams-only subset of install_all_tools.sh, mirroring
# install_spark_only.sh's shape -- skips Flink/Storm/Spark/Grafana
# downloads, for when only Kafka Streams benchmarking is needed. Installs
# Kafka + Prometheus + the Kafka JMX exporter, same paths/config
# install_all_tools.sh uses, so the controller's Ansible playbooks work
# identically either way.
#
# Unlike Flink/Spark/Storm, there is no separate engine binary to download
# here -- Kafka Streams is a library, not a cluster daemon (see
# KAFKA_STREAMS_RISINGWAVE_PLAN.md Part A.1). The job jar itself is built
# from PStreamBench_workload_generator/kafkastreams/dsp_kafkastreams_jobs
# and copied to each node at job-submit time by kafkastreams-setup-job.yaml,
# not baked in here.

# Copy custom_producer.py
cp /local/repository/custom_producer.py /home/playground/

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

# Copy prometheus exporter library (reused for both the Kafka broker's own
# JMX, port 7075, and every Kafka Streams app instance's JMX, port 7076 --
# see kafkastreams-provide-job.yaml).
cp /local/repository/jmx_prometheus_javaagent.jar /home/playground/kafka_2.12/libs/jmx_prometheus_javaagent.jar

# Copy prometheus exporter config (includes the kafka.streams rules added
# for this engine, see jmx_exporter.yml) -- used by the broker's own
# exporter (port 7075). Per-job instances (port 7076) use a separate,
# minimal config (jmx_exporter_kafkastreams_app.yml) containing only the
# kafka.streams rules, since a per-job JVM is a Streams client, never a
# broker, and can't expose any kafka.server/kafka.coordinator bean the
# broker-oriented rules in jmx_exporter.yml match against -- see that
# file's header comment and kafkastreams-provide-job.yaml for why carrying
# the full shared config there caused every per-job JMX HTTP server to
# hang within seconds of starting.
cp /local/repository/jmx_exporter.yml /home/playground/kafka_2.12/config/jmx_exporter.yml
cp /local/repository/jmx_exporter_kafkastreams_app.yml /home/playground/kafka_2.12/config/jmx_exporter_kafkastreams_app.yml
