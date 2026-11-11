# RisingWave-only subset of install_all_tools.sh, mirroring
# install_kafkastreams_only.sh's shape -- skips Flink/Storm/Spark/Grafana
# downloads, for when only RisingWave benchmarking is needed. Installs
# Kafka + Prometheus + the RisingWave binary, same paths/config
# install_all_tools.sh uses, so the controller's Ansible playbooks work
# identically either way.
#
# Unlike Kafka Streams, RisingWave IS a real cluster daemon (meta/frontend/
# compute processes, Part B.1) -- but unlike Flink/Storm/Spark, it needs no
# JVM/Maven toolchain at all, just a single self-contained Rust binary.
# It also needs no JMX-exporter bridge the way Kafka Streams/the Kafka
# broker do -- it exposes Prometheus metrics natively (Part B.4), confirmed
# live via its own :1222/:1250/:2222 endpoints.

# Copy custom_producer.py
cp /local/repository/custom_producer.py /home/playground/

# Downloading prometheus
curl -L https://github.com/prometheus/prometheus/releases/download/v2.42.0/prometheus-2.42.0.linux-amd64.tar.gz > /home/playground/zip/prometheus.tar.gz
tar zxf /home/playground/zip/prometheus.tar.gz -C /home/playground/

# Download kafka (shared infra -- RisingWave workloads read/write the same
# Kafka topics every other engine does)
curl -L https://archive.apache.org/dist/kafka/3.8.0/kafka_2.12-3.8.0.tgz > /home/playground/zip/kafka.tgz
tar zxf /home/playground/zip/kafka.tgz -C /home/playground/
mv /home/playground/kafka_2.12-3.8.0 /home/playground/kafka_2.12

# removing default kafka property
rm /home/playground/kafka_2.12/config/server.properties

# Copying the server.properties to /home/playground
cp /local/repository/server.properties /home/playground/kafka_2.12/config

# Copy prometheus exporter library + config for the Kafka broker's own JMX
# (port 7075) -- reused as-is, no RisingWave-specific rules needed here.
cp /local/repository/jmx_prometheus_javaagent.jar /home/playground/kafka_2.12/libs/jmx_prometheus_javaagent.jar
cp /local/repository/jmx_exporter.yml /home/playground/kafka_2.12/config/jmx_exporter.yml

# libpython3.12 -- RisingWave's binary dynamically links against it (Python
# UDF support), even the "plain" (non-"all-in-one") release tarball.
# Confirmed live (2026-07-21): omitting this produces
# "error while loading shared libraries: libpython3.12.so.1.0: cannot open
# shared object file" on startup, on a stock Ubuntu 22.04 node (which ships
# Python 3.10, not 3.12).
sudo add-apt-repository -y ppa:deadsnakes/ppa
sudo apt-get update -qq
sudo apt-get install -y -qq libpython3.12

# Download RisingWave (single self-contained binary, no install step beyond
# extracting the tarball)
curl -L https://github.com/risingwavelabs/risingwave/releases/download/v3.0.1/risingwave-v3.0.1-x86_64-unknown-linux.tar.gz > /home/playground/zip/risingwave.tar.gz
mkdir -p /home/playground/risingwave
tar zxf /home/playground/zip/risingwave.tar.gz -C /home/playground/risingwave

# MinIO + mc (client) -- only needed on the master node (where meta +
# frontend run), but installed here unconditionally for simplicity, same as
# every other per-node binary this script installs. Provides the shared
# object store the Hummock state store needs across a real multi-node
# cluster. Confirmed live (2026-07-22): a local-filesystem state store
# (hummock+fs://) only works when every RisingWave process shares one
# filesystem (single-node `playground` mode) -- across genuinely separate
# compute nodes it breaks, since hummock+fs is not a shared/networked
# filesystem. See risingwave-minio-start.yaml's header comment for the full
# failure mode. This matches RisingWave's own docker-compose-distributed.yml
# reference architecture, which also uses MinIO for this reason.
curl -L https://dl.min.io/server/minio/release/linux-amd64/minio -o /home/playground/minio
chmod +x /home/playground/minio
curl -L https://dl.min.io/client/mc/release/linux-amd64/mc -o /home/playground/mc
chmod +x /home/playground/mc
