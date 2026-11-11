#!/bin/bash
# Remote (real CloudLab, multi-node) setup for RisingWave benchmark testing.
# Counterpart of setup_local.sh -- same idempotent-step philosophy, but
# provisions a genuine distributed cluster (Kafka+ZK across all nodes,
# RisingWave meta+frontend+MinIO on the master, one compute node per
# worker) instead of a single local process.
#
# Confirmed live end-to-end (2026-07-27) on a fresh 4-node CloudLab m510
# allocation (1 master + 3 workers): all 24 workload/query combos ran with
# zero null metrics after the fixes this script's steps already include.
#
# Prerequisites this script assumes (not automated -- these are one-time,
# manual CloudLab account/experiment setup steps, not per-run steps):
#   - An active CloudLab experiment with the node hostnames you pass in.
#   - Passwordless SSH (key auth) from this machine to every node as SSH_USER.
#   - Passwordless sudo on every node for SSH_USER (CloudLab default).
#   - `ansible-playbook` available locally (this script pip-installs it into
#     a venv if missing -- see ANSIBLE_VENV below).
#
# No dataset transfer to the remote nodes is needed or performed -- Kafka's
# default advertised hostname resolves externally on CloudLab (confirmed:
# node0.<experiment>.utah.cloudlab.us is a public CNAME for its ms____
# hostname), so producers run from wherever you invoke them (typically this
# machine), the same way custom_producer.py already works locally.
#
# Usage:
#   ./setup_remote.sh <main_node> <worker_node1> [worker_node2] ...
#   ./setup_remote.sh --stop <main_node> <worker_node1> [worker_node2] ...
#
# Example (the 4-node cluster this script was validated against):
#   ./setup_remote.sh ms1320.utah.cloudlab.us ms1344.utah.cloudlab.us \
#       ms1319.utah.cloudlab.us ms1325.utah.cloudlab.us

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CONTROLLER_DIR="$SCRIPT_DIR/../PStreamBench_controller/dsp_be"
PLAYBOOK_DIR="$CONTROLLER_DIR/utils/playbook"
SSH_USER="${SSH_USER:-pagnih03}"
SSH_KEY="${SSH_KEY:-$HOME/.ssh/id_rsa}"
ANSIBLE_VENV="${ANSIBLE_VENV:-$HOME/.pstreambench_ansible_venv}"

log() { echo "[setup_remote] $*"; }

SUBCOMMAND=""
if [ "$1" = "--stop" ]; then
  SUBCOMMAND="stop"
  shift
fi

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 [--stop] <main_node> <worker_node1> [worker_node2] ..." >&2
  exit 1
fi

MAIN_NODE="$1"; shift
WORKER_NODES=("$@")
ALL_NODES=("$MAIN_NODE" "${WORKER_NODES[@]}")

ssh_node() { ssh -i "$SSH_KEY" -o ConnectTimeout=10 "$SSH_USER@$1" "${@:2}"; }

# --- --stop: kill every service on every node, then exit ------------------
if [ "$SUBCOMMAND" = "stop" ]; then
  log "Stopping RisingWave/Kafka/Zookeeper/Prometheus/MinIO/node_exporter on all ${#ALL_NODES[@]} nodes..."
  for host in "${ALL_NODES[@]}"; do
    ssh_node "$host" "
      pkill -f '[r]isingwave meta' || true
      pkill -f '[r]isingwave frontend' || true
      pkill -f '[r]isingwave compute' || true
      pkill -f '[m]inio server' || true
      pkill -f '[p]rometheus' || true
      pkill -f '[n]ode_exporter' || true
      sudo pkill -f '[k]afka.Kafka' || true
      sudo pkill -f '[z]ookeeper' || true
    " &
  done
  wait
  log "Done."
  exit 0
fi

# --- 0. Local ansible-playbook availability --------------------------------
if ! command -v ansible-playbook >/dev/null 2>&1; then
  if [ ! -x "$ANSIBLE_VENV/bin/ansible-playbook" ]; then
    log "ansible-playbook not found -- creating a venv at $ANSIBLE_VENV and installing ansible..."
    python3 -m venv "$ANSIBLE_VENV"
    "$ANSIBLE_VENV/bin/pip" install --quiet ansible
  fi
  ANSIBLE_PLAYBOOK_BIN="$ANSIBLE_VENV/bin/ansible-playbook"
else
  ANSIBLE_PLAYBOOK_BIN="$(command -v ansible-playbook)"
fi
log "Using ansible-playbook: $ANSIBLE_PLAYBOOK_BIN"

# --- 1. Per-node bootstrap install (idempotent, parallel) ------------------
# Installs the same binaries install_risingwave_only.sh does, but pulled
# directly via curl on each node instead of assuming /local/repository is
# mounted (confirmed live 2026-07-27: a plain CloudLab node allocation does
# NOT have /local/repository -- that only exists when the experiment is
# instantiated from a profile that mounts this repo, which a manual/ad hoc
# allocation does not do).
BOOTSTRAP_SCRIPT=$(mktemp)
cat > "$BOOTSTRAP_SCRIPT" << 'BOOTSTRAP_EOF'
#!/bin/bash
set -e
if [ -x /home/playground/risingwave/risingwave ] && [ -x /home/playground/kafka_2.12/bin/kafka-server-start.sh ]; then
  echo "ALREADY_BOOTSTRAPPED"
  exit 0
fi
sudo mkdir -p /home/playground/zip /home/playground/kafka-logs
sudo chown -R "$(id -u)":"$(id -g)" /home/playground

sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3-pip python3-venv unzip openjdk-11-jdk

# Producer dependencies always go into a dedicated venv, never system pip
# (see feedback_use_venv_for_python project memory) -- referenced by its
# full path wherever custom_producer.py is invoked on this node (e.g.
# tasks/producer/producer-run.yaml), not via the bare `python3`/`pip3`.
python3 -m venv /home/playground/venv
/home/playground/venv/bin/pip install --quiet --upgrade pip
/home/playground/venv/bin/pip install --quiet click confluent-kafka pandas

# libpython3.12 -- RisingWave v3.0.1 binary dynamically links against it
# (Python UDF support). Confirmed live (2026-07-21): omitting this produces
# "error while loading shared libraries: libpython3.12.so.1.0" on stock
# Ubuntu 22.04 (ships Python 3.10, not 3.12).
sudo add-apt-repository -y ppa:deadsnakes/ppa
sudo apt-get update -qq
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -qq libpython3.12

if [ ! -d /home/playground/prometheus-2.42.0.linux-amd64 ]; then
  curl -sL https://github.com/prometheus/prometheus/releases/download/v2.42.0/prometheus-2.42.0.linux-amd64.tar.gz -o /home/playground/zip/prometheus.tar.gz
  tar zxf /home/playground/zip/prometheus.tar.gz -C /home/playground/
fi

if [ ! -d /home/playground/kafka_2.12 ]; then
  curl -sL https://archive.apache.org/dist/kafka/3.8.0/kafka_2.12-3.8.0.tgz -o /home/playground/zip/kafka.tgz
  tar zxf /home/playground/zip/kafka.tgz -C /home/playground/
  mv /home/playground/kafka_2.12-3.8.0 /home/playground/kafka_2.12
  rm -f /home/playground/kafka_2.12/config/server.properties
fi

if [ ! -x /home/playground/risingwave/risingwave ]; then
  curl -sL https://github.com/risingwavelabs/risingwave/releases/download/v3.0.1/risingwave-v3.0.1-x86_64-unknown-linux.tar.gz -o /home/playground/zip/risingwave.tar.gz
  mkdir -p /home/playground/risingwave
  tar zxf /home/playground/zip/risingwave.tar.gz -C /home/playground/risingwave
fi

if [ ! -x /home/playground/minio ]; then
  curl -sL https://dl.min.io/server/minio/release/linux-amd64/minio -o /home/playground/minio
  chmod +x /home/playground/minio
  curl -sL https://dl.min.io/client/mc/release/linux-amd64/mc -o /home/playground/mc
  chmod +x /home/playground/mc
fi

echo "BOOTSTRAP_DONE"
BOOTSTRAP_EOF

log "Bootstrapping ${#ALL_NODES[@]} nodes in parallel (skips nodes already provisioned)..."
for host in "${ALL_NODES[@]}"; do
  scp -q -i "$SSH_KEY" -o ConnectTimeout=10 "$BOOTSTRAP_SCRIPT" "$SSH_USER@$host:/tmp/bootstrap.sh" &
done
wait
for host in "${ALL_NODES[@]}"; do
  ssh_node "$host" "bash /tmp/bootstrap.sh" > "/tmp/pstreambench_bootstrap_$host.log" 2>&1 &
done
wait
rm -f "$BOOTSTRAP_SCRIPT"
for host in "${ALL_NODES[@]}"; do
  if grep -qE "BOOTSTRAP_DONE|ALREADY_BOOTSTRAPPED" "/tmp/pstreambench_bootstrap_$host.log"; then
    log "  [ok] $host bootstrapped."
  else
    log "  [WARN] $host bootstrap may have failed -- check /tmp/pstreambench_bootstrap_$host.log"
  fi
done

# --- 2. Config files (server.properties, JMX exporter) ---------------------
log "Copying Kafka config + JMX exporter files to all nodes..."
for host in "${ALL_NODES[@]}"; do
  (
    scp -q -i "$SSH_KEY" -o ConnectTimeout=10 \
      "$SCRIPT_DIR/server.properties" "$SCRIPT_DIR/jmx_prometheus_javaagent.jar" "$SCRIPT_DIR/jmx_exporter.yml" \
      "$SSH_USER@$host:/tmp/"
    ssh_node "$host" "
      cp /tmp/server.properties /home/playground/kafka_2.12/config/server.properties
      cp /tmp/jmx_prometheus_javaagent.jar /home/playground/kafka_2.12/libs/jmx_prometheus_javaagent.jar
      cp /tmp/jmx_exporter.yml /home/playground/kafka_2.12/config/jmx_exporter.yml
    "
  ) &
done
wait

# --- 3. Generate inventory.yml + prometheus.yml -----------------------------
log "Generating inventory.yml and prometheus.yml for this node set..."
{
  echo "all:"
  echo "  hosts:"
  i=0
  for host in "${ALL_NODES[@]}"; do
    echo "    $host:"
    echo "      ansible_user: $SSH_USER"
    echo "      ansible_ssh_private_key_file: $SSH_KEY"
    echo "      id: $i"
    i=$((i + 1))
  done
  echo "alpha:"
  echo "  hosts:"
  echo "    $MAIN_NODE:"
  echo "      ansible_user: $SSH_USER"
  echo "      ansible_ssh_private_key_file: $SSH_KEY"
  echo "      id: 0"
  echo "betas:"
  echo "  hosts:"
  i=0
  for host in "${WORKER_NODES[@]}"; do
    echo "    $host:"
    echo "      ansible_user: $SSH_USER"
    echo "      ansible_ssh_private_key_file: $SSH_KEY"
    echo "      id: $i"
    i=$((i + 1))
  done
} > "$PLAYBOOK_DIR/inventory.yml"

MAIN_SHORT="${MAIN_NODE%%.*}"
WORKER_SHORTS=()
for host in "${WORKER_NODES[@]}"; do WORKER_SHORTS+=("${host%%.*}"); done
join_by() { local IFS="$1"; shift; echo "$*"; }

python3 - "$CONTROLLER_DIR/infra/prometheus_files/prometheus.yml" "$MAIN_NODE" "$MAIN_SHORT" "${WORKER_NODES[@]}" << 'PYEOF'
import sys, yaml
target, main_node, main_short = sys.argv[1], sys.argv[2], sys.argv[3]
workers = sys.argv[4:]
worker_shorts = [w.split(".")[0] for w in workers]
with open(target) as f:
    cfg = yaml.safe_load(f)
by_job = {sc["job_name"]: sc for sc in cfg["scrape_configs"]}
by_job["taskmanager"]["static_configs"][0]["targets"] = [f"{w}:9250" for w in workers]
by_job["kafka"]["static_configs"][0]["targets"] = ["localhost:7075"] + [f"{w}:7075" for w in worker_shorts]
by_job["node_exporter"]["static_configs"][0]["targets"] = [f"{w}:9100" for w in workers]
by_job["spark_driver"]["static_configs"][0]["targets"] = [f"{main_short}:4040"]
by_job["kafkastreams_app"]["static_configs"][0]["targets"] = ["localhost:7076"] + [f"{w}:7076" for w in worker_shorts]
by_job["risingwave"]["static_configs"][0]["targets"] = [f"{main_node}:1250", f"{main_node}:2222"] + [f"{w}:1222" for w in workers]
with open(target, "w") as f:
    yaml.dump(cfg, f, sort_keys=False, default_flow_style=None)
PYEOF

# --- 4. Run Ansible playbooks ------------------------------------------------
log "Starting Kafka + Zookeeper on all nodes..."
ZK_SERVERS=""
i=0
for host in "${ALL_NODES[@]}"; do
  short="${host%%.*}"
  ZK_SERVERS="${ZK_SERVERS}server.${i}=${short}:2888:3888"$'\n'
  i=$((i + 1))
done
"$ANSIBLE_PLAYBOOK_BIN" -i "$PLAYBOOK_DIR/inventory.yml" --private-key "$SSH_KEY" \
  -e "BASE_DIR=$CONTROLLER_DIR INSTALLATION_DIR=/home/playground BROKER_ID=0" \
  -e "ZOOKEEPER_SERVERS=${ZK_SERVERS}" \
  "$PLAYBOOK_DIR/cluster-complex-kafka-start.yaml"

log "Starting RisingWave meta+frontend+MinIO+Prometheus on $MAIN_NODE..."
log "(Grafana steps will fail unless Grafana is separately installed -- harmless, not needed for metrics collection via Prometheus directly.)"
"$ANSIBLE_PLAYBOOK_BIN" -i "$PLAYBOOK_DIR/inventory.yml" --private-key "$SSH_KEY" \
  -e "BASE_DIR=$CONTROLLER_DIR INSTALLATION_DIR=/home/playground" \
  "$PLAYBOOK_DIR/cluster-complex-risingwave-master-create.yaml" || true

log "Starting RisingWave compute + node_exporter on worker nodes..."
"$ANSIBLE_PLAYBOOK_BIN" -i "$PLAYBOOK_DIR/inventory.yml" --private-key "$SSH_KEY" \
  -e "BASE_DIR=$CONTROLLER_DIR INSTALLATION_DIR=/home/playground main_ip=$MAIN_NODE" \
  "$PLAYBOOK_DIR/cluster-complex-risingwave-worker-create.yaml"

log ""
log "=== Ready ==="
log "RisingWave: psql -h $MAIN_NODE -p 4566 -d dev -U root"
log "Kafka:      $MAIN_NODE:9092"
log "Prometheus: http://$MAIN_NODE:9090"
log ""
log "Verify: psql -h $MAIN_NODE -p 4566 -d dev -U root -c 'SELECT * FROM rw_catalog.rw_worker_nodes;'"
log "        should show 1 META + 1 FRONTEND + ${#WORKER_NODES[@]} COMPUTE_NODE, all RUNNING."
log ""
log "NOTE: DROP SINK/MATERIALIZED VIEW/SOURCE against a real distributed"
log "cluster can take 30-60+ seconds (confirmed live 2026-07-27) -- if"
log "RisingWave prints 'has taken more than 30 secs, likely due to high"
log "barrier latency', run 'RECOVER;' in a separate psql session, per its"
log "own suggestion. This is normal distributed behavior, not a hang."
log ""
log "To stop everything: $0 --stop $MAIN_NODE ${WORKER_NODES[*]}"
