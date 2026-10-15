#!/bin/bash
# Provisions a set of raw CloudLab nodes for PStreamBench and creates a
# cluster from them via the controller API (Apache Spark by default; set
# CLUSTER_TYPE for any other engine) -- everything that was previously done
# by hand, one node reallocation at a time.
#
# Usage:
#   ./setup_new_cluster.sh <master_host> <worker_host> [<worker_host> ...]
#
# Example:
#   ./setup_new_cluster.sh ms1037.utah.cloudlab.us ms1302.utah.cloudlab.us \
#       ms1325.utah.cloudlab.us ms1303.utah.cloudlab.us ms1312.utah.cloudlab.us
#
# Env vars (all optional, shown with their defaults):
#   SSH_USER=pagnih03
#   CONTROLLER_URL=http://localhost:8000
#   CLUSTER_TYPE=Apache Spark   # set to "Apache Kafka Streams" (or any other
#                               # cluster_type the controller understands) to
#                               # provision a different engine
#   CLUSTER_NAME=spark-cluster-<timestamp>   # (prefix follows CLUSTER_TYPE
#                                             # once set -- see below)
#   CLUSTER_SLOTS=8
#   HARDWARE_TYPE=m510
#   DATASET_BATCH_SIZE=3   # nodes per batch when running download_datasets.sh,
#                          # to avoid Kaggle API rate limiting
#   INSTALL_SCRIPT=install_all_tools.sh  # set to install_spark_only.sh or
#                                         # install_kafkastreams_only.sh to
#                                         # skip the slow Flink/Storm/Grafana
#                                         # downloads when only one engine is
#                                         # needed
#
# What it does, per node, in order:
#   1. rsync this directory to /local/repository (resumable on network drops)
#   2. pre_conditioning.sh  (Java/Python, /home/playground mount, kaggle creds)
#   3. install_all_tools.sh (Kafka/Spark/Flink/Storm/Prometheus/Grafana)
#   4. download_datasets.sh (in small batches, sequentially, to dodge Kaggle
#      rate limits -- this is the one step that reliably breaks if all nodes
#      hit the Kaggle API at once)
# Then, once every node is done:
#   5. Register each node with the controller (POST /infra/create/node/0)
#   6. Create the cluster (POST /infra/create) -- this is what actually
#      triggers Kafka+Zookeeper start, the chosen engine's per-node
#      start/deploy (or, for Kafka Streams, nothing beyond Kafka/Zookeeper --
#      see the Part A.1 no-op cluster-create branch in views.py), Prometheus
#      +Grafana on the master, node_exporter on the workers.
#
# Safe to re-run: rsync/pre_conditioning.sh/install_all_tools.sh/
# download_datasets.sh are all idempotent-ish (they overwrite in place),
# and node/cluster creation will just fail loudly with a clear message if
# a node or cluster name is already registered.

set -uo pipefail

SSH_USER="${SSH_USER:-pagnih03}"
CONTROLLER_URL="${CONTROLLER_URL:-http://localhost:8000}"
CLUSTER_TYPE="${CLUSTER_TYPE:-Apache Spark}"
CLUSTER_NAME="${CLUSTER_NAME:-spark-cluster-$(date +%s)}"
CLUSTER_SLOTS="${CLUSTER_SLOTS:-8}"
HARDWARE_TYPE="${HARDWARE_TYPE:-m510}"
DATASET_BATCH_SIZE="${DATASET_BATCH_SIZE:-3}"
INSTALL_SCRIPT="${INSTALL_SCRIPT:-install_all_tools.sh}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LOG_DIR="$(mktemp -d /tmp/pstreambench-setup.XXXXXX)"

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <master_host> <worker_host> [<worker_host> ...]" >&2
    exit 1
fi

MASTER="$1"
shift
WORKERS=("$@")
ALL_HOSTS=("$MASTER" "${WORKERS[@]}")

echo "=== PStreamBench cluster setup ==="
echo "Master:  $MASTER"
echo "Workers: ${WORKERS[*]}"
echo "Cluster: $CLUSTER_NAME (slots=$CLUSTER_SLOTS, hardware=$HARDWARE_TYPE)"
echo "Logs:    $LOG_DIR"
echo

ssh_opts=(-o ConnectTimeout=10 -o BatchMode=yes)

fail() {
    echo "FATAL: $1" >&2
    exit 1
}

step_header() {
    echo
    echo "--- $1 ---"
}

# ---------------------------------------------------------------------------
# Step 0: trust host keys (first-time connection to a new node)
# ---------------------------------------------------------------------------
step_header "Adding host keys"
for host in "${ALL_HOSTS[@]}"; do
    ssh-keyscan -H "$host" >> ~/.ssh/known_hosts 2>/dev/null
done

# ---------------------------------------------------------------------------
# Step 1: copy setup files (resumable -- rsync --partial picks up where a
# dropped connection left off on re-run, unlike plain scp)
# ---------------------------------------------------------------------------
step_header "Copying setup files to all nodes"
for host in "${ALL_HOSTS[@]}"; do
    (
        ssh "${ssh_opts[@]}" "$SSH_USER@$host" \
            "sudo mkdir -p /local/repository && sudo chown $SSH_USER /local/repository" \
            || { echo "$host: failed to prep /local/repository" > "$LOG_DIR/${host}_copy.log"; exit 1; }
        for attempt in 1 2 3 4 5; do
            rsync -az --partial --timeout=60 -e "ssh -o ConnectTimeout=10" \
                "$SCRIPT_DIR/" "$SSH_USER@$host:/local/repository/" \
                > "$LOG_DIR/${host}_copy.log" 2>&1 && exit 0
            sleep 5
        done
        echo "$host: rsync failed after 5 attempts" >> "$LOG_DIR/${host}_copy.log"
        exit 1
    ) &
done
wait
for host in "${ALL_HOSTS[@]}"; do
    count=$(ssh "${ssh_opts[@]}" "$SSH_USER@$host" "ls /local/repository/ 2>/dev/null | wc -l" | tr -d ' ')
    src_count=$(ls "$SCRIPT_DIR" | wc -l | tr -d ' ')
    if [ "$count" != "$src_count" ]; then
        fail "$host: file copy incomplete ($count/$src_count files) -- check $LOG_DIR/${host}_copy.log"
    fi
done
echo "All nodes have the full file set."

# ---------------------------------------------------------------------------
# Step 2: pre_conditioning.sh
# ---------------------------------------------------------------------------
step_header "Running pre_conditioning.sh on all nodes"
for host in "${ALL_HOSTS[@]}"; do
    ssh "${ssh_opts[@]}" "$SSH_USER@$host" "cd /local/repository && sudo bash pre_conditioning.sh" \
        > "$LOG_DIR/${host}_precondition.log" 2>&1 &
done
wait
for host in "${ALL_HOSTS[@]}"; do
    if ! ssh "${ssh_opts[@]}" "$SSH_USER@$host" "mount | grep -q playground"; then
        fail "$host: /home/playground did not get mounted -- check $LOG_DIR/${host}_precondition.log"
    fi
done
echo "/home/playground mounted on all nodes."

# ---------------------------------------------------------------------------
# Step 3: install_all_tools.sh (or install_spark_only.sh)
# ---------------------------------------------------------------------------
step_header "Running $INSTALL_SCRIPT on all nodes"
for host in "${ALL_HOSTS[@]}"; do
    ssh "${ssh_opts[@]}" "$SSH_USER@$host" "cd /local/repository && sudo bash $INSTALL_SCRIPT" \
        > "$LOG_DIR/${host}_installtools.log" 2>&1 &
done
wait
for host in "${ALL_HOSTS[@]}"; do
    # kafka_2.12 is installed by every *_only.sh variant (Kafka is shared
    # broker infra for every engine). install_kafkastreams_only.sh and
    # install_risingwave_only.sh are the two variants that do NOT also
    # install Spark (Kafka Streams is a library, not a cluster binary --
    # see KAFKA_STREAMS_RISINGWAVE_PLAN.md Part A.1; RisingWave needs no
    # JVM/Maven toolchain at all, see install_risingwave_only.sh's own
    # header comment) -- confirmed live (2026-08-10) this branch was
    # missing install_risingwave_only.sh, causing a false-negative FATAL
    # on an otherwise-successful install (kafka_2.12 and risingwave were
    # both present, spark-3.5.1-bin-hadoop3 was correctly absent). Every
    # other script, including the default install_all_tools.sh, does
    # install Spark, so this must default to checking for it and only skip
    # that check for the variants that truly don't produce it -- and for
    # RisingWave specifically, check for its own binary dir instead of
    # just skipping the Spark check with no positive check at all.
    check="test -d /home/playground/kafka_2.12"
    case "$INSTALL_SCRIPT" in
        install_kafkastreams_only.sh) ;;
        install_risingwave_only.sh) check="$check && test -d /home/playground/risingwave" ;;
        *) check="$check && test -d /home/playground/spark-3.5.1-bin-hadoop3" ;;
    esac
    if ! ssh "${ssh_opts[@]}" "$SSH_USER@$host" "$check"; then
        fail "$host: expected install output missing after $INSTALL_SCRIPT -- check $LOG_DIR/${host}_installtools.log"
    fi
done
echo "$INSTALL_SCRIPT completed on all nodes."

# ---------------------------------------------------------------------------
# Step 4: download_datasets.sh, in small batches (Kaggle rate-limits
# aggressively if every node hits the API at the same time)
# ---------------------------------------------------------------------------
step_header "Running download_datasets.sh (batches of $DATASET_BATCH_SIZE, to dodge Kaggle rate limits)"
batch=()
run_batch() {
    for host in "${batch[@]}"; do
        ssh "${ssh_opts[@]}" "$SSH_USER@$host" "cd /local/repository && bash download_datasets.sh" \
            > "$LOG_DIR/${host}_datasets.log" 2>&1 &
    done
    wait
}
for host in "${ALL_HOSTS[@]}"; do
    batch+=("$host")
    if [ "${#batch[@]}" -ge "$DATASET_BATCH_SIZE" ]; then
        run_batch
        batch=()
    fi
done
if [ "${#batch[@]}" -gt 0 ]; then
    run_batch
fi
for host in "${ALL_HOSTS[@]}"; do
    if grep -qi "429\|Missing username" "$LOG_DIR/${host}_datasets.log" 2>/dev/null; then
        echo "WARNING: $host hit a dataset download error (rate limit or credentials) -- check $LOG_DIR/${host}_datasets.log" >&2
        echo "         Retry manually with: ssh $SSH_USER@$host 'cd /local/repository && bash download_datasets.sh'" >&2
    fi
done
echo "Dataset download pass complete (see warnings above for any that need a retry)."

# ---------------------------------------------------------------------------
# Step 5: register nodes with the controller
# ---------------------------------------------------------------------------
step_header "Registering nodes with the controller"
for host in "${ALL_HOSTS[@]}"; do
    resp=$(curl -s -X POST "$CONTROLLER_URL/infra/create/node/0" \
        -H "Content-Type: application/json" \
        -d "{\"domain_name\": \"$host\", \"user\": \"$SSH_USER\"}")
    echo "$host: $resp"
done

# ---------------------------------------------------------------------------
# Step 6: create the cluster (this is what actually starts Kafka/Zookeeper,
# Spark master+workers, Prometheus, Grafana, node_exporter)
# ---------------------------------------------------------------------------
step_header "Creating cluster '$CLUSTER_NAME'"
num_workers="${#WORKERS[@]}"
resp=$(curl -s -X POST "$CONTROLLER_URL/infra/create" \
    -H "Content-Type: application/json" \
    -d "{\"cluster_name\": \"$CLUSTER_NAME\", \"cluster_type\": \"$CLUSTER_TYPE\", \"cluster_num_tm\": $num_workers, \"cluster_tm_slots\": $CLUSTER_SLOTS, \"hardware_cloudlab\": \"$HARDWARE_TYPE\"}" \
    --max-time 1800)
echo "$resp"

echo
echo "=== Done. Logs kept at $LOG_DIR ==="
