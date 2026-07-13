#!/usr/bin/env python3
"""
PStreamBench Spark benchmark sweep.

Sweeps every validated Spark workload/query across:
  - event rate: 100k, 500k, 1M events/sec (job_class_input)
  - parallelism degree: 2, 4, 8, 16, 40 (80 is reserved for c6525-25g/100g
    hardware -- pass --parallelism-levels to include it on that hardware)

job_pds array rule for each sweep point (see SPARK_VALIDATION.md and the
"benchmark sweep pd rule" project memory):
  - source (first slot) fixed at 2
  - sink (last slot) fixed at 1
  - everything in between = the swept parallelism value
  - jobs whose array has exactly 2 slots (Linear Road Q3/Q4 -- no distinct
    sink stage, "mapper" does compute+output together) become [2, 1] at
    every sweep point, i.e. they have no parallelism dimension at all; the
    sweep only runs them once per event rate instead of once per
    (event rate, parallelism) pair, to avoid submitting the identical job
    five times over.

Each config in configs/*.json is a full job-creation payload as already
validated manually against the controller earlier in this project (correct
job_pds *length*, window sizes, thresholds, etc. per job/query) -- this
script only overrides job_pds, job_class_input, and job_run_time per sweep
point.

Usage:
    python3 run_sweep.py --cluster-id <id> --main-node-ip <host> run
    python3 run_sweep.py --cluster-id <id> --main-node-ip <host> status
    python3 run_sweep.py --cluster-id <id> --main-node-ip <host> run --dry-run
"""
import json
import subprocess
import time
from pathlib import Path

import click
import requests

SCRIPT_DIR = Path(__file__).resolve().parent
CONFIGS_DIR = SCRIPT_DIR / "configs"
MANIFEST_PATH = SCRIPT_DIR / "manifest.json"

DEFAULT_EVENT_RATES = [100_000, 500_000, 1_000_000]
DEFAULT_PARALLELISM_LEVELS = [2, 4, 8, 16, 32, 40]
RUN_TIME_MINUTES = 5
SSH_USER = "pagnih03"
JOB_CREATE_TIMEOUT_SECONDS = 900  # 5 min run + producer/ansible/cleanup overhead


def job_name_for(job_class):
    return job_class.replace(" ", "")


def build_pd_array(base_pds, parallelism):
    n = len(base_pds)
    if n == 2:
        return ["2", "1"]
    return ["2"] + [str(parallelism)] * (n - 2) + ["1"]


def has_parallelism_dimension(base_pds):
    return len(base_pds) != 2


def load_configs():
    configs = []
    for path in sorted(CONFIGS_DIR.glob("*.json")):
        payload = json.loads(path.read_text())
        configs.append({"name": path.stem, "path": path, "base_payload": payload})
    return configs


def load_manifest():
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text())
    return {}


def save_manifest(manifest):
    MANIFEST_PATH.write_text(json.dumps(manifest, indent=2, sort_keys=True))


def run_key(config_name, event_rate, parallelism):
    return f"{config_name}|{event_rate}|{parallelism}"


def ssh_run(main_node_ip, cmd, timeout=30):
    return subprocess.run(
        ["ssh", "-o", "ConnectTimeout=10", f"{SSH_USER}@{main_node_ip}", cmd],
        capture_output=True, text=True, timeout=timeout,
    )


def check_kafka_zk_healthy(main_node_ip):
    try:
        r = ssh_run(main_node_ip, "echo srvr | nc -w3 localhost 2181 | head -1", timeout=20)
        return "Zookeeper version" in r.stdout
    except subprocess.TimeoutExpired:
        return False


def restart_kafka_zk_quorum(nodes):
    """nodes: list of dicts {host, zk_myid, broker_id}, master first."""
    click.echo("  [recover] restarting Zookeeper on all nodes...")
    for n in nodes:
        ssh_run(n["host"],
                "nohup sudo /bin/bash /home/playground/kafka_2.12/bin/zookeeper-server-start.sh "
                "/home/playground/zookeeper.properties > /tmp/zk_restart.log 2>&1 & disown",
                timeout=20)
    time.sleep(25)
    click.echo("  [recover] restarting Kafka brokers on all nodes...")
    for n in nodes:
        ssh_run(n["host"],
                "nohup sudo /bin/bash -c "
                "'KAFKA_OPTS=\"-javaagent:/home/playground/kafka_2.12/libs/jmx_prometheus_javaagent.jar=7075:"
                "/home/playground/kafka_2.12/config/jmx_exporter.yml\" "
                "/home/playground/kafka_2.12/bin/kafka-server-start.sh "
                f"/home/playground/kafka_2.12/config/server.properties --override broker.id={n['broker_id']}' "
                "> /tmp/kafka_restart.log 2>&1 & disown",
                timeout=20)
    time.sleep(25)


def check_driver_log_clean(main_node_ip, job_name):
    try:
        r = ssh_run(main_node_ip,
                    f"grep 'ERROR\\|Exception' /tmp/spark-driver-{job_name}.log 2>/dev/null "
                    f"| grep -v 'aborting\\|aborted\\|CoarseGrainedScheduler\\|TransportRequestHandler'",
                    timeout=20)
        return r.stdout.strip() == ""
    except subprocess.TimeoutExpired:
        return False


def build_grid(configs, event_rates, parallelism_levels):
    grid = []
    for cfg in configs:
        base_pds = cfg["base_payload"]["job_pds"]
        if has_parallelism_dimension(base_pds):
            for event_rate in event_rates:
                for parallelism in parallelism_levels:
                    grid.append((cfg, event_rate, parallelism))
        else:
            for event_rate in event_rates:
                grid.append((cfg, event_rate, None))
    return grid


def submit_job(controller_url, cluster_id, payload):
    resp = requests.post(
        f"{controller_url}/infra/jobcreate/{cluster_id}",
        json=payload,
        timeout=JOB_CREATE_TIMEOUT_SECONDS,
    )
    return resp


@click.group()
@click.option("--cluster-id", required=True, help="Controller cluster UUID")
@click.option("--main-node-ip", required=True, help="Spark master hostname")
@click.option("--controller-url", default="http://localhost:8000")
@click.option("--nodes-file", default=None,
              help="JSON file: [{\"host\":..,\"zk_myid\":..,\"broker_id\":..}] for ZK/Kafka auto-recovery")
@click.option("--event-rates", default=",".join(str(x) for x in DEFAULT_EVENT_RATES),
              help="Comma-separated event rates")
@click.option("--parallelism-levels", default=",".join(str(x) for x in DEFAULT_PARALLELISM_LEVELS),
              help="Comma-separated parallelism levels (add 80 on c6525-25g/100g hardware)")
@click.pass_context
def cli(ctx, cluster_id, main_node_ip, controller_url, nodes_file, event_rates, parallelism_levels):
    ctx.ensure_object(dict)
    ctx.obj["cluster_id"] = cluster_id
    ctx.obj["main_node_ip"] = main_node_ip
    ctx.obj["controller_url"] = controller_url
    ctx.obj["nodes"] = json.loads(Path(nodes_file).read_text()) if nodes_file else []
    ctx.obj["event_rates"] = [int(x) for x in event_rates.split(",")]
    ctx.obj["parallelism_levels"] = [int(x) for x in parallelism_levels.split(",")]


@cli.command()
@click.pass_context
def status(ctx):
    """Print sweep progress."""
    configs = load_configs()
    grid = build_grid(configs, ctx.obj["event_rates"], ctx.obj["parallelism_levels"])
    manifest = load_manifest()
    done = sum(1 for cfg, er, pd in grid if manifest.get(run_key(cfg["name"], er, pd), {}).get("status") == "done")
    failed = sum(1 for cfg, er, pd in grid if manifest.get(run_key(cfg["name"], er, pd), {}).get("status") == "failed")
    click.echo(f"Total sweep points: {len(grid)}")
    click.echo(f"Done:   {done}")
    click.echo(f"Failed: {failed}")
    click.echo(f"Remaining: {len(grid) - done - failed}")
    if failed:
        click.echo("\nFailed runs:")
        for cfg, er, pd in grid:
            entry = manifest.get(run_key(cfg["name"], er, pd), {})
            if entry.get("status") == "failed":
                click.echo(f"  {cfg['name']} rate={er} pd={pd}: {entry.get('error', '?')}")


@cli.command()
@click.option("--dry-run", is_flag=True, help="Print the grid without submitting any jobs")
@click.pass_context
def run(ctx, dry_run):
    """Run the full sweep, resuming from manifest.json if it exists."""
    cluster_id = ctx.obj["cluster_id"]
    main_node_ip = ctx.obj["main_node_ip"]
    controller_url = ctx.obj["controller_url"]
    nodes = ctx.obj["nodes"]

    configs = load_configs()
    grid = build_grid(configs, ctx.obj["event_rates"], ctx.obj["parallelism_levels"])
    manifest = load_manifest()

    if dry_run:
        for cfg, event_rate, parallelism in grid:
            key = run_key(cfg["name"], event_rate, parallelism)
            status_str = manifest.get(key, {}).get("status", "pending")
            click.echo(f"{key} [{status_str}]")
        click.echo(f"\nTotal: {len(grid)} sweep points")
        return

    total = len(grid)
    click.echo(f"=== Starting sweep: {total} total runs ===")

    for i, (cfg, event_rate, parallelism) in enumerate(grid, 1):
        key = run_key(cfg["name"], event_rate, parallelism)
        entry = manifest.get(key, {})
        if entry.get("status") == "done":
            continue

        job_class = cfg["base_payload"]["job_class"]
        query = cfg["base_payload"]["job_query_name"]
        job_name = job_name_for(job_class)
        base_pds = cfg["base_payload"]["job_pds"]
        pd_array = build_pd_array(base_pds, parallelism)

        payload = dict(cfg["base_payload"])
        payload["job_pds"] = pd_array
        payload["job_class_input"] = event_rate
        payload["job_run_time"] = RUN_TIME_MINUTES

        click.echo(f"\n[{i}/{total}] {job_class} q={query} rate={event_rate} pd={pd_array}")

        attempts_left = 2
        last_error = None
        succeeded = False

        while attempts_left > 0 and not succeeded:
            attempts_left -= 1

            if not check_kafka_zk_healthy(main_node_ip):
                if nodes:
                    click.echo("  Kafka/Zookeeper unhealthy, attempting recovery...")
                    restart_kafka_zk_quorum(nodes)
                    if not check_kafka_zk_healthy(main_node_ip):
                        last_error = "kafka/zookeeper recovery failed"
                        continue
                else:
                    last_error = "kafka/zookeeper unhealthy, no --nodes-file to recover"
                    continue

            try:
                resp = submit_job(controller_url, cluster_id, payload)
            except requests.exceptions.RequestException as e:
                last_error = f"jobcreate request failed: {e}"
                continue

            if resp.status_code != 200:
                last_error = f"jobcreate returned HTTP {resp.status_code}: {resp.text[:300]}"
                continue

            if not check_driver_log_clean(main_node_ip, job_name):
                last_error = "driver log contained a real (non-shutdown-noise) error"
                continue

            succeeded = True

        if succeeded:
            manifest[key] = {"status": "done", "timestamp": time.time()}
            click.echo(f"  [OK] {job_class} q={query} rate={event_rate} pd={pd_array} "
                       f"-- run {i}/{total} complete")
        else:
            manifest[key] = {"status": "failed", "error": last_error, "timestamp": time.time()}
            click.echo(f"  [FAILED] {job_class} q={query} rate={event_rate} pd={pd_array}: {last_error}")

        save_manifest(manifest)

    done = sum(1 for v in manifest.values() if v.get("status") == "done")
    failed = sum(1 for v in manifest.values() if v.get("status") == "failed")
    click.echo(f"\n=== Sweep pass complete: {done} done, {failed} failed, out of {total} ===")


if __name__ == "__main__":
    cli()
