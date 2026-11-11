import os
import re
import subprocess
import sys
import threading
import time
import psycopg2
import requests
import yaml
import numpy as np
import paramiko
from django.conf import settings

from infra.models import Cluster, Nodes
from utils import constants_flink, constants_storm, constants_spark, constants_kafkastreams, constants_risingwave


class JobRunner():
    """ Logic to configure and run jobs
    """

    def __init__(self, cluster: Cluster, iterations: int, samples: int):
        self.cluster = cluster
        self.iterations = iterations
        self.samples = samples
        self.runs = iterations * samples

    def make_configs(self, config: dict) -> list:
        num_slots = len(config["job_pds"])
        max_slots = 0

        if self.cluster.cluster_type == "Apache Flink":
            max_slots = self.cluster.slots * self.cluster.nodes
        elif self.cluster.cluster_type == "Apache Spark":
            max_slots = self.cluster.slots * self.cluster.nodes
        elif self.cluster.cluster_type == "Apache Kafka Streams":
            # cluster.slots is used as num.stream.threads per instance (see
            # _run_kafkastreams_job); cluster.nodes instances run in total
            # (every node, no separate master -- Part A.1).
            max_slots = self.cluster.slots * self.cluster.nodes
        elif self.cluster.cluster_type == "RisingWave":
            # cluster.nodes compute nodes (workers only, master runs no
            # compute -- Part B.1), cluster.slots as each compute node's own
            # parallelism (actor count per fragment).
            max_slots = self.cluster.slots * self.cluster.nodes
        elif self.cluster.cluster_type == "Apache Storm":
            hwtype = ""
            with open('hwtype.txt', 'r') as f:
                hwtype = f.readline().split("=")[-1]
            cores = {"m510": 8, "c6525-25g": 16, "c6320": 28}
            max_slots = cores[hwtype] * self.cluster.nodes

        # min/max values
        pds_max = np.full(num_slots, max_slots, dtype=int)
        pds_min = np.ones(num_slots, dtype=int)
        pds_steps = np.array(config["job_pds_steps"], dtype=int)
        input_max = int(config["job_class_input_max"])
        input_min = int(config["job_class_input_min"])
        window_max = int(config["job_window_size_max"])
        window_min = int(config["job_window_size_min"])
        slide_max = int(config["job_window_slide_size_max"])
        slide_min = int(config["job_window_slide_size_min"])

        # generate parameters
        if config["job_enumeration_strategy"] == "Random":
            pds = self._enum_random(self.iterations, pds_min, pds_max)
            input = self._random(self.runs, input_min, input_max)
            window = self._random(self.runs, window_min, window_max)
            slide = self._random(self.runs, slide_min, slide_max)
        elif config["job_enumeration_strategy"] == "MinMax":
            pds_max = np.array(config["job_pds_max"], dtype=int)
            pds_min = np.array(config["job_pds_min"], dtype=int)
            pds = self._enum_all(pds_min, pds_max, pds_steps)
            input = self._random(self.runs, input_min, input_max)
            window = self._random(self.runs, window_min, window_max)
            slide = self._random(self.runs, slide_min, slide_max)
        elif config["job_enumeration_strategy"] == "Exhaustive":
            pds = self._enum_all(pds_min, pds_max, pds_steps)
            input = self._random(self.runs, input_min, input_max)
            window = self._random(self.runs, window_min, window_max)
            slide = self._random(self.runs, slide_min, slide_max)
        elif config["job_enumeration_strategy"] == "Rulebased":
            distance = np.array(config["job_pds_max"], dtype=int)
            input = [int(config["job_class_input"]) for _ in range(self.runs)]
            rulebased_pds = self._get_rulebased(input[0],
                                                config["job_selectivities"])
            pds = self._enum_all(rulebased_pds - distance, rulebased_pds +
                                 distance, pds_steps)
            pds = np.minimum(pds, pds_max)  # limit to available slots
            pds = np.maximum(pds, pds_min)  # guarantee at least min pd
            window = self._random(self.runs, window_min, window_max)
            slide = self._random(self.runs, slide_min, slide_max)
        elif config["job_enumeration_strategy"] == "Custom":
            settings = config["job_custom_strategy"]
            pds = [[setting for setting in line.split(",")]
                   for line in settings.split("\n")]
            pds = np.array(pds)
            input = self._random(len(pds), input_min, input_max)
            window = self._random(len(pds), window_min, window_max)
            slide = self._random(len(pds), slide_min, slide_max)
            self.iterations = len(pds)
            self.samples = 1
        else:
            pds = [np.array(config["job_pds"]) for _ in range(self.iterations)]
            input = [config["job_class_input"] for _ in range(self.runs)]
            window = [config["job_window_size"] for _ in range(self.runs)]
            slide = [config["job_window_slide_size"] for _ in range(self.runs)]
        timeout = int(config["job_run_time"])

        # slice list of pds by strategy
        if config["job_enumeration_strategy"] == "Custom":
            pass
        elif config["job_iteration_strategy"] == "From Top":
            pds = pds[:self.iterations]
        elif config["job_iteration_strategy"] == "From Bottom":
            pds = pds[-self.iterations:]
        elif config["job_iteration_strategy"] == "Random":
            pds = pds[np.random.choice(len(pds), size=self.iterations,
                                       replace=False)]

        # print settings
        for i in range(self.iterations):
            for j in range(self.samples):
                run = (i * self.samples) + j
                print("Job config", "{}/{}".format(i, j),
                      "with pd:", pds[i % len(pds)],
                      "input:", input[run % len(input)],
                      "window:", window[run % len(input)],
                      "slide:", slide[run % len(slide)],
                      "timeout:", timeout)

        # loop through parametes
        for i in range(self.iterations):
            for j in range(self.samples):
                run = (i * self.samples) + j
                config["job_iteration"] = i
                config["job_sample"] = j
                config["job_pds"] = pds[i % len(pds)].astype(str)
                config["job_class_input"] = int(input[run % len(input)])
                config["job_window_size"] = int(window[run % len(window)])
                config["job_window_slide_size"] = int(slide[run % len(slide)])
                config["job_run_time"] = int(timeout)
                yield config

    def _enum_random(self, samples: int, min: list, max: list) -> np.ndarray:
        """Generates random arrays between min and max
        """

        assert len(min) == len(max)
        return np.random.randint(min, [max + 1 for _ in range(samples)])

    def _enum_all(self, minimum: list, maximum: list,
                  steps: list) -> np.ndarray:
        """Generates all possible combinations between min and max
        """

        assert len(minimum) == len(maximum) == len(steps)
        # make arrays with all possible values per position
        ranges = [np.arange(minimum[i], maximum[i] + 1, steps[i])
                  for i in range(len(minimum))]
        # combine ranges to a 2d array
        mesh = np.meshgrid(*ranges, indexing='ij')
        # flatten the grid to get a list of all possible combinations
        return np.stack(mesh, axis=-1).reshape(-1, len(minimum))

    def _get_rulebased(self, source_input: int, selectivities: list) -> list:
        pds = []
        for selectivity in selectivities:
            input = pds[-1]["output"] if pds != [] \
                else source_input
            output = input * float(selectivity)
            pd = int(0.000006 * input ** 1.17)
            pds.append({"value": pd, "output": output})

        return np.asarray([pd["value"] for pd in pds])

    def _random(self, samples: int, min: int, max: int) -> np.ndarray:
        """Generates random arrays between min and max
        """

        assert isinstance(min, int)
        assert isinstance(max, int)
        return np.random.randint(min, [max + 1 for _ in range(samples)])

    def prepare_jobs(self, config: dict):
        from infra.views import _inventory_manipulation

        main_domain_name = self.cluster.main_node_ip
        config['main_node_ip'] = self.cluster.main_node_ip
        slave_domain_names = self.cluster.slave_node_ip.split(',')  # ['pc11.e.de','pc22.e.rt']
        main_node = Nodes.objects.get(domain_name=main_domain_name)  # Node( domain_name ='pc555.emulab.net', user='ddirks')
        alpha = [{'host_name': main_node.domain_name, 'user': main_node.user}]
        betas = []
        for slave_domain_name in slave_domain_names:
            slave_node = Nodes.objects.get(domain_name=slave_domain_name)
            betas.append({'host_name': slave_node.domain_name, 'user': slave_node.user})
        config['betas_num'] = len(betas)
        hosts = {'alpha': alpha, 'betas': betas}
        _inventory_manipulation(hosts)
        return config

    def run(self, config: dict) -> int:
        """Runs a job with the given parameters"""

        if self.cluster.cluster_type == "Apache Flink":
            return self._run_flink_job(config)
        elif self.cluster.cluster_type == "Apache Storm":
            return self._run_storm_job(config)
        elif self.cluster.cluster_type == "Apache Spark":
            return self._run_spark_job(config)
        elif self.cluster.cluster_type == "Apache Kafka Streams":
            return self._run_kafkastreams_job(config)
        elif self.cluster.cluster_type == "RisingWave":
            return self._run_risingwave_job(config)

    def _run_flink_job(self, config: dict) -> int:
        from infra.views import _config_manipulation_ansible, \
                _configure_and_run_playbooks

        self.ansible_variables = _config_manipulation_ansible(config, constants_flink)

        # Structural race, not just a timing fluke (confirmed live,
        # 2026-09-13): '/cluster-simple-job.yaml' runs the job-submission
        # play AND the producer-run play in one sequential ansible-playbook
        # invocation. The producer task blocks for the job's *entire*
        # job_run_time (it's supposed to keep producing for the whole run,
        # and gets reaped by its own `timeout <seconds>` wrapper at the end
        # -- that non-zero return is expected, not a bug, hence
        # ignore_errors on that task). _configure_and_run_playbooks() below
        # is therefore blocking for the full job_run_time too. Meanwhile
        # Flink's OWN job driver (see WordCountJob.java's
        # `while (System.currentTimeMillis() < end) {}` then
        # `client.cancel()`) self-cancels the job after that SAME
        # job_run_time, independently and concurrently. A job-id check
        # placed *after* _configure_and_run_playbooks() returns -- as a
        # retry loop was, until this fix -- always runs after the job has
        # already been self-cancelled: confirmed live, the job was already
        # CANCELED on the very first poll attempt. The fix is to poll
        # concurrently with the blocking playbook call (in a background
        # thread started *before* it), not sequentially after.
        # A RUNNING job already on the cluster at submission time is not
        # necessarily stale progress from THIS submission -- it can be a
        # previous job that was never actually killed. Confirmed live
        # (2026-09-13): flink-kill-job.yaml's cancel command silently
        # no-op'd for weeks (hardcoded "flink-1.16.0/bin/flink", but this
        # cluster runs 1.16.2 -- the task is `async`/`poll: 0`, so Ansible
        # reports "changed" regardless of whether the command even found
        # the binary), leaking a job that stayed RUNNING for 40+ minutes.
        # The poller below used to accept the *first* RUNNING job it saw,
        # which on that occasion was the 40-minute-old leaked job, still
        # RUNNING on the very first poll (before the real new job had even
        # been scheduled) -- so job_id silently pointed at completely
        # unrelated stale data for the rest of that run. Fixed by
        # snapshotting every job id already present (in any state) right
        # before submission, and only ever accepting a RUNNING job whose id
        # is NOT in that snapshot -- i.e. a job that is genuinely new.
        url = "http://" + config['main_node_ip'] + ":8086/jobs/overview"
        discovered = {"job_id": 0}
        stop_polling = threading.Event()

        # Pre-flight: don't just detect a leftover RUNNING job (see snapshot
        # below), actively clear it first. A leaked job doesn't just risk
        # job-id confusion -- it keeps occupying task slots on a
        # slot-limited cluster (5 nodes x 16 slots = 80 total here) for as
        # long as it runs, which across a multi-hour sweep can eventually
        # starve every subsequent submission of slots even once job-id
        # detection itself is race-free. Cancelled directly via Flink's
        # REST API (PATCH .../jobs/<id>?mode=cancel) rather than relying on
        # the Ansible kill task, since that path already proved fragile
        # once (flink-kill-job.yaml's stale-version bug, fixed separately,
        # but this check stays as defense-in-depth against any future kill
        # failure for whatever reason).
        try:
            preflight_resp = requests.get(url, timeout=8)
            for job_in_cluster in preflight_resp.json().get("jobs", []):
                if job_in_cluster['state'] == 'RUNNING':
                    print(f"WARNING: found leftover RUNNING Flink job {job_in_cluster['jid']} "
                          f"before submitting a new one -- cancelling it now.")
                    try:
                        requests.patch(
                            "http://" + config['main_node_ip'] + ":8086/jobs/" + job_in_cluster['jid'] + "?mode=cancel",
                            timeout=10,
                        )
                    except requests.exceptions.RequestException as e:
                        print(f"WARNING: failed to cancel leftover job {job_in_cluster['jid']}: {e!r}")
            time.sleep(5)
        except requests.exceptions.RequestException:
            pass

        try:
            preexisting_resp = requests.get(url, timeout=8)
            preexisting_job_ids = {j['jid'] for j in preexisting_resp.json().get("jobs", [])}
        except requests.exceptions.RequestException:
            preexisting_job_ids = set()

        def _poll_for_running_job():
            while not stop_polling.is_set():
                try:
                    resp = requests.get(url, timeout=5)
                    for job_in_cluster in resp.json().get("jobs", []):
                        if job_in_cluster['state'] == 'RUNNING' and job_in_cluster['jid'] not in preexisting_job_ids:
                            discovered["job_id"] = job_in_cluster['jid']
                            return
                except requests.exceptions.RequestException:
                    pass
                stop_polling.wait(2)

        poller = threading.Thread(target=_poll_for_running_job, daemon=True)
        poller.start()

        _configure_and_run_playbooks('/cluster-simple-job.yaml', extravar=self.ansible_variables, distributed=True)

        stop_polling.set()
        poller.join(timeout=5)
        job_id = discovered["job_id"]

        return job_id

    def _run_storm_job(self, config: dict) -> str:
        from infra.views import _config_manipulation_ansible, \
                _configure_and_run_playbooks

        config["num_workers"] = self.cluster.slots * self.cluster.nodes
        self.ansible_variables = _config_manipulation_ansible(config, constants_storm)

        _configure_and_run_playbooks('/cluster-simple-storm-job.yaml',
                                     extravar=self.ansible_variables,
                                     distributed=True)
        url = "http://" + config['main_node_ip'] + \
              ":8080/api/v1/topology/summary"

        # A single immediate query here used to silently return
        # topology_id=0 whenever Nimbus hadn't yet listed the
        # just-submitted topology as ACTIVE by the instant the ansible
        # playbook call returned (confirmed live, 2026-11-11: Ad Analytics
        # ds2/1M's collection step crashed with a KeyError on
        # response["name"] inside getprom_storm.py's main(), because
        # job_id="0" doesn't exist on Nimbus -- get_logs()'s
        # subprocess.run(check=True) swallows that exception in its own
        # except clause, so the view still returned 200 success with no
        # evaluationMetrics/metaInfo file ever written for that job).
        # Retry for a short window instead of querying exactly once,
        # mirroring _run_flink_job's own background-poller fix for the
        # identical class of registration-lag race.
        topology_id = 0
        deadline = time.time() + 30
        while time.time() < deadline and not topology_id:
            topologies = requests.get(url)
            for topology in topologies.json()["topologies"]:
                if topology["name"] != config["job_class"].replace(" ", ""):
                    continue
                if topology["status"] == "ACTIVE":
                    topology_id = topology["id"]
                    break
            if not topology_id:
                time.sleep(2)

        time.sleep(int(config['job_run_time'])*60)
        return topology_id

    def _run_spark_job(self, config: dict) -> str:
        from infra.views import _config_manipulation_ansible, \
                _configure_and_run_playbooks

        config["num_workers"] = self.cluster.slots * self.cluster.nodes
        self.ansible_variables = _config_manipulation_ansible(config, constants_spark)

        _configure_and_run_playbooks('/cluster-simple-spark-job.yaml',
                                     extravar=self.ansible_variables,
                                     distributed=True)

        # Spark Standalone has no numeric job id like Flink/Storm -- match by
        # the --name given to spark-submit (job_name) instead, on the
        # master's JSON status API (custom SPARK_MASTER_WEBUI_PORT, see
        # infra/spark_files/sample-spark-env.sh).
        url = "http://" + config['main_node_ip'] + ":8087/json/"
        job_name = self.ansible_variables["job_name"]
        application_id = ""
        requested_cluster_apps = requests.get(url)
        for app in requested_cluster_apps.json().get("activeapps", []):
            if app["name"] == job_name:
                application_id = app["id"]
                break

        time.sleep(int(config['job_run_time'])*60)
        return application_id or job_name

    def _run_kafkastreams_job(self, config: dict) -> str:
        from infra.views import _config_manipulation_ansible, \
                _configure_and_run_playbooks

        config["num_workers"] = self.cluster.slots * self.cluster.nodes
        self.ansible_variables = _config_manipulation_ansible(config, constants_kafkastreams)

        # Kafka Streams' application.id doubles as its consumer group id --
        # every instance across every node must share the exact same value
        # to join one group (see WordCountTopology's javadoc and Part A.1),
        # so it's computed once here (not per-host in the playbook) and
        # passed down as a single extravar. Timestamped so repeated runs of
        # the same job_name don't inherit a stale consumer group's offsets/
        # state from a previous run.
        self.ansible_variables["kafkastreams_application_id"] = \
            "{}-{}".format(self.ansible_variables["job_name"], int(time.time()))

        _configure_and_run_playbooks('/cluster-simple-kafkastreams-job.yaml',
                                     extravar=self.ansible_variables,
                                     distributed=True)

        # No cluster REST API to poll for a numeric job id (see Part A.1) --
        # the application.id is the closest thing to one, and is what
        # getprom_kafkastreams.py/kafkastreams-kill-job.yaml key off of.
        job_id = self.ansible_variables["kafkastreams_application_id"]

        # No extra sleep here, unlike _run_flink_job/_run_spark_job: the
        # playbook call above has *already* blocked for the job's full
        # duration by the time it returns (producer-run.yaml's `timeout
        # {{ job_run_time }} ...` is a synchronous shell task, not
        # fire-and-forget). An additional time.sleep(job_run_time*60) here
        # doubles the wait and pushes get_logs()'s Prometheus query window
        # (a 5-minute lookback from "now") well past when the Kafka Streams
        # JVMs actually exited, making every query come back empty.
        return job_id

    def _run_risingwave_job(self, config: dict) -> str:
        from infra.views import _config_manipulation_ansible, \
                _configure_and_run_playbooks, _risingwave_job_manipulation

        config["num_workers"] = self.cluster.slots * self.cluster.nodes
        self.ansible_variables = _config_manipulation_ansible(config, constants_risingwave)

        resolved_program = _risingwave_job_manipulation(
            self.ansible_variables["job_program"],
            self.ansible_variables["producer_bootstrap_server"],
            window_size_ms=self.ansible_variables.get("job_window_size"),
            window_slide_ms=self.ansible_variables.get("job_window_slide_size"),
            threshold=self.ansible_variables.get("job_threshold"),
            query_number=self.ansible_variables.get("job_query_number"))
        self.ansible_variables["risingwave_sql_file"] = os.path.basename(resolved_program)

        # RisingWave jobs are SQL files (job_program is a .sql path, see
        # constants_risingwave.py), not JVM programs -- there's no numeric/
        # timestamped job id the way Flink/Storm have one, and no
        # consumer-group-style shared id the way Kafka Streams' application.id
        # is (Part B.3). The SQL files themselves declare fixed object names
        # (word_count_source/_mv/_sink, see word_count.sql), so job identity
        # for kill/cleanup purposes is a deterministic snake_case prefix
        # derived from job_name (e.g. "WordCount" -> "word_count") -- this
        # requires every future workload's .sql file to follow the same
        # <snake_case_job_name>_source/_mv/_sink naming convention.
        object_prefix = re.sub(r'([A-Z])', r'_\1', self.ansible_variables["job_name"]).lower().lstrip('_')
        self.ansible_variables["risingwave_object_prefix"] = object_prefix

        _configure_and_run_playbooks('/cluster-simple-risingwave-job.yaml',
                                     extravar=self.ansible_variables,
                                     distributed=True)

        time.sleep(int(config['job_run_time'])*60)
        return object_prefix

    def run_enumerated(self, config: dict):
        """Runs an enumeration strategy on the cluster using calculator

        See: ../datagen/views.py#_run_plan_generator()
        """

        cmd = [
            "~/flink/bin/flink",
            "run",
            "--target",
            "kubernetes-session",
            "-Dkubernetes.cluster-id=plangeneratorflink-cluster",
            "-Dkubernetes.namespace=plangeneratorflink-namespace",
            "-Dkubernetes.rest-service.exposed.type=NodePort",
            "~//flink/lib/plangeneratorflink-1.0-SNAPSHOT.jar",
            "--logdir",
            "~/pgf-results",
            "--mode",
            "test",
            "--numTopos",
            config["numberOfTopol"],
            "--environment",
            "kubernetes",
            "--enumerationStrategy",
            config["enumerationStrategy"],
            "--duration",
            config["executionTime"],
            "--templates",
            config["testtempl"],
        ]
        print(" ".join(cmd))

        masternode_file_path = settings.DSP_MANAGMENT_DIR + "/masterNode"
        username_masternode_file_path = settings.DSP_MANAGMENT_DIR + "/pgf-env"

        with open(masternode_file_path, 'r') as f:
            content = f.read()
            hostname = content.split('=')[1].strip()
            print(hostname)

        with open(username_masternode_file_path, 'r') as f:
            content = f.read()
            lines = content.split('\n')
            for line in lines:
                if line.startswith('usernameNodes='):
                    nodeusername = line.split('=')[1].strip()
            print(nodeusername)

        # Connect to remote node using ssh
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname, username=nodeusername)

        # Run command on remote node
        stdin, stdout, stderr = ssh.exec_command(cmd)

        # Print output of command on remote node
        print(stdout.read().decode())
        # print(stderr.read().decode())

        # Close the SSH connection
        ssh.close()

    def get_logs(self, config: dict):
        # run the script with job Id and main node ip
        if self.cluster.cluster_type == "Apache Flink":
            script_loc = os.path.join(settings.BASE_DIR, 'infra/reporting_scripts')
            script_name = os.path.join(script_loc, 'getprom.py')
        elif self.cluster.cluster_type == "Apache Storm":
            script_loc = os.path.join(settings.BASE_DIR, 'infra/reporting_scripts')
            script_name = os.path.join(script_loc, 'getprom_storm.py')
        elif self.cluster.cluster_type == "Apache Spark":
            script_loc = os.path.join(settings.BASE_DIR, 'infra/reporting_scripts')
            script_name = os.path.join(script_loc, 'getprom_spark.py')
        elif self.cluster.cluster_type == "Apache Kafka Streams":
            script_loc = os.path.join(settings.BASE_DIR, 'infra/reporting_scripts')
            script_name = os.path.join(script_loc, 'getprom_kafkastreams.py')
        elif self.cluster.cluster_type == "RisingWave":
            script_loc = os.path.join(settings.BASE_DIR, 'infra/reporting_scripts')
            script_name = os.path.join(script_loc, 'getprom_risingwave.py')
        script_arguments = ["--main_node_ip", str(config['main_node_ip']),
                            "--job_id", str(config['job_id']),
                            '--job_run_time', str(config['job_run_time']),
                            '--job_parallelization', ','.join(config['job_pds']),
                            '--job_query_number', str(config['job_query_name']),
                            '--job_window_size', str(config['job_window_size']),
                            '--job_window_slide_size', str(config['job_window_slide_size']),
                            '--job_sample', str(config['job_sample']),
                            '--job_iteration', str(config['job_iteration']),
                            '--producer_event_per_second', str(config['job_class_input']),
                            '--hardware_type', str(self.cluster.hardware_type),
                            '--num_nodes', str(self.cluster.nodes),
                            '--num_slots', str(self.cluster.slots),
                            '--enumeration_strategy', str(config['job_enumeration_strategy']),
                            ]
        if self.cluster.cluster_type in ("Apache Spark", "Apache Kafka Streams", "RisingWave"):
            script_arguments += ['--job_class', str(config['job_class'])]

        # Use subprocess to run the script with arguments. sys.executable
        # (not a bare "python3") -- confirmed live (2026-08-25) that a bare
        # "python3" resolves via the subprocess's inherited PATH to the
        # system/Homebrew interpreter, not this project's venv, silently
        # crashing on `import click`/`import requests` before writing any
        # metrics file at all, no matter which engine's getprom_*.py script
        # this is. sys.executable is always this same process's own
        # interpreter, guaranteed to have every package the Django server
        # itself needed to even start.
        try:
            subprocess.run([sys.executable, script_name] + script_arguments,
                           check=True)
        except subprocess.CalledProcessError as e:
            print(f"Error running the script: {e}")
        return config

    def is_terminated(self, config: dict) -> bool:
        if self.cluster.cluster_type == "Apache Flink":
            url = "http://" + config['main_node_ip'] + ":8086/v1/jobs/overview"
            requested_cluster_jobs = requests.get(url)
            for job_in_cluster in requested_cluster_jobs.json()["jobs"]:
                state = job_in_cluster['state']
                if state == 'RUNNING':
                    return False

        elif self.cluster.cluster_type == "Apache Storm":
            url = "http://" + config['main_node_ip'] + ":8080/api/v1/topology/summary"
            topologies = requests.get(url)
            for topology in topologies.json()["topologies"]:
                if topology["name"] != config["job_class"].replace(" ", ""):
                    continue
                if topology["status"] == "ACTIVE":
                    return False

        elif self.cluster.cluster_type == "Apache Spark":
            url = "http://" + config['main_node_ip'] + ":8087/json/"
            job_name = config["job_class"].replace(" ", "")
            apps = requests.get(url)
            for app in apps.json().get("activeapps", []):
                if app["name"] == job_name:
                    return False

        elif self.cluster.cluster_type == "Apache Kafka Streams":
            # No cluster REST API to poll (Part A.1) -- by the time this is
            # called, _run_kafkastreams_job has already slept for the job's
            # full waitTimeToCancel duration, and every instance
            # self-terminates via streams.close() after that same duration
            # (see WordCountTopology), so it's already finished. terminate()
            # remains as a safety net (pkill by application id), same
            # reasoning as Spark's -- see kafkastreams-kill-job.yaml.
            return True

        elif self.cluster.cluster_type == "RisingWave":
            # Unlike Kafka Streams, a RisingWave streaming query does NOT
            # self-terminate after job_run_time -- CREATE MATERIALIZED VIEW/
            # SINK/SOURCE are persistent objects that keep running until
            # explicitly dropped. This must be a REAL check against the
            # catalog, not a hardcoded False -- AnsibleClusterJobCreation's
            # caller does:
            #   if not is_terminated(): terminate(); wait_for_termination()
            # and wait_for_termination() polls is_terminated() in a
            # `while not ...: sleep(5)` loop. A hardcoded False here caused a
            # genuine infinite loop (confirmed live, 2026-07-21): terminate()
            # runs once and drops the objects, but is_terminated() kept
            # reporting False forever afterward, so the poll loop never
            # exited and the whole --nothreading dev server hung on this one
            # request indefinitely.
            object_prefix = re.sub(r'([A-Z])', r'_\1', config["job_class"].replace(" ", "")).lower().lstrip('_')
            conn = psycopg2.connect(host=config['main_node_ip'], port=4566, dbname="dev", user="root")
            try:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT 1 FROM rw_catalog.rw_materialized_views WHERE name = %s",
                        (object_prefix + "_mv",),
                    )
                    return cur.fetchone() is None
            finally:
                conn.close()
        return True

    def terminate(self, config: dict, job_id: int):
        from infra.views import _config_manipulation_ansible, \
                _configure_and_run_playbooks

        print("Terminate job...")

        if self.cluster.cluster_type == "Apache Flink":
            self.ansible_variables["job_id"] = job_id

            _configure_and_run_playbooks('/cluster-simple-flink-job-kill.yaml',
                                         extravar=self.ansible_variables,
                                         distributed=True)
        elif self.cluster.cluster_type == "Apache Storm":
            self.ansible_variables = _config_manipulation_ansible(config, constants_storm)

            _configure_and_run_playbooks('/cluster-simple-storm-job-kill.yaml',
                                         extravar=self.ansible_variables,
                                         distributed=True)
        elif self.cluster.cluster_type == "Apache Spark":
            self.ansible_variables = _config_manipulation_ansible(config, constants_spark)

            _configure_and_run_playbooks('/cluster-simple-spark-job-kill.yaml',
                                         extravar=self.ansible_variables,
                                         distributed=True)
        elif self.cluster.cluster_type == "Apache Kafka Streams":
            self.ansible_variables = _config_manipulation_ansible(config, constants_kafkastreams)
            self.ansible_variables["kafkastreams_application_id"] = job_id

            _configure_and_run_playbooks('/cluster-simple-kafkastreams-job-kill.yaml',
                                         extravar=self.ansible_variables,
                                         distributed=True)
        elif self.cluster.cluster_type == "RisingWave":
            self.ansible_variables = _config_manipulation_ansible(config, constants_risingwave)
            # job_id here IS the object prefix (see _run_risingwave_job) --
            # DROP SINK/MV/SOURCE by name, not a pkill (there's no process
            # to kill on the client side; the pipeline lives in the
            # cluster's meta/compute nodes).
            self.ansible_variables["risingwave_object_prefix"] = job_id

            _configure_and_run_playbooks('/cluster-simple-risingwave-job-kill.yaml',
                                         extravar=self.ansible_variables,
                                         distributed=True)

    def wait_for_termination(self, config: dict):
        """Blocks until job is terminated"""

        print("Wait for termination...")
        while not self.is_terminated(config):
            time.sleep(5)

    def cleanup_jobs(self, config: dict):
        from infra.views import _configure_and_run_playbooks

        _configure_and_run_playbooks('/trigger-topic-deletion.yaml',
                                     extravar=self.ansible_variables,
                                     distributed=True)
        return config
