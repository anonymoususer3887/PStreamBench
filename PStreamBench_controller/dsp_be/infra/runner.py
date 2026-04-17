import os
import subprocess
import time
import requests
import yaml
import numpy as np
import paramiko
from django.conf import settings

from infra.models import Cluster, Nodes
from utils import constants_flink, constants_storm


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

    def _run_flink_job(self, config: dict) -> int:
        from infra.views import _config_manipulation_ansible, \
                _configure_and_run_playbooks

        self.ansible_variables = _config_manipulation_ansible(config, constants_flink)

        _configure_and_run_playbooks('/cluster-simple-job.yaml', extravar=self.ansible_variables, distributed=True)
        url = "http://" + config['main_node_ip'] + ":8086/v1/jobs/overview"
        job_id = 0
        requested_cluster_jobs = requests.get(url)
        for job_in_cluster in requested_cluster_jobs.json()["jobs"]:
            state = job_in_cluster['state']
            if state == 'RUNNING':
                job_id = job_in_cluster['jid']
                break

        time.sleep(int(config['job_run_time'])*60)
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
        topology_id = 0
        topologies = requests.get(url)
        for topology in topologies.json()["topologies"]:
            if topology["name"] != config["job_class"].replace(" ", ""):
                continue
            if topology["status"] == "ACTIVE":
                topology_id = topology["id"]
                break

        time.sleep(int(config['job_run_time'])*60)
        return topology_id

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

        # Use subprocess to run the script with arguments
        try:
            subprocess.run(["python3", script_name] + script_arguments,
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
