# Create your views here.
import re
import json
import os
import shutil
import requests
import yaml

# Must be set before any ansible.* import: ansible.constants resolves config
# values at import time, so setting this env var later (e.g. inside a
# request handler) is too late even across server restarts if the module
# was already imported once this process started.
#
# The "Timeout (Ns) waiting for privilege escalation prompt" failures seen
# under load are NOT controlled by become_timeout (not even a real ansible
# config key in this version) -- ssh.py computes that wait as
# `2 + self.get_option('timeout')`, i.e. 2 seconds plus the general
# connection timeout. Since context.CLIARGS below never sets 'timeout',
# that resolves to 0 through this in-process PlaybookExecutor/DataLoader
# usage (unlike the ansible-playbook CLI, which would pick up ansible.cfg's
# timeout=10 default), giving exactly the observed 2s. Set it directly.
os.environ.setdefault("ANSIBLE_TIMEOUT", "30")

from ansible import context
from ansible.cli import CLI
from ansible.executor.playbook_executor import PlaybookExecutor
from ansible.inventory.manager import InventoryManager
from ansible.module_utils.common.collections import ImmutableDict
from ansible.parsing.dataloader import DataLoader
from ansible.vars.manager import VariableManager
from django.conf import settings
from django.http import JsonResponse
from django.views import View
from yaml.loader import SafeLoader

from infra.models import Cluster, Nodes
from infra.runner import JobRunner
from utils import constants_flink, constants_storm, constants_spark, constants_kafkastreams


def getprom_manipulataion(configs, getprompath):
    
    #TO DO: Change for report/views.py/nw utilization in get individualgraph()
    with open('hwtype.txt', 'w') as file1:
        file1.write('hardware_type=' + configs["hardware_cloudlab"])

    # Read the file
    with open(getprompath, 'r') as file:
        content = file.read()

    if configs["hardware_cloudlab"] == 'd710' or configs["hardware_cloudlab"] == 'm510' or configs["hardware_cloudlab"] == 'c6320':

        # Modify the string
        modified_content = re.sub(r'flink_taskmanager_System_Network_[a-zA-Z0-9_]+_SendRate', 'flink_taskmanager_System_Network_eno1_SendRate', content)
    
    if configs["hardware_cloudlab"] == 'xl170':
        modified_content = re.sub(r'flink_taskmanager_System_Network_[a-zA-Z0-9_]+_SendRate', 'flink_taskmanager_System_Network_eno49np0_SendRate', content)
    
    if configs["hardware_cloudlab"] == 'c6525-25g' or configs["hardware_cloudlab"] == 'c6525-100g':
        modified_content = re.sub(r'flink_taskmanager_System_Network_[a-zA-Z0-9_]+_SendRate', 'flink_taskmanager_System_Network_eno33np0_SendRate', content)
    
    # Write the updated content back to the file
    with open(getprompath, 'w') as file:
        file.write(modified_content)

    print("File updated successfully.")
def grafana_manipulation(configs, clusterjsonpath):
    
    # Read the JSON file
    with open(clusterjsonpath, 'r') as file:
        data = json.load(file)
    if configs["hardware_cloudlab"] == 'd710' or configs["hardware_cloudlab"] == 'm510' or configs["hardware_cloudlab"] == 'c6320':
        
        # Modify the value associated with the query
        data["panels"][3]["targets"][0]["expr"] = "flink_taskmanager_System_Network_eno1_SendRate"
    
    if configs["hardware_cloudlab"] == 'xl170':
        
        # Modify the value associated with the query
        data["panels"][3]["targets"][0]["expr"] = "flink_taskmanager_System_Network_eno49np0_SendRate"
    
    if configs["hardware_cloudlab"] == 'c6525-25g' or configs["hardware_cloudlab"] == 'c6525-100g':
        
        # Modify the value associated with the query
        data["panels"][3]["targets"][0]["expr"] = "flink_taskmanager_System_Network_eno33np0_SendRate"

    # Write the updated content back to the same file
    with open(clusterjsonpath, 'w') as file:
        json.dump(data, file, indent=2)

    print("File updated successfully.")
   

def _configure_and_run_playbooks(ansible_script_name, extravar=None,distributed=False):
    """ Configure the playbook
    """


    context.CLIARGS = ImmutableDict(connection='local', syntax=False, module_path=None, start_at_task=None,
                                    forks=10, become=None,
                                    become_method=None, become_user=None, check=False, diff=False)
    if extravar is None:
        extravar = dict()
    basevars = dict()
    
    if distributed:
        context.CLIARGS = ImmutableDict(connection='ssh',verbosity=True, syntax=False, module_path=None, start_at_task=None,
                                    forks=10, become=True, become_method='sudo', check=False, diff=False)
        
    
    basevars["BASE_DIR"] = str(settings.BASE_DIR)
    basevars["INSTALLATION_DIR"] = "/home/playground"

    

    extravar.update(basevars)
    data_loader = DataLoader()
    # Setting the base directory for Dataloader()
    # refer dsp_be/settings.py to find how BASE_DIR is being set.
    data_loader.set_basedir(os.path.join(settings.BASE_DIR, 'utils', 'playbook'))

    # Ansible's context should take these CLI arguments. setting up the config for Ansible

    #context.CLIARGS = ImmutableDict(connection=connection, syntax=False, module_path=None, start_at_task=None,
    #                                forks=10, become=None,
    #                                become_method=None, become_user=None, check=False, diff=False)
    """context.CLIARGS = ImmutableDict(tags={}, listtags=False, listtasks=False, listhosts=False, syntax=False, 
    connection='ssh', module_path=None, forks=10, remote_user=None, private_key_file=None, ssh_common_args=None, 
    ssh_extra_args=None, sftp_extra_args=None, scp_extra_args=None, become=True, become_method=None, 
    become_user=None, verbosity=True, check=True, start_at_task=None) """

    # inventory is the yaml for storing target nodes ip address
    # inventory.yaml contains the host group and the corresponding ip addresses of the nodes
    inventory_path = os.path.join(data_loader._basedir, 'inventory.yml')

    # We will need password later when we need to deploy remote nodes.
    password = {}

    # InventoryManager is to manage the inventory(nodes ip addresses)

    inventory_manager = InventoryManager(loader=data_loader, sources=inventory_path)

    # Ansible has the property of taking variables that makes Ansible dynamic. for e.g. if we want to choose
    # different versions of Flink or Kafka etc.
    variable_manager = VariableManager(loader=data_loader, inventory=inventory_manager, version_info=CLI.version_info(gitinfo=False))

    # passing the extravar to the variable_manager
    variable_manager._options_vars = extravar

    playbook_executor = PlaybookExecutor([data_loader._basedir + ansible_script_name], inventory_manager, variable_manager, data_loader, password)

    playbook_executor.run()

def _flink_config_manipulation(configs,jobmgr,tmgr):
    """ Edit and save the configs
    """
    # Creating a flink conf file from a default conf file
    source = 'infra/flink_files/sample-flink-conf.yaml'
    target = 'infra/flink_files/flink-conf.yaml'

    # shell utility to perform shell file/directory commands
    shutil.copy(source, target)

    # Open and load the temporary flink conf yaml file
    with open(target) as f:
        # loading the yaml file to a python dictionary(flink_configs)
        flink_configs = yaml.load(f, Loader=SafeLoader)

    flink_configs["taskmanager.numberOfTaskSlots"] = int(configs["cluster_tm_slots"])
    flink_configs["jobmanager.rpc.address"] = jobmgr
    flink_configs["taskmanager.host"] = tmgr
    
        
        
    # Dump the new configs
    with open(target, 'w') as f:
        yaml.dump(flink_configs, f, sort_keys=False, default_flow_style=False)

    return target


def _storm_config_manipulation(configs, nimbus, node):
    """ Edit and save the configs
    """

    source = 'infra/storm_files/sample-storm.yaml'
    target = 'infra/storm_files/storm.yaml'
    shutil.copy(source, target)

    with open(target) as f:
        storm_config = yaml.load(f, Loader=SafeLoader)

    port = 6700
    ports = []
    for i in range(int(configs["cluster_tm_slots"])):
        ports.append(port)
        port += 1

    storm_config["storm.local.hostname"] = node
    storm_config["supervisor.slots.ports"] = ports
    storm_config["nimbus.seeds"] = [nimbus]
    # sample-storm.yaml's default ("localhost") is wrong for a real
    # multi-node cluster -- every node would try to reach its OWN local
    # ZooKeeper, which only the master actually runs (same class of bug
    # fixed for RisingWave's multi-node ZK ensemble this session; here
    # Storm just reuses the single ZK instance already running for Kafka
    # on the master, standard practice since Storm and Kafka use separate
    # znode namespaces under the same ensemble).
    storm_config["storm.zookeeper.servers"] = [nimbus]

    with open(target, 'w') as f:
        yaml.dump(storm_config, f, sort_keys=False, default_flow_style=False)

    return target


def _spark_config_manipulation(configs, master, worker):
    """ Edit and save spark-defaults.conf + spark-env.sh, mirroring
        _flink_config_manipulation()'s "copy sample -> substitute -> write"
        pattern. Called once per node (like _flink_config_manipulation),
        so 'worker' is the node this particular spark-env.sh is destined
        for (== master when configuring the master's own node).
    """

    worker_cores = int(configs["cluster_tm_slots"])
    total_cores = max(worker_cores * int(configs["cluster_num_tm"]), 1)

    conf_source = 'infra/spark_files/sample-spark-defaults.conf'
    conf_target = 'infra/spark_files/spark-defaults.conf'
    with open(conf_source) as f:
        content = f.read()
    content = content.replace('__MASTER_HOST__', master)
    content = content.replace('__WORKER_CORES_TOTAL__', str(total_cores))
    with open(conf_target, 'w') as f:
        f.write(content)

    env_source = 'infra/spark_files/sample-spark-env.sh'
    env_target = 'infra/spark_files/spark-env.sh'
    with open(env_source) as f:
        content = f.read()
    content = content.replace('__MASTER_HOST__', master)
    content = content.replace('__WORKER_CORES__', str(worker_cores))
    content = content.replace('__LOCAL_HOST__', worker)
    with open(env_target, 'w') as f:
        f.write(content)

    return conf_target


def _risingwave_job_manipulation(job_program, kafka_bootstrap_server,
                                 window_size_ms=None, window_slide_ms=None,
                                 threshold=None, query_number=None):
    """ Substitute the __KAFKA_BOOTSTRAP_SERVER__ / __WINDOW_SIZE_SEC__ /
        __WINDOW_SLIDE_SEC__ / __THRESHOLD__ placeholders in a workload's
        .sql file with real values, mirroring
        _spark_config_manipulation()'s "copy sample -> substitute -> write"
        pattern. job_program is a path like "wordcount/word_count.sql",
        relative to PStreamBench_workload_generator/risingwave/
        dsp_risingwave_jobs/ (see constants_risingwave.py's *_PROGRAM
        values) -- unlike Flink/Spark/Kafka Streams' jars (pre-built and
        pre-copied into infra/<engine>_files/ as a one-time build step),
        RisingWave's .sql source needs no build step, so it's read directly
        from the workload_generator tree at job-submission time.

        window_size_ms/window_slide_ms/threshold are only present in the
        workloads whose .sql files actually use them (e.g. smart_grid.sql's
        HOP window, bargain_index.sql's __THRESHOLD__) -- passing None for
        a workload whose file has no such placeholder is harmless since
        .replace() on a missing substring is a no-op.

        Window sizes come in as milliseconds (matching every other engine's
        millisecond-based windowing API) but are converted to whole seconds
        here before substitution: confirmed live (2026-07-24) that
        RisingWave's HOP() window-function arguments only accept a LITERAL
        interval of the form INTERVAL '<int> second' -- not
        INTERVAL '<n>' MILLISECOND (fails to parse inside HOP's argument
        list specifically, though it parses -- silently wrong -- as a
        bare SELECT expression), not INTERVAL '<n> millisecond'/'ms' (unit
        not recognized at all), and not a computed expression like
        (n/1000)*INTERVAL '1 second' (HOP requires a literal, rejects
        computed intervals). This means sub-second window precision isn't
        achievable at all right now -- ms values are rounded to the nearest
        whole second (minimum 1s) -- which is an acceptable trade-off since
        every workload's actual test windows are seconds-to-minutes, not
        sub-second, in practice.

        query_number selects between multiple query variants of the same
        workload -- unlike Flink/Kafka Streams (one Java class branches at
        runtime on a "-query N" CLI arg), a single .sql file can't
        conditionally execute different DDL, so a second query variant is
        its own file named "<base>_q<N>.sql" (e.g. smart_grid_q2.sql
        alongside smart_grid.sql, which implicitly is query 1). Falls back
        to job_program unchanged if query_number is "1"/None/not given, or
        if no such variant file exists yet (keeps this backwards-compatible
        with every workload that only has one query implemented so far).
    """
    if query_number and str(query_number) != "1":
        base, ext = os.path.splitext(job_program)
        variant_program = "{}_q{}{}".format(base, query_number, ext)
        variant_source = os.path.join(str(settings.BASE_DIR), '..', '..',
                                      'PStreamBench_workload_generator', 'risingwave',
                                      'dsp_risingwave_jobs', variant_program)
        if os.path.exists(variant_source):
            job_program = variant_program

    source = os.path.join(str(settings.BASE_DIR), '..', '..',
                          'PStreamBench_workload_generator', 'risingwave',
                          'dsp_risingwave_jobs', job_program)
    target_dir = 'infra/risingwave_files'
    os.makedirs(target_dir, exist_ok=True)
    target = os.path.join(target_dir, os.path.basename(job_program))

    with open(source) as f:
        content = f.read()
    content = content.replace('__KAFKA_BOOTSTRAP_SERVER__', kafka_bootstrap_server)
    if window_size_ms is not None:
        window_size_sec = max(1, round(int(window_size_ms) / 1000))
        content = content.replace('__WINDOW_SIZE_SEC__', str(window_size_sec))
        # Click Analytics Q3 reuses __WINDOW_SIZE_MS__ as a plain row-count
        # bucket divisor (count-window approximation via ROW_NUMBER()), not
        # as a time interval -- so it still needs the raw millisecond value,
        # not the seconds-converted one used for HOP() interval literals.
        content = content.replace('__WINDOW_SIZE_MS__', str(window_size_ms))
    if window_slide_ms is not None:
        window_slide_sec = max(1, round(int(window_slide_ms) / 1000))
        content = content.replace('__WINDOW_SLIDE_SEC__', str(window_slide_sec))
        content = content.replace('__WINDOW_SLIDE_MS__', str(window_slide_ms))
    if threshold is not None:
        content = content.replace('__THRESHOLD__', str(threshold))
    with open(target, 'w') as f:
        f.write(content)

    return job_program

    return target


def _prometheus_config_manipulation(nodes, main_node=None):
    """ Edit and save the configs
    """
    # Creating a flink conf file from a default conf file
    source = 'infra/prometheus_files/sample_prometheus.yml'
    target = 'infra/prometheus_files/prometheus.yml'

    # shell utility to perform shell file/directory commands
    shutil.copy(source, target)

    tm_nodes = ['{0}:9250'.format(node) for node in nodes]
    kafka_nodes = ['localhost:7075'] + ['{0}:7075'.format(node.split(".")[0]) for node in nodes]
    exporter_nodes = ['{0}:9100'.format(node) for node in nodes]
    # One Kafka Streams instance runs per node -- master included, per the
    # fixed-replica deployment in KAFKA_STREAMS_RISINGWAVE_PLAN.md Part A.1 --
    # each with its own JMX exporter on 7076. "localhost" covers the master
    # (where Prometheus itself runs), same trick kafka_nodes above uses.
    kafkastreams_app_nodes = ['localhost:7076'] + ['{0}:7076'.format(node.split(".")[0]) for node in nodes]

    # Open and load the temporary flink conf yaml file
    with open(source) as f:
        # loading the yaml file to a python dictionary(flink_configs)
        prometheus_configs = yaml.load(f, Loader=SafeLoader)

    prometheus_configs['scrape_configs'][1]['static_configs'][0]['targets'] = tm_nodes
    prometheus_configs['scrape_configs'][2]['static_configs'][0]['targets'] = kafka_nodes
    prometheus_configs['scrape_configs'][3]['static_configs'][0]['targets'] = exporter_nodes
    prometheus_configs['scrape_configs'][7]['static_configs'][0]['targets'] = kafkastreams_app_nodes

    if main_node is not None:
        # Spark's driver UI/metrics servlet binds to the node's own
        # hostname (e.g. "ms1303:4040"), not localhost/0.0.0.0, so this
        # can't be a static "localhost" target like kafka/push_gateway.
        main_node_short = main_node.split(".")[0]
        prometheus_configs['scrape_configs'][6]['static_configs'][0]['targets'] = ['{0}:4040'.format(main_node_short)]

        # RisingWave: one compute-node target per worker (nodes) plus the
        # master's meta (:1250) and frontend (:2222) targets -- see
        # KAFKA_STREAMS_RISINGWAVE_PLAN.md Part B.1/B.4.
        risingwave_nodes = ['{0}:1250'.format(main_node), '{0}:2222'.format(main_node)] + \
                           ['{0}:1222'.format(node) for node in nodes]
        prometheus_configs['scrape_configs'][8]['static_configs'][0]['targets'] = risingwave_nodes

    # Dump the new configs
    with open(target, 'w') as f:
        yaml.dump(prometheus_configs, f, sort_keys=False, default_flow_style=None)

    return target

def _inventory_manipulation(groups):
    """ Edit and save the configs
        hosts = { 
                    'alphas': [{
                        host_name: 'pc122.ee.com',
                        user: 'ddirks'
                    }],
                    'betas': [{
                        host_name: 'pc123.ee.com',
                        user: 'ddirks'
                    }, {
                        host_name: 'pc124.ee.com',
                        user: 'ddirks'
                    }]
                } 
    """
    
    
    target = 'utils/playbook/inventory.yml'

    inventory = dict()
    for group in groups:
        host_list = dict()
        for i in range(len(groups[group])):
            host_inventory_properties = {
                'ansible_user': groups[group][i]['user'],
                'ansible_ssh_private_key_file': '~/.ssh/id_rsa',
                'id': i
            }
            host_list[groups[group][i]['host_name']] = host_inventory_properties
        inventory[group] = { 'hosts': host_list }


    # Dump the new configs
    with open(target, 'w', encoding="utf-8") as f:
        yaml.dump(inventory, f, sort_keys=False, default_flow_style=False,allow_unicode=True,encoding= None)

    return target

def _delete_tmp_config_file(flink_config_file):
    """ Delete temporary config file
    """

    os.remove(flink_config_file)

class AnsibleClusterCreate(View):
    """
    API endpoint that allows users to create cluster using ansible
    """

    @staticmethod
    def post(request):
        """ Upload configs and runs playbook
        """
        
        configs = json.loads(request.body)
        # checking if a clustername was provided
        if not configs["cluster_name"]:
            return JsonResponse(data={'status': 'Please check input configs', 'success': False},
                                status=400)

        # Iterating through all the Cluster objects from the DB and checking
        # if the provided cluster name already exists
        for cluster_instance in Cluster.objects.all():
            if configs["cluster_name"] == cluster_instance.name:
                return JsonResponse(data={'status': 'Another cluster with same name exists', 'success': False},
                                    status=406)
        
        if len(Nodes.objects.all()) == 0:
            return JsonResponse(data={'status': 'No nodes present', 'success': False},
                                    status=406)
        
        nodes_free = []                                    
        
        for node in Nodes.objects.all():
            node_occupied = False
            for cluster in Cluster.objects.all():
                if node.domain_name == cluster.main_node_ip:
                    node_occupied = True
                if node.domain_name in cluster.slave_node_ip.split(','):
                    node_occupied = True          
            if not node_occupied:        
                nodes_free.append(node)
                

        if len(nodes_free) < int(configs['cluster_num_tm'])+1:    
            return JsonResponse(data={'status': 'No free nodes present based on the requirements', 'success': False},
                                    status=406)
            
        # If no such cluster name exists, we create a new cluster object(row in the Cluster Table) 
        clusterjsonpath = os.path.abspath(os.path.join(settings.BASE_DIR, '../dsp_be/infra/grafana_files/cluster.json'))
        getprompath = os.path.abspath(os.path.join(settings.BASE_DIR, '../dsp_be/infra/reporting_scripts/getprom.py'))
        print(clusterjsonpath)
        print(getprompath)
        
        
        #clusterjsonpath = '/home/legion/dsps/dsp_be/infra/grafana_files/cluster.json'
        #getprompath = '/home/legion/dsps/dsp_be/infra/reporting_scripts/getprom.py'
        #reportviewspath = '/home/legion/dsps/dsp_be/report/views.py'
        grafana_manipulation(configs, clusterjsonpath)
        getprom_manipulataion(configs, getprompath)
        #reportviews_manipulataion(configs, reportviewspath)

        tm_nodes = []
        for count in range(1, int(configs['cluster_num_tm'])+1):
            tm_nodes.append(nodes_free[count].domain_name)
        tmgr_list = ','.join(tm_nodes)
        _prometheus_config_manipulation(tm_nodes, main_node=nodes_free[0].domain_name)

        # setup Zookeeper and Kafka
        kafka_nodes = []
        for count in range(0, int(configs['cluster_num_tm'])+1):
            kafka_nodes.append({"host_name": nodes_free[count].domain_name,
                                "user": nodes_free[count].user})
        _inventory_manipulation({"all": kafka_nodes})
        _configure_and_run_playbooks("/cluster-complex-kafka-start.yaml",
                                     extravar={"BROKER_ID": 0,
                                               "ZOOKEEPER_SERVERS": _zookeeper_servers(kafka_nodes)},
                                     distributed=True)

        hosts = dict()
        hosts = {'alpha': [{'host_name': nodes_free[0].domain_name, 'user': nodes_free[0].user}]}
        _inventory_manipulation(hosts)

        if configs["cluster_type"] == "Apache Flink":
            _flink_config_manipulation(configs,
                                       jobmgr=nodes_free[0].domain_name.split(".")[0],
                                       tmgr=nodes_free[0].domain_name.split(".")[0])
            _configure_and_run_playbooks("/cluster-complex-jm-create.yaml",
                                         distributed=True)

            for count in range(1, int(configs['cluster_num_tm'])+1):
                _flink_config_manipulation(configs,
                                           jobmgr=nodes_free[0].domain_name.split(".")[0],
                                           tmgr=nodes_free[count].domain_name.split(".")[0])
                # beta_nodes.append({'hosts':nodes_free[count].domain_name, 'user':nodes_free[count].user})
                hosts = {'betas': [{'host_name': nodes_free[count].domain_name,
                                    'user': nodes_free[count].user}]}
                _inventory_manipulation(hosts)
                _configure_and_run_playbooks("/cluster-complex-tm-create.yaml",
                                             distributed=True)
        elif configs["cluster_type"] == "Apache Storm":
            _storm_config_manipulation(configs,
                                       nimbus=nodes_free[0].domain_name.split(".")[0],
                                       node=nodes_free[0].domain_name.split(".")[0])
            _configure_and_run_playbooks("/cluster-complex-storm-nimbus-create.yaml",
                                         distributed=True)

            for count in range(1, int(configs['cluster_num_tm'])+1):
                _storm_config_manipulation(configs,
                                           nimbus=nodes_free[0].domain_name.split(".")[0],
                                           node=nodes_free[count].domain_name.split(".")[0])
                hosts = {'betas': [{'host_name': nodes_free[count].domain_name,
                                    'user': nodes_free[count].user}]}
                _inventory_manipulation(hosts)
                _configure_and_run_playbooks("/cluster-complex-storm-worker-create.yaml",
                                             distributed=True)
        elif configs["cluster_type"] == "Apache Spark":
            _spark_config_manipulation(configs,
                                       master=nodes_free[0].domain_name.split(".")[0],
                                       worker=nodes_free[0].domain_name.split(".")[0])
            _configure_and_run_playbooks("/cluster-complex-spark-master-create.yaml",
                                         distributed=True)

            for count in range(1, int(configs['cluster_num_tm'])+1):
                _spark_config_manipulation(configs,
                                           master=nodes_free[0].domain_name.split(".")[0],
                                           worker=nodes_free[count].domain_name.split(".")[0])
                hosts = {'betas': [{'host_name': nodes_free[count].domain_name,
                                    'user': nodes_free[count].user}]}
                _inventory_manipulation(hosts)
                _configure_and_run_playbooks("/cluster-complex-spark-worker-create.yaml",
                                             extravar={"main_ip": nodes_free[0].domain_name.split(".")[0]},
                                             distributed=True)
        elif configs["cluster_type"] == "Apache Kafka Streams":
            # No per-node engine daemon to install/config here (unlike
            # Flink/Storm/Spark) -- Kafka Streams has no coordinator process
            # and no persistent cluster of its own; every node just runs the
            # shared Kafka broker (started unconditionally above/below for
            # every engine) and launches its own job JVM at job-submit time,
            # see KAFKA_STREAMS_RISINGWAVE_PLAN.md Part A.1 and
            # runner.py::_run_kafkastreams_job. Prometheus/Grafana/
            # node_exporter setup is NOT shared infra, though -- every other
            # engine bundles it into its own complex-cluster create
            # playbooks, so this still needs its own (monitoring-only)
            # equivalent.
            _configure_and_run_playbooks("/cluster-complex-kafkastreams-master-create.yaml",
                                         distributed=True)

            for count in range(1, int(configs['cluster_num_tm'])+1):
                hosts = {'betas': [{'host_name': nodes_free[count].domain_name,
                                    'user': nodes_free[count].user}]}
                _inventory_manipulation(hosts)
                _configure_and_run_playbooks("/cluster-complex-kafkastreams-worker-create.yaml",
                                             distributed=True)
        elif configs["cluster_type"] == "RisingWave":
            # meta+frontend on the master node, one compute node per worker
            # -- mirrors Flink's JobManager/TaskManager placement for fair
            # resource comparison, per KAFKA_STREAMS_RISINGWAVE_PLAN.md Part
            # B.1. No per-node config template to render (unlike Flink/
            # Spark/Storm) -- RisingWave's CLI flags are self-sufficient, so
            # hostnames are passed straight through as Ansible extravars.
            _configure_and_run_playbooks("/cluster-complex-risingwave-master-create.yaml",
                                         distributed=True)

            for count in range(1, int(configs['cluster_num_tm'])+1):
                hosts = {'betas': [{'host_name': nodes_free[count].domain_name,
                                    'user': nodes_free[count].user}]}
                _inventory_manipulation(hosts)
                _configure_and_run_playbooks("/cluster-complex-risingwave-worker-create.yaml",
                                             extravar={"main_ip": nodes_free[0].domain_name},
                                             distributed=True)

        Cluster.objects.create(name=configs["cluster_name"],
                               main_node_ip=nodes_free[0].domain_name,
                               slave_node_ip=tmgr_list,
                               cluster_type=configs["cluster_type"],
                               hardware_type=configs["hardware_cloudlab"],
                               nodes=configs["cluster_num_tm"],
                               slots=configs["cluster_tm_slots"])

        #print(Cluster.objects.get(name=configs["cluster_name"]))
        #_delete_tmp_config_file(flink_config_temp)
        return JsonResponse(data={'status': "Cluster created successfully",'success': True},
                            status=200)


def _zookeeper_servers(nodes):
    return "\n".join([
        "server.{}={}:2888:3888".format(i, nodes[i]["host_name"].split(".")[0])
        for i in range(len(nodes))])


class AnsibleClusterStart(View):
    """ API Endpoint to start the cluster
    """

    @staticmethod
    def get(request, **kwargs):
        """ Respond to GET requests 



            param kwargs: named URL parameters, part of the API
        """

        cluster_id = kwargs['id']
        
        cluster = Cluster.objects.get(id=cluster_id)
        
        main_domain_name = cluster.main_node_ip # Ex: 'pc555.emulab.net'
        slave_domain_names = cluster.slave_node_ip.split(',') # ['pc11.e.de','pc22.e.rt']
        main_node = Nodes.objects.get(domain_name=main_domain_name) # Node( domain_name ='pc555.emulab.net', user='ddirks')
        alpha = [{'host_name':main_node.domain_name,'user':main_node.user}]
        betas = []
        for slave_domain_name in slave_domain_names:
            slave_node = Nodes.objects.get(domain_name=slave_domain_name) 
            betas.append({'host_name':slave_node.domain_name, 'user':slave_node.user})

        # start zookeeper & kafka
        _inventory_manipulation({"all": alpha + betas})
        _configure_and_run_playbooks("/cluster-complex-kafka-start.yaml",
                                     extravar={"BROKER_ID": 0,
                                               "ZOOKEEPER_SERVERS": _zookeeper_servers(alpha + betas)},
                                     distributed=True)

        hosts = {'alpha': alpha, 'betas':betas}
        _inventory_manipulation(hosts)

        if cluster.cluster_type == "Apache Flink":
            _configure_and_run_playbooks('/cluster-complex-jm-start.yaml', distributed=True)
            _configure_and_run_playbooks('/cluster-complex-tm-start.yaml', distributed=True)
        elif cluster.cluster_type == "Apache Storm":
            _configure_and_run_playbooks('/cluster-complex-storm-nimbus-start.yaml', distributed=True)
            _configure_and_run_playbooks('/cluster-complex-storm-worker-start.yaml', distributed=True)
        elif cluster.cluster_type == "Apache Spark":
            _configure_and_run_playbooks('/cluster-complex-spark-master-start.yaml', distributed=True)
            _configure_and_run_playbooks('/cluster-complex-spark-worker-start.yaml',
                                         extravar={"main_ip": main_domain_name.split(".")[0]},
                                         distributed=True)
        elif cluster.cluster_type == "Apache Kafka Streams":
            # No standing engine daemon of its own to (re)start -- the
            # shared Kafka/Zookeeper playbook above already brought that up.
            # Prometheus/Grafana still need restarting here though, exactly
            # like Spark's master-start.yaml does.
            _configure_and_run_playbooks('/cluster-complex-kafkastreams-master-start.yaml', distributed=True)
        elif cluster.cluster_type == "RisingWave":
            _configure_and_run_playbooks('/cluster-complex-risingwave-master-start.yaml', distributed=True)
            _configure_and_run_playbooks('/cluster-complex-risingwave-worker-start.yaml',
                                         extravar={"main_ip": main_domain_name},
                                         distributed=True)
        else:
            raise Exception
        response_status = 200
        data = {'message': 'Started cluster'}
        return JsonResponse(data, status=response_status)

class AnsibleClusterStop(View):
    """ API Endpoint to stop the cluster
    """

    @staticmethod
    def get(request, **kwargs):
        """ Respond to GET requests 

            param kwargs: named URL parameters, part of the API
        """

        cluster_id = kwargs['id']

        cluster = Cluster.objects.get(id=cluster_id)
        
        main_domain_name = cluster.main_node_ip # Ex: 'pc555.emulab.net'
        slave_domain_names = cluster.slave_node_ip.split(',') # ['pc11.e.de','pc22.e.rt']
        main_node = Nodes.objects.get(domain_name=main_domain_name) # Node( domain_name ='pc555.emulab.net', user='ddirks')
        alpha = [{'host_name':main_node.domain_name,'user':main_node.user}]
        betas = []
        for slave_domain_name in slave_domain_names:
            slave_node = Nodes.objects.get(domain_name=slave_domain_name) 
            betas.append({'host_name':slave_node.domain_name, 'user':slave_node.user})
        hosts = {'alpha': alpha, 'betas':betas}
        _inventory_manipulation(hosts)

        if cluster.cluster_type == "Apache Flink":
            _configure_and_run_playbooks('/cluster-complex-jm-stop.yaml', distributed=True)
            _configure_and_run_playbooks('/cluster-complex-tm-stop.yaml', distributed=True)
        elif cluster.cluster_type == "Apache Storm":
            _configure_and_run_playbooks('/cluster-complex-storm-nimbus-stop.yaml', distributed=True)
            _configure_and_run_playbooks('/cluster-complex-storm-worker-stop.yaml', distributed=True)
        elif cluster.cluster_type == "Apache Spark":
            _configure_and_run_playbooks('/cluster-complex-spark-master-stop.yaml', distributed=True)
            _configure_and_run_playbooks('/cluster-complex-spark-worker-stop.yaml', distributed=True)
        elif cluster.cluster_type == "Apache Kafka Streams":
            # See the matching AnsibleClusterStart branch.
            _configure_and_run_playbooks('/cluster-complex-kafkastreams-master-stop.yaml', distributed=True)
        elif cluster.cluster_type == "RisingWave":
            _configure_and_run_playbooks('/cluster-complex-risingwave-master-stop.yaml', distributed=True)
            _configure_and_run_playbooks('/cluster-complex-risingwave-worker-stop.yaml', distributed=True)
        else:
            raise Exception
        response_status = 200
        data = {'message': 'Stopped cluster'}
        return JsonResponse(data, status=response_status)

class AnsibleClusterDelete(View):
    """ API Endpoint to delete the cluster
    """

    @staticmethod
    def delete(request, **kwargs):
        """ Respond to GET requests 

            param kwargs: named URL parameters, part of the API
        """

        cluster_id = kwargs['id']

        cluster = Cluster.objects.get(id=cluster_id)
        try:
            main_domain_name = cluster.main_node_ip # Ex: 'pc555.emulab.net'
            slave_domain_names = cluster.slave_node_ip.split(',') # ['pc11.e.de','pc22.e.rt']
            main_node = Nodes.objects.get(domain_name=main_domain_name) # Node( domain_name ='pc555.emulab.net', user='ddirks')
            alpha = [{'host_name':main_node.domain_name,'user':main_node.user}]
            betas = []
            for slave_domain_name in slave_domain_names:
                slave_node = Nodes.objects.get(domain_name=slave_domain_name) 
                betas.append({'host_name':slave_node.domain_name, 'user':slave_node.user})
            hosts = {'alpha': alpha, 'betas':betas}
            _inventory_manipulation(hosts)



            _configure_and_run_playbooks('/cluster-simple-delete.yaml')
            Cluster.objects.get(id=cluster_id).delete()
        except:
            Cluster.objects.get(id=cluster_id).delete()
        response_status = 200
        data = {'message': 'Deleted cluster'}
        return JsonResponse(data=data, status=response_status)

# noinspection PyBroadException
class AnsibleClusterGetAllCluster(View):
    """ API Endpoint to get all the cluster
    """

    @staticmethod
    def get(request, **kwargs):
        """ Respond to GET requests

            param kwargs: named URL parameters, part of the API endpoint
        """

        user_id = kwargs['id']
        
        list_of_all_clusters = list()
        for cluster_instance in Cluster.objects.all():
            
            data = dict()
            data['name'] = cluster_instance.name
            data['id'] = cluster_instance.id
            data['creation_date'] = cluster_instance.creation_date
            data['main_node_ip'] = cluster_instance.main_node_ip
            data['cluster_type'] = cluster_instance.cluster_type
            print(data)
            # Requesting cluster specific details by making an API call to
            # Flink using specific node id(localhost in this case)

            try:
                if cluster_instance.cluster_type == "Apache Flink":
                    requests.get("http://" + data['main_node_ip'] + ":8086")
                elif cluster_instance.cluster_type == "Apache Storm":
                    requests.get("http://" + data['main_node_ip'] + ":8080")
                elif cluster_instance.cluster_type == "Apache Spark":
                    requests.get("http://" + data['main_node_ip'] + ":8087")
                elif cluster_instance.cluster_type == "Apache Kafka Streams":
                    # Kafka Streams has no cluster-level daemon/REST API of
                    # its own to poll (it only runs as a process for the
                    # duration of a job, see runner.py::_run_kafkastreams_job)
                    # -- "Running" here means the shared Kafka broker this
                    # engine depends on is up, via its own JMX exporter
                    # (already started for every engine by
                    # cluster-complex-kafka-start.yaml). Explicit timeout:
                    # requests.get() has no default one, and this whole view
                    # loops over every registered cluster on one
                    # --nothreading dev server -- a single unresponsive
                    # target (e.g. the long-running JMX exporter hanging
                    # after extended scraping) would otherwise freeze every
                    # cluster's status check, not just this one.
                    requests.get("http://" + data['main_node_ip'] + ":7075", timeout=5)
                elif cluster_instance.cluster_type == "RisingWave":
                    # No REST job-overview API (Part B.3) -- hit the meta
                    # node's own dashboard (:5691), which serves a real HTTP
                    # page (confirmed live) and only runs while meta is up.
                    requests.get("http://" + data['main_node_ip'] + ":5691", timeout=5)
                else:
                    raise Exception

                data['status'] = 'Running'
            except:
                data['status'] = 'Stopped'
            
            list_of_all_clusters.append(data)
        prepared_data = {"list_of_cluster": list_of_all_clusters}
        print('prepared data is:::::::::::::::::::')
        print(prepared_data)
        
        return JsonResponse(data=prepared_data, status=200)

# noinspection PyBroadException
class AnsibleClusterGetCluster(View):
    """ API Endpoint to delete the cluster
    """

    @staticmethod
    def get(request, **kwargs):
        """ Respond to GET requests

            param kwargs: named URL parameters, part of the API endpoint
        """

        cluster_id = kwargs['id']

        data = Cluster.objects.get(id=cluster_id)

        prepared_data = dict()
        prepared_data['id'] = data.id
        prepared_data['name'] = data.name
        try:
            if data.cluster_type == "Apache Flink":
                requests.get("http://" + data.main_node_ip + ":8086")
            elif data.cluster_type == "Apache Storm":
                requests.get("http://" + data.main_node_ip + ":8080")
            elif data.cluster_type == "Apache Spark":
                requests.get("http://" + data.main_node_ip + ":8087")
            elif data.cluster_type == "Apache Kafka Streams":
                # See the identical branch in AnsibleClusterGetAllCluster.
                requests.get("http://" + data.main_node_ip + ":7075", timeout=5)
            elif data.cluster_type == "RisingWave":
                # See the identical branch in AnsibleClusterGetAllCluster.
                requests.get("http://" + data.main_node_ip + ":5691", timeout=5)
            else:
                raise Exception

            prepared_data['status'] = 'Running'
        except:
            prepared_data['status'] = 'Stopped'
        return JsonResponse(data=prepared_data, status=200)


class AnsibleClusterJobCreation(View):
    """ API Endpoint to create a job
    """

    @staticmethod
    def post(request, **kwargs):
        cluster_id = kwargs['id']
        cluster = Cluster.objects.get(id=cluster_id)
        configs = json.loads(request.body)
        print(configs)

        iterations = int(configs['job_iterations']) \
            if not configs["job_enumeration_strategy"] == "None" else 1
        runner = JobRunner(cluster, iterations,
                           int(configs['num_of_times_job_run']))

        configs = runner.prepare_jobs(configs)
        executed = []
        for c in runner.make_configs(configs):
            print("Run job {} with parallelism degree {}"
                  .format(c["job_sample"], c["job_pds"]))
            print("Full config:", c)
            job_id = runner.run(c)
            c["job_id"] = job_id
            runner.get_logs(c)
            executed.append(c)
            if not runner.is_terminated(c):
                runner.terminate(c, job_id)
                runner.wait_for_termination(c)
        runner.cleanup_jobs(configs)

        return JsonResponse({'status': 'Cluster Job created successfully',
                             'success': True})


def _config_manipulation_ansible(configurations, constants = constants_flink):
    
    ansible_vars = dict()
    ansible_vars["job_run_time"] = int(configurations['job_run_time'])*60
    #ansible_vars["main_node_ip"] = configurations["main_node_ip"]
    #ansible_vars["job_class"] = configurations["job_class"]

    if constants is constants_kafkastreams:
        # -graceMs, consumed by com.kom.dsp.utils.WindowConfig (see
        # kafkastreams-provide-job.yaml) -- -1 (the default) means "let
        # WindowConfig pick its own default", not "zero grace". Set once
        # here rather than in each of the ~14 job_class branches below,
        # since grace period is a Kafka-Streams-specific concept the other
        # engines' constants modules have no equivalent field for.
        ansible_vars["job_grace_period_ms"] = configurations.get('job_grace_period_ms', -1)

    print(configurations)
    
    ansible_vars['partition'] = configurations['job_pds'][0]
    print("partition#################################################",ansible_vars['partition'])
    print(configurations)

    if configurations['job_class'] == constants.WORD_COUNT:
        ansible_vars["job_name"] = constants.WORD_COUNT.replace(" ", "")
        ansible_vars["job_program"] = constants.WORD_COUNT_PROGRAM
        ansible_vars["google_lateness"] = configurations['job_google_lateness']
        ansible_vars["job_parallelization"] = ','.join(configurations['job_pds'])
        ansible_vars["job_query_number"] = configurations['job_query_name']
        ansible_vars["job_mode"] = configurations['job_class_input_type']
        ansible_vars["job_window_size"] = configurations['job_window_size']
        ansible_vars["job_window_slide_size"] = configurations['job_window_slide_size']
        ansible_vars["producer_program"] = "Word_Count"
        producer_replicas = 1
        ansible_vars["producer_event_per_second"] = \
            int(int(configurations['job_class_input']) /
                (configurations['betas_num'] * producer_replicas))
        ansible_vars["producer_instances"] = list(range(producer_replicas))
        ansible_vars["job_threshold"] = configurations['job_threshold']
        ansible_vars["num_workers"] = configurations["num_workers"] \
            if "num_workers" in configurations else 0

        if configurations['job_class_input_type'] == constants.KAFKA:
            ansible_vars["producer_bootstrap_server"] = configurations['main_node_ip'].split(".")[0] + ':9092'
            ansible_vars["main_ip"] = configurations['main_node_ip'].split(".")[0]
            ansible_vars["job_input"] = constants.WORD_COUNT_INPUT
            ansible_vars["job_output"] = constants.WORD_COUNT_OUTPUT
        else:
            ansible_vars["job_input"] = configurations['job_class_input']
            ansible_vars["job_output"] = configurations['job_class_input']

    elif configurations['job_class'] == constants.SMART_GRID:
        ansible_vars["job_name"] = constants.SMART_GRID.replace(" ", "")
        ansible_vars["job_program"] = constants.SMART_GRID_PROGRAM
        ansible_vars["job_parallelization"] = ','.join(configurations['job_pds'])
        ansible_vars["google_lateness"] = configurations['job_google_lateness']
        ansible_vars["job_query_number"] = configurations['job_query_name']
        ansible_vars["job_mode"] = configurations['job_class_input_type']
        ansible_vars["job_window_size"] = configurations['job_window_size']
        ansible_vars["job_window_slide_size"] = configurations['job_window_slide_size']
        
        ansible_vars["producer_program"] = "Smart_Grid"
        producer_replicas = 1
        ansible_vars["producer_event_per_second"] = \
            int(int(configurations['job_class_input']) /
                (configurations['betas_num'] * producer_replicas))
        ansible_vars["producer_instances"] = list(range(producer_replicas))
        ansible_vars["job_threshold"] = configurations['job_threshold']
        ansible_vars["num_workers"] = configurations["num_workers"] \
            if "num_workers" in configurations else 0
        if configurations['job_class_input_type'] == constants.KAFKA:
            ansible_vars["producer_bootstrap_server"] = configurations['main_node_ip'].split(".")[0]+":9092"
            ansible_vars["main_ip"] = configurations['main_node_ip'].split(".")[0]
            ansible_vars["job_input"] = constants.SMART_GRID_INPUT
            ansible_vars["job_output"] = constants.SMART_GRID_OUTPUT
        else:
            ansible_vars["job_input"] = configurations['job_class_input']
            ansible_vars["job_output"] = configurations['job_class_input']

    elif configurations['job_class'] == constants.AD_ANALYTICS:
        ansible_vars["job_name"] = constants.AD_ANALYTICS.replace(" ", "")
        ansible_vars["job_program"] = constants.AD_ANALYTICS_PROGRAM
        ansible_vars["job_parallelization"] = ','.join(configurations['job_pds'])
        ansible_vars["google_lateness"] = configurations['job_google_lateness']
        ansible_vars["job_query_number"] = configurations['job_query_name']
        ansible_vars["job_mode"] = configurations['job_class_input_type']
        ansible_vars["job_window_size"] = configurations['job_window_size']
        ansible_vars["job_window_slide_size"] = configurations['job_window_slide_size']
        ansible_vars["producer_program"] = "Ad_Analytics"
        producer_replicas = 1
        ansible_vars["producer_event_per_second"] = \
            int(int(configurations['job_class_input']) /
                (configurations['betas_num'] * producer_replicas))
        ansible_vars["producer_instances"] = list(range(producer_replicas))
        ansible_vars["job_threshold"] = configurations['job_threshold']
        ansible_vars["num_workers"] = configurations["num_workers"] \
            if "num_workers" in configurations else 0
        if configurations['job_class_input_type'] == constants.KAFKA:
            ansible_vars["producer_bootstrap_server"] = configurations['main_node_ip'].split(".")[0]+":9092"
            ansible_vars["main_ip"] = configurations['main_node_ip'].split(".")[0]
            ansible_vars["job_input"] = constants.AD_ANALYTICS_INPUT
            ansible_vars["job_output"] = constants.AD_ANALYTICS_OUTPUT

    elif configurations['job_class'] == constants.GOOGLE_CLOUD_MONITORING:
        ansible_vars["job_name"] = constants.GOOGLE_CLOUD_MONITORING.replace(" ", "")
        ansible_vars["job_program"] = constants.GOOGLE_CLOUD_MONITORING_PROGRAM
        ansible_vars["job_parallelization"] = ','.join(configurations['job_pds'])
        ansible_vars["job_query_number"] = configurations['job_query_name']
        ansible_vars["google_lateness"] = configurations['job_google_lateness']
        ansible_vars["job_mode"] = configurations['job_class_input_type']
        ansible_vars["job_window_size"] = configurations['job_window_size']
        ansible_vars["job_window_slide_size"] = configurations['job_window_slide_size']
        ansible_vars["producer_program"] = "Google_Cloud_Monitoring"
        producer_replicas = 1
        ansible_vars["producer_event_per_second"] = \
            int(int(configurations['job_class_input']) /
                (configurations['betas_num'] * producer_replicas))
        ansible_vars["producer_instances"] = list(range(producer_replicas))
        ansible_vars["job_threshold"] = configurations['job_threshold']
        ansible_vars["num_workers"] = configurations["num_workers"] \
            if "num_workers" in configurations else 0
        if configurations['job_class_input_type'] == constants.KAFKA:
            ansible_vars["producer_bootstrap_server"] = configurations['main_node_ip'].split(".")[0]+":9092"
            ansible_vars["main_ip"] = configurations['main_node_ip'].split(".")[0]
            ansible_vars["job_input"] = constants.GOOGLE_CLOUD_MONITORING_INPUT
            ansible_vars["job_output"] = constants.GOOGLE_CLOUD_MONITORING_OUTPUT
    
    elif configurations['job_class'] == constants.SENTIMENT_ANALYSIS:
        ansible_vars["job_name"] = constants.SENTIMENT_ANALYSIS.replace(" ", "")
        ansible_vars["job_program"] = constants.SENTIMENT_ANALYSIS_PROGRAM
        ansible_vars["job_parallelization"] = ','.join(configurations['job_pds'])
        ansible_vars["job_query_number"] = configurations['job_query_name']
        ansible_vars["google_lateness"] = configurations['job_google_lateness']
        ansible_vars["job_mode"] = configurations['job_class_input_type']
        ansible_vars["job_window_size"] = configurations['job_window_size']
        ansible_vars["job_window_slide_size"] = configurations['job_window_slide_size']
        ansible_vars["producer_program"] = "Sentiment_Analysis"
        producer_replicas = 1
        ansible_vars["producer_event_per_second"] = \
            int(int(configurations['job_class_input']) /
                (configurations['betas_num'] * producer_replicas))
        ansible_vars["producer_instances"] = list(range(producer_replicas))
        ansible_vars["job_threshold"] = configurations['job_threshold']
        ansible_vars["num_workers"] = configurations["num_workers"] \
            if "num_workers" in configurations else 0
        if configurations['job_class_input_type'] == constants.KAFKA:
            ansible_vars["producer_bootstrap_server"] = configurations['main_node_ip'].split(".")[0]+":9092"
            ansible_vars["main_ip"] = configurations['main_node_ip'].split(".")[0]
            ansible_vars["job_input"] = constants.SENTIMENT_ANALYSIS_INPUT
            ansible_vars["job_output"] = constants.SENTIMENT_ANALYSIS_OUTPUT

    elif configurations['job_class'] == constants.SPIKE_DETECTION:
        ansible_vars["job_name"] = constants.SPIKE_DETECTION.replace(" ", "")
        ansible_vars["job_program"] = constants.SPIKE_DETECTION_PROGRAM
        ansible_vars["job_parallelization"] = ','.join(configurations['job_pds'])
        ansible_vars["job_query_number"] = configurations['job_query_name']
        ansible_vars["google_lateness"] = configurations['job_google_lateness']
        ansible_vars["job_mode"] = configurations['job_class_input_type']
        ansible_vars["job_window_size"] = configurations['job_window_size']
        ansible_vars["job_window_slide_size"] = configurations['job_window_slide_size']
        ansible_vars["producer_program"] = "Spike_detection"
        producer_replicas = 1
        ansible_vars["producer_event_per_second"] = \
            int(int(configurations['job_class_input']) /
                (configurations['betas_num'] * producer_replicas))
        ansible_vars["producer_instances"] = list(range(producer_replicas))
        ansible_vars["job_threshold"] = configurations['job_threshold']
        ansible_vars["num_workers"] = configurations["num_workers"] \
            if "num_workers" in configurations else 0
        if configurations['job_class_input_type'] == constants.KAFKA:
            ansible_vars["producer_bootstrap_server"] = configurations['main_node_ip'].split(".")[0]+":9092"
            ansible_vars["main_ip"] = configurations['main_node_ip'].split(".")[0]
            ansible_vars["job_input"] = constants.SPIKE_DETECTION_INPUT
            ansible_vars["job_output"] = constants.SPIKE_DETECTION_OUTPUT

    elif configurations['job_class'] == constants.LOG_ANALYZER:
        ansible_vars["job_name"] = constants.LOG_ANALYZER.replace(" ", "")
        ansible_vars["job_program"] = constants.LOG_ANALYZER_PROGRAM
        ansible_vars["job_parallelization"] = ','.join(configurations['job_pds'])
        ansible_vars["job_query_number"] = configurations['job_query_name']
        ansible_vars["google_lateness"] = configurations['job_google_lateness']
        ansible_vars["job_mode"] = configurations['job_class_input_type']
        ansible_vars["job_window_size"] = configurations['job_window_size']
        ansible_vars["job_window_slide_size"] = configurations['job_window_slide_size']
        ansible_vars["producer_program"] = "Log_Processing"
        producer_replicas = 1
        ansible_vars["producer_event_per_second"] = \
            int(int(configurations['job_class_input']) /
                (configurations['betas_num'] * producer_replicas))
        ansible_vars["producer_instances"] = list(range(producer_replicas))
        ansible_vars["job_threshold"] = configurations['job_threshold']
        ansible_vars["num_workers"] = configurations["num_workers"] \
            if "num_workers" in configurations else 0
        if configurations['job_class_input_type'] == constants.KAFKA:
            ansible_vars["producer_bootstrap_server"] = configurations['main_node_ip'].split(".")[0]+":9092"
            ansible_vars["main_ip"] = configurations['main_node_ip'].split(".")[0]
            ansible_vars["job_input"] = constants.LOG_ANALYZER_INPUT
            ansible_vars["job_output"] = constants.LOG_ANALYZER_OUTPUT

    elif configurations['job_class'] == constants.TRENDING_TOPICS:
        ansible_vars["job_name"] = constants.TRENDING_TOPICS.replace(" ", "")
        ansible_vars["job_program"] = constants.TRENDING_TOPICS_PROGRAM
        ansible_vars["job_parallelization"] = ','.join(configurations['job_pds'])
        ansible_vars["job_query_number"] = configurations['job_query_name']
        ansible_vars["google_lateness"] = configurations['job_google_lateness']
        ansible_vars["job_mode"] = configurations['job_class_input_type']
        ansible_vars["job_window_size"] = configurations['job_window_size']
        ansible_vars["job_window_slide_size"] = configurations['job_window_slide_size']
        ansible_vars["producer_program"] = "Trending_Topic"
        producer_replicas = 1
        ansible_vars["producer_event_per_second"] = \
            int(int(configurations['job_class_input']) /
                (configurations['betas_num'] * producer_replicas))
        ansible_vars["producer_instances"] = list(range(producer_replicas))
        ansible_vars["job_threshold"] = configurations['job_threshold']
        ansible_vars["num_workers"] = configurations["num_workers"] \
            if "num_workers" in configurations else 0
        if configurations['job_class_input_type'] == constants.KAFKA:
            ansible_vars["producer_bootstrap_server"] = configurations['main_node_ip'].split(".")[0]+":9092"
            ansible_vars["main_ip"] = configurations['main_node_ip'].split(".")[0]
            ansible_vars["job_input"] = constants.TRENDING_TOPICS_INPUT
            ansible_vars["job_output"] = constants.TRENDING_TOPICS_OUTPUT

    
    elif configurations['job_class'] == constants.BARGAIN_INDEX:
        ansible_vars["job_name"] = constants.BARGAIN_INDEX.replace(" ", "")
        ansible_vars["job_program"] = constants.BARGAIN_INDEX_PROGRAM
        ansible_vars["job_parallelization"] = ','.join(configurations['job_pds'])
        ansible_vars["job_query_number"] = configurations['job_query_name']
        ansible_vars["google_lateness"] = configurations['job_google_lateness']
        ansible_vars["job_mode"] = configurations['job_class_input_type']
        ansible_vars["job_window_size"] = configurations['job_window_size']
        ansible_vars["job_window_slide_size"] = configurations['job_window_slide_size']
        ansible_vars["producer_program"] = "Bargain_Index"
        producer_replicas = 1
        ansible_vars["producer_event_per_second"] = \
            int(int(configurations['job_class_input']) /
                (configurations['betas_num'] * producer_replicas))
        ansible_vars["producer_instances"] = list(range(producer_replicas))
        ansible_vars["job_threshold"] = configurations['job_threshold']
        ansible_vars["num_workers"] = configurations["num_workers"] \
            if "num_workers" in configurations else 0
        if configurations['job_class_input_type'] == constants.KAFKA:
            ansible_vars["producer_bootstrap_server"] = configurations['main_node_ip'].split(".")[0]+":9092"
            ansible_vars["main_ip"] = configurations['main_node_ip'].split(".")[0]
            ansible_vars["job_input"] = constants.BARGAIN_INDEX_INPUT
            ansible_vars["job_output"] = constants.BARGAIN_INDEX_OUTPUT

    
    elif configurations['job_class'] == constants.CLICK_ANALYTICS:
        ansible_vars["job_name"] = constants.CLICK_ANALYTICS.replace(" ", "")
        ansible_vars["job_program"] = constants.CLICK_ANALYTICS_PROGRAM
        ansible_vars["job_parallelization"] = ','.join(configurations['job_pds'])
        ansible_vars["job_query_number"] = configurations['job_query_name']
        ansible_vars["google_lateness"] = configurations['job_google_lateness']
        ansible_vars["job_mode"] = configurations['job_class_input_type']
        ansible_vars["job_window_size"] = configurations['job_window_size']
        ansible_vars["job_window_slide_size"] = configurations['job_window_slide_size']
        ansible_vars["producer_program"] = "Click_Analytics"
        producer_replicas = 1
        ansible_vars["producer_event_per_second"] = \
            int(int(configurations['job_class_input']) /
                (configurations['betas_num'] * producer_replicas))
        ansible_vars["producer_instances"] = list(range(producer_replicas))
        ansible_vars["job_threshold"] = configurations['job_threshold']
        ansible_vars["num_workers"] = configurations["num_workers"] \
            if "num_workers" in configurations else 0
        if configurations['job_class_input_type'] == constants.KAFKA:
            ansible_vars["producer_bootstrap_server"] = configurations['main_node_ip'].split(".")[0]+":9092"
            ansible_vars["main_ip"] = configurations['main_node_ip'].split(".")[0]
            ansible_vars["job_input"] = constants.CLICK_ANALYTICS_INPUT
            ansible_vars["job_output"] = constants.CLICK_ANALYTICS_OUTPUT

    elif configurations['job_class'] == constants.LINEAR_ROAD:
        ansible_vars["job_name"] = constants.LINEAR_ROAD.replace(" ", "")
        ansible_vars["job_program"] = constants.LINEAR_ROAD_PROGRAM
        ansible_vars["job_parallelization"] = ','.join(configurations['job_pds'])
        ansible_vars["job_query_number"] = configurations['job_query_name']
        ansible_vars["google_lateness"] = configurations['job_google_lateness']
        ansible_vars["job_mode"] = configurations['job_class_input_type']
        ansible_vars["job_window_size"] = configurations['job_window_size']
        ansible_vars["job_window_slide_size"] = configurations['job_window_slide_size']
        ansible_vars["producer_program"] = "LRB"
        producer_replicas = 1
        ansible_vars["producer_event_per_second"] = \
            int(int(configurations['job_class_input']) /
                (configurations['betas_num'] * producer_replicas))
        ansible_vars["producer_instances"] = list(range(producer_replicas))
        ansible_vars["job_threshold"] = configurations['job_threshold']
        ansible_vars["num_workers"] = configurations["num_workers"] \
            if "num_workers" in configurations else 0
        if configurations['job_class_input_type'] == constants.KAFKA:
            ansible_vars["producer_bootstrap_server"] = configurations['main_node_ip'].split(".")[0]+":9092"
            ansible_vars["main_ip"] = configurations['main_node_ip'].split(".")[0]
            ansible_vars["job_input"] = constants.LINEAR_ROAD_INPUT
            ansible_vars["job_output"] = constants.LINEAR_ROAD_OUTPUT

    elif configurations['job_class'] == constants.TPCH:
        ansible_vars["job_name"] = constants.TPCH.replace(" ", "")
        ansible_vars["job_program"] = constants.TPCH_PROGRAM
        ansible_vars["job_parallelization"] = ','.join(configurations['job_pds'])
        ansible_vars["job_query_number"] = configurations['job_query_name']
        ansible_vars["google_lateness"] = configurations['job_google_lateness']
        ansible_vars["job_mode"] = configurations['job_class_input_type']
        ansible_vars["job_window_size"] = configurations['job_window_size']
        ansible_vars["job_window_slide_size"] = configurations['job_window_slide_size']
        ansible_vars["producer_program"] = "Tpch"
        producer_replicas = 1
        ansible_vars["producer_event_per_second"] = \
            int(int(configurations['job_class_input']) /
                (configurations['betas_num'] * producer_replicas))
        ansible_vars["producer_instances"] = list(range(producer_replicas))
        ansible_vars["job_threshold"] = configurations['job_threshold']
        ansible_vars["num_workers"] = configurations["num_workers"] \
            if "num_workers" in configurations else 0
        if configurations['job_class_input_type'] == constants.KAFKA:
            ansible_vars["producer_bootstrap_server"] = configurations['main_node_ip'].split(".")[0]+":9092"
            ansible_vars["main_ip"] = configurations['main_node_ip'].split(".")[0]
            ansible_vars["job_input"] = constants.TPCH_INPUT
            ansible_vars["job_output"] = constants.TPCH_OUTPUT
    
    elif configurations['job_class'] == constants.MACHINE_OUTLIER:
        ansible_vars["job_name"] = constants.MACHINE_OUTLIER.replace(" ", "")
        ansible_vars["job_program"] = constants.MACHINE_OUTLIER_PROGRAM
        ansible_vars["job_parallelization"] = ','.join(configurations['job_pds'])
        ansible_vars["job_query_number"] = configurations['job_query_name']
        ansible_vars["google_lateness"] = configurations['job_google_lateness']
        ansible_vars["job_mode"] = configurations['job_class_input_type']
        ansible_vars["job_window_size"] = configurations['job_window_size']
        ansible_vars["job_window_slide_size"] = configurations['job_window_slide_size']
        ansible_vars["producer_program"] = "Machine_Outlier"
        producer_replicas = 1
        ansible_vars["producer_event_per_second"] = \
            int(int(configurations['job_class_input']) /
                (configurations['betas_num'] * producer_replicas))
        ansible_vars["producer_instances"] = list(range(producer_replicas))
        ansible_vars["job_threshold"] = configurations['job_threshold']
        ansible_vars["num_workers"] = configurations["num_workers"] \
            if "num_workers" in configurations else 0
        if configurations['job_class_input_type'] == constants.KAFKA:
            ansible_vars["producer_bootstrap_server"] = configurations['main_node_ip'].split(".")[0]+":9092"
            ansible_vars["main_ip"] = configurations['main_node_ip'].split(".")[0]
            ansible_vars["job_input"] = constants.MACHINE_OUTLIER_INPUT
            ansible_vars["job_output"] = constants.MACHINE_OUTLIER_OUTPUT

    elif configurations['job_class'] == constants.TRAFFIC_MONITORING:
        ansible_vars["job_name"] = constants.TRAFFIC_MONITORING.replace(" ", "")
        ansible_vars["job_program"] = constants.TRAFFIC_MONITORING_PROGRAM
        ansible_vars["job_parallelization"] = ','.join(configurations['job_pds'])
        ansible_vars["job_query_number"] = configurations['job_query_name']
        ansible_vars["google_lateness"] = configurations['job_google_lateness']
        ansible_vars["job_mode"] = configurations['job_class_input_type']
        ansible_vars["job_window_size"] = configurations['job_window_size']
        ansible_vars["job_window_slide_size"] = configurations['job_window_slide_size']
        ansible_vars["producer_program"] = "Traffic_Monitoring"
        producer_replicas = 1
        ansible_vars["producer_event_per_second"] = \
            int(int(configurations['job_class_input']) /
                (configurations['betas_num'] * producer_replicas))
        ansible_vars["producer_instances"] = list(range(producer_replicas))
        ansible_vars["job_threshold"] = configurations['job_threshold']
        ansible_vars["num_workers"] = configurations["num_workers"] \
            if "num_workers" in configurations else 0
        if configurations['job_class_input_type'] == constants.KAFKA:
            ansible_vars["producer_bootstrap_server"] = configurations['main_node_ip'].split(".")[0]+":9092"
            ansible_vars["main_ip"] = configurations['main_node_ip'].split(".")[0]
            ansible_vars["job_input"] = constants.TRAFFIC_MONITORING_INPUT
            ansible_vars["job_output"] = constants.TRAFFIC_MONITORING_OUTPUT

    elif configurations['job_class'] == getattr(constants, 'LINEAR_QUERY', None):
        # Synthetic workload: the topology generates its own tuples in-process
        # (-source synthetic), so no Kafka producer runs for it (skip_producer).
        # Sink still writes to Kafka, same as the real workloads.
        ansible_vars["job_name"] = constants.LINEAR_QUERY.replace(" ", "")
        ansible_vars["job_program"] = constants.LINEAR_QUERY_PROGRAM
        ansible_vars["google_lateness"] = configurations['job_google_lateness']
        ansible_vars["job_parallelization"] = ','.join(configurations['job_pds'])
        ansible_vars["job_query_number"] = configurations['job_query_name']
        ansible_vars["job_mode"] = constants.KAFKA
        ansible_vars["job_event_rate"] = int(configurations['job_class_input'])
        ansible_vars["job_window_size"] = configurations['job_window_size']
        ansible_vars["job_window_slide_size"] = configurations['job_window_slide_size']
        ansible_vars["job_threshold"] = configurations['job_threshold']
        ansible_vars["num_workers"] = configurations["num_workers"] \
            if "num_workers" in configurations else 0
        ansible_vars["synthetic_job"] = True
        ansible_vars["producer_bootstrap_server"] = configurations['main_node_ip'].split(".")[0] + ':9092'
        ansible_vars["main_ip"] = configurations['main_node_ip'].split(".")[0]
        ansible_vars["job_input"] = constants.LINEAR_QUERY_INPUT
        ansible_vars["job_output"] = constants.LINEAR_QUERY_OUTPUT

    print(ansible_vars)
    return ansible_vars

class AnsibleNodeGetAll(View):

    @staticmethod
    def get(request, **kwargs):
        list_of_all_nodes = list()
        for node in Nodes.objects.all():
            
            data = dict()
            data['domain_name'] = node.domain_name
            data['creation_date'] = node.creation_date
            data['user'] = node.user

                
            
            list_of_all_nodes.append(data)
        prepared_data = {"list_of_nodes": list_of_all_nodes}
        
        return JsonResponse(data=prepared_data, status=200)

class CreateNode(View):
    
    @staticmethod
    def post(request,**kwargs):

        # loading request.body to a python dictionary(configs)
        configs = json.loads(request.body)
        
        # checking if a clustername was provided
        if not configs["domain_name"]:
            return JsonResponse(data={'status': 'Please check node names', 'success': False},
                                status=406)

        # Iterating through all the Cluster objects from the DB and checking
        # if the provided cluster name already exists
        for node in Nodes.objects.all():
            if configs["domain_name"] == node.domain_name:
                return JsonResponse(data={'status': 'Another node with same domain-name exists', 'success': False},
                                    status=406)
        # If no such cluster name exists, we create a new cluster object(row in the Cluster Table)
        Nodes.objects.create(domain_name=configs["domain_name"],user=configs["user"])
        
        return JsonResponse(data={'status': "Node created successfully",'success': True},
                            status=200)

class DeleteNode(View):
    
    @staticmethod
    def delete(request,**kwargs):
        node_domain_name = kwargs['domain_name']

        Nodes.objects.get(domain_name=node_domain_name).delete()
        
        response_status = 200
        data = {'message': 'Deleted node'}
        return JsonResponse(data=data, status=response_status)
