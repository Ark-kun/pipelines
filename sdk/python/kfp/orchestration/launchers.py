#%%
from collections import OrderedDict
from pathlib import Path, PurePosixPath
import kfp

from kfp.components import create_component_from_func, create_graph_component_from_pipeline_func
from kfp.components._components import _resolve_command_line_and_paths
from kfp.components._structures import *
from kfp.components._naming import _sanitize_file_name, _sanitize_kubernetes_resource_name

#%%
def run_container_tasks(
    task_specs: OrderedDict,
    graph_input_arguments: dict,
    task_launcher,
    output_uri_generator,
):
    import os

    graph_input_arguments = graph_input_arguments or {}

    def generate_execution_id_for_task(task_spec):
        return str(id(task_spec))

    #task_id_to_task_map = {}
    task_id_to_output_uris_map = {} # Task ID -> output name -> path

    for task_id, task in task_specs.items():
        execution_id = generate_execution_id_for_task(task)
        # resolving task arguments
        resolved_argument_values = {}
        resolved_argument_paths = {}
        for input_name, argument in task.arguments.items():
            resolved_argument_path = None
            resolved_argument_value = None
            #if isinstance(argument, str):
            if isinstance(argument, (str, int, float)):
                resolved_argument_value = str(argument)
                #resolved_argument_path = ???
            elif isinstance(argument, GraphInputArgument):
                resolved_argument_value = graph_input_arguments[argument.graph_input.input_name]
                #resolved_argument_path = ???
            elif isinstance(argument, TaskOutputArgument):
                resolved_argument_uri = task_id_to_output_uris_map[argument.task_output.task_id][argument.task_output.output_name]
                resolved_argument_value = download_string(resolved_argument_uri) # TODO: Refactor value resolving to be lazy
            else:
                raise TypeError("Unsupported argument type: {} - {}.".format(str(type(argument).__name__), str(argument)))

            if resolved_argument_path:
                resolved_argument_paths[input_name] = resolved_argument_path
            if resolved_argument_value:
                resolved_argument_values[input_name] = resolved_argument_value

        component_spec = task.component_ref.spec

        output_uris_map = {
            output.name: output_uri_generator.generate_execution_output_uri(execution_id, output.name)
            for output in component_spec.outputs
        }
        task_id_to_output_uris_map[task_id] = output_uris_map

        resolved_task_spec = TaskSpec(
            component_ref=task.component_ref,
            arguments=resolved_argument_values,
        )

        task_launcher.launch_container_task(
            task_spec=resolved_task_spec,
            input_uris_map=resolved_argument_paths,
            output_uris_map=output_uris_map,
        )


class LocalPathGenerator:
    def __init__(self, output_root_dir: str):
        self.output_root_dir = output_root_dir

    def generate_execution_output_uri(self, execution_id: str, output_name: str) -> str:
        from pathlib import Path
        _single_io_file_name = 'data'
        path = str(Path(self.output_root_dir) / str(execution_id) / _sanitize_file_name(output_name) / _single_io_file_name)
        return path

class UriPathGenerator:
    def __init__(self, output_root_dir_uri: str):
        self.output_root_dir_uri = output_root_dir_uri

    def generate_execution_output_uri(self, execution_id: str, output_name: str) -> str:
        from pathlib import Path
        _single_io_file_name = 'data'
        import urllib
        relative_uri = '/'.join(str(execution_id), _sanitize_file_name(output_name), _single_io_file_name)
        uri = urllib.parse.urljoin(self.output_root_dir_uri, relative_uri)
        return uri

def upload(source_local_path: str, destination_uri: str):
    # Only local URIs are supported here now
    dest_path = destination_uri
    import shutil
    Path(dest_path).parent.mkdir(parents=True, exist_ok=True)
    #shutil.copytree(source_local_path, dest_path, symlinks=True)
    shutil.copy(source_local_path, dest_path, follow_symlinks=False)


def download(source_uri: str, destination_local_path: str):
    # Only local URIs are supported here now
    import shutil
    shutil.copytree(source_uri, destination_local_path, symlinks=True)


def download_string(source_uri: str) -> str:
    # Only local URIs are supported here now
    return Path(source_uri).read_text()


def download_bytes(source_uri: str) -> bytes:
    # Only local URIs are supported here now
    return Path(source_uri).read_bytes()


class LocalEnvironmentContainerLauncher:
    def launch_container_task(
        self,
        task_spec,
        input_uris_map: dict = None,
        output_uris_map: dict = None,
    ):
        input_uris_map = input_uris_map or {}
        output_uris_map = output_uris_map or {}


        import os
        import sys
        import subprocess

        import tempfile
        with tempfile.TemporaryDirectory() as tempdir:
            host_workdir = os.path.join(tempdir, 'work')
            #host_logdir = os.path.join(tempdir, 'logs')
            host_input_paths_map = {name: os.path.join(tempdir, 'inputs', _sanitize_file_name(name), 'data') for name in input_uris_map.keys()} # Or just user random temp dirs/subdirs
            host_output_paths_map = {name: os.path.join(tempdir, 'outputs', _sanitize_file_name(name), 'data') for name in output_uris_map.keys()} # Or just user random temp dirs/subdirs

            Path(host_workdir).mkdir(parents=True, exist_ok=True)

            # Getting input data
            for input_name, input_uri in input_uris_map.items():
                input_host_path = host_input_paths_map[input_name]
                Path(input_host_path).parent.mkdir(parents=True, exist_ok=True)
                download(input_uri, input_host_path)

            for output_host_path in host_output_paths_map.values():
                Path(output_host_path).parent.mkdir(parents=True, exist_ok=True)

            component_spec = task_spec.component_ref.spec

            resolved_cmd = _resolve_command_line_and_paths(
                component_spec=component_spec,
                arguments=task_spec.arguments,
                input_path_generator=host_input_paths_map.get,
                output_path_generator=host_output_paths_map.get,
            )

            process_env = os.environ.copy()
            process_env.update(component_spec.implementation.container.env or {})

            res = subprocess.run(
                args=resolved_cmd.command + resolved_cmd.args,
                env=process_env,
                cwd=host_workdir,
            )

            # Storing the output data
            for output_name, output_uri in output_uris_map.items():
                output_host_path = host_output_paths_map[output_name]
                upload(output_host_path, output_uri)

            #print(res)


class DockerContainerLauncher:
    def launch_container_task(
        self,
        task_spec,
        input_uris_map: dict = None,
        output_uris_map: dict = None,
    ):
        input_uris_map = input_uris_map or {}
        output_uris_map = output_uris_map or {}

        input_names = list(input_uris_map.keys())
        output_names = list(output_uris_map.keys())

        import os
        import shutil
        import subprocess
        import sys
        import tempfile

        #with tempfile.TemporaryDirectory() as tempdir: # OSError: [WinError 145] The directory is not empty: 'C:\\Users\\Ark\\AppData\\Local\\Temp\\tmpgej_f0zp\\outputs'
        try:
            tempdir = tempfile.mkdtemp()

            host_workdir = os.path.join(tempdir, 'work')
            #host_logdir = os.path.join(tempdir, 'logs')
            host_input_paths_map = {name: os.path.join(tempdir, 'inputs', _sanitize_file_name(name), 'data') for name in input_names} # Or just user random temp dirs/subdirs
            host_output_paths_map = {name: os.path.join(tempdir, 'outputs', _sanitize_file_name(name), 'data') for name in output_names} # Or just user random temp dirs/subdirs

            Path(host_workdir).mkdir(parents=True, exist_ok=True)

            # Getting input data
            for input_name, input_uri in input_uris_map.items():
                input_host_path = host_input_paths_map[input_name]
                Path(input_host_path).parent.mkdir(parents=True, exist_ok=True)
                download(input_uri, input_host_path)

            for output_host_path in host_output_paths_map.values():
                Path(output_host_path).parent.mkdir(parents=True, exist_ok=True)

            container_input_root = '/tmp/inputs/'
            container_output_root = '/tmp/outputs/'
            container_input_paths_map = {name: str(PurePosixPath(container_input_root) / _sanitize_file_name(name) / 'data') for name in input_names} # Or just user random temp dirs/subdirs
            container_output_paths_map = {name: str(PurePosixPath(container_output_root) / _sanitize_file_name(name) / 'data') for name in output_names} # Or just user random temp dirs/subdirs

            component_spec = task_spec.component_ref.spec

            resolved_cmd = _resolve_command_line_and_paths(
                component_spec=component_spec,
                arguments=task_spec.arguments,
                input_path_generator=container_input_paths_map.get,
                output_path_generator=container_output_paths_map.get,
            )

            container_env = component_spec.implementation.container.env or {}

            volumes = {}
            for input_name in input_names:
                host_dir = os.path.dirname(host_input_paths_map[input_name])
                container_dir = os.path.dirname(container_input_paths_map[input_name])
                volumes[host_dir] = dict(
                    bind=container_dir,
                    #mode='ro',
                    mode='rw', # We're copying the input data anyways, so it's OK if the container modifies it.
                )
            for output_name in output_names:
                host_dir = os.path.dirname(host_output_paths_map[output_name])
                container_dir = os.path.dirname(container_output_paths_map[output_name])
                volumes[host_dir] = dict(
                    bind=container_dir,
                    mode='rw',
                )

            import docker
            docker_client = docker.from_env()
            container_res = docker_client.containers.run(
                image=component_spec.implementation.container.image,
                entrypoint=resolved_cmd.command,
                command=resolved_cmd.args,
                environment=container_env,
                #remove=True,
                volumes=volumes,
            )

            print('Container logs:')
            print(container_res)

            # Storing the output data
            for output_name, output_uri in output_uris_map.items():
                output_host_path = host_output_paths_map[output_name]
                upload(output_host_path, output_uri)
        finally:
            shutil.rmtree(tempdir, ignore_errors=True)


import kubernetes
import logging 

def wait_for_pod_to_stop_pending(client, pod_name: str, timeout_seconds=30):
    logging.info('wait_for_pod_to_stop_pending({})'.format(pod_name))
    pod_watch = kubernetes.watch.Watch()
    #label_selector=pod_name does not work
    core_api = kubernetes.client.CoreV1Api(api_client = client)
    for event in pod_watch.stream(core_api.list_pod_for_all_namespaces, timeout_seconds=timeout_seconds):
        event_type = event['type']
        obj = event['object'] #Also event['raw_object']
        kind = obj.kind
        name = obj.metadata.name
        if kind == 'Pod' and name == pod_name:
            phase = obj.status.phase #One of Pending,Running,Succeeded,Failed,Unknown
            if phase != 'Pending':
                pod_watch.stop()
                return phase
    return None


def wait_for_pod_to_succeed_or_fail(client, pod_name: str, timeout_seconds=30):
    logging.info('wait_for_pod_to_succeed_or_fail({})'.format(pod_name))
    pod_watch = kubernetes.watch.Watch()
    #label_selector=pod_name does not work
    core_api = kubernetes.client.CoreV1Api(api_client = client)
    for event in pod_watch.stream(core_api.list_pod_for_all_namespaces, timeout_seconds=timeout_seconds):
        event_type = event['type']
        obj = event['object'] #Also event['raw_object']
        kind = obj.kind
        name = obj.metadata.name
        if kind == 'Pod' and name == pod_name:
            phase = obj.status.phase #One of Pending,Running,Succeeded,Failed,Unknown
            if phase == 'Succeeded' or phase == 'Failed':
                pod_watch.stop()
                return phase
    return None


class LocalKubernetesContainerLauncher:
    '''Launcher that uses single-node Kubernetes (uses hostPath for data passing)'''
    def __init__(self, namespace: str = "default", service_account: str = None, service_account_name: str = None, client: 'kubernetes.client.ApiClient' = None):
        self._namespace = namespace
        self._service_account = service_account
        self._service_account_name = service_account_name

        import kubernetes
        if client:
            self._k8s_client = client
        else:
            #configuration = kubernetes.client.Configuration()
            try:
                kubernetes.config.load_incluster_config()
            except:
                kubernetes.config.load_kube_config()
            self._k8s_client = kubernetes.client.ApiClient()

    def launch_container_task(
        self,
        task_spec,
        input_uris_map: dict = None,
        output_uris_map: dict = None,
    ):
        input_uris_map = input_uris_map or {}
        output_uris_map = output_uris_map or {}

        input_names = list(input_uris_map.keys())
        output_names = list(output_uris_map.keys())

        import os
        import shutil
        import subprocess
        import sys
        import tempfile

        #with tempfile.TemporaryDirectory() as tempdir: # OSError: [WinError 145] The directory is not empty: 'C:\\Users\\Ark\\AppData\\Local\\Temp\\tmpgej_f0zp\\outputs'
        try:
            tempdir = tempfile.mkdtemp()

            host_workdir = os.path.join(tempdir, 'work')
            #host_logdir = os.path.join(tempdir, 'logs')
            host_input_paths_map = {name: os.path.join(tempdir, 'inputs', _sanitize_file_name(name), 'data') for name in input_names} # Or just user random temp dirs/subdirs
            host_output_paths_map = {name: os.path.join(tempdir, 'outputs', _sanitize_file_name(name), 'data') for name in output_names} # Or just user random temp dirs/subdirs

            Path(host_workdir).mkdir(parents=True, exist_ok=True)

            # Getting input data
            for input_name, input_uri in input_uris_map.items():
                input_host_path = host_input_paths_map[input_name]
                Path(input_host_path).parent.mkdir(parents=True, exist_ok=True)
                download(input_uri, input_host_path)

            for output_host_path in host_output_paths_map.values():
                Path(output_host_path).parent.mkdir(parents=True, exist_ok=True)

            container_input_root = '/tmp/inputs/'
            container_output_root = '/tmp/outputs/'
            container_input_paths_map = {name: str(PurePosixPath(container_input_root) / _sanitize_file_name(name) / 'data') for name in input_names} # Or just user random temp dirs/subdirs
            container_output_paths_map = {name: str(PurePosixPath(container_output_root) / _sanitize_file_name(name) / 'data') for name in output_names} # Or just user random temp dirs/subdirs

            component_spec = task_spec.component_ref.spec

            resolved_cmd = _resolve_command_line_and_paths(
                component_spec=component_spec,
                arguments=task_spec.arguments,
                input_path_generator=container_input_paths_map.get,
                output_path_generator=container_output_paths_map.get,
            )

            import kubernetes
            volumes = []
            volume_mounts = []

            for input_name in input_names:
                host_dir = os.path.dirname(host_input_paths_map[input_name])
                host_dir = '/' + host_dir.replace('\\', '/').replace(':', '') # Fix for Windows https://github.com/kubernetes/kubernetes/issues/59876
                container_dir = os.path.dirname(container_input_paths_map[input_name])
                volume_name = _sanitize_kubernetes_resource_name('inputs-' + input_name)
                volumes.append(
                    kubernetes.client.V1Volume(
                        name=volume_name,
                        host_path=kubernetes.client.V1HostPathVolumeSource(
                            path=host_dir,
                            #type=?
                        )
                    )
                )
                volume_mounts.append(
                    kubernetes.client.V1VolumeMount(
                        name=volume_name,
                        mount_path=container_dir,
                        #mount_propagation=?
                        read_only=False, # We're copying the input data anyways, so it's OK if the container modifies it.
                        #sub_path=....
                    )
                )
            for output_name in output_names:
                host_dir = os.path.dirname(host_output_paths_map[output_name])
                host_dir = '/' + host_dir.replace('\\', '/').replace(':', '') # Fix for Windows https://github.com/kubernetes/kubernetes/issues/59876
                container_dir = os.path.dirname(container_output_paths_map[output_name])
                volume_name = _sanitize_kubernetes_resource_name('outputs-' + output_name)
                volumes.append(
                    kubernetes.client.V1Volume(
                        name=volume_name,
                        host_path=kubernetes.client.V1HostPathVolumeSource(
                            path=host_dir,
                            #type=?
                        )
                    )
                )
                volume_mounts.append(
                    kubernetes.client.V1VolumeMount(
                        name=volume_name,
                        mount_path=container_dir,
                        #sub_path=....
                    )
                )

            container_env = [
                kubernetes.client.V1EnvVar(name=name, value=value)
                for name, value in (component_spec.implementation.container.env or {}).items()
            ]
            main_container_spec = kubernetes.client.V1Container(
                name='main',
                image=component_spec.implementation.container.image,
                command=resolved_cmd.command,
                args=resolved_cmd.args,
                env=container_env,
                volume_mounts=volume_mounts,
            )

            pod_spec=kubernetes.client.V1PodSpec(
                init_containers=[],
                containers=[
                    main_container_spec,
                ],
                volumes=volumes,
                restart_policy='Never',
                service_account=self._service_account,
                service_account_name=self._service_account_name,
            )

            pod=kubernetes.client.V1Pod(
                api_version='v1',
                kind='Pod',
                metadata=kubernetes.client.V1ObjectMeta(
                    #name='',
                    generate_name='task-pod-',
                    #namespace=self._namespace,
                    labels={},
                    annotations={},
                    owner_references=[
                        #kubernetes.client.V1OwnerReference(),
                    ],
                ),
                spec=pod_spec,
            )

            core_api = kubernetes.client.CoreV1Api(api_client=self._k8s_client)
            pod_res = core_api.create_namespaced_pod(
                namespace=self._namespace,
                body=pod,
            )

            print('Pod name:')
            print(pod_res.metadata.name)

            pod_name = pod_res.metadata.name
            wait_for_pod_to_stop_pending(client=self._k8s_client, pod_name=pod_name)
            wait_for_pod_to_succeed_or_fail(client=self._k8s_client, pod_name=pod_name, timeout_seconds=30)

            # Storing the output data
            for output_name, output_uri in output_uris_map.items():
                output_host_path = host_output_paths_map[output_name]
                upload(output_host_path, output_uri)
        finally:
            shutil.rmtree(tempdir, ignore_errors=True)



# %%
def add(a: int, b: int) -> int:
    print('add(a={};b={})'.format(a, b))
    return a + b

add_op = create_component_from_func(add, base_image='python:3.8')

task = add_op(3, 5)

def pipeline1_func():
    task1 = add_op(3, 5)
    task2 = add_op(3, task1.outputs['Output'])

pipeline1_component = create_graph_component_from_pipeline_func(pipeline1_func)
#%%

pipeline1_task = pipeline1_component()

run_container_tasks(
    task_specs=pipeline1_task.component_ref.spec.implementation.graph._toposorted_tasks,
    graph_input_arguments=pipeline1_task.arguments,
    #task_launcher=LocalEnvironmentContainerLauncher(),
    #task_launcher=DockerContainerLauncher(),
    task_launcher=LocalKubernetesContainerLauncher(),
    #output_uri_generator = LocalPathGenerator('./data2/'), #'A:\\_All\\My.Work\\2018.Google\\pipelines_worktree1\\sdk\\python\\kfp\\orchestration\\',
    #output_uri_generator = LocalPathGenerator('A:\\_All\\My.Work\\2018.Google\\pipelines_worktree1\\sdk\\python\\kfp\\orchestration\\data2\\'),
    output_uri_generator = LocalPathGenerator('./data3/'),
)
