from typing import List, Mapping


class KanikoImageBuilder:
    def __init__(self, staging_gcs_dir, namespace='kubeflow', timeout=600):
        self.staging_gcs_dir = staging_gcs_dir
        self.namespace = namespace
        self.timeout = timeout

    def build_image_from_context_and_dockerfile(image_name: str, context_dir: str):
        local_tarball_path = os.path.join(local_build_dir, 'docker.tmp.tar.gz')



        from ._gcs_helper import GCSHelper
        GCSHelper.upload_gcs_file(local_tarball_path, self._gcs_path)
        kaniko_spec = self._generate_kaniko_spec(namespace=namespace,
                                                arc_dockerfile_name=self._arc_dockerfile_name,
                                                gcs_path=self._gcs_path,
                                                target_image=self._target_image)
        # Run kaniko job
        logging.info('Start a kaniko job for build.')
        from ._k8s_helper import K8sHelper
        k8s_helper = K8sHelper()
        k8s_helper.run_job(kaniko_spec, timeout)
        logging.info('Kaniko job complete.')


def build_image_from_dockerfile(image_name: str, dockerfile_path: str, image_builder: KanikoImageBuilder, include_files : Mapping[str, str] = None):
    ...


def build_image_from_dockerfile_text(image_name : str, dockerfile_text : str, image_builder: KanikoImageBuilder, include_files : Mapping[str, str] = None):
    ...


def build_image(image_name : str, base_image: str, image_builder: KanikoImageBuilder, apt_packages : List[str] = None, python_packages : List[str] = None, include_files : Mapping[str, str] = None):
    ...





Suggested new APIs:

```python
#module: kfp.containers:

class KanikoImageBuilder:
    def __init__(self, staging_gcs_dir, namespace='kubeflow', timeout=600):
        ...

#New version of kfp.compiler.build_docker_image
def build_image_from_dockerfile(image_name: str, dockerfile_path: str, image_builder: KanikoImageBuilder, include_files : Mapping[str, str] = None):
    ...

#New version of %%docker magic
def build_image_from_dockerfile_text(image_name : str, dockerfile_text : str, image_builder: KanikoImageBuilder, include_files : Mapping[str, str] = None):
    ...

def build_image(image_name : str, base_image: str, image_builder: KanikoImageBuilder, apt_packages : List[str] = None, python_packages : List[str] = None, copy_files : Mapping[str, str] = None):
    ...
```

API usage:
```python
kfp.containers.build_image(
    image_name='gcr.io/my-images/my-component',
    image_builder=KanikoImageBuilder('gs://my-bucket/tmp/'),
    python_packages=['pandas', 'seaborn'],
    copy_files={
        './src': '/container_path/src',
        '../common', '/container_path/common',
    },
)
```