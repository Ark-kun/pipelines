# Copyright 2018-2019 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



import sys


# This handler is called whenever the @pipeline decorator is applied.
# It can be used by command-line DSL compiler to inject code that runs for every pipeline definition.
_pipeline_decorator_handler = None


def pipeline(name : str = None, description : str = None):
  """Decorator of pipeline functions.

  Usage:
  ```python
  @pipeline(
    name='my awesome pipeline',
    description='Is it really awesome?'
  )
  def my_pipeline(a: PipelineParam, b: PipelineParam):
    ...
  ```
  """
  def _pipeline(func):
    if name:
      func._component_human_name = name
    if description:
      func._component_description = description

    if _pipeline_decorator_handler:
      return _pipeline_decorator_handler(func) or func
    else:
      return func

  return _pipeline

class PipelineConf():
  """PipelineConf contains pipeline level settings
  """
  def __init__(self):
    self.image_pull_secrets = []
    self.timeout = 0
    self.ttl_seconds_after_finished = -1
    self.artifact_location = None
    self.op_transformers = []

  def set_image_pull_secrets(self, image_pull_secrets):
    """Configures the pipeline level imagepullsecret

    Args:
      image_pull_secrets: a list of Kubernetes V1LocalObjectReference
      For detailed description, check Kubernetes V1LocalObjectReference definition
      https://github.com/kubernetes-client/python/blob/master/kubernetes/docs/V1LocalObjectReference.md
    """
    self.image_pull_secrets = image_pull_secrets
    return self

  def set_timeout(self, seconds: int):
    """Configures the pipeline level timeout

    Args:
      seconds: number of seconds for timeout
    """
    self.timeout = seconds
    return self

  def set_ttl_seconds_after_finished(self, seconds: int):
    """Configures the ttl after the pipeline has finished.

    Args:
      seconds: number of seconds for the workflow to be garbage collected after it is finished.
    """
    self.ttl_seconds_after_finished = seconds
    return self

  def set_artifact_location(self, artifact_location):
    """Configures the pipeline level artifact location.

    Example::

      from kfp.dsl import ArtifactLocation, get_pipeline_conf, pipeline
      from kubernetes.client.models import V1SecretKeySelector


      @pipeline(name='foo', description='hello world')
      def foo_pipeline(tag: str, pull_image_policy: str):
        '''A demo pipeline'''
        # create artifact location object
        artifact_location = ArtifactLocation.s3(
                              bucket="foo",
                              endpoint="minio-service:9000",
                              insecure=True,
                              access_key_secret=V1SecretKeySelector(name="minio", key="accesskey"),
                              secret_key_secret=V1SecretKeySelector(name="minio", key="secretkey"))
        # config pipeline level artifact location
        conf = get_pipeline_conf().set_artifact_location(artifact_location)

        # rest of codes
        ...

    Args:
      artifact_location: V1alpha1ArtifactLocation object
      For detailed description, check Argo V1alpha1ArtifactLocation definition
      https://github.com/e2fyi/argo-models/blob/release-2.2/argo/models/v1alpha1_artifact_location.py
      https://github.com/argoproj/argo/blob/release-2.2/api/openapi-spec/swagger.json
    """
    self.artifact_location = artifact_location
    return self

  def add_op_transformer(self, transformer):
    """Configures the op_transformers which will be applied to all ops in the pipeline.

    Args:
      transformer: a function that takes a ContainOp as input and returns a ContainerOp
    """
    self.op_transformers.append(transformer)



_current_pipeline_conf = None


def get_pipeline_conf():
  """Configure the pipeline level setting to the current pipeline
    Note: call the function inside the user defined pipeline function.
  """
  return _current_pipeline_conf

#TODO: Add back the Pipeline.add_pipeline(name, description) deprecated function that TFX has taken dependency on.


class Pipeline:
  @staticmethod
  @DeprecationWarning
  def add_pipeline(name, description, func):
    """Add a pipeline function with the specified name and description."""
    import warnings
    warnings.warn("Pipeline.add_pipeline method is part of the private compiler API which is not supported and no longer needed. Please use the optional @pipeline decorator to mark piepline functions.", DeprecationWarning)

    # Applying the @pipeline decorator to the pipeline function
    func = pipeline(name=name, description=description)(func)
