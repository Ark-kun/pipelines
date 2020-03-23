# Lint as: python3
# Copyright 2020 Google LLC. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for tfx.dsl.components._bridge."""

#import sys
#import subprocess
#try:
#  import kfp
#except ModuleNotFoundError:
#  #print(subprocess.run(['ls', '-la', '/usr/bin/']))
#  print(subprocess.run(['find', '/', '-name', 'python3']))
#  subprocess.run([sys.executable or 'python3', '-m', 'pip', 'install', 'kfp>=0.2.2', '--user'])

import os

from tfx.orchestration import metadata
from tfx.orchestration import pipeline
from tfx.orchestration import kubeflow
from tfx.orchestration.kubeflow import kubeflow_dag_runner
import tensorflow as tf

###XXX###from tfx.dsl.components import create_component_from_func
###XXX###from tfx.dsl.components import enable_new_components
###XXX###from tfx.dsl.components import InputPath
###XXX###from tfx.dsl.components import OutputPath

from _bridge import create_component_from_func, enable_new_components, InputPath, OutputPath


# Python functions that we want to use in TFX pipeline
# Ideally these components should just use standard file operations.
# Currently TFX only passes data URIs to components (not data values or files).
# By specification, paths received for parameters marked as InputPath/OutputPath
# should be local file paths, not URIs. But we have to violate this rule until
# data file passing is available. Until that, the components have to install
# tensorflow and use tensorflow.gfile to read/write data.
# TODO(avolkov): Simplify the components once data file passing support is added.


# Reading data files
def print_text_gfile(text_path: InputPath()): # Not specifying any type in InputPath so that any data can be printed
  """Print text."""
  import tensorflow as tf
  with tf.io.gfile.GFile(text_path, 'r') as reader:
    for line in reader:
      print(line, end='')


# Writing data files
def produce_1000_numbers_gfile(
    numbers_path: OutputPath('List of numbers'),  # Custom type
):
  """Produces numbers from 0 to 999."""
  import tensorflow as tf
  with tf.io.gfile.GFile(numbers_path, 'w') as writer:
    for i in range(1000):
      writer.write(str(i) + '\n')


# Reading and writing data files
def split_text_lines_gfile(
    source_path: InputPath(), # InputUri()
    odd_lines_path: OutputPath(), # OutputUri()
    even_lines_path: OutputPath(), # OutputUri()
):
  import tensorflow as tf
  with tf.io.gfile.GFile(source_path, 'r') as reader:
    with tf.io.gfile.GFile(odd_lines_path, 'w') as odd_writer:
      with tf.io.gfile.GFile(even_lines_path, 'w') as even_writer:
        while True:
          line = reader.readline()
          if line:
            break
          odd_writer.write(line)
          line = reader.readline()
          if line:
            break
          even_writer.write(line)


@enable_new_components()
def create_text_split_gfile_pipeline_tasks():
  # These calls can be switched to decorators for simpler python components
  produce_1000_numbers_gfile_op = create_component_from_func(
    produce_1000_numbers_gfile,
    base_image='tensorflow/tensorflow:2.0.1-py3', # Needed for GFile
  )
  split_text_lines_gfile_op = create_component_from_func(
    split_text_lines_gfile,
    base_image='tensorflow/tensorflow:2.0.1-py3', # Needed for GFile
  )
  print_text_gfile_op = create_component_from_func(
    print_text_gfile,
    base_image='tensorflow/tensorflow:2.0.1-py3', # Needed for GFile
  )

  produce_numbers_task = produce_1000_numbers_gfile_op()
  split_numbers_task = split_text_lines_gfile_op(
      produce_numbers_task.outputs['numbers'],
  )
  print_odd_lines_task = print_text_gfile_op(
      split_numbers_task.outputs['odd_lines'],
      #instance_name='print_odd_lines',
  )
  #print_even_lines_task = print_text_gfile_op(
  #    split_numbers_task.outputs['even_lines'],
  #    instance_name='print_even_lines',
  #)
  #RuntimeError: Duplicated component_id PrintTextGfile for component type __main__.PrintTextGfil
  #TypeError: Print text gfile() got an unexpected keyword argument 'instance_name'

  tasks = [
      produce_numbers_task,
      split_numbers_task,
      print_odd_lines_task,
      #print_even_lines_task,
  ]
  return tasks


class ComponentBridgeTests(tf.test.TestCase):

  def setUp(self):
    super(ComponentBridgeTests, self).setUp()
    self._test_dir = os.path.join(
        os.environ.get('TEST_UNDECLARED_OUTPUTS_DIR', self.get_temp_dir()),
        self._testMethodName)
    
    self._test_dir = os.getcwd()

    self._pipeline_name = 'component_bridge_test'
    self._pipeline_root = os.path.join(self._test_dir, 'tfx', 'pipelines',
                                       self._pipeline_name)
    self._metadata_path = os.path.join(self._test_dir, 'tfx', 'metadata',
                                       self._pipeline_name, 'metadata.db')

    self._pipeline_root = os.path.join('gs://avolkov/tmp/' + self._test_dir, 'tfx', 'pipelines',
                                       self._pipeline_name)
    self._metadata_path = os.path.join('gs://avolkov/tmp/' + self._test_dir, 'tfx', 'metadata',
                                       self._pipeline_name, 'metadata.db')


  def test_python_component_pipeline_with_kubeflow_runner(self):
    pipeline_func = create_text_split_gfile_pipeline_tasks
    tast_pipeline = pipeline.Pipeline(
        pipeline_name=self._pipeline_name,
        pipeline_root=self._pipeline_root,
        components=pipeline_func(),
        enable_cache=True,
        metadata_connection_config=metadata.sqlite_metadata_connection_config(
            self._metadata_path),
        additional_pipeline_args={},
    )

    #pipeline_package_filename = self._pipeline_name + '.tar.gz'
    pipeline_package_filename = self._pipeline_name + '.yaml'
    pipeline_pachage_path = os.path.join(
        self._test_dir,
        pipeline_package_filename
    )

    # XXX
    from tfx.orchestration.kubeflow.proto import kubeflow_pb2

    metadata_config = kubeflow_pb2.KubeflowMetadataConfig(
      grpc_config=kubeflow_pb2.KubeflowGrpcMetadataConfig(
        #grpc_service_host=kubeflow_pb2.ConfigValue(value='metadata-service'),
        grpc_service_host=kubeflow_pb2.ConfigValue(value='10.15.251.250'),
        grpc_service_port=kubeflow_pb2.ConfigValue(value='8080'),
      ) 
    )

    runner_config = kubeflow_dag_runner.KubeflowDagRunnerConfig(
        #kubeflow_metadata_config=kubeflow_dag_runner.get_default_kubeflow_metadata_config(),
        kubeflow_metadata_config=metadata_config,
        # Specify custom docker image to use.
        #tfx_image=tfx_image,
        tfx_image='gcr.io/avolkov-31337/tensorflow/tfx:0.22.dev2', # Artifact Fixes
        #supported_launcher_classes=[
        #    in_process_component_launcher.InProcessComponentLauncher,
        #    kubernetes_component_launcher.KubernetesComponentLauncher,
        #],
        #default_component_configs=[k8s_config],
    )

    runner = kubeflow.kubeflow_dag_runner.KubeflowDagRunner(
        output_dir=self._test_dir,
        output_filename=pipeline_package_filename,
        config=runner_config, #
    )
    runner.run(tast_pipeline)

    self.assertTrue(os.path.exists(pipeline_pachage_path))
    self.assertTrue(tf.io.gfile.exists(pipeline_pachage_path))

if __name__ == "__main__":
  tf.test.main()