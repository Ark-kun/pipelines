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

###XXX### from __future__ import google_type_annotations

__all__ = [
    'create_tfx_component_class_from_spec',
    'enable_new_components',
    'InputUri',
    'OutputUri',
]

import contextlib
import json
from typing import Any, Mapping, Type

from kfp.components import create_component_from_func
from kfp.components import InputPath
from kfp.components import OutputPath
from kfp.components import structures
from kfp.components._components import _resolve_command_line_and_paths
from kfp.components._naming import _sanitize_python_function_name


###XXX###from google3.third_party.ml_metadata.proto.metadata_store_pb2 import ArtifactType
###XXX###from google3.third_party.tfx.components.base.base_component import BaseComponent
###XXX###from google3.third_party.tfx.components.base.executor_spec import ExecutorContainerSpec
###XXX###from google3.third_party.tfx.types import component_spec as tfx_component_spec
###XXX###from google3.third_party.tfx.types.artifact import Artifact
###XXX###from google3.third_party.tfx.types.channel import Channel
###XXX###from google3.third_party.tfx.types.component_spec import ChannelParameter


from ml_metadata.proto.metadata_store_pb2 import ArtifactType
from tfx.components.base.base_component import BaseComponent
from tfx.components.base.executor_spec import ExecutorContainerSpec
from tfx.types import component_spec as tfx_component_spec
from tfx.types.artifact import Artifact
from tfx.types.channel import Channel
from tfx.types.component_spec import ChannelParameter


ANY_ARTIFACT_TYPE_NAME = 'Any'


def _create_artifact_type(type_struct) -> ArtifactType:
  if not type_struct:
    type_name = ANY_ARTIFACT_TYPE_NAME
  elif isinstance(type_struct, str):
    type_name = str(type_struct)
  else:
    type_name = json.dumps(type_struct)
  return ArtifactType(name=type_name)


def _create_channel_parameter(type_name: str) -> ChannelParameter:
  return ChannelParameter(mlmd_artifact_type=_create_artifact_type(type_name))


def _create_empty_artifact(type_name: str) -> Artifact:
  return Artifact(mlmd_artifact_type=_create_artifact_type(type_name))


def _create_channel_with_empty_artifact(type_name: str) -> Channel:
  artifact_type = _create_artifact_type(type_name)
  return Channel(
      mlmd_artifact_type=artifact_type,
      artifacts=[
          Artifact(artifact_type),
      ],
  )


def type_to_type_struct(typ):
  if not typ:
    return None
  return str(typ.__module__) + '.' + str(typ.__name__)


class InputUri:
  def __init__(self, data_type: type = None, type_struct: str = None):
    if type_struct:
      self._data_type_struct = type_struct
    else:
      self._data_type_struct = type_to_type_struct(data_type)

  def to_dict(self):
    properties = {
        'data_type': self._data_type_struct,
        'io_kind': 'read',
    }
    return {'Uri': properties}


class OutputUri:
  def __init__(self, data_type: type = None, type_struct: str = None):
    if type_struct:
      self._data_type_struct = type_struct
    else:
      self._data_type_struct = type_to_type_struct(data_type)

  def to_dict(self):
    properties = {
        'data_type': self._data_type_struct,
        'io_kind': 'write',
    }
    return {'Uri': properties}


class ExecutionProperty:
  def __init__(self, data_type: type = None, type_struct: str = None):
    if type_struct:
      self._data_type_struct = type_struct
    else:
      self._data_type_struct = type_to_type_struct(data_type)

  def to_dict(self):
    properties = {
        'data_type': self._data_type_struct,
    }
    return {'QueryableProperty': properties}


def create_tfx_component_class_from_spec(
    component_spec: structures.ComponentSpec,
) -> BaseComponent:
  container_spec = component_spec.implementation.container

  if container_spec is None:
    raise TypeError(
        'Only components with container implementation can be instantiated in TFX at this moment.'
    )

  input_name_to_python = {
      input.name: _sanitize_python_function_name(input.name)
      for input in component_spec.inputs or []
  }
  output_name_to_python = {
      output.name: _sanitize_python_function_name(output.name)
      for output in component_spec.outputs or []
  }

  # FIX: The {{input_dict["{name}"][0].value}} placeholder is not implemented yet.
  # @jxzheng and @avolkov are working on this.
  component_arguments = {
      input.name: '{{{{input_dict["{name}"][0].value}}}}'.format(
          name=input_name_to_python[input.name])
      for input in component_spec.inputs or []
  }

  # FIX: The {{input_dict["{name}"][0].local_path}} placeholder is not implemented yet.
  # @avolkov is working on this.
  def input_path_uri_generator(name):
    #return '{{{{input_dict["{name}"][0].local_path}}}}'.format(name=input_name_to_python[name])
    return '{{{{input_dict["{name}"][0].uri}}}}'.format(
        name=input_name_to_python[name])

  def output_path_uri_generator(name):
    #return '{{{{output_dict["{name}"][0].local_path}}}}'.format(name=output_name_to_python[name])
    return '{{{{output_dict["{name}"][0].uri}}}}'.format(
        name=output_name_to_python[name])

  resolved_cmd = _resolve_command_line_and_paths(
      component_spec=component_spec,
      arguments=component_arguments,
      # FIX: Remove this workaround vvv
      input_path_generator=input_path_uri_generator,
      output_path_generator=output_path_uri_generator,
  )

  resolved_command = resolved_cmd.command
  resolved_args = resolved_cmd.args

  component_class_name = _sanitize_python_function_name(component_spec.name or
                                                        'Component')
  component_class_name = ''.join(
      word.title() for word in component_class_name.split('_'))

  component_class_doc = (component_spec.name or '') + '\n' + (
      component_spec.description or '')

  input_channel_parameters = {
      input_name_to_python[input.name]:
      _create_channel_parameter(input.type)
      for input in component_spec.inputs or []
  }

  output_channel_parameters = {
      output_name_to_python[output.name]:
      _create_channel_parameter(output.type)
      for output in component_spec.outputs or []
  }

  default_input_channels = {
      input_name_to_python[input.name]:
      _create_channel_with_empty_artifact(input.type)
      for input in component_spec.inputs or []
      if input.optional
  }

  output_channels = {
      output_name_to_python[output.name]:
      _create_channel_with_empty_artifact(output.type)
      for output in component_spec.outputs or []
  }

  execution_parameters = {}

  tfx_component_spec_class = type(
      component_class_name + 'Spec',
      (tfx_component_spec.ComponentSpec,),
      dict(
          PARAMETERS=execution_parameters,
          INPUTS=input_channel_parameters,
          OUTPUTS=output_channel_parameters,
          __doc__=component_class_doc,
      ),
  )
  print(tfx_component_spec_class.INPUTS)

  def tfx_component_class_init(self, **kwargs):
    #instance_name = kwargs.pop('instance_name', None)
    arguments = {}
    arguments.update(default_input_channels)
    arguments.update(output_channels)
    arguments.update(kwargs)

    BaseComponent.__init__(self, spec=self.__class__.SPEC_CLASS(**arguments))

  tfx_component_class = type(
      component_class_name,
      (BaseComponent,),
      dict(
          SPEC_CLASS=tfx_component_spec_class,
          EXECUTOR_SPEC=ExecutorContainerSpec(
              image=container_spec.image,
              command=resolved_command,
              args=resolved_args,
          ),
          __init__=tfx_component_class_init,
          __doc__=component_class_doc,
      ),
  )

  return tfx_component_class


def _create_tfx_task_from_component_spec_and_arguments(
    component_spec: structures.ComponentSpec,
    arguments: Mapping[str, Any],
    component_ref: structures.ComponentReference,
) -> BaseComponent:
  tfx_component_class = create_tfx_component_class_from_spec(component_spec)
  tfx_task = tfx_component_class(**arguments)
  return tfx_task


class enable_new_components(contextlib.ContextDecorator):
  def __enter__(self):
    from kfp import components
    self.old_handler = components._components._container_task_constructor
    components._components._container_task_constructor = _create_tfx_task_from_component_spec_and_arguments

  def __exit__(self, exc_type, exc, exc_tb):
    from kfp import components
    components._components._container_task_constructor = self.old_handler
