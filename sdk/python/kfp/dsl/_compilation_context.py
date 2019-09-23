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

from . import _active_compilation_context
from ..components import _dsl_bridge, _components
from ..components._naming import _make_name_unique_by_adding_index
from ..dsl import _container_op
from ..dsl._ops_group import OpsGroup
from ..dsl._pipeline import pipeline, PipelineConf
from ..dsl import _pipeline


#TODO: Pipeline is in fact an opsgroup, refactor the code.
class _CompilationContext():
  """A pipeline contains a list of operators.

  This class is not supposed to be used by pipeline authors since pipeline authors can use
  pipeline functions (decorated with @pipeline) to reference their pipelines. This class
  is useful for implementing a compiler. For example, the compiler can use the following
  to get the pipeline object and its ops:

  ```python
  with Pipeline() as p:
    pipeline_func(*args_list)

  traverse(p.ops)
  ```
  """

  @staticmethod
  def add_pipeline(name, description, func):
    """Add a pipeline function with the specified name and description."""
    # Applying the @pipeline decorator to the pipeline function
    func = pipeline(name=name, description=description)(func)

  def __init__(self, name: str):
    """Create a new instance of Pipeline.

    Args:
      name: the name of the pipeline. Once deployed, the name will show up in Pipeline System UI.
    """
    self.name = name
    self.ops = {}
    # Add the root group.
    self.groups = [OpsGroup('pipeline', name=name)]
    self.group_id = 0
    self._metadata = None

  def __enter__(self):
    if _active_compilation_context._active_context:
      raise Exception('Nested pipelines are not allowed.')

    _active_compilation_context._active_context = self
    _pipeline._current_pipeline_conf = PipelineConf()
    self.conf = _pipeline._current_pipeline_conf
    self._old_container_task_constructor = _components._container_task_constructor
    _components._container_task_constructor = _dsl_bridge._create_container_op_from_component_and_arguments

    def register_op_and_generate_id(op):
      return self.add_op(op, op.is_exit_handler)

    self._old__register_op_handler = _container_op._register_op_handler
    _container_op._register_op_handler = register_op_and_generate_id
    return self

  def __exit__(self, *args):
    _active_compilation_context._active_context = None
    _pipeline._current_pipeline_conf = None
    _container_op._register_op_handler = self._old__register_op_handler
    _components._container_task_constructor = self._old_container_task_constructor

  def add_op(self, op: _container_op.BaseOp, define_only: bool):
    """Add a new operator.

    Args:
      op: An operator of ContainerOp, ResourceOp or their inherited types.

    Returns
      op_name: a unique op name.
    """
    #If there is an existing op with this name then generate a new name.
    op_name = _make_name_unique_by_adding_index(op.human_name, list(self.ops.keys()), ' ')

    self.ops[op_name] = op
    if not define_only:
      self.groups[-1].ops.append(op)

    return op_name

  def push_ops_group(self, group: 'OpsGroup'):
    """Push an OpsGroup into the stack.

    Args:
      group: An OpsGroup. Typically it is one of ExitHandler, Branch, and Loop.
    """
    self.groups[-1].groups.append(group)
    self.groups.append(group)

  def pop_ops_group(self):
    """Remove the current OpsGroup from the stack."""
    del self.groups[-1]

  def remove_op_from_groups(self, op):
    for group in self.groups:
      group.remove_op_recursive(op)

  def get_next_group_id(self):
    """Get next id for a new group. """

    self.group_id += 1
    return self.group_id

  def _set_metadata(self, metadata):
    '''_set_metadata passes the containerop the metadata information
    Args:
      metadata (ComponentMeta): component metadata
    '''
    self._metadata = metadata


