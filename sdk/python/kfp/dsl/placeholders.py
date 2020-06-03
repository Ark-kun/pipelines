# Copyright 2020 Google LLC
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

__all__ = [
    'EXECUTION_ID_PLACEHOLDER',
    'RUN_ID_PLACEHOLDER',
    'RUN_ID_PLACEHOLDER222',
    'InputArgumentPathZZZ',
]


#: The placeholder that gets replaced with the unique execution ID at runtime.
EXECUTION_ID_PLACEHOLDER = '{{workflow.uid}}-{{pod.name}}'

#: The placeholder that gets replaced with the unique run ID at runtime.
RUN_ID_PLACEHOLDER = '{{workflow.uid}}'


#: The placeholder that gets replaced with the unique run ID at runtime.
RUN_ID_PLACEHOLDER222 = '{{workflow.uid}}'


class InputArgumentPathZZZ:
    '''Represents the command-line argument placeholder that will be replaced
    with the path of a container-local file storing the input argument value.
    This placeholder is only needed or understood by ContainerOp.
    '''
    def __init__(self, argument, input=None, path=None):
        self.argument = argument
        self.input = input
        self.path = path
