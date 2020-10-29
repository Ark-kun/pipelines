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

import pathlib

import kfp
from kfp.v2 import dsl
import kfp.v2.compiler as compiler

component_op_1 = kfp.components.load_component_from_text("""
name: Write
inputs:
- {name: text, type: String, description: 'Content to be written'}
outputs:
- {name: data, type: Dataset, description: 'Dataset'}
implementation:
  container:
    image: google/cloud-sdk:slim
    command:
    - sh
    - -c
    - |
      set -e -x
      echo "$0" > "$1"
    - {inputValue: text}
    - {outputPath: data}
""")

component_op_2 = kfp.components.load_component_from_text("""
name: Read
inputs:
- {name: data, type: Dataset, description: 'Dataset'}
implementation:
  container:
    image: google/cloud-sdk:slim
    command:
    - sh
    - -c
    - |
      set -e -x
      cat "$0"
    - {inputPath: data}
""")

@dsl.pipeline(name='simple-two-step-pipeline')
def simple_pipeline(text='Hello world!',):
  component_1 = component_op_1(text=text)
  component_2 = component_op_2(
      data=component_1.outputs['data'])


if __name__ == '__main__':
  compiler.Compiler().compile(simple_pipeline, __file__ + '.json')
