# Copyright 2018 Google LLC
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

import os
import sys
import unittest
from pathlib import Path


import kfp


class TaskCompilationTestCase(unittest.TestCase):
    def test_compile_single_container_op(self):
        component_text = '''\
name: Some component
inputs:
- {name: a}
- {name: b}
implementation:
  container:
    image: busybox
    command: [echo, {inputValue: a}, {inputValue: b}]
'''
        task1_factory = kfp.components.load_component_from_text(component_text)
        task1 = task1_factory('Hello', 'World')
        workflow = kfp.compiler.Compiler()._compile_task(task1)
        #self.maxDiff = None
        #self.assertEqual(workflow['spec']['templates'][1]['dag']['tasks'], {})
        self.assertEqual(workflow['spec']['templates'][0]['container']['image'], 'busybox')


if __name__ == '__main__':
    unittest.main()
