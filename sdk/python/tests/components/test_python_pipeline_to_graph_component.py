# Copyright 2019 Google LLC
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
from collections import OrderedDict
from pathlib import Path

import kfp.components as comp
from kfp.components._python_to_graph_component import create_graph_component_spec_from_pipeline_func


class PythonPipelineToGraphComponentTestCase(unittest.TestCase):
    def test_handle_creating_graph_component_from_pipeline_that_uses_container_components(self):
        test_data_dir = Path(__file__).parent / 'test_data'
        producer_op = comp.load_component_from_file(str(test_data_dir / 'component_with_0_inputs_and_2_outputs.component.yaml'))
        processor_op = comp.load_component_from_file(str(test_data_dir / 'component_with_2_inputs_and_2_outputs.component.yaml'))
        consumer_op = comp.load_component_from_file(str(test_data_dir / 'component_with_2_inputs_and_0_outputs.component.yaml'))

        def pipeline1(pipeline_param_1: int):
            producer_task = producer_op()
            processor_task = processor_op(pipeline_param_1, producer_task.outputs['Output 2'])
            consumer_task = consumer_op(processor_task.outputs['Output 1'], processor_task.outputs['Output 2'])

            return OrderedDict([ # You can safely return normal dict in python 3.6+
                ('Pipeline output 1', producer_task.outputs['Output 1']),
                ('Pipeline output 2', processor_task.outputs['Output 2']),
            ])

        graph_component = create_graph_component_spec_from_pipeline_func(pipeline1)

        self.assertEqual(len(graph_component.inputs), 1)
        self.assertListEqual([input.name for input in graph_component.inputs], ['pipeline_param_1']) #Relies on human name conversion function stability
        self.assertListEqual([output.name for output in graph_component.outputs], ['Pipeline output 1', 'Pipeline output 2'])
        self.assertEqual(len(graph_component.implementation.graph.tasks), 3)

    def test_create_component_from_real_pipeline_retail_product_stockout_prediction(self):
        from test_data.retail_product_stockout_prediction_pipeline import retail_product_stockout_prediction_pipeline

        graph_component = create_graph_component_spec_from_pipeline_func(retail_product_stockout_prediction_pipeline)

        import yaml
        expected_component_spec_path = str(Path(__file__).parent / 'test_data' / 'retail_product_stockout_prediction_pipeline.component.yaml')
        with open(expected_component_spec_path) as f:
            expected_dict = yaml.safe_load(f)

        self.assertEqual(expected_dict, graph_component.to_dict())


    def test_graph_component_roundtrip(self):
        # We want to test that graph component created from pipeline that instantiates a loaded graph component is identical to that loaded graph component
        # In short: create_graph_component_spec_from_pipeline_func(load_component_from_file(file1)).save(file2) --> file2 == file1
        # There are couple of reasons why this test needs to be more complicated:
        # * The factory function returned by `load_component_from_file(file1)` cannot be a correct pipeline function:
        # * * The return value is TaskSpec instead of tuple/dict and the outputs are in TaskSpec.outputs
        # * * Some default values in the signature are _DefaultValue wrappers that are not supported by `annotation_to_type_struct`
        # * * The docstring is not equal to the original compoennt description, because it additionally contains the component name
        # We have to patch those discrepancies in this test to help the reasults fully match.

        # This test relies on the fact that at this moment when graph component is instantiated, its child tasks are merged in the pipeline graph and are not put in a subgraph.
        # When that feature is added, this test needs to be disabled or reworked.

        graph_component_path = str(Path(__file__).parent / 'test_data' / 'retail_product_stockout_prediction_pipeline.component.yaml')
        graph_op = comp.load_component_from_file(graph_component_path)
        graph_component1 = graph_op.component_spec

        import inspect
        signature = inspect.signature(graph_op)
        fixed_parameters = [
            parameter.replace(default=parameter.default.value) if isinstance(parameter.default, kfp.components._components._DefaultValue) else parameter
            for parameter in signature.parameters.values()
        ]
        fixed_signature = signature.replace(parameters=fixed_parameters)

        def pipeline2(*args, **kwargs):
            return graph_op(*args, **kwargs).outputs
        pipeline2.__name__ = graph_op.__name__
        pipeline2.__doc__ = graph_op.component_spec.description
        pipeline2.__signature__ = fixed_signature

        graph_component2 = create_graph_component_spec_from_pipeline_func(pipeline2)
        self.maxDiff = None
        self.assertEqual(graph_component1.to_dict(), graph_component2.to_dict())


if __name__ == '__main__':
    unittest.main()
