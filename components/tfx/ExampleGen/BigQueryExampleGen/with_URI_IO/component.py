# flake8: noqa

from typing import NamedTuple

def BigQueryExampleGen(
    output_examples_uri: 'ExamplesUri',
    input_config: {'JsonObject': {'data_type': 'proto:tfx.components.example_gen.Input'}},
    output_config: {'JsonObject': {'data_type': 'proto:tfx.components.example_gen.Output'}},
    custom_config: {'JsonObject': {'data_type': 'proto:tfx.components.example_gen.CustomConfig'}} = None,
    beam_pipeline_args: list = None,
) -> NamedTuple('Outputs', [
    ('examples_uri', 'ExamplesUri'),
]):
    from tfx.components import BigQueryExampleGen as component_class

    #Generated code
    import json
    import os
    import tempfile
    import tensorflow
    import uuid
    from google.protobuf import json_format, message
    from tfx.types import channel_utils, artifact_utils
    from tfx.components.base import base_executor

    arguments = locals().copy()

    component_class_args = {}

    for name, execution_parameter in component_class.SPEC_CLASS.PARAMETERS.items():
        argument_value_obj = argument_value = arguments.get(name, None)
        if argument_value is None:
            continue
        parameter_type = execution_parameter.type
        if isinstance(parameter_type, type) and issubclass(parameter_type, message.Message): # Maybe FIX: execution_parameter.type can also be a tuple
            argument_value_obj = parameter_type()
            json_format.Parse(argument_value, argument_value_obj)
        component_class_args[name] = argument_value_obj

    for name, channel_parameter in component_class.SPEC_CLASS.INPUTS.items():
        artifact_path = arguments.get(name + '_uri', None) or arguments.get(name + '_path', None)
        if artifact_path:
            artifact = channel_parameter.type()
            artifact.uri = artifact_path + '/' # ?
            if channel_parameter.type.PROPERTIES and 'split_names' in channel_parameter.type.PROPERTIES:
                # Recovering splits
                subdirs = tensorflow.io.gfile.listdir(artifact_path)
                # Workaround for https://github.com/tensorflow/tensorflow/issues/39167
                subdirs = [subdir.rstrip('/') for subdir in subdirs]
                artifact.split_names = artifact_utils.encode_split_names(sorted(subdirs))
            component_class_args[name] = channel_utils.as_channel([artifact])

    component_class_instance = component_class(**component_class_args)

    input_dict = {name: channel.get() for name, channel in component_class_instance.inputs.get_all().items()}
    output_dict = {name: channel.get() for name, channel in component_class_instance.outputs.get_all().items()}
    exec_properties = component_class_instance.exec_properties

    # Generating paths for output artifacts
    for name, artifacts in output_dict.items():
        base_artifact_path = arguments.get('output_' + name + '_uri', None) or arguments.get(name + '_path', None)
        if base_artifact_path:
            # Are there still cases where output channel has multiple artifacts?
            for idx, artifact in enumerate(artifacts):
                subdir = str(idx + 1) if idx > 0 else ''
                artifact.uri = os.path.join(base_artifact_path, subdir)  # Ends with '/'

    print('component instance: ' + str(component_class_instance))

    # Workaround for a TFX+Beam bug to make DataflowRunner work.
    # Remove after the next release that has https://github.com/tensorflow/tfx/commit/ddb01c02426d59e8bd541e3fd3cbaaf68779b2df
    import tfx
    tfx.version.__version__ += 'dev'

    executor_context = base_executor.BaseExecutor.Context(
        beam_pipeline_args=beam_pipeline_args,
        tmp_dir=tempfile.gettempdir(),
        unique_id=uuid.uuid4(),
    )
    executor = component_class_instance.executor_spec.executor_class(executor_context)
    executor.Do(
        input_dict=input_dict,
        output_dict=output_dict,
        exec_properties=exec_properties,
    )

    return (output_examples_uri, )


if __name__ == '__main__':
    import kfp
    kfp.components.create_component_from_func(
        BigQueryExampleGen,
        base_image='tensorflow/tfx:0.21.4',
        output_component_file='component.yaml'
    )
