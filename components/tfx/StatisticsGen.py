from kfp.components import InputPath, OutputPath


def StatisticsGen(
    # Inputs
    input_data_path: InputPath('ExamplesPath'),
    #input_data_path: 'ExamplesPath',

    # Outputs
    output_path: OutputPath('ExampleStatistics'),
    #output_path: 'ExampleStatistics',
):
#) -> NamedTuple('Outputs', [
#    ('output', 'ExampleStatistics'),
#]):
    """Construct a StatisticsGen component.

    Args:
      input_data: A Channel of `ExamplesPath` type, likely generated by the
        [ExampleGen component](https://www.tensorflow.org/tfx/guide/examplegen).
        This needs to contain two splits labeled `train` and `eval`. _required_
      # examples: Forwards compatibility alias for the `input_data` argument.
    Returns:
      output: `ExampleStatistics` channel for statistics of each split
        provided in the input examples.
    """

    import json
    import os
    from google.protobuf import json_format
    from tfx.types import standard_artifacts
    from tfx.types import channel_utils

    # Create input dict.
    input_base_path = input_data_path
    input_artifact_class = standard_artifacts.Examples
    # Recovering splits
    splits = sorted(os.listdir(input_data_path))
    input_data_artifacts = []
    for split in splits:
        artifact = input_artifact_class()
        artifact.split = split
        artifact.uri = os.path.join(input_base_path, split) + '/'
        input_data_artifacts.append(artifact)
    input_data_channel = channel_utils.as_channel(input_data_artifacts)

    from tfx.components.statistics_gen.component import StatisticsGen
    component_class_instance = StatisticsGen(
        input_data=input_data_channel,
    )

    input_dict = {name: channel.get() for name, channel in component_class_instance.inputs.get_all().items()}
    output_dict = {name: channel.get() for name, channel in component_class_instance.outputs.get_all().items()}
    exec_properties = component_class_instance.exec_properties

    # Generating paths for output artifacts
    for output_artifact in output_dict['output']:
        output_artifact.uri = os.path.join(output_path, output_artifact.split) # Default split is ''

    print('Component instance: ' + str(component_class_instance))

    executor = component_class_instance.executor_spec.executor_class()
    executor.Do(
        input_dict=input_dict,
        output_dict=output_dict,
        exec_properties=exec_properties,
    )
    #return (output_path,)


if __name__ == '__main__':
    import kfp
    kfp.components.func_to_container_op(
        StatisticsGen,
        base_image='tensorflow/tfx:0.15.0rc0',
        output_component_file='StatisticsGen.component.yaml'
    )
