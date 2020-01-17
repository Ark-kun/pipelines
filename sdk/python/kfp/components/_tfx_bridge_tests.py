#%%
import kfp
import tfx

from kfp.components._structures import *
from kfp.components import func_to_container_op as create_component_from_func
import kfp.components as components

#from ._tfx_bridge import create_tfx_component_from_kfp_component
from _tfx_bridge import create_tfx_component_from_kfp_component


@create_component_from_func
def add_op(a: 'Complex number', b: 'Complex number') -> 'Complex number':
    """Adds two numbers"""
    return a + b


#comp_spec2 = ComponentSpec(
#    name='AAA'
#    imputs=[
#
#    ],
#    implementation=ContainerImplementation(
#        image='...'
#    )
#)


#comp_spec3 = load_component('component.yaml').component_spec

component_spec = add_op.component_spec


tfx_component1 = create_tfx_component_from_kfp_component(add_op.component_spec)

tfx_component_instance1 = tfx_component1(
    a=tfx.types.Channel(type_name='Complex number'),
    b=tfx.types.Channel(type_name='Complex number'),
    output=tfx.types.Channel(type_name='Complex number'),
)

tfx_component_instance2 = tfx_component1(
    a=tfx.types.Channel(type_name='Complex number'),
    b=tfx.types.Channel(type_name='Complex number'),
#    output=tfx.types.Channel(type_name='Complex number'),
)

tfx_component_instance1.to_json_dict()

#{
#    'driver_class': tfx.components.base.base_driver.BaseDriver,
#    'executor_spec': <tfx.components.base.executor_spec.ExecutorContainerSpec at 0x7f3d930c26d0>,
#    '_instance_name': None,
#    'spec': <AddOpSpec at 0x7f3d927b61d0>,
#}

tfx_component_instance1.executor_spec.to_json_dict()

#{'image': 'tensorflow/tensorflow:1.13.2-py3',
# 'command': ['python3',
#  '-u',
#  '-c',
#  'def add_op(a: float, b: float) -> float:\n    """Adds two numbers"""\n    return a + b\n\ndef _serialize_float(float_value: float) -> str:\n    if isinstance(float_value, str):\n        return float_value\n    if not isinstance(float_value, (float, int)):\n        raise TypeError(\'Value "{}" has type "{}" instead of float.\'.format(str(float_value), str(type(float_value))))\n    return str(float_value)\n\nimport argparse\n_parser = argparse.ArgumentParser(prog=\'Add op\', description=\'Adds two numbers\')\n_parser.add_argument("--a", dest="a", type=float, required=True, default=argparse.SUPPRESS)\n_parser.add_argument("--b", dest="b", type=float, required=True, default=argparse.SUPPRESS)\n_parser.add_argument("----output-paths", dest="_output_paths", type=str, nargs=1)\n_parsed_args = vars(_parser.parse_args())\n_output_files = _parsed_args.pop("_output_paths", [])\n\n_outputs = add_op(**_parsed_args)\n\nif not hasattr(_outputs, \'__getitem__\') or isinstance(_outputs, str):\n    _outputs = [_outputs]\n\n_output_serializers = [\n    _serialize_float,\n\n]\n\nimport os\nfor idx, output_file in enumerate(_output_files):\n    try:\n        os.makedirs(os.path.dirname(output_file))\n    except OSError:\n        pass\n    with open(output_file, \'w\') as f:\n        f.write(_output_serializers[idx](_outputs[idx]))\n'],
# 'args': ['--a',
#  '{{inputs.a.value}}',
#  '--b',
#  '{{inputs.b.value}}',
#  '----output-paths',
#  '{{outputs.output.uri}}']}

#tfx_component_instance2.outputs

# %%
tfx_component_instance3 = tfx_component1(
    a=tfx_component_instance1.outputs['output'],
    b=tfx_component_instance2.outputs['output'],
)

print(tfx_component_instance3)

# %%

task4 = add_op(3, 5)
print(type(task4))
print(task4)
