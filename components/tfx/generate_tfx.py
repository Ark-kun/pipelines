#%%
import sys
sys.path.append(r'a:/_All/My.Creativity.Coding.3rd-party/_Incoming/tensorflow-tfx')

#%%
from google import protobuf
#%%
from collections import OrderedDict
from google.protobuf.descriptor import FieldDescriptor

protobuf_type_to_type = OrderedDict()
protobuf_type_to_type[FieldDescriptor.TYPE_DOUBLE]  = float
protobuf_type_to_type[FieldDescriptor.TYPE_FLOAT]   = float
protobuf_type_to_type[FieldDescriptor.TYPE_INT64]   = int
protobuf_type_to_type[FieldDescriptor.TYPE_UINT64]  = int
protobuf_type_to_type[FieldDescriptor.TYPE_INT32]   = int
protobuf_type_to_type[FieldDescriptor.TYPE_FIXED64] = int
protobuf_type_to_type[FieldDescriptor.TYPE_FIXED32] = int
protobuf_type_to_type[FieldDescriptor.TYPE_BOOL]    = bool
protobuf_type_to_type[FieldDescriptor.TYPE_STRING]  = str
protobuf_type_to_type[FieldDescriptor.TYPE_GROUP] = 'TYPE_GROUP' # ?
protobuf_type_to_type[FieldDescriptor.TYPE_MESSAGE] = 'TYPE_MESSAGE' # ?
protobuf_type_to_type[FieldDescriptor.TYPE_BYTES]   = bytes
protobuf_type_to_type[FieldDescriptor.TYPE_UINT32]  = int
protobuf_type_to_type[FieldDescriptor.TYPE_ENUM]    = int # ?
protobuf_type_to_type[FieldDescriptor.TYPE_SFIXED32] = int
protobuf_type_to_type[FieldDescriptor.TYPE_SFIXED64] = int
protobuf_type_to_type[FieldDescriptor.TYPE_SINT32] = int
protobuf_type_to_type[FieldDescriptor.TYPE_SINT64] = int

protobuf_label_to_str = OrderedDict()
protobuf_label_to_str[FieldDescriptor.LABEL_OPTIONAL] = 'LABEL_OPTIONAL'
protobuf_label_to_str[FieldDescriptor.LABEL_REQUIRED] = 'LABEL_REQUIRED'
protobuf_label_to_str[FieldDescriptor.LABEL_REPEATED] = 'LABEL_REPEATED'

#%%
import tfx.proto as tfx_proto

from tfx.proto import example_gen_pb2
message_class = example_gen_pb2.Input

[(field.name, protobuf_type_to_type[field.type], field.label, field.default_value, field.message_type) for field in message_class.DESCRIPTOR.fields]


#%%
def print_message_descriptor1(message_descriptor):
    print('Message: ' + message_descriptor.name)
    attr_names = ['name', 'label', 'has_default_value', 'default_value', 'message_type']
    for field in message_class.DESCRIPTOR.fields:
        print('  Field: ' + field.name)
        print('    _label_type: ' + protobuf_label_to_str[field.label])
        for attr_name in attr_names:
            print('    ' + attr_name + ': ' + str(getattr(field, attr_name)))

def print_message_descriptor(message_descriptor, indent=''):
    print(indent + 'Message: ' + message_descriptor.name)
    attr_names = [
        'name',
        #'label',
        'has_default_value',
        'default_value',
        'message_type',
    ]
    for field in message_descriptor.fields:
        print(indent + '  Field: ' + field.name)
        print(indent + '    _type: ' + str(protobuf_type_to_type[field.type]))
        print(indent + '    label_type: ' + protobuf_label_to_str[field.label])
        for attr_name in attr_names:
            print(indent + '    ' + attr_name + ': ' + str(getattr(field, attr_name)))
        if type(field.message_type).__name__ == 'MessageDescriptor':
            print_message_descriptor(field.message_type, indent + '    ')

print_message_descriptor(message_class.DESCRIPTOR)

#%%
print_message_descriptor(example_gen_pb2.Input.Split.DESCRIPTOR)

#%%

def flatten_message_descriptor(message_descriptor, prefix=''):
    for field in message_descriptor.fields:
        parameter_name = prefix + field.name
        if field.type == FieldDescriptor.TYPE_MESSAGE:
            parameter_type_name = field.message_type.name
        else:
            parameter_type_name = protobuf_type_to_type[field.type].__name__

        if field.type == FieldDescriptor.TYPE_MESSAGE and field.label != FieldDescriptor.LABEL_REPEATED:
            flatten_message_descriptor(field.message_type, prefix=parameter_name + '_')
            continue
        else:
            if field.label == FieldDescriptor.LABEL_REPEATED:
                parameter_type_name = 'List_' + parameter_type_name

            line = parameter_name + ': ' + parameter_type_name
            if field.has_default_value:
                line = line + ' = ' + repr(field.default_value)
            print(line)


#%%
dict(example_gen_pb2.DESCRIPTOR.message_types_by_name)

#%%
all_proto_module_names = [
    'tfx.proto.example_gen_pb2',
    'tfx.proto.trainer_pb2',
    'tfx.proto.pusher_pb2',
    'tfx.proto.evaluator_pb2',
]

import importlib

all_proto_file_descriptors = [
    importlib.import_module(proto_module_name).DESCRIPTOR
    for proto_module_name in all_proto_module_names
]

all_proto_message_descriptors = {}
for proto_file_descriptor in all_proto_file_descriptors:
    all_proto_message_descriptors.update(proto_file_descriptor.message_types_by_name)


#%%
all_proto_message_descriptors

#%%

for name, message_descriptor in all_proto_message_descriptors.items():
    print()
    print_message_descriptor(message_descriptor)

#%%
for name, message_descriptor in all_proto_message_descriptors.items():
    print()
    print('Message: ' + message_descriptor.name)
    flatten_message_descriptor(message_descriptor)

#%%
#import tfx.components as tfx_components

from tfx.components.example_gen.csv_example_gen.component import CsvExampleGen

component_class = CsvExampleGen

#%%
dir(component_class)

#%%
dir(component_class.SPEC_CLASS)

#%%
component_class.SPEC_CLASS.INPUTS

#%%

def convert_tfx_component(component_class):
    inputs = []
    for name, input in component_class.SPEC_CLASS.INPUTS.items():
        input_struct = {
            'name': name,
            'type_name': input.type_name,
            'optional': input.optional,
        }
        inputs.append(input_struct)
    parameters = []
    for name, parameter in component_class.SPEC_CLASS.PARAMETERS.items():
        parameter_struct = {
            'name': name,
            'type_name': parameter.type.__name__,
            'optional': parameter.optional,
            'is_parameter': True,
        }
        parameters.append(parameter_struct)
    outputs = []
    for name, output in component_class.SPEC_CLASS.OUTPUTS.items():
        output_struct = {
            'name': name,
            'type_name': output.type_name,
            'optional': output.optional,
        }
        outputs.append(output_struct)
    
    component_spec = {
        'inputs': inputs,
        'outputs': outputs,
        'parameters': parameters,
    }
    return component_spec

convert_tfx_component(component_class)

#%%
# === from tfx.utils.dsl_utils import external_input

from typing import Text
from tfx import types
from tfx.types import channel_utils
from tfx.types import standard_artifacts


def external_input(uri: Text) -> types.Channel:
  """Helper function to declare external input.

  Args:
    uri: external path.

  Returns:
    input channel.
  """
  instance = standard_artifacts.ExternalArtifact()
  instance.uri = uri
  return channel_utils.as_channel([instance])

#%%

args_dict = {
    'input_base': external_input('zzz'),
}
# We do not need the component instance. We need executor instance
#tfx_task = component_class(**args_dict)
#tfx_task

#%%
dir(component_class)

#%%

executor = component_class.EXECUTOR_SPEC.executor_class()
executor


#%%
#executor.Context().beam_pipeline_args
#executor.Context()._unique_id

#%%

# Create input dict.
input_base = standard_artifacts.ExternalArtifact()
input_base.uri = os.path.join(input_data_dir, 'external')
input_dict = {'input_base': [input_base]}

# Create output dict.
train_examples = standard_artifacts.Examples(split='train')
train_examples.uri = os.path.join(output_data_dir, 'train')
eval_examples = standard_artifacts.Examples(split='eval')
eval_examples.uri = os.path.join(output_data_dir, 'eval')
output_dict = {'examples': [train_examples, eval_examples]}

# Create exec proterties.
exec_properties = {
    'input_config':
        json_format.MessageToJson(
            example_gen_pb2.Input(splits=[
                example_gen_pb2.Input.Split(name='csv', pattern='csv/*'),
            ])),
    'output_config':
        json_format.MessageToJson(
            example_gen_pb2.Output(
                split_config=example_gen_pb2.SplitConfig(splits=[
                    example_gen_pb2.SplitConfig.Split(
                        name='train', hash_buckets=2),
                    example_gen_pb2.SplitConfig.Split(
                        name='eval', hash_buckets=1)
                ])))
}

#%%
executor.Do(
    input_dict=input_dict,
    output_dict=output_dict,
    exec_properties=exec_properties,
)

#%%


