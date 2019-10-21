#%%

import sys
sys.path.append(r'a:/_All/My.Creativity.Coding.3rd-party/_Incoming/tensorflow-tfx')

#%%
import tfx.proto.example_gen_pb2



#%%
tfx.proto.example_gen_pb2.Input

#%%
tfx.proto.example_gen_pb2.DESCRIPTOR

#%%
tfx.proto.example_gen_pb2.Input.__dict__

#%%
tfx.proto.example_gen_pb2.Input.DESCRIPTOR

#%%
dir(tfx.proto.example_gen_pb2.Input.DESCRIPTOR)

#%%

list(tfx.proto.example_gen_pb2.Input.DESCRIPTOR.fields)

#%%
dir(list(tfx.proto.example_gen_pb2.Input.DESCRIPTOR.fields)[0])

#%%

message_class = tfx.proto.example_gen_pb2.Input

#%%

from google.protobuf.json_format import MessageToDict, ParseDict

from tfx.proto import example_gen_pb2
MessageToDict(example_gen_pb2.Input(splits=[example_gen_pb2.Input.Split(name='name1', pattern='pattern1'), example_gen_pb2.Input.Split(name='name2', pattern='pattern2')]))
{'splits': [{'name': 'name1', 'pattern': 'pattern1'}, {'name': 'name2', 'pattern': 'pattern2'}]}

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
    attr_names = ['name', 'label', 'has_default_value', 'default_value', 'message_type']
    for field in message_class.DESCRIPTOR.fields:
        print(indent + '  Field: ' + field.name)
        print(indent + '    _label_type: ' + protobuf_label_to_str[field.label])
        for attr_name in attr_names:
            print(indent + '    ' + attr_name + ': ' + str(getattr(field, attr_name)))
        if type(field.message_type).__name__ == 'MessageDescriptor':
            print('ZZZ')
            #print_message_descriptor(field.message_type, indent + '    ')
            print(indent + '    ' + 'Descriptor name: ' + field.message_type.name)

print_message_descriptor(message_class.DESCRIPTOR)

#%%
dir(message_class)

#%%

def flatten_message_descriptor(message_descriptor, indent=''):
    print(indent + 'Message: ' + message_descriptor.name)
    attr_names = ['name', 'label', 'has_default_value', 'default_value', 'message_type']
    for field in message_class.DESCRIPTOR.fields:
        print(indent + '  Field: ' + field.name)
        print(indent + '    _label_type: ' + protobuf_label_to_str[field.label])
        for attr_name in attr_names:
            print(indent + '    ' + attr_name + ': ' + str(getattr(field, attr_name)))
        if type(field.message_type).__name__ == 'MessageDescriptor':
            print_message_descriptor(field.message_type, indent + '    ')