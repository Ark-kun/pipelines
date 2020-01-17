__all__ = [
    'create_tfx_component_from_kfp_component',
    'tfx_component_resolving_context',
]

from contextlib import contextmanager

import tfx
from tfx.components.base.base_component import BaseComponent
from tfx.components.base.executor_spec import ExecutorContainerSpec
#from ml_metadata.proto.metadata_store_pb2 import ArtifactType


def create_tfx_component_from_kfp_component(
    component_spec: 'ComponentSpec',
) -> BaseComponent:
    from kfp.components._components import _resolve_command_line_and_paths
    from kfp.components._naming import _sanitize_python_function_name

    container_spec = component_spec.implementation.container

    # FIX: The {{inputs.{name}.value}} placeholder is not implemented yet. Jiaxiao works on this.
    component_arguments = {
        input.name: '{{{{inputs.{name}.value}}}}'.format(name=_sanitize_python_function_name(input.name))
        for input in component_spec.inputs or []  
    }

    # FIX: The {{inputs.{name}.local_path}} placeholder is not implemented yet. Me and Jiaxiao will work on this.
    def input_path_uri_generator(name):
        #return '{{{{inputs.{name}.local_path}}}}'.format(name=_sanitize_python_function_name(name))
        return '{{{{inputs.{name}.uri}}}}'.format(name=_sanitize_python_function_name(name))

    def output_path_uri_generator(name):
        #return '{{{{outputs.{name}.local_path}}}}'.format(name=_sanitize_python_function_name(name))
        return '{{{{outputs.{name}.uri}}}}'.format(name=_sanitize_python_function_name(name))

    resolved_cmd = _resolve_command_line_and_paths(
        component_spec=component_spec,
        arguments=component_arguments,
        input_path_generator=input_path_uri_generator, # FIX: Remove this broken workaround
        output_path_generator=output_path_uri_generator, # FIX: Remove this broken workaround
    )

    resolved_command = resolved_cmd.command
    resolved_args = resolved_cmd.args

    if container_spec is None:
        raise TypeError('Only components with container implementation can be instantiated in TFX at this moment.')

    component_class_name = _sanitize_python_function_name(component_spec.name or 'Component')
    component_class_name = ''.join(word.title() for word in component_class_name.split('_'))

    component_class_doc = (component_spec.name or "") + "\n" + (component_spec.description or "")

    input_channel_parameters = {
        _sanitize_python_function_name(input.name): tfx.types.component_spec.ChannelParameter(
            type_name=str(input.type),
        )
        for input in component_spec.inputs or []
    }

    output_channel_parameters = {
        _sanitize_python_function_name(output.name): tfx.types.component_spec.ChannelParameter(
            type_name=str(output.type),
        )
        for output in component_spec.outputs or []
    }


    default_input_channels = {
        _sanitize_python_function_name(input.name): tfx.types.Channel(
            type_name=str(input.type),
            artifacts=[
                tfx.types.Artifact(
                    type_name=str(input.type),
                )
            ],
        )
        for input in component_spec.inputs or []
        if input.optional == True
    }

    output_channels = {
        _sanitize_python_function_name(output.name): tfx.types.Channel(
            type_name=str(output.type),
            artifacts=[
                tfx.types.Artifact(
                    type_name=str(output.type),
                )
            ],
        )
        for output in component_spec.outputs or []
    }


    execution_parameters={}

    tfx_component_spec_class = type(
        component_class_name + "Spec",
        (tfx.types.ComponentSpec,),
        dict(
            PARAMETERS=execution_parameters,
            INPUTS=input_channel_parameters,
            OUTPUTS=output_channel_parameters,
            __module__ = None, # Check deserialization
            __doc__ = component_class_doc,
        ),
    )

    # TODO: Add default channel construction for outputs and optional inputs
    # TODO: ?Allow passing constant value or channel for every input
    # Works
    def tfx_component_class_init(self, **kwargs):
        arguments = {}
        arguments.update(default_input_channels)
        arguments.update(output_channels)
        arguments.update(kwargs)

        #super().__init__(self.__class__.SPEC_CLASS(**arguments)) # RuntimeError: super(): __class__ cell not found
        #super().__init__(self, self.__class__.SPEC_CLASS(**arguments)) # RuntimeError: super(): __class__ cell not found
        BaseComponent.__init__(self, spec=self.__class__.SPEC_CLASS(**arguments))

    # Works too
    #def tfx_component_class_init2(self, **kwargs):
    #    super(tfx_component_class, self).__init__(
    #        spec=self.__class__.SPEC_CLASS(**kwargs),
    #    )
    #
    #tfx_component_class.__init__ = tfx_component_class_init2

    tfx_component_class = type(
        component_class_name,
        (BaseComponent,),
        dict(
            SPEC_CLASS=tfx_component_spec_class,
            EXECUTOR_SPEC=ExecutorContainerSpec(
                image=container_spec.image,
                command=resolved_command,
                args=resolved_args,
            ),
            __init__=tfx_component_class_init,
            __doc__=component_class_doc,
            __module__=None, # Check deserialization
        ),
    )


    return tfx_component_class



def create_tfx_base_component_from_component_and_arguments(
    component_spec: 'ComponentSpec',
    arguments: dict,
    component_ref: 'ComponentReference',
) -> BaseComponent:
    tfx_component_class = create_tfx_component_from_kfp_component(component_spec)
    tfx_task = tfx_component_class(**arguments)
    return tfx_task


@contextmanager
def tfx_component_resolving_context():
    from kfp import components
    old_handler = components._components._container_task_constructor
    try:
        components._components._container_task_constructor = create_tfx_base_component_from_component_and_arguments
        yield None
    finally:
        components._components._container_task_constructor = old_handler
