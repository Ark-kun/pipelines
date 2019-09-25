from typing import NamedTuple


def automl_create_model_for_tables(
    gcp_project_id: str,
    gcp_region: str,
    display_name: str,
    dataset_id: str,
    target_column_path: str = None,
    input_feature_column_paths: list = None,
    optimization_objective: str = 'MAXIMIZE_AU_PRC',
    train_budget_milli_node_hours: int = 1000,
) -> NamedTuple('Outputs', [('model_path', str), ('model_id', str)]):
    import sys
    import subprocess
    subprocess.run([sys.executable, '-m', 'pip3', 'install', 'google-cloud-automl==0.4.0', '--quiet', '--no-warn-script-location'], env={'PIP_DISABLE_PIP_VERSION_CHECK': '1'}, check=True)

    from google.cloud import automl
    client = automl.AutoMlClient()

    location_path = client.location_path(gcp_project_id, gcp_region)
    model_dict = {
        'display_name': display_name,
        'dataset_id': dataset_id, 
        'tables_model_metadata': {
            'target_column_spec': automl.types.ColumnSpec(name=target_column_path),
            'input_feature_column_specs': [automl.types.ColumnSpec(name=path) for path in input_feature_column_paths] if input_feature_column_paths else None,
            'optimization_objective': optimization_objective,
            'train_budget_milli_node_hours': train_budget_milli_node_hours,
        },  
    }

    create_model_response = client.create_model(location_path, model_dict)
    print('Create model operation: {}'.format(create_model_response.operation))
    result = create_model_response.result()
    print(result)
    model_name = result.name
    model_id = model_name.rsplit('/', 1)[-1]
    return (model_name, model_id)


if __name__ == '__main__':
    import kfp
    kfp.components.func_to_container_op(automl_create_model_for_tables, output_component_file='component.yaml')
