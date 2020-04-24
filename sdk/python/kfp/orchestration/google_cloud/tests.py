import json
import logging

from kfp import components
from kfp.components import load_component_from_url


#automl_pipeline = components.load_component_from_url('https://raw.githubusercontent.com/kubeflow/pipelines/master/sdk/python/tests/components/test_data/retail_product_stockout_prediction_pipeline.component.yaml')

download_from_gcs_op = load_component_from_url('https://raw.githubusercontent.com/kubeflow/pipelines/d013b8535666641ca5a5be6ce67e69e044bbf076/components/google-cloud/storage/download/component.yaml')
CsvExampleGen_op    = load_component_from_url('https://raw.githubusercontent.com/kubeflow/pipelines/025c424a/components/tfx/ExampleGen/CsvExampleGen/component.yaml')
StatisticsGen_op    = load_component_from_url('https://raw.githubusercontent.com/kubeflow/pipelines/025c424a/components/tfx/StatisticsGen/component.yaml')
SchemaGen_op        = load_component_from_url('https://raw.githubusercontent.com/kubeflow/pipelines/025c424a/components/tfx/SchemaGen/component.yaml')
ExampleValidator_op = load_component_from_url('https://raw.githubusercontent.com/kubeflow/pipelines/025c424a/components/tfx/ExampleValidator/component.yaml')
Transform_op        = load_component_from_url('https://raw.githubusercontent.com/kubeflow/pipelines/025c424a/components/tfx/Transform/component.yaml')
Trainer_op          = load_component_from_url('https://raw.githubusercontent.com/kubeflow/pipelines/025c424a/components/tfx/Trainer/component.yaml')
Evaluator_op        = load_component_from_url('https://raw.githubusercontent.com/kubeflow/pipelines/025c424a/components/tfx/Evaluator/component.yaml')

def tfx_pipeline(
    input_data_uri,
):
    #Only S3/GCS is supported for now.
    module_file = 'gs://ml-pipeline-playground/tensorflow-tfx-repo/tfx/examples/chicago_taxi_pipeline/taxi_utils.py'

    download_task = download_from_gcs_op(
        input_data_uri,
    )

    examples_task = CsvExampleGen_op(
        input_base=download_task.outputs['Data'],
        input_config=json.dumps({
            "splits": [
                {'name': 'data', 'pattern': '*.csv'},
            ]
        }),
        output_config=json.dumps({
            "splitConfig": {
                "splits": [
                    {'name': 'train', 'hash_buckets': 2},
                    {'name': 'eval', 'hash_buckets': 1},
                ]
            }
        }),
    )
    
    statistics_task = StatisticsGen_op(
        examples_task.outputs['example_artifacts'],
    )
   
    schema_task = SchemaGen_op(
        statistics_task.outputs['output'],
    )

    # Performs anomaly detection based on statistics and data schema.
    validator_task = ExampleValidator_op(
        stats=statistics_task.outputs['output'],
        schema=schema_task.outputs['output'],
    )

    # Performs transformations and feature engineering in training and serving.
    transform_task = Transform_op(
        input_data=examples_task.outputs['example_artifacts'],
        schema=schema_task.outputs['output'],
        module_file=module_file,
    )

    trainer_task = Trainer_op(
        module_file=module_file,
        examples=transform_task.outputs['transformed_examples'],
        schema=schema_task.outputs['output'],
        transform_output=transform_task.outputs['transform_output'],
        train_args=json.dumps({'num_steps': 10000}),
        eval_args=json.dumps({'num_steps': 5000}),
    )

    # Uses TFMA to compute a evaluation statistics over features of a model.
    model_analyzer = Evaluator_op(
        examples=examples_task.outputs['example_artifacts'],
        model_exports=trainer_task.outputs['output'],
        feature_slicing_spec=json.dumps({
            'specs': [
                {'column_for_slicing': ['trip_start_hour']},
            ],
        }),
    )


null_op = components.load_component_from_text('''
name: No-op
implementation:
  container:
    image: gcr.io/managed-pipeline-test/cloud-sdk:278.0.0
    command:
    - echo
    - Hello world!
''')

param_op = components.load_component_from_text('''
name: Param
inputs:
- {name: param1, default: param1_default}
implementation:
  container:
    image: gcr.io/managed-pipeline-test/cloud-sdk:278.0.0
    command:
    - echo
    - {inputValue: param1}
''')


producer_op = components.load_component_from_text('''
name: Producer
outputs:
- {name: output1}
implementation:
  container:
    image: gcr.io/managed-pipeline-test/cloud-sdk:278.0.0
    command:
    - sh
    - -c
    - 'echo "$0" >"$1"'
    - Hello
    - {outputPath: output1}
''')


transformer_op = components.load_component_from_text('''
name: Transformer
inputs:
- {name: input1}
outputs:
- {name: output1}
implementation:
  container:
    image: gcr.io/managed-pipeline-test/cloud-sdk:278.0.0
    command:
    - cp
    - -r
    - {inputPath: input1}
    - {outputPath: output1}
''')


test_gsutil_op = components.load_component_from_text('''
name: Test gsutil
outputs:
- {name: output1}
implementation:
  container:
    image: python:3.7
    command:
    - sh
    - -c
    - gsutil version -l >"$0"
    - {outputPath: output1}
''')


def pipeline0():
    null_op() # works
    param_op('world 1') # works
    producer_task = producer_op()
    transformer_op(input1=producer_task.outputs['output1'])

def pipeline1():
    test_gsutil_op()


logging.getLogger().setLevel(logging.INFO)

from ._pipeline_runner import run_pipeline_func_on_google_cloud

pipeline_response = run_pipeline_func_on_google_cloud(
#pipeline_job = compile_pipeline_job_for_caip(
    pipeline_func=tfx_pipeline,
    #pipeline_func=pipeline0,
    #pipeline_func=pipeline1,
    arguments=dict(
        input_data_uri='gs://ml-pipeline-playground/tensorflow-tfx-repo/tfx/components/testdata/external/csv',
    ),
    #pipeline_root='gs://avolkov/tmp/run_pipeline_on_google_cloud_ai_platform-tfx_pipeline/',
    pipeline_root='gs://managed-pipeline-container-sample-root/users/avolkov/tmp/',
    pipeline_context='context1'
)

pipeline_response.wait_for_completion()

print(pipeline_response.current_state['jobDetail'])

#with open('pipeline_job.json', 'w') as pipeline_file:
#    json.dump(pipeline_job, pipeline_file)
#with open('pipeline_response.json', 'w') as pipeline_response_file:
#    json.dump(pipeline_response, pipeline_response_file)
