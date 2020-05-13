import kfp
from kfp.components import load_component_from_url, load_component_from_text

dataset_op = load_component_from_url('https://raw.githubusercontent.com/Ark-kun/pipelines/Components---Ark---WIP/components/Ark-kun/_datasets/Chicago%20Taxi/CSV%20-%20All%20columns/component.yaml')
xgboost_train_op = load_component_from_url('https://raw.githubusercontent.com/Ark-kun/pipelines/Components---Ark---WIP/components/xgboost/Train/component.yaml')
xgboost_predict_op = load_component_from_url('https://raw.githubusercontent.com/Ark-kun/pipelines/Components---Ark---WIP/components/xgboost/Predict/component.yaml')

cut_op = load_component_from_text('''
name: Cut
inputs:
- {name: Text}
- {name: Fields}
- {name: Delimiter, default: '\t'}
outputs:
- {name: Text}
implementation:
  container:
    image: busybox
    command:
    - sh
    - -exc
    - |
      mkdir -p "$(dirname "$1")"
      <"$0" cut -f "$2" -d "$3" >"$1"
    - {inputPath: Text}
    - {outputPath: Text}
    - {inputValue: Fields}
    - {inputValue: Delimiter}
''')

skip_header_op = load_component_from_text('''
name: Skip header line
inputs:
- {name: Text}
outputs:
- {name: Text}
implementation:
  container:
    image: busybox
    command:
    - sh
    - -exc
    - |
      mkdir -p "$(dirname "$1")"
      <"$0" tail -n +2 | tr -d '"' >"$1"
    - {inputPath: Text}
    - {outputPath: Text}
''')


def dataset_pipeline():
    #dataset_op()
    get_training_data_task = dataset_op(
        #where='trip_start_timestamp >= "2019-01-01" AND trip_start_timestamp < "2019-02-01"',
        where='trip_start_timestamp>="2019-01-01"',
        #select='tips,trip_seconds,trip_miles,pickup_community_area,dropoff_community_area,fare,tolls,extras,trip_total,payment_type',
        select='tips,trip_seconds,trip_miles,pickup_community_area,dropoff_community_area,fare,tolls,extras,trip_total',
        limit=100000,
    )
    
    #eval_data_task = cut_op(
    #    text=get_training_data_task.output,
    #    delimiter=',',
    #    fields='2-',
    #)
    
    training_data = skip_header_op(get_training_data_task.output).output
    
    xgboost_train_task = xgboost_train_op(
        #training_data=get_training_data_task.output,
        training_data=training_data,
        label_column=0,
        objective='reg:linear',
        num_iterations=200,
    )
    
    #xgboost_predict_op(
    #    data=eval_data_task.output,
    #    model=xgboost_train_task.outputs['model'],
    #)
    
    xgboost_predict_op(
        #data=get_training_data_task.output,
        data=training_data,
        model=xgboost_train_task.outputs['model'],
        label_column=0,
    )

kfp_endpoint='https://7b9be24a9240fb0e-dot-us-central2.pipelines.googleusercontent.com/'
kfp.Client(host=kfp_endpoint).create_run_from_pipeline_func(dataset_pipeline, arguments={})
