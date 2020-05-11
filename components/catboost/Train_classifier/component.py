from kfp.components import InputPath, OutputPath, create_component_from_func

def catboost_train_classifier(
    training_data_path: InputPath('CSV'),
    model_path: OutputPath('CatBoostClassifierModel'),
    starting_model_path: InputPath('CatBoostClassifierModel') = None,
    label_column: int = 0,

    loss_function: str = 'Logloss',
    num_iterations: int = 500,
    learning_rate : float = None,
    depth: int = 6,
    random_seed: int = 0,

    cat_features: list = None,
    text_features: list = None,
):
    '''Train a CatBoost classifier model.

    Args:
        training_data_path: Path for the training data in CSV format.
        model_path: Output path for the trained model in binary CatBoostClassifier format.
        starting_model_path: Path for the existing trained model to start from.
        label_column: Column containing the label data.

        num_iterations: 

        num_boost_rounds: Number of boosting iterations.
        booster_params: Parameters for the booster. See https://xgboost.readthedocs.io/en/latest/parameter.html
        objective: The learning task and the corresponding learning objective.
            See https://xgboost-clone.readthedocs.io/en/latest/parameter.html#learning-task-parameters
            The most common values are:
            "reg:linear" - linear regression
            "reg:logistic" - logistic regression
            "binary:logistic" - logistic regression for binary classification, output probability (default)
            "binary:logitraw" - logistic regression for binary classification, output score before logistic transformation
    '''
    import tempfile
    from pathlib import Path

    from catboost import CatBoostClassifier, Pool

    column_descriptions = {label_column, 'Label'}
    column_description_path = tempfile.NamedTemporaryFile(delete=False).name
    with open(column_description_path, 'w') as column_description_file:
        for idx, kind in column_descriptions.items():
            column_description_file.write('{}\t{}\n'.format(idx, kind))

    train_data = Pool(
        training_data_path,
        column_description=column_description_path,
    )

    model = CatBoostClassifier(
        iterations=num_iterations,
        depth=depth,
        learning_rate=learning_rate,
        loss_function=loss_function,
        verbose=True,
    )

    model.fit(
        train_data,
        train_labels=None,
        cat_features=cat_features,
        text_features=text_features,         
        init_model=starting_model_path,
        #verbose=False,
        plot=True,
    )
    Path(model_path).parent.mkdir(parents=True, exist_ok=True)
    model.save_model(model_path)


if __name__ == '__main__':
    create_component_from_func(
        catboost_train_classifier,
        output_component_file='component.yaml',
        base_image='python:3.7',
        packages_to_install=['catboost==0.22']
    )
