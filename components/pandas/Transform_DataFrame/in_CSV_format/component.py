from kfp.components import InputPath, OutputPath, create_component_from_func

def Pandas_Transform_DataFrame_in_CSV_format(
    table_path: InputPath('CSV'),
    output_table_path: OutputPath('CSV'),
    transform_code: 'PythonCode',
):
    '''Transform DataFrame loaded from a CSV file.

    Inputs:
        table: Table to transform.
        transform_code: Transformation code. Code is written in Python and can consist of multiple lines.
            The DataFrame variable is called "df".
            Examples:
            - `df['prod'] = df['X'] * df['Y']`
            - `df = df[['X', 'prod']]`
            - `df.insert(0, "is_positive", df["X"] > 0)`

    Outputs:
        output_data: Transformed table.

    Annotations:
        author: Alexey Volkov <alexey.volkov@ark-kun.com>
    '''
    import pandas

    df = pandas.read_csv(
        table_path,
    )
    exec(transform_code)
    df.to_csv(
        output_table_path,
        index=False,
    )


if __name__ == '__main__':
    Pandas_Transform_DataFrame_in_CSV_format_op = create_component_from_func(
        Pandas_Transform_DataFrame_in_CSV_format,
        output_component_file='component.yaml',
        base_image='python:3.7',
        packages_to_install=[
            'pandas==1.0.4',
        ],
    )
