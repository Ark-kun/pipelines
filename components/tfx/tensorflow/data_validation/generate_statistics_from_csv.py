from kfp.components import InputPath, OutputPath

def generate_statistics_from_csv_data(
    #data_location: str, -> data_path May be used in def generate_statistics_from_csv_uri
    data_path: InputPath('CSV'),
    statistics_path: OutputPath('JsonObject_tensorflow_metadata.proto.v0.statistics_pb2.DatasetFeatureStatisticsList'),
    column_names: list = None, # List[str]
    delimiter: str = ',',
    #output_path: str = None,
    stats_options: 'JsonObject_tensorflow_data_validation.statistics.stats_option.StatsOptions' = None, # = options.StatsOptions(),
    #pipeline_options: Optional[PipelineOptions] = None,
    #compression_type: str = CompressionTypes.AUTO,
#) -> 'JsonObject_tensorflow_metadata.proto.v0.statistics_pb2.DatasetFeatureStatisticsList':
):
    """Compute data statistics from CSV files.
  
    Runs a Beam pipeline to compute the data statistics and return the result
    data statistics proto.
  
    This is a convenience method for users with data in CSV format.
    Users with data in unsupported file/data formats, or users who wish
    to create their own Beam pipelines need to use the 'GenerateStatistics'
    PTransform API directly instead.
  
    Args:
      #data_location: The location of the input data files.
      data: The input data files.
      column_names: A list of column names to be treated as the CSV header. Order
        must match the order in the input CSV files. If this argument is not
        specified, we assume the first line in the input CSV files as the
        header. Note that this option is valid only for 'csv' input file format.
      delimiter: A one-character string used to separate fields in a CSV file.
      #output_path: The file path to output data statistics result to. If None, we
      #  use a temporary directory. It will be a TFRecord file containing a single
      #  data statistics proto, and can be read with the 'load_statistics' API.
      #  If you run this function on Google Cloud, you must specify an
      #  output_path. Specifying None may cause an error.
      stats_options: `tfdv.StatsOptions` for generating data statistics.
      #pipeline_options: Optional beam pipeline options. This allows users to
      #  specify various beam pipeline execution parameters like pipeline runner
      #  (DirectRunner or DataflowRunner), cloud dataflow service project id, etc.
      #  See https://cloud.google.com/dataflow/pipelines/specifying-exec-params for
      #  more details.
      #compression_type: Used to handle compressed input files. Default value is
      #  CompressionTypes.AUTO, in which case the file_path's extension will be
      #  used to detect the compression.
  
    Returns:
      statistics: A JSON-serialized DatasetFeatureStatisticsList proto.
    """
    from google.protobuf import json_format
    from tensorflow_data_validation.statistics.stats_option import StatsOptions
    from tensorflow_data_validation.utils.stats_gen_lib import generate_statistics_from_csv

    if stats_options:
        stats_options_obj = json_format.Parse(stats_options)
    else:
        stats_options_obj = StatsOptions()

    generate_statistics_from_csv(
        data_location=data_path,
        output_path=statistics_path,
        column_names=column_names,
        delimiter=delimiter,
        stats_options=stats_options_obj,
    )