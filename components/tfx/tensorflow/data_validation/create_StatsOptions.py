def create_StatsOptions(
    #generators: List[stats_generator.StatsGenerator] = None,
    feature_whitelist: list = None, # List[str]
    #schema: schema_pb2.Schema = None,
    weight_feature: list = None, # List[str]
    #slice_functions: List[types.SliceFunction] = None,
    sample_count: int = None,
    sample_rate: float = None,
    num_top_values: int = 20,
    frequency_threshold: int = 1,
    weighted_frequency_threshold: float = 1.0,
    num_rank_histogram_buckets: int = 1000,
    num_values_histogram_buckets: int = 10,
    num_histogram_buckets: int = 10,
    num_quantiles_histogram_buckets: int = 10,
    epsilon: float = 0.01,
    infer_type_from_schema: bool = False,
    desired_batch_size: int = None,
    enable_semantic_domain_stats: bool = False,
    semantic_domain_stats_sample_rate: float = None,
) -> 'JsonObject_tensorflow_data_validation.statistics.stats_option.StatsOptions':
    """Initializes statistics options.

    Args:
      generators: An optional list of statistics generators. A statistics
        generator must extend either CombinerStatsGenerator or
        TransformStatsGenerator.
      feature_whitelist: An optional list of names of the features to calculate
        statistics for.
      schema: An optional tensorflow_metadata Schema proto. Currently we use the
        schema to infer categorical and bytes features.
      weight_feature: An optional feature name whose numeric value represents
          the weight of an example.
      slice_functions: An optional list of functions that generate slice keys
        for each example. Each slice function should take an example dict as
        input and return a list of zero or more slice keys.
      sample_count: An optional number of examples to include in the sample. If
        specified, statistics is computed over the sample. Only one of
        sample_count or sample_rate can be specified. Note that since TFDV
        batches input examples, the sample count is only a desired count and we
        may include more examples in certain cases.
      sample_rate: An optional sampling rate. If specified, statistics is
        computed over the sample. Only one of sample_count or sample_rate can
        be specified.
      num_top_values: An optional number of most frequent feature values to keep
        for string features.
      frequency_threshold: An optional minimum number of examples the most
        frequent values must be present in.
      weighted_frequency_threshold: An optional minimum weighted number of
        examples the most frequent weighted values must be present in. This
        option is only relevant when a weight_feature is specified.
      num_rank_histogram_buckets: An optional number of buckets in the rank
        histogram for string features.
      num_values_histogram_buckets: An optional number of buckets in a quantiles
        histogram for the number of values per Feature, which is stored in
        CommonStatistics.num_values_histogram.
      num_histogram_buckets: An optional number of buckets in a standard
        NumericStatistics.histogram with equal-width buckets.
      num_quantiles_histogram_buckets: An optional number of buckets in a
        quantiles NumericStatistics.histogram.
      epsilon: An optional error tolerance for the computation of quantiles,
        typically a small fraction close to zero (e.g. 0.01). Higher values of
        epsilon increase the quantile approximation, and hence result in more
        unequal buckets, but could improve performance, and resource
        consumption.
      infer_type_from_schema: A boolean to indicate whether the feature types
          should be inferred from the schema. If set to True, an input schema
          must be provided. This flag is used only when generating statistics
          on CSV data.
      desired_batch_size: An optional number of examples to include in each
        batch that is passed to the statistics generators.
      enable_semantic_domain_stats: If True statistics for semantic domains are
        generated (e.g: image, text domains).
      semantic_domain_stats_sample_rate: An optional sampling rate for semantic
        domain statistics. If specified, semantic domain statistics is computed
        over a sample.
    """
    arguments = locals()
    #arguments.pop(, None)

    from google.protobuf import json_format
    from tensorflow_data_validation.statistics.stats_option import StatsOptions

    stats_options_obj = StatsOptions(**arguments)

    return json_format.MessageToJson(stats_options_obj)
