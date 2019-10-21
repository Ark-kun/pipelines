from kfp.components import InputPath, OutputPath

def infer_schema(
    statistics_path: InputPath('JsonObject_tensorflow_metadata.proto.v0.statistics_pb2.DatasetFeatureStatisticsList'),
    infer_feature_shape: bool = True,
    max_string_domain_size: int = 100,
    #schema_transformations: Optional[List[
    #    Callable[[tensorflow_metadata.proto.v0.schema_pb2.Schema, tensorflow_metadata.proto.v0.statistics_pb2.DatasetFeatureStatistics],
    #             tensorflow_metadata.proto.v0.schema_pb2.Schema]]] = None
    schema_path: OutputPath('JsonObject_tensorflow_metadata.proto.v0.schema_pb2.Schema'),
#) -> 'JsonObject_tensorflow_metadata.proto.v0.schema_pb2.Schema':
):
    import json
    from tensorflow_data_validation.api.validation_api import infer_schema
    from tensorflow_metadata.proto.v0.statistics_pb2 import DatasetFeatureStatisticsList
    from google.protobuf import json_format

    statistics = DatasetFeatureStatisticsList()
    with open(statistics_path, 'r') as statistics_file:
        statistics_dict = json.load(statistics_file)
        json_format.ParseDict(statistics_dict, statistics)

    schema = infer_schema(
        statistics=statistics,
        infer_feature_shape=infer_feature_shape,
        max_string_domain_size=max_string_domain_size,
    )

    #return json_format.MessageToJson(schema)
    with open(schema_path, 'w') as schema_file:
        schema_json = json_format.MessageToJson(schema)
        schema_file.write(schema_json)
