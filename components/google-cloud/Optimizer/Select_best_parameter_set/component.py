from typing import NamedTuple

from kfp.components import create_component_from_func


def get_best_parameter_set(
    metrics_for_parameter_sets: list,
    metric_name: str = 'metric',
    maximize: bool = False,
) -> NamedTuple('Outputs', [
    ('parameters', dict),
    ('metrics', dict),
    ('metric_value', float),
]):
    """Selects the best parameter set based on metrics.

    Annotations:
        author: Alexey Volkov <alexey.volkov@ark-kun.com>

    Args:
        metrics_for_parameter_sets: List of parameter sets and evaluation metrics for them. Each list item contains "parameters" dict and "metrics" dict. Example: {"parameters": {"p1": 1.1, "p2": 2.2}, "metrics": {"metric1": 101, "metric2": 102} }
        metric_name: Name of the metric to use
    """
    min_or_max = max if maximize else min
    best_parameter_set_and_metrics = min_or_max(metrics_for_parameter_sets, key=lambda entry: float(entry['metrics'][metric_name]))
    
    return (
        best_parameter_set_and_metrics['parameters'],
        best_parameter_set_and_metrics['metrics'],
        best_parameter_set_and_metrics['metrics'][metric_name],
    )


if __name__ == '__main__':
    get_best_parameter_set_op = create_component_from_func(
        get_best_parameter_set,
        base_image='python:3.8',
        output_component_file='component.yaml',
    )
