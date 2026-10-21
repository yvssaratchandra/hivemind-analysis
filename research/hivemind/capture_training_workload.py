"""Explicit X/B naming adapter for the existing paired iid workload."""
from compute_workload import evaluate_models as _evaluate_models


def evaluate_models(models, x, y):
    """Preserve original evaluator arithmetic, naming the attacker X instead of B."""
    result = _evaluate_models({'B' if n == 'X' else n: v for n,v in models.items()}, x, y)
    for key in ('per_node', 'node_status'):
        if 'B' in result.get(key, {}):
            result[key]['X'] = result[key].pop('B')
    return result
