

def eval_latency_avg(target_system):
    return 0


def evaluate(target_system, metrics):
    result = {}
    for metric, sub_metric_dict in metrics.items():
        for sub_metric, threshold_func_str in sub_metric_dict.items():
            metric_full_name = f'{metric}_{sub_metric}'
            eval_function = globals().get(f'eval_{metric_full_name}', None)
            if not eval_function:
                print(f'No evaluation function for eval_{metric_full_name}.')
                continue
            value = eval_function(target_system)
            threshold_func = eval(threshold_func_str)  ##EVAL!!!!!
            eval_passed = threshold_func(value)
            result[metric_full_name] = {
                'value': value,
                'threshold': threshold_func_str,
                'passed': eval_passed,
            }
            if not eval_passed:
                result['passed'] = False

    return result
