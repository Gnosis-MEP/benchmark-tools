import re

from benchmark_tools.logging import setup_logging


class BaseEvaluation():

    def __init__(self, *args, **kwargs):
        self.logging_level = kwargs['logging_level']
        self.logger = setup_logging(self.__class__.__name__, self.logging_level)
        self.prepare_treshold_functions(kwargs['threshold_functions'])

    def prepare_treshold_functions(self, threshold_functions_dict):
        self.threshold_functions = {}
        for metric, function_str in threshold_functions_dict.items():
            # AAAAAAAAAAAAARGGG!!! EVAAAAAAAAAL!!!!!!
            # BEWARE THIS IS EXTREMELLY INSECURE!!!
            # maybe use some limitation on the scope, just to use simple lambda
            self.threshold_functions[metric] = {
                'function': eval(function_str),
                'str': function_str
            }

    def get_regexp_threshold_function(self, metric):
        for func_key in self.threshold_functions.keys():
            search = re.search(func_key, metric)
            if search:
                return self.threshold_functions[func_key]

    def verify_threshold_for_metric(self, metric, value):
        threshold_function_data = self.threshold_functions.get(metric, None)
        if not threshold_function_data:
            threshold_function_data = self.get_regexp_threshold_function(metric)

        if not threshold_function_data:
            raise Exception(f'No threshold function for {metric}.')
        threshold_function = threshold_function_data['function']
        threshold_function_str = threshold_function_data['str']
        threshold_ok = threshold_function(value)
        return {
            'value': value,
            'threshold': threshold_function_str,
            'passed': threshold_ok,
        }

    def verify_thresholds(self, metrics_result):
        result = {
            'passed': True
        }
        for metric, value in metrics_result.items():
            metric_result = self.verify_threshold_for_metric(metric, value)
            result[metric] = metric_result
            if metric_result['passed'] is False:
                result['passed'] = False
        return result

    def run(self, *args, **kwargs):
        raise NotImplementedError
