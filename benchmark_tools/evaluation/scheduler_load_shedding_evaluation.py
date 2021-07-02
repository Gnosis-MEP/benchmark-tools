import requests

from benchmark_tools.evaluation.base import BaseEvaluation


class SchedulerLoadSheddingEvaluation(BaseEvaluation):
    JAEGER_TRACES_URL_FORMAT = (
        'api/traces?'
        'limit=700000000&lookback=6h&maxDuration&minDuration&'
        'operation={operation}&service={service}'
    )

    def __init__(self, *args, **kwargs):
        super(SchedulerLoadSheddingEvaluation, self).__init__(*args, **kwargs)
        self.jaeger_api_host = kwargs['jaeger_api_host']

    def get_traces(self):
        end_point = self.JAEGER_TRACES_URL_FORMAT.format(
            operation='process_data_event', service='Scheduler')
        traces_url = f'{self.jaeger_api_host}/{end_point}'
        req = requests.get(traces_url)
        traces = req.json()['data']
        return traces

    def has_load_shedding(self, trace):
        for span in trace['spans']:
            if 'log_event_load_shedding' == span['operationName']:
                return True
        return False

    def get_total_load_shedding(self, traces):
        traces_with_load_shedding = [t for t in traces if self.has_load_shedding(t)]
        return len(traces_with_load_shedding)

    def calculate_load_shedding_rate(self, traces):
        total_traces = len(traces)
        self.logger.debug(f'Total event traces being analysed: {total_traces} ')
        total_load_shedding = self.get_total_load_shedding(traces)
        load_shedding_rate = total_load_shedding / total_traces
        return {
            'load_shedding_rate': load_shedding_rate,
            'data_points': total_traces
        }

    def run(self):
        self.logger.debug('Evaluation for Scheduler overall Load Shedding Rate is running...')
        traces = self.get_traces()
        results = self.calculate_load_shedding_rate(traces)
        return self.verify_thresholds(results)


def run(jaeger_api_host, threshold_functions, logging_level):
    evaluation = SchedulerLoadSheddingEvaluation(
        jaeger_api_host=jaeger_api_host,
        threshold_functions=threshold_functions,
        logging_level=logging_level
    )
    return evaluation.run()


if __name__ == '__main__':
    kwargs = {
        "jaeger_api_host": "http://localhost:16686",
        "threshold_functions": {
            "load_shedding_rate": "lambda x: x < (0.27 * 1.05)",
            "data_points": "lambda x: x < (2 * 1.1)",
        },
        "logging_level": "DEBUG"
    }
    print(run(**kwargs))
