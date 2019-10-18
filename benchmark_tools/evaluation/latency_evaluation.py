import statistics
from functools import reduce
import requests

from benchmark_tools.evaluation.base import BaseEvaluation


class LatencyEvaluation(BaseEvaluation):
    JAEGER_TRACES_URL_FORMAT = (
        'api/traces?'
        'limit=-1&lookback=6h&maxDuration&minDuration&'
        'operation={operation}&service={service}'
    )

    def __init__(self, *args, **kwargs):
        super(LatencyEvaluation, self).__init__(*args, **kwargs)
        self.jaeger_api_host = kwargs['jaeger_api_host']

    def calculate_average(self, values):
        return reduce(lambda a, b: a + b, values) / len(values)

    def get_trace_latency(self, trace):
        first_start_time = trace['spans'][0]['startTime']
        last_start_time = trace['spans'][-1]['startTime']
        last_duration = trace['spans'][-1]['duration']
        total_trace_time_mm_sec = (last_start_time + last_duration) - first_start_time
        total_trace_time_sec = total_trace_time_mm_sec / 10**6
        return total_trace_time_sec

    def get_traces(self):
        end_point = self.JAEGER_TRACES_URL_FORMAT.format(
            operation='tracer_injection', service='Forwarder')
        traces_url = f'{self.jaeger_api_host}/{end_point}'
        req = requests.get(traces_url)
        traces = req.json()['data']
        return traces

    def calculate_latency_metrics(self, traces):
        self.logger.debug(f'Total event traces being analysed: {len(traces)} ')
        latencies = [self.get_trace_latency(trace) for trace in traces]
        latency_avg = self.calculate_average(latencies)
        latency_std = statistics.stdev(latencies)
        return {
            'latency_avg': latency_avg,
            'latency_std': latency_std,
        }

    def run(self):
        self.logger.debug('Evaluation for Latency is running...')

        traces = self.get_traces()
        results = self.calculate_latency_metrics(traces)
        return self.verify_thresholds(results)


def run(jaeger_api_host, threshold_functions, logging_level):
    evaluation = LatencyEvaluation(
        jaeger_api_host=jaeger_api_host,
        threshold_functions=threshold_functions,
        logging_level=logging_level
    )
    return evaluation.run()


if __name__ == '__main__':
    kwargs = {
        "jaeger_api_host": "http://localhost:16686",
        "threshold_functions": {
            "latency_avg": "lambda x: x < (3 * 1.1)",
            "latency_std": "lambda x: x < (2 * 1.1)",
        },
        "logging_level": "DEBUG"
    }
    print(run(**kwargs))
