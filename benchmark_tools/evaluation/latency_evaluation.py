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

    def get_traces(self):
        end_point = self.JAEGER_TRACES_URL_FORMAT.format(
            operation='tracer_injection', service='Forwarder')
        traces_url = f'{self.jaeger_api_host}/{end_point}'
        req = requests.get(traces_url)
        first_start_time = req.json()['data'][0]['spans'][0]['startTime']
        last_start_time = req.json()['data'][0]['spans'][-1]['startTime']
        last_duration = req.json()['data'][0]['spans'][-1]['duration']
        total_trace_time = (last_start_time + last_duration) - first_start_time
        print(total_trace_time)
        return req

    def run(self):
        result = self.get_traces()
        return self.verify_thresholds(result)


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
        },
        "logging_level": "DEBUG"
    }
    print(run(**kwargs))
