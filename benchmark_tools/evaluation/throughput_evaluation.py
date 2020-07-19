# Author : Indeep Singh
# Class : ThroughputEvaluation
# Date : 07-July-2020
# Description : This python script is to calculate a base throughput evaluation of the system.

from benchmark_tools.evaluation.latency_evaluation import LatencyEvaluation


class ThroughputEvaluation(LatencyEvaluation):
    JAEGER_TRACES_URL_FORMAT = (
        'api/traces?'
        'limit=700000000&lookback=6h&maxDuration&minDuration&'
        'operation={operation}&service={service}'
    )

    def __init__(self, *args, **kwargs):
        super(ThroughputEvaluation, self).__init__(*args, **kwargs)
        self.jaeger_api_host = kwargs['jaeger_api_host']

    def calculate_throughput(self, avg_latency):
        retval = (1/avg_latency)
        return retval  # this will give us the required throughput of the system.

    def run(self):
        self.logger.debug('Running Throughput Evaluation.')
        traces = self.get_traces()
        avg_latency = self.calculate_latency_metrics(traces)
        throughput = self.calculate_throughput(avg_latency['latency_avg'])
        result = {
            'throughput_avg': throughput
        }
        return self.verify_thresholds(result)


def run(jaeger_api_host, threshold_functions, logging_level):
    evaluation = ThroughputEvaluation(
        jaeger_api_host=jaeger_api_host,
        threshold_functions=threshold_functions,
        logging_level=logging_level
    )
    return evaluation.run()


if __name__ == '__main__':
    kwargs = {
        "jaeger_api_host": "http://localhost:16686",
        "threshold_functions": {
            "throughput": "lambda x: x > 10",
        },
        "logging_level": "DEBUG"
    }
    print(run(**kwargs))
