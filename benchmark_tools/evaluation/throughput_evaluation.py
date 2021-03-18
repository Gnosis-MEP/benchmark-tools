# Author : Indeep Singh
# Class : ThroughputEvaluation
# Date : 07-July-2020
# Description : This python script is to calculate a base throughput evaluation of the system.
import requests
from benchmark_tools.evaluation.base import BaseEvaluation


class ThroughputEvaluation(BaseEvaluation):
    JAEGER_TRACES_URL_FORMAT = (
        'api/traces?'
        'limit=700000000&lookback=20h&maxDuration&minDuration&'
        'operation={operation}&service={service}'
    )

    def __init__(self, *args, **kwargs):
        super(ThroughputEvaluation, self).__init__(*args, **kwargs)
        self.jaeger_api_host = kwargs['jaeger_api_host']

    def get_traces(self):
        end_point = self.JAEGER_TRACES_URL_FORMAT.format(
            operation='publish_next_event', service='PreProcessing')
        traces_url = f'{self.jaeger_api_host}/{end_point}'
        req = requests.get(traces_url)
        traces = req.json()['data']
        return traces

    def order_traces(self, traces):
        ordered_traces = sorted(traces, key=lambda t: t['spans'][0]['startTime'])
        return ordered_traces

    def order_traces_by_last(self, traces):
        def span_endtime_key(span):
            start_time = span['startTime']
            duration = span['duration']
            return start_time + duration

        def trace_spans_endtime_key(trace):
            ordered_spans = sorted(trace['spans'], key=span_endtime_key)
            trace['spans'] = ordered_spans
            last_span = ordered_spans[-1]

            start_time = last_span['startTime']
            duration = last_span['duration']
            trace_last_span_time = start_time + duration
            return trace_last_span_time
        ordered_traces = sorted(traces, key=trace_spans_endtime_key)
        return ordered_traces

    def get_trace_latency(self, trace):
        first_start_time = trace['spans'][0]['startTime']
        last_start_time = trace['spans'][-1]['startTime']
        last_duration = trace['spans'][-1]['duration']
        total_trace_time_mm_sec = (last_start_time + last_duration) - first_start_time
        total_trace_time_sec = total_trace_time_mm_sec / 10**6
        return total_trace_time_sec

    def get_initial_time(self, traces):
        traces = self.order_traces(traces)
        first_trace = traces[0]
        first_start_time = first_trace['spans'][0]['startTime']
        return first_start_time

    def get_end_time(self, traces):
        ordered_traces = self.order_traces_by_last(traces)
        last_trace = ordered_traces[-1]
        last_start_time = last_trace['spans'][-1]['startTime']
        last_duration = last_trace['spans'][-1]['duration']
        return last_start_time + last_duration

    def calculate_throughput_metrics(self, traces):
        initial_time = self.get_initial_time(traces)
        end_time = self.get_end_time(traces)
        total_time_mm_sec = end_time - initial_time
        total_time_sec = total_time_mm_sec / 10**6
        total_events = len(traces)
        throughput_fps = (total_events / total_time_sec)

        return {
            'throughput_fps': throughput_fps
        }

    def run(self):
        self.logger.debug('Running Throughput Evaluation.')
        traces = self.get_traces()
        result = self.calculate_throughput_metrics(traces)
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
