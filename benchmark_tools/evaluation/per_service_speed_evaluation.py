import statistics
from functools import reduce
import requests

from benchmark_tools.evaluation.base import BaseEvaluation


class PerServiceSpeedEvaluation(BaseEvaluation):
    JAEGER_TRACES_URL_FORMAT = (
        'api/traces?'
        'limit=700000000&lookback=6h&maxDuration&minDuration&'
        'service={service}'
    )
    JAEGER_OPERATIONS_URL_FORMAT = 'api/services/{service}/operations'
    JAEGER_SERVICES_URL = 'api/services'

    def __init__(self, *args, **kwargs):
        super(PerServiceSpeedEvaluation, self).__init__(*args, **kwargs)
        self.jaeger_api_host = kwargs['jaeger_api_host']
        services = kwargs['services']
        if services == 'all':
            services = self.get_services()
        self.services = services

    def calculate_average(self, values):
        return reduce(lambda a, b: a + b, values) / len(values)

    def get_service_process_id(self, trace, service):
        for p_id, process in trace['processes'].items():
            if process['serviceName'] == service:
                return p_id

    def get_traces_operations_speed(self, traces, service):
        results_per_operation = {}
        for trace in traces:
            service_process_id = self.get_service_process_id(trace, service)
            for span in trace['spans']:
                if span['processID'] == service_process_id:
                    duration_micro_sec = span['duration']
                    operation = span['operationName']
                    results_per_operation.setdefault(operation, []).append(duration_micro_sec)
        return results_per_operation

    def calculate_service_operations_average_and_std(self, results_per_operation, service):
        results = {}
        for operation, values in results_per_operation.items():
            # avg = self.calculate_average(values)
            # std = statistics.stdev(values)
            avg = self.calculate_average(values) / 10**6
            results[f'{service}_{operation}_avg'] = avg
            try:
                std = statistics.stdev([v / 10**6 for v in values])
                results[f'{service}_{operation}_std'] = std
            except:
                pass

        return results

    def get_traces_per_service(self, service):
        end_point = self.JAEGER_TRACES_URL_FORMAT.format(service=service)
        traces_url = f'{self.jaeger_api_host}/{end_point}'
        req = requests.get(traces_url)
        traces = req.json()['data']
        return traces

    def get_traces_from_file(self, service):
        with open('....e2e_early_filtering_pipeline/AnyCars/cloudseg/AdaptivePublisher-process_next_frame.json', 'r') as f:
            return json.load(f)['data']

    def get_services(self):
        end_point = self.JAEGER_SERVICES_URL
        services_url = f'{self.jaeger_api_host}/{end_point}'
        req = requests.get(services_url)
        services = req.json()['data']
        return services

    def get_service_operations(self, service):
        end_point = self.JAEGER_OPERATIONS_URL_FORMAT.format(service=service)
        endpoint_url = f'{self.jaeger_api_host}/{end_point}'
        req = requests.get(endpoint_url)
        services = req.json()['data']
        return services

    def calculate_service_operations_latency_metrics(self, traces, service):
        self.logger.debug(f'Total event traces being analysed: {len(traces)} ')
        latencies = [self.get_trace_latency(trace) for trace in traces]
        latency_avg = self.calculate_average(latencies)
        latency_std = statistics.stdev(latencies)
        return {
            'latency_avg': latency_avg,
            'latency_std': latency_std,
        }

    def run(self):
        self.logger.debug('Evaluation for Services Speed is running...')
        results = {}
        for service in self.services:
            if 'jaeger' in service:
                continue
            traces = self.get_traces_per_service(service)
            # traces = self.get_traces_from_file(service)
            results_per_operation = self.get_traces_operations_speed(traces, service)
            averaged_results = self.calculate_service_operations_average_and_std(results_per_operation, service)
            results.update(averaged_results)
        return self.verify_thresholds(results)


def run(jaeger_api_host, services, threshold_functions, logging_level):
    evaluation = PerServiceSpeedEvaluation(
        jaeger_api_host=jaeger_api_host,
        services=services,
        threshold_functions=threshold_functions,
        logging_level=logging_level
    )
    return evaluation.run()


if __name__ == '__main__':
    kwargs = {
        "jaeger_api_host": "http://172.17.0.1:16686",
        "services": ["AdaptivePublisher", "ImageResizerService", "PytorchObjectDetectionService"],
        "threshold_functions": {
            # ".*_serialize_and_write_event_with_trace_avg": "lambda x: x < 0.02",
            # ".*_serialize_and_write_event_with_trace_std": "lambda x: x < 0.02",
            # ".*_tracer_injection_avg": "lambda x: x < 0.0001",
            # ".*_tracer_injection_std": "lambda x: x < 0.0001",
            # ".*_process_data_event_avg": "lambda x: x < 0.05",
            # ".*_process_data_event_std": "lambda x: x < 0.05",
            # ".*_publish_next_event_avg": "lambda x: x < 0.01",
            # ".*_publish_next_event_std": "lambda x: x < 0.01",
            # ".*_process_action_avg": "lambda x: x < 0.1",
            # ".*_process_action_std": "lambda x: x < 0.1",
            ".*": "lambda x: True",
            # "ObjectDetectionService_process_data_event_avg": "lambda x: x < (0.4)",
            # "ObjectDetectionService_process_data_event_std": "lambda x: x < (0.4)",
        },
        "logging_level": "DEBUG"
    }
    import json
    print(json.dumps(run(**kwargs), indent=4))
