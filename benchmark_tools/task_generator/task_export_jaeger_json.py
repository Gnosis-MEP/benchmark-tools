#!/usr/bin/env python

import json
import os

import requests

from benchmark_tools.task_generator.base import BaseTask


class JaegerExporter(BaseTask):
    JAEGER_TRACES_URL_FORMAT = (
        'api/traces?'
        'limit=700000000&lookback=6h&maxDuration&minDuration&'
        'operation={operation}&service={service}'
    )

    def __init__(self, *args, **kwargs):
        super(JaegerExporter, self).__init__(*args, **kwargs)
        self.jaeger_api_host = kwargs['jaeger_api_host']

    def get_traces(self, service, operation):
        end_point = self.JAEGER_TRACES_URL_FORMAT.format(
            operation=operation, service=service)
        traces_url = f'{self.jaeger_api_host}/{end_point}'
        req = requests.get(traces_url)
        traces = req.json()
        return traces

    def process_action(self, action_data):
        if not super(JaegerExporter, self).process_action(action_data):
            action = action_data.get('action', '')
            if action in ['exportTraces']:
                service = action_data['service']
                operation = action_data['operation']
                output_path = action_data['output_path']
                traces = self.get_traces(service, operation)
                output_file = self.save_traces_js_file_to_path(service, operation, traces, output_path)
                self.logger.info(f'Output file: {output_file}')
                return True
        return False

    def save_traces_js_file_to_path(self, service, operation, traces, output_path):
        traces_js_file = os.path.join(output_path, f'{service}-{operation}.json')
        with open(traces_js_file, 'w') as f:
            json.dump(traces, f)
        return traces_js_file


def run(actions, jaeger_api_host, logging_level):
    task = JaegerExporter(
        actions=actions,
        jaeger_api_host=jaeger_api_host,
        logging_level=logging_level
    )
    task.execute_actions()


if __name__ == '__main__':
    # output_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    # kwargs = {
    #     "jaeger_api_host": "http://localhost:16686",
    #     "logging_level": "DEBUG",
    #     "actions": [
    #         {
    #             'action': 'exportTraces',
    #             'service': 'PreProcessing',
    #             'operation': 'publish_next_event',
    #             'output_path': output_path,
    #         }
    #     ]
    # }


    kwargs = {
        "jaeger_api_host": "http://172.17.0.1:16686",
        "logging_level": "DEBUG",
        "actions": [
            {
                "action": "exportTraces",
                "service": "AdaptivePublisher",
                "operation": "process_next_frame",
                "output_path": "/service/outputs"
            }
        ]
    }

    print(run(**kwargs))

# if __name__ == '__main__':
#     pass
#     # from benchmark_tools.conf import (
#     #     LOGGING_LEVEL,
#     #     REDIS_ADDRESS,
#     #     REDIS_PORT,
#     # )
#     # import sys
#     # json_path = sys.argv[1]
#     # with open(json_path, 'r') as f:
#     #     actions = json.load(f)
#     run(
#         actions=actions,
#         redis_address=REDIS_ADDRESS,
#         redis_port=REDIS_PORT,
#         input_cmd_stream_key=QUERY_MANAGER_CMD_KEY,
#         logging_level=LOGGING_LEVEL
#     )
