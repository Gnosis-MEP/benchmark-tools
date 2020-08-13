#!/usr/bin/env python

import datetime
import os
import time

import requests

from benchmark_tools.task_generator.base import BaseTask


class WaitEventTraceTimeuot(BaseTask):
    JAEGER_TRACES_URL_FORMAT = (
        'api/traces?'
        'limit=1&start={start_mm_timestamp}&maxDuration&minDuration&'
        'operation={operation}&service={service}'
    )

    def __init__(self, *args, **kwargs):
        super(WaitEventTraceTimeuot, self).__init__(*args, **kwargs)
        self.jaeger_api_host = kwargs['jaeger_api_host']

    def get_traces_last_seconds(self, service, operation, lookback_seconds):
        last_seconds_ts = datetime.datetime.now().timestamp() - lookback_seconds
        last_seconds_mm_ts = int(last_seconds_ts * 10**6)
        end_point = self.JAEGER_TRACES_URL_FORMAT.format(
            start_mm_timestamp=last_seconds_mm_ts, operation=operation, service=service)
        traces_url = f'{self.jaeger_api_host}/{end_point}'
        req = requests.get(traces_url)
        traces = req.json()
        return traces

    def wait_timeout_event_trace(self, action_data):
        service = action_data['service']
        operation = action_data['operation']
        wait_retry_time = float(action_data['wait_retry_time'])
        event_timeout = float(action_data['event_timeout'])
        forced_stop_timeout_limit = float(action_data['forced_stop_timeout_limit'])

        start_ts = datetime.datetime.now().timestamp()
        keep_waiting = True
        last_trace = None
        last_ts = start_ts
        while keep_waiting:
            self.logger.info(f'Waiting {wait_retry_time} before retry...')
            time.sleep(wait_retry_time)
            traces_resp = self.get_traces_last_seconds(service, operation, wait_retry_time)
            traces = traces_resp['data']
            last_trace = traces[0] if len(traces_resp['data']) != 0 else last_trace
            event_is_timeout = False
            task_is_forced_timeout = False
            ts_now = datetime.datetime.now().timestamp()

            if last_trace is not None:
                last_start_time = last_trace['spans'][-1]['startTime']
                last_duration = last_trace['spans'][-1]['duration']
                last_mm_ts = last_start_time + last_duration
                last_ts = last_mm_ts / 10**6

            current_timeout = ts_now - last_ts
            event_is_timeout = current_timeout > event_timeout
            task_is_forced_timeout = (ts_now - start_ts) > forced_stop_timeout_limit
            if event_is_timeout or task_is_forced_timeout:
                keep_waiting = False
                self.logger.info(
                    f'Stop wainting..')
            self.logger.info(
                f'Event timeout: {event_is_timeout}; Forced Timeout {task_is_forced_timeout}; Current Timeout {current_timeout}; Forced Timeout limit {forced_stop_timeout_limit}'
            )

        end_ts = datetime.datetime.now().timestamp()
        total_wait = end_ts - start_ts
        self.logger.info(f'Total wait in seconds: {total_wait}')

    def process_action(self, action_data):
        if not super(WaitEventTraceTimeuot, self).process_action(action_data):
            action = action_data.get('action', '')
            if action == 'wait_timeout_event_trace':
                self.wait_timeout_event_trace(action_data)
                return True
        return False


def run(actions, jaeger_api_host, logging_level):
    task = WaitEventTraceTimeuot(
        actions=actions,
        jaeger_api_host=jaeger_api_host,
        logging_level=logging_level
    )
    task.execute_actions()


if __name__ == '__main__':
    output_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    kwargs = {
        "jaeger_api_host": "http://localhost:16686",
        "logging_level": "DEBUG",
        "actions": [
            {
                'action': 'wait_timeout_event_trace',
                'service': 'PreProcessing',
                'operation': 'publish_next_event',
                'wait_retry_time': 10,
                'event_timeout': 30,
                'forced_stop_timeout_limit': 60
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
