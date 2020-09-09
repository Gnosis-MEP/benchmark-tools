#!/usr/bin/env python

import datetime
import os
import time

import redis

from event_service_utils.streams.redis import RedisStreamFactory

from benchmark_tools.task_generator.base import BaseTask


class WaitRedisStreamSizeTimeout(BaseTask):

    def __init__(self, *args, **kwargs):
        super(WaitRedisStreamSizeTimeout, self).__init__(*args, **kwargs)
        self.stream_factory = kwargs['stream_factory']

    def get_total_pending_cg_stream(self, stream_key):
        redis_db = self.stream_factory.redis_db
        bad_return_value = redis_db.xlen(stream_key)
        cg_name = f'cg-{stream_key}'
        try:
            cgroups_info = redis_db.xinfo_groups(stream_key)
            if not cgroups_info:
                return bad_return_value

            cgroup = None
            for cg in cgroups_info:
                if cg['name'] == cg_name.encode('utf-8'):
                    cgroup = cg
                    break

            if not cgroup:
                return bad_return_value

            last_delivered_id = cgroup['last-delivered-id']

            if last_delivered_id is None:
                return bad_return_value

            not_consumed_stream_events_list = redis_db.xread({stream_key: last_delivered_id})
            if len(not_consumed_stream_events_list) == 0:
                return 0

            return len(not_consumed_stream_events_list[0][1])
        except redis.ResponseError:
            total_pending = bad_return_value

        return total_pending

    def wait_stream_size(self, action_data):
        stream_key = action_data['stream_key']
        stream_size = int(action_data['stream_size'])
        wait_retry_time = float(action_data['wait_retry_time'])
        forced_stop_timeout_limit = float(action_data['forced_stop_timeout_limit'])

        start_ts = datetime.datetime.now().timestamp()
        keep_waiting = True
        while keep_waiting:
            self.logger.info(f'Waiting {wait_retry_time} before retry...')
            time.sleep(wait_retry_time)

            current_stream_size = self.get_total_pending_cg_stream(stream_key)
            stream_size_is_correct = current_stream_size == stream_size

            ts_now = datetime.datetime.now().timestamp()
            task_is_forced_timeout = (ts_now - start_ts) > forced_stop_timeout_limit
            if stream_size_is_correct or task_is_forced_timeout:
                keep_waiting = False
                self.logger.info(
                    f'Stop wainting..')
            self.logger.info(
                (
                    f'Stream/size: {stream_key}/{current_stream_size}; Stream size is correct: {stream_size_is_correct}; '
                    f'Forced Timeout {task_is_forced_timeout}; Forced Timeout limit {forced_stop_timeout_limit}'
                )
            )

        end_ts = datetime.datetime.now().timestamp()
        total_wait = end_ts - start_ts
        self.logger.info(f'Total wait in seconds: {total_wait}')

    def process_action(self, action_data):
        if not super(WaitRedisStreamSizeTimeout, self).process_action(action_data):
            action = action_data.get('action', '')
            if action == 'wait_stream_size':
                self.wait_stream_size(action_data)
                return True
        return False


def run(actions, redis_address, redis_port, logging_level):
    stream_factory = RedisStreamFactory(host=redis_address, port=redis_port)
    task = WaitRedisStreamSizeTimeout(
        actions=actions,
        stream_factory=stream_factory,
        logging_level=logging_level
    )
    task.execute_actions()


if __name__ == '__main__':

    output_path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    kwargs = {
        "redis_address": "localhost",
        "redis_port": "6379",
        "logging_level": "DEBUG",
        "actions": [
            {
                "action": "wait_stream_size",
                "stream_key": "object-detection-ssd-gpu-data",
                "stream_size": "0",
                'wait_retry_time': 5,
                'forced_stop_timeout_limit': 10
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
