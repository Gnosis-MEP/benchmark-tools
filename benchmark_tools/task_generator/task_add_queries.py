#!/usr/bin/env python
import time
import json
import uuid
from event_service_utils.streams.redis import RedisStreamFactory

from benchmark_tools.task_generator.base import BaseTask

from benchmark_tools.conf import (
    REDIS_ADDRESS,
    REDIS_PORT,
    QUERY_MANAGER_CMD_KEY,
)


def new_action_msg(action, event_data):
    event_data['action'] = action
    event_data.update({'id': str(uuid.uuid4())})
    return {'event': json.dumps(event_data)}


class TaskAddQueries(BaseTask):

    def __init__(self, *args, **kwargs):
        super(TaskAddQueries, self).__init__(*args, **kwargs)
        self.input_cmd_stream_key = kwargs['input_cmd_stream_key']
        self.stream_factory = kwargs['stream_factory']
        self.input_cmd_stream = self.stream_factory.create(self.input_cmd_stream_key, stype='streamOnly')

    def process_action(self, action_data):
        if not super(TaskAddQueries, self).process_action(action_data):
            action = action_data.get('action', '')
            if action in ['addQuery', 'delQuery']:
                formated_action = self.map_query_info_to_expected_format(action_data)
                new_msg = new_action_msg('action', formated_action)
                self.input_cmd_stream.write_events(new_msg)
                return True
        return False

    def map_query_info_to_expected_format(self, query_data):
        event_data = {
            'query': query_data['query'],
            'subscriber_id': query_data['subscriber_id'],
            'query_num': query_data['query_id'],
        }
        return event_data


def run(*args, **kwargs):
    if not kwargs:
        kwargs = {}
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
    kwargs.update(input_cmd_stream_key=QUERY_MANAGER_CMD_KEY, stream_factory=stream_factory)
    task = TaskAddQueries(*args, **kwargs)


if __name__ == '__main__':
    run()
