#!/usr/bin/env python
import json
import uuid
from event_service_utils.streams.redis import RedisStreamFactory

from benchmark_tools.task_generator.base import BaseTask


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
                new_msg = new_action_msg(action, formated_action)
                self.input_cmd_stream.write_events(new_msg)
                return True
        return False

    def map_query_info_to_expected_format(self, query_data):
        if 'add' in query_data['action']:
            event_data = {
                'query': query_data['query'],
                'subscriber_id': query_data['subscriber_id'],
            }
        else:
            event_data = {
                'subscriber_id': query_data['subscriber_id'],
                'query_name': query_data['query_id'],
            }
        return event_data


def run(actions, redis_address, redis_port, input_cmd_stream_key, logging_level):
    stream_factory = RedisStreamFactory(host=redis_address, port=redis_port)
    task = TaskAddQueries(
        actions=actions,
        stream_factory=stream_factory,
        input_cmd_stream_key=input_cmd_stream_key,
        logging_level=logging_level
    )
    task.execute_actions()


if __name__ == '__main__':
    pass
    # from benchmark_tools.conf import (
    #     LOGGING_LEVEL,
    #     REDIS_ADDRESS,
    #     REDIS_PORT,
    # )
    # import sys
    # json_path = sys.argv[1]
    # with open(json_path, 'r') as f:
    #     actions = json.load(f)
    # run(
    #     actions=actions,
    #     redis_address=REDIS_ADDRESS,
    #     redis_port=REDIS_PORT,
    #     input_cmd_stream_key=QUERY_MANAGER_CMD_KEY,
    #     logging_level=LOGGING_LEVEL
    # )
