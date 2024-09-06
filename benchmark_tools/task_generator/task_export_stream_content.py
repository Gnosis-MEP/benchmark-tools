#!/usr/bin/env python
import hashlib
import os
from event_service_utils.streams.redis import RedisStreamFactory

from benchmark_tools.task_generator.base import BaseTask


class TaskExportStreamContent(BaseTask):

    def __init__(self, *args, **kwargs):
        super(TaskExportStreamContent, self).__init__(*args, **kwargs)
        self.stream_factory = kwargs['stream_factory']
        self.output_path = kwargs['output_path']

    def sub_event_handler(self, json_msg, output_file):
        event_key = b'event' if b'event' in json_msg else 'event'
        event_json = json_msg.get(event_key, '{}').decode('utf-8')
        with open(output_file, 'a') as f:
            f.write(event_json + '\n')

    def stream_export(self, stream_key, output_file):
        stream = self.stream_factory.create(
            stream_key, stype='streamOnly'
        )
        last_entry_id = stream.last_msg_id
        stream.last_msg_id = None
        if os.path.exists(output_file):
            os.remove(output_file)
        read_all = False
        self.logger.info(f'Last entry id: {last_entry_id}')
        while not read_all:
            event_list = stream.read_events(count=1)
            for event_tuple in event_list:
                event_id, json_msg = event_tuple
                try:
                    self.sub_event_handler(json_msg, output_file)
                except Exception as e:
                    self.logger.error(f'Error processing {json_msg}:')
                    self.logger.exception(e)
                if event_id == last_entry_id:
                    self.logger.info(f'Read event from Last entry id: {event_id}')
                    read_all = True


    def process_action(self, action_data):
        if not super(TaskExportStreamContent, self).process_action(action_data):
            action = action_data.get('action', '')
            if action == 'exportQueryStream':
                subscriber_id = action_data['subscriber_id']
                query_name = action_data['query_name']
                stream_key = action_data.get('stream_key')
                if stream_key is None:
                    stream_key = hashlib.md5(f"{subscriber_id}_{query_name}".encode('utf-8')).hexdigest()
                output_file = os.path.join(self.output_path, f'export_{stream_key}.jl')
                self.logger.info(
                    f'Exporting stream content for {subscriber_id}-{query_name} ("{stream_key}" stream): outputing to Json lines file: {output_file}'
                )
                self.stream_export(stream_key, output_file)
                return True
        return False


def run(actions, redis_address, redis_port, output_path, logging_level):
    stream_factory = RedisStreamFactory(host=redis_address, port=redis_port)
    task = TaskExportStreamContent(
        actions=actions,
        stream_factory=stream_factory,
        output_path=output_path,
        logging_level=logging_level
    )
    task.execute_actions()


if __name__ == '__main__':
    import hashlib
    import sys
    query_name = sys.argv[1]
    subscriber_id = 'sub_id'


    kwargs = {
        "redis_address": "172.17.0.1",
        "redis_port": "6379",
        "output_path": "/service/outputs",
        "logging_level": "DEBUG",
        "actions": [
            {
                "action": "exportQueryStream",
                "subscriber_id": subscriber_id,
                "query_name": query_name
            }
        ]
    }
    run(**kwargs)
