#!/usr/bin/env python
import hashlib
import os
import threading
from event_service_utils.streams.redis import RedisStreamFactory

from benchmark_tools.task_generator.base import BaseTask


class TaskAddBackgroundSubscriberEventsExporter(BaseTask):

    def __init__(self, *args, **kwargs):
        super(TaskAddBackgroundSubscriberEventsExporter, self).__init__(*args, **kwargs)
        self.stream_factory = kwargs['stream_factory']
        self.output_path = kwargs['output_path']
        self.threads = []

    def sub_event_handler(self, json_msg, output_file):
        event_key = b'event' if b'event' in json_msg else 'event'
        event_json = json_msg.get(event_key, '{}').decode('utf-8')
        with open(output_file, 'a') as f:
            f.write(event_json + '\n')

    def subscription_export(self, subscriber_id, stream_key, output_file):
        subscriber_query_output_stream = self.stream_factory.create(
            stream_key, stype='streamAndConsumer'
        )
        if os.path.exists(output_file):
            os.remove(output_file)
        while True:
            event_list = subscriber_query_output_stream.read_events(count=10)
            for event_tuple in event_list:
                event_id, json_msg = event_tuple
                self.logger.debug(f'new event {json_msg}')
                try:
                    self.sub_event_handler(json_msg, output_file)
                except Exception as e:
                    self.logger.error(f'Error processing {json_msg}:')
                    self.logger.exception(e)

    def background_subscription_export(self, subscriber_id, stream_key, output_file):
        subscriber_thread = threading.Thread(
            target=self.subscription_export,
            args=(subscriber_id, stream_key, output_file,),
            daemon=True
        )
        subscriber_thread.start()
        self.threads.append(subscriber_thread)
        return

    def process_action(self, action_data):
        if not super(TaskAddBackgroundSubscriberEventsExporter, self).process_action(action_data):
            action = action_data.get('action', '')
            if action == 'exportSubscribeToQuery':
                subscriber_id = action_data['subscriber_id']
                stream_key = action_data['stream_key']
                # if stream_key is None:
                #     stream_key = hashlib.md5(f"{subscriber_id}_{query_num}".encode('utf-8')).hexdigest()
                output_file = os.path.join(self.output_path, f'subscription_{stream_key}.jl')
                self.logger.info(
                    f'Subscribing for {subscriber_id}: {stream_key} at stream "{stream_key}" and outputing to Json lines file: {output_file}'
                )
                self.background_subscription_export(subscriber_id, stream_key, output_file)
                return True
        return False


def run(actions, redis_address, redis_port, output_path, logging_level):
    stream_factory = RedisStreamFactory(host=redis_address, port=redis_port)
    task = TaskAddBackgroundSubscriberEventsExporter(
        actions=actions,
        stream_factory=stream_factory,
        output_path=output_path,
        logging_level=logging_level
    )
    task.execute_actions()


if __name__ == '__main__':
    kwargs = {
        "redis_address": "localhost",
        "redis_port": "6379",
        "output_path": "./outputs",
        "logging_level": "DEBUG",
        "actions": [
            {
                "action": "exportSubscribeToQuery",
                "subscriber_id": "3",
                "stream_key": "abc"
            }
        ]
    }
    run(**kwargs)
    import time
    time.sleep(70)
