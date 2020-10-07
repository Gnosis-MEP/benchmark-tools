#!/usr/bin/env python
import time
import datetime
import threading

from event_service_utils.streams.redis import RedisStreamFactory

from benchmark_tools.task_generator.base import BaseTask


class TaskAddBackgroundMockedStreamConsumer(BaseTask):

    def __init__(self, *args, **kwargs):
        super(TaskAddBackgroundMockedStreamConsumer, self).__init__(*args, **kwargs)
        self.stream_factory = kwargs['stream_factory']
        self.threads = []

    def consume_events(self, stream_key, processing_time, max_time):
        stream = self.stream_factory.create(
            stream_key
        )
        init_time = datetime.datetime.now()
        total_time = 0
        total_events = 0
        while total_time < max_time:
            event_list = stream.read_events(count=1)
            for event_tuple in event_list:
                event_id, json_msg = event_tuple
                time.sleep(processing_time)
                total_events += 1
                self.logger.debug(f'Consumed new event: {json_msg}')

            total_time = datetime.datetime.now().timestamp() - init_time.timestamp()
        self.logger.info(
            f'Finished consuming on stream "{stream_key}".'
            f'Total events consummed: {total_events}. Total Time: {total_time}'
            f' Time for each event: expected={processing_time} real={total_time/total_events}.'
        )

    def background_consume_events(self, stream_key, processing_time, max_time):
        pub_thread = threading.Thread(
            target=self.consume_events,
            args=(stream_key, processing_time, max_time),
            daemon=True
        )
        pub_thread.start()
        self.threads.append(pub_thread)
        return

    def process_action(self, action_data):
        if not super(TaskAddBackgroundMockedStreamConsumer, self).process_action(action_data):
            action = action_data.get('action', '')
            if action == 'consumeStream':
                stream_key = action_data['stream_key']
                processing_time = action_data['processing_time']
                max_time = action_data['max_time']
                self.logger.info(
                    f'Consuming events from {stream_key} with a processing time of {processing_time} per event.'
                    f' With max_time={max_time}.'
                )
                self.background_consume_events(stream_key, processing_time, max_time)
                return True
        return False


def run(actions, redis_address, redis_port, logging_level):
    stream_factory = RedisStreamFactory(host=redis_address, port=redis_port)
    task = TaskAddBackgroundMockedStreamConsumer(
        actions=actions,
        stream_factory=stream_factory,
        logging_level=logging_level
    )
    task.execute_actions()


if __name__ == '__main__':
    kwargs = {
        "redis_address": "localhost",
        "redis_port": "6379",
        "logging_level": "DEBUG",
        "actions": [
            {
                "action": "consumeStream",
                "stream_key": "some-stream-key",
                "processing_time": 0.1,
                "max_time": 10
            },
            {
                "action": "consumeStream",
                "stream_key": "some-stream-key2",
                "processing_time": 0.5,
                "max_time": 10
            }
        ]
    }
    run(**kwargs)
    import time
    time.sleep(70)
