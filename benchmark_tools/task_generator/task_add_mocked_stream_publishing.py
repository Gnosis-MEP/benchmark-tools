#!/usr/bin/env python
import datetime
import json
import time
import threading
import uuid

from event_service_utils.streams.redis import RedisStreamFactory

from benchmark_tools.task_generator.base import BaseTask


class TaskAddBackgroundMockedStream(BaseTask):

    def __init__(self, *args, **kwargs):
        super(TaskAddBackgroundMockedStream, self).__init__(*args, **kwargs)
        self.stream_factory = kwargs['stream_factory']
        self.threads = []

    def should_keep_publishing(self, total_events, total_time, max_events, max_time):
        if max_events is not None:
            if total_events >= max_events:
                return False
        if max_time is not None:
            if total_time >= max_time:
                return False
        return True

    def create_event_from_template(self, proc_random_id, event_index, event_template):
        event_data = event_template.copy()
        event_data.update({
            'id': self.event_index_and_random_event_id(proc_random_id, event_index),
        })
        return event_data

    def default_event_serializer(self, event_data):
        event_msg = {'event': json.dumps(event_data)}
        return event_msg

    def event_index_and_random_event_id(self, proc_random_id, event_index):
        return f'{proc_random_id}:{event_index}'

    def process_random_id(self):
        return str(uuid.uuid4())

    def ensure_fps(self, expected_event_pub_time, last_event_ts):
        current_ts = datetime.datetime.now().timestamp()
        time_since_last_event = current_ts - last_event_ts
        if time_since_last_event < expected_event_pub_time:
            missing_event_interval = expected_event_pub_time - time_since_last_event
            time.sleep(missing_event_interval * 0.85)

    def publish_events(self, pub_id, stream_key, fps, event_template, max_events=None, max_time=None):
        proc_random_id = pub_id
        stream = self.stream_factory.create(
            stream_key, stype='streamOnly'
        )
        init_time = datetime.datetime.now()
        total_time = 0
        total_events = 0
        event_index = 0
        last_event_ts = init_time.timestamp()
        expected_event_pub_time = 1 / fps
        while self.should_keep_publishing(total_events, total_time, max_events, max_time):
            self.ensure_fps(expected_event_pub_time, last_event_ts)

            event_data = self.create_event_from_template(proc_random_id, event_index, event_template)
            stream.write_events(self.default_event_serializer(event_data))
            self.logger.debug(f'Published new event: {event_data}')
            last_event_ts = datetime.datetime.now().timestamp()
            event_index += 1
            total_events += 1
            total_time = datetime.datetime.now().timestamp() - init_time.timestamp()
        self.logger.info(
            f'Finished publishing on stream "{stream_key}". (process random id: "{proc_random_id}"). '
            f'Total events published: {total_events}. Total Time: {total_time}'
            f'Actual FPS: {total_events/total_time}'
        )

    def background_publish_events(self, pub_id, stream_key, fps, event_template, max_events, max_time):
        pub_thread = threading.Thread(
            target=self.publish_events,
            args=(pub_id, stream_key, fps, event_template),
            kwargs={'max_events': max_events, 'max_time': max_time},
            daemon=True
        )
        pub_thread.start()
        self.threads.append(pub_thread)
        return

    def process_action(self, action_data):
        if not super(TaskAddBackgroundMockedStream, self).process_action(action_data):
            action = action_data.get('action', '')
            if action == 'publishToStream':
                stream_key = action_data['stream_key']
                fps = action_data['fps']
                event_template = action_data['event_template']
                max_events = action_data.get('max_events')
                max_time = action_data.get('max_time')
                pub_id = action_data.get('pub_id', self.process_random_id())
                if max_time is None and max_events is None:
                    raise Exception('max_events and max_time are undefined. At least one should be defined.')
                self.logger.info((
                    f'"{pub_id}" Publishing "{event_template}" events into'
                    f' {stream_key} stream at {fps} FPS. With max_events={max_events} and max_time={max_time}'
                ))
                self.background_publish_events(pub_id, stream_key, fps, event_template, max_events, max_time)
                return True
        return False


def run(actions, redis_address, redis_port, logging_level, max_stream_length):
    stream_factory = RedisStreamFactory(host=redis_address, port=redis_port, max_stream_length=max_stream_length)
    task = TaskAddBackgroundMockedStream(
        actions=actions,
        stream_factory=stream_factory,
        logging_level=logging_level
    )
    task.execute_actions()


if __name__ == '__main__':
    kwargs = {
        "redis_address": "localhost",
        "redis_port": "6379",
        "logging_level": "INFO",
        "max_stream_length": None,
        "actions": [
            {
                "action": "publishToStream",
                "stream_key": "some-stream-key",
                "fps": 120,
                "max_events": 340,
                # "max_time": 10,
                "event_template": {
                    "buffer_stream_key": "publisher-1-bufferstream-key",
                    "publisher_id": "publisher-id-1",
                    "source": "rtmp://172.17.0.1/live/mystream",
                    "image_url": "c8d025d3-8c3a-460c-a6f5-cabb7b179807",
                    "vekg": {},
                    "width": 640,
                    "height": 480,
                    "color_channels": "BGR",
                    "query_ids": [],
                }

            },
            {
                "action": "publishToStream",
                "stream_key": "some-stream-key2",
                "fps": 120,
                # "max_events": 340,
                "max_time": 4,
                "event_template": {
                    "buffer_stream_key": "publisher-1-bufferstream-key",
                    "publisher_id": "publisher-id-1",
                    "source": "rtmp://172.17.0.1/live/mystream",
                    "image_url": "c8d025d3-8c3a-460c-a6f5-cabb7b179807",
                    "vekg": {},
                    "width": 640,
                    "height": 480,
                    "color_channels": "BGR",
                    "query_ids": [],
                }

            }
        ]
    }
    run(**kwargs)
    import time
    time.sleep(70)
