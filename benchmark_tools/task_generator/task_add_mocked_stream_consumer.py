#!/usr/bin/env python
import datetime
import json
import threading
import time

from opentracing.ext import tags
from opentracing.propagation import Format
from event_service_utils.tracing.jaeger import init_tracer
from event_service_utils.streams.redis import RedisStreamFactory

from benchmark_tools.task_generator.base import BaseTask


EVENT_ID_TAG = 'event-id'

class TaskAddBackgroundMockedStreamConsumer(BaseTask):

    def __init__(self, *args, **kwargs):
        super(TaskAddBackgroundMockedStreamConsumer, self).__init__(*args, **kwargs)
        self.stream_factory = kwargs['stream_factory']
        self.tracer_configs = kwargs['tracer_configs']
        self.tracer = init_tracer('MockedStreamConsumer', **self.tracer_configs)
        self.threads = []

    def get_event_tracer_kwargs(self, event_data):
        tracer_kwargs = {}
        tracer_data = event_data.get('tracer', {})
        tracer_headers = tracer_data.get('headers', {})
        if tracer_headers:
            span_ctx = self.tracer.extract(Format.HTTP_HEADERS, tracer_headers)
            tracer_kwargs.update({
                'child_of': span_ctx
            })
        else:
            self.logger.info(f'No tracer id found on event id: {event_data["id"]}')
            self.logger.info(
                (
                    'Will start a new tracer id.'
                    'If this event came from another service '
                    'this will likelly cause confusion in the current event tracing')
            )
        return tracer_kwargs

    def event_trace_for_method_with_event_data(
            self, method, method_args, method_kwargs, get_event_tracer=False, tracer_tags=None):
        span_name = 'consume_stream'
        if tracer_tags is None:
            tracer_tags = {}

        tracer_kwargs = {}
        if get_event_tracer:
            event_data = method_kwargs['event_data']
            tracer_kwargs = self.get_event_tracer_kwargs(event_data)
        with self.tracer.start_active_span(span_name, **tracer_kwargs) as scope:
            for tag, value in tracer_tags.items():
                scope.span.set_tag(tag, value)
            method(*method_args, **method_kwargs)

    def process_data_event(self, event_data, json_msg, processing_time, time_before_deserialization):
        current_processing_time = datetime.datetime.now().timestamp() - time_before_deserialization
        missing_processing_time = processing_time - current_processing_time
        if missing_processing_time > 0:
            time.sleep(missing_processing_time)
        # this is a fake method

    def process_data_event_wrapper(self, event_data, json_msg, processing_time, time_before_deserialization):
        self.event_trace_for_method_with_event_data(
            method=self.process_data_event,
            method_args=(),
            method_kwargs={
                'event_data': event_data,
                'json_msg': json_msg,
                'processing_time': processing_time,
                'time_before_deserialization': time_before_deserialization,
            },
            get_event_tracer=True,
            tracer_tags={
                tags.SPAN_KIND: tags.SPAN_KIND_CONSUMER,
                EVENT_ID_TAG: event_data['id'],
            }
        )

    def default_event_deserializer(self, json_msg):
        event_key = b'event' if b'event' in json_msg else 'event'
        event_json = json_msg.get(event_key, '{}')
        event_data = json.loads(event_json)
        return event_data

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
                time_before_deserialization = datetime.datetime.now().timestamp()
                event_data = self.default_event_deserializer(json_msg)
                self.process_data_event_wrapper(event_data, json_msg, processing_time, time_before_deserialization)
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


def run(actions, redis_address, redis_port, tracer_configs, logging_level):
    stream_factory = RedisStreamFactory(host=redis_address, port=redis_port)
    task = TaskAddBackgroundMockedStreamConsumer(
        actions=actions,
        stream_factory=stream_factory,
        tracer_configs=tracer_configs,
        logging_level=logging_level
    )
    task.execute_actions()


if __name__ == '__main__':
    kwargs = {
        "redis_address": "localhost",
        "redis_port": "6379",
        "tracer_configs": {
            "reporting_host": "localhost",
            "reporting_port": "6831",
        },
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
