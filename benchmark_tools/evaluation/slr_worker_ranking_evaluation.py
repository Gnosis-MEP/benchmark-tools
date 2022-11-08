import json

from event_service_utils.streams.redis import RedisStreamFactory

from benchmark_tools.evaluation.base import BaseEvaluation


class SLRWorkerRankingEvaluation(BaseEvaluation):

    def __init__(self, *args, **kwargs):
        super(SLRWorkerRankingEvaluation, self).__init__(*args, **kwargs)
        self.stream_factory = kwargs['stream_factory']
        self.stream_key = kwargs['stream_key']

        # self.event_count = kwargs['event_count']
        # self.expected_event = kwargs['expected_event']
        self.expected_ranking_index = kwargs['expected_ranking_index']

        # self.event_field_comparison = kwargs['event_field_comparison']
        # [{
        #     'path_ret': "list(event_data['slr_profiles'].values())[0]"
        # }]
        # self.comparison_eval_by_field_paths = kwargs['event_field_comparison_paths']
        self.events_compared = []


    def read_and_process_all_stream_by_key(self, stream_key):
        stream = self.stream_factory.create(
            stream_key, stype='streamOnly'
        )
        event_list = stream.single_io_stream.read(block=1)
        for event_tuple in event_list:
            event_id, json_msg = event_tuple
            try:
                self.event_handler(json_msg)
            except Exception as e:
                self.logger.error(f'Error processing {json_msg}:')
                self.logger.exception(e)


    def has_contradiction_on_ranking(self, ranking_index, best_only=False):
        comp_ranking_index = ranking_index
        baseline_ranking = self.expected_ranking_index
        if best_only:
            comp_ranking_index = comp_ranking_index[:1]
            baseline_ranking = baseline_ranking[:1]
        if len(comp_ranking_index) != len(baseline_ranking):
            return True

        equivalents = [comp_ranking_index[i] == baseline_ranking[i] for i in range(len(baseline_ranking))]
        return not all(equivalents)

    def compare_event(self, event_data):
        slr_profile = list(event_data['slr_profiles'].values())[0]
        ranking_index = slr_profile['ranking_index']
        comparison_data = {
            'comp_ranking_index': ranking_index,
            'has_contradiction_on_best': self.has_contradiction_on_ranking(ranking_index, best_only=True),
            'has_contradiction_on_any': self.has_contradiction_on_ranking(ranking_index, best_only=False),
        }
        return comparison_data

    def event_handler(self, json_msg):
        is_binary = b'event' in json_msg
        event_key = b'event' if is_binary else 'event'
        default_content = b'{}' if is_binary else '{}'
        event_json = json_msg.get(event_key, default_content)
        if is_binary:
            event_json = event_json.decode('utf-8')

        event_data = json.loads(event_json)
        self.events_compared.append(self.compare_event(event_data))

    def calculate_metrics(self):
        c_rate_best = 0
        c_rate_any = 0
        total = len(self.events_compared)
        for comp_result in self.events_compared:
            if comp_result['has_contradiction_on_best']:
                c_rate_best += 1

            if comp_result['has_contradiction_on_any']:
                c_rate_any += 1

        if total != 0:
            c_rate_best = c_rate_best / total
            c_rate_any = c_rate_any / total
        results = {
            'total_events': total,
            'c_rate_best': c_rate_best,
            'c_rate_any': c_rate_any,
        }
        return results

    def run(self):
        self.logger.debug(f'Evaluation for SLR Worker Ranking index against the ranking: {self.expected_ranking_index}')
        self.read_and_process_all_stream_by_key(self.stream_key)
        results = self.calculate_metrics()
        return self.verify_thresholds(results)


def run(redis_address, redis_port, stream_key, expected_ranking_index, threshold_functions, logging_level):
    stream_factory = RedisStreamFactory(host=redis_address, port=redis_port, block=1)
    evaluation = SLRWorkerRankingEvaluation(
        stream_factory=stream_factory,
        stream_key=stream_key,
        expected_ranking_index=expected_ranking_index,
        threshold_functions=threshold_functions,
        logging_level=logging_level
    )
    return evaluation.run()


if __name__ == '__main__':
    kwargs = {
        "redis_address": "localhost",
        "redis_port": "6379",
        "stream_key": "ServiceSLRProfilesRanked",
        "expected_ranking_index": [0, 1, 5, 11, 6, 7, 2, 3, 4, 10, 8, 9],
        "threshold_functions": {
            "has_contradiction_on_best": "lambda b: b == False",
            "has_contradiction_on_any": "lambda b: b == False",
        },
        "logging_level": "DEBUG"
    }
    print(run(**kwargs))
