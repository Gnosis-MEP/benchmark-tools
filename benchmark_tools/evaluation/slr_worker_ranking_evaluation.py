import json
import os

from event_service_utils.streams.redis import RedisStreamFactory

from benchmark_tools.evaluation.base import BaseEvaluation


class SLRWorkerRankingEvaluation(BaseEvaluation):

    def __init__(self, *args, **kwargs):
        super(SLRWorkerRankingEvaluation, self).__init__(*args, **kwargs)
        self.stream_factory = kwargs['stream_factory']
        self.stream_key = kwargs['stream_key']
        self.output_path = kwargs['output_path']
        self.output_file = os.path.join(self.output_path, f'stream_{self.stream_key}.jl')

        # self.event_count = kwargs['event_count']
        # self.expected_event = kwargs['expected_event']
        self.expected_ranking_index = kwargs['expected_ranking_index']
        self.expected_ranking_scores = kwargs['expected_ranking_scores']
        self.similarity_rounding_places = kwargs['similarity_rounding_places']
        self.expected_similar_indexes_pairs = self.create_similar_indexes_pairs_from_scores(
            self.expected_ranking_scores, decimal_places=self.similarity_rounding_places)

        # self.event_field_comparison = kwargs['event_field_comparison']
        # [{
        #     'path_ret': "list(event_data['slr_profiles'].values())[0]"
        # }]
        # self.comparison_eval_by_field_paths = kwargs['event_field_comparison_paths']
        self.events_compared = []
        self.profiles_compared = {}

    def read_and_process_all_stream_by_key(self, stream_key, output_file):
        stream = self.stream_factory.create(
            stream_key, stype='streamOnly'
        )

        if os.path.exists(output_file):
            os.remove(output_file)

        event_list = stream.single_io_stream.read(block=1)
        for event_tuple in event_list:
            event_id, json_msg = event_tuple
            try:
                self.event_handler(json_msg, output_file)
            except Exception as e:
                self.logger.error(f'Error processing {json_msg}:')
                self.logger.exception(e)

    def has_contradiction_on_ranking(self, ranking_index, best_only=False, similar_index_pairs=None):
        comp_ranking_index = ranking_index
        baseline_ranking = self.expected_ranking_index
        if best_only:
            comp_ranking_index = comp_ranking_index[:1]
            baseline_ranking = baseline_ranking[:1]
        if len(comp_ranking_index) != len(baseline_ranking):
            return True

        equivalents = []
        for i in range(len(baseline_ranking)):
            is_equivalent = comp_ranking_index[i] == baseline_ranking[i]

            # (0, 1), (0, 2), (1, 2)
            # comp_ranking_index = [1, 0, 2, 3, 4]
            # baseline_ranking = [0, 1, 2, 3, 4]
            # alt: [0, 1, 2, 3, 4], [1, 0, 2, 3, 4], [2, 0, 1, 3, 4], [2, 1, 0, 3, 4], [0, 2, 1, 3, 4], [1, 2, 0, 3, 4]
            if similar_index_pairs is not None and not is_equivalent:
                a_b_similar_pair = (comp_ranking_index[i], baseline_ranking[i])
                b_a_similar_pair = (baseline_ranking[i], comp_ranking_index[i])
                has_similar_index = a_b_similar_pair in similar_index_pairs or b_a_similar_pair in similar_index_pairs
                if has_similar_index:
                    is_equivalent = True
            equivalents.append(is_equivalent)

        return not all(equivalents)

    def create_similar_indexes_pairs_from_scores(self, ranking_scores, decimal_places=3):
        rounded_scores = [round(s, decimal_places) for s in ranking_scores]

        similar_indexes_pairs = set()
        for i in range(len(rounded_scores)):
            score = rounded_scores[i]
            for j in range(len(rounded_scores)):
                if i <= j:
                    continue
                other_score = rounded_scores[j]
                if score == other_score:
                    similar_indexes_pairs.add((i, j))

        return similar_indexes_pairs


    def create_valid_similar_indexes_from_ranking_scores(self, ranking_scores):
        similar_indexes_pairs = self.create_similar_indexes_pairs_from_scores(
            ranking_scores, decimal_places=self.similarity_rounding_places)

        valid_pairs = []
        for similar_index_pair in similar_indexes_pairs:
            reversed_similar_index_pair = tuple(reversed(similar_index_pair))
            is_valid = similar_index_pair in self.expected_similar_indexes_pairs or reversed_similar_index_pair in self.expected_similar_indexes_pairs
            if is_valid:
                valid_pairs.append(similar_index_pair)
        return valid_pairs

    def compare_profile(self, slr_profile_id, slr_profile):
        if slr_profile_id in self.profiles_compared.keys():
            return None
        ranking_index = slr_profile['ranking_index']
        ranking_scores = slr_profile['ranking_scores']
        has_contradiction_on_any = self.has_contradiction_on_ranking(ranking_index, best_only=False)
        has_contradiction_on_best =  self.has_contradiction_on_ranking(ranking_index, best_only=True)


        similar_has_contradiction_on_any = has_contradiction_on_any
        similar_has_contradiction_on_best = has_contradiction_on_best

        if has_contradiction_on_any:
            similar_index_pairs = self.create_valid_similar_indexes_from_ranking_scores(ranking_scores)
            similar_has_contradiction_on_any = self.has_contradiction_on_ranking(
                ranking_index, best_only=False, similar_index_pairs=similar_index_pairs)
            similar_has_contradiction_on_best = self.has_contradiction_on_ranking(
                ranking_index, best_only=True, similar_index_pairs=similar_index_pairs)


        comparison_data = {
            'comp_ranking_index': ranking_index,
            'comp_ranking_scores': ranking_scores,
            'exact':{
                'has_contradiction_on_best': has_contradiction_on_best,
                'has_contradiction_on_any': has_contradiction_on_any,
            },
            'similar':{
                'has_contradiction_on_best': similar_has_contradiction_on_best,
                'has_contradiction_on_any': similar_has_contradiction_on_any,
            }
        }

        return comparison_data

    def compare_event(self, event_data):
        for slr_profile_id, slr_profile in event_data['slr_profiles'].items():
            comparison_data = self.compare_profile(slr_profile_id, slr_profile)
            if comparison_data is not None:
                self.profiles_compared[slr_profile_id] = comparison_data
        return event_data

    def export_event_to_jl_file(self, event_json, output_file):
        with open(output_file, 'a') as f:
            f.write(event_json + '\n')

    def event_handler(self, json_msg, output_file):
        is_binary = b'event' in json_msg
        event_key = b'event' if is_binary else 'event'
        default_content = b'{}' if is_binary else '{}'
        event_json = json_msg.get(event_key, default_content)
        if is_binary:
            event_json = event_json.decode('utf-8')

        event_data = json.loads(event_json)
        self.events_compared.append(self.compare_event(event_data))
        self.export_event_to_jl_file(event_json, output_file)

    def get_contradiction_rates(self, is_exact=True):
        c_rate_best = 0
        c_rate_any = 0
        total = len(self.profiles_compared.keys())
        avg_rankings_scores = []
        for comp_result_dict in self.profiles_compared.values():
            comp_result = comp_result_dict['exact'] if is_exact else comp_result_dict['similar']
            if comp_result['has_contradiction_on_best']:
                c_rate_best += 1

            if comp_result['has_contradiction_on_any']:
                c_rate_any += 1
            if len(avg_rankings_scores) == 0:
                avg_rankings_scores = comp_result_dict['comp_ranking_scores']
            else:
                for alt_index, ranking_score in enumerate(comp_result_dict['comp_ranking_scores']):
                    avg_rankings_scores[alt_index] += ranking_score


        if total != 0:
            c_rate_best = c_rate_best / total
            c_rate_any = c_rate_any / total

            for alt_index in range(len(avg_rankings_scores)):
                avg_rankings_scores[alt_index] = avg_rankings_scores[alt_index] / total

        return c_rate_best, c_rate_any, avg_rankings_scores.copy()


    def calculate_metrics(self):
        total_events = len(self.events_compared)
        total_profiles = len(self.profiles_compared)
        exact_c_rate_best, exact_c_rate_any, avg_rankings_scores  = self.get_contradiction_rates(is_exact=True)
        similar_c_rate_best,similar_c_rate_any, _  = self.get_contradiction_rates(is_exact=False)
        results = {
            'total_events': total_events,
            'total_profiles': total_profiles,
            'exact_c_rate_best': exact_c_rate_best,
            'exact_c_rate_any': exact_c_rate_any,
            'similar_c_rate_best': similar_c_rate_best,
            'similar_c_rate_any': similar_c_rate_any,
            'avg_rankings_scores': avg_rankings_scores,
        }
        return results

    def run(self):
        self.logger.debug(f'Evaluation for SLR Worker Ranking index against the ranking: {self.expected_ranking_index}')
        self.read_and_process_all_stream_by_key(self.stream_key, self.output_file)
        results = self.calculate_metrics()
        return self.verify_thresholds(results)


def run(redis_address, redis_port, stream_key, expected_ranking_index, expected_ranking_scores, similarity_rounding_places, output_path, threshold_functions, logging_level):
    stream_factory = RedisStreamFactory(host=redis_address, port=redis_port, block=1)
    evaluation = SLRWorkerRankingEvaluation(
        stream_factory=stream_factory,
        stream_key=stream_key,
        expected_ranking_index=expected_ranking_index,
        expected_ranking_scores=expected_ranking_scores,
        similarity_rounding_places=similarity_rounding_places,
        output_path=output_path,
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
