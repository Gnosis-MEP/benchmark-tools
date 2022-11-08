# Author : Indeep Singh
# Class : ThroughputTestCase
# Date : 07-July-2020
# Description : This python script is a unit test file to Throughput Evaluation.


import unittest
from unittest.mock import patch

from benchmark_tools.evaluation.slr_worker_ranking_evaluation import SLRWorkerRankingEvaluation


class SLRWorkerRankingTestCase(unittest.TestCase):

    def setUp(self):
        self.evaluation = SLRWorkerRankingEvaluation(
            logging_level='ERROR',
            threshold_functions={},
            stream_factory='stream_factory',
            # redis_address="localhost",
            # redis_port="6379",
            stream_key="ServiceSLRProfilesRanked",
            expected_ranking_index=[0, 1, 5, 11, 6, 7, 2, 3, 4, 10, 8, 9],
        )

    @patch('benchmark_tools.evaluation.slr_worker_ranking_evaluation.SLRWorkerRankingEvaluation.read_and_process_all_stream_by_key')
    @patch('benchmark_tools.evaluation.slr_worker_ranking_evaluation.SLRWorkerRankingEvaluation.calculate_metrics')
    @patch('benchmark_tools.evaluation.slr_worker_ranking_evaluation.SLRWorkerRankingEvaluation.verify_thresholds')
    def test_run_should_call_necessary_functions(self, mocked_threshold, mocked_calculate, mocked_read):
        self.evaluation.run()
        self.assertTrue(mocked_threshold.called)
        self.assertTrue(mocked_calculate.called)
        self.assertTrue(mocked_read.called)

    def test_calculate_metrics_should_return_correctly(self):

        mocked_comparison = {'some': 'comparison'}
        self.evaluation.events_compared = [
            {
                'comp_ranking_index': [0, 5, 1, 11, 6, 7, 2, 3, 4, 10, 8, 9],
                'has_contradiction_on_best': False,
                'has_contradiction_on_any': True,
            },
            {
                'comp_ranking_index': [1, 5, 0, 11, 6, 7, 2, 3, 4, 10, 8, 9],
                'has_contradiction_on_best': True,
                'has_contradiction_on_any': True,
            },
            {
                'comp_ranking_index': [0, 1, 5, 11, 6, 7, 2, 3, 4, 10, 8, 9],
                'has_contradiction_on_best': False,
                'has_contradiction_on_any': False,
            }
        ]
        ret = self.evaluation.calculate_metrics()

        expedted_res = {
            'c_rate_any': 0.666, 'c_rate_best': 0.333, 'total_events': 3
        }

        self.assertAlmostEqual(ret['c_rate_any'], expedted_res['c_rate_any'], places=2)
        self.assertAlmostEqual(ret['c_rate_best'], expedted_res['c_rate_best'], places=2)
        self.assertEqual(ret['total_events'], expedted_res['total_events'])

    @patch('benchmark_tools.evaluation.slr_worker_ranking_evaluation.SLRWorkerRankingEvaluation.compare_event')
    def test_event_handle_should_call_comparison_and_save_it(self, mocked_comp):
        redis_msg = {b'event': b'{"some": "json"}'}
        expected_comp = {'some': 'comp'}
        mocked_comp.return_value = expected_comp
        self.evaluation.event_handler(redis_msg)
        self.assertEqual(len(self.evaluation.events_compared), 1)
        self.assertDictEqual(self.evaluation.events_compared[0], expected_comp)

    def test_false_has_contradiction_on_ranking_for_all_when_same_ranking(self):
        compared_ranking = [0, 1, 5, 11, 6, 7, 2, 3, 4, 10, 8, 9]
        self.assertFalse(self.evaluation.has_contradiction_on_ranking(ranking_index=compared_ranking, best_only=False))

    def test_true_has_contradiction_on_ranking_for_all_when_different_ranking(self):
        compared_ranking = [1, 0, 7, 11, 6, 5, 2, 9, 4, 10, 8, 3]
        self.assertTrue(self.evaluation.has_contradiction_on_ranking(ranking_index=compared_ranking, best_only=False))

    def test_false_has_contradiction_on_ranking_for_best_when_same_ranking_first_only(self):
        compared_ranking = [0, 5, 1, 11, 6, 5, 2, 9, 4, 10, 8, 3]
        self.assertFalse(self.evaluation.has_contradiction_on_ranking(ranking_index=compared_ranking, best_only=True))

    def test_true_has_contradiction_on_ranking_for_best_when_different_ranking(self):
        compared_ranking = [1, 0, 5, 11, 6, 7, 2, 3, 4, 10, 8, 9]
        self.assertTrue(self.evaluation.has_contradiction_on_ranking(ranking_index=compared_ranking, best_only=True))

    def test_compare_event_returns_correctly(self):
        event_data = {
            "service_type": "ObjectDetection",
            "slr_profiles":{
                "ObjectDetection-0.7-0.7-0.3": {
                    "query_ids": ["f817a712e1906879abade4f3ac893d0e"],
                    "criteria_weights": [0.7, 0.7, 0.3],
                    "alternatives_ids": [
                        "worker-000-data",
                        "worker-001-data",
                        "worker-002-data",
                        "worker-003-data",
                        "worker-004-data",
                        "worker-005-data",
                        "worker-006-data",
                        "worker-007-data",
                        "worker-008-data",
                        "worker-009-data",
                        "worker-010-data",
                        "worker-011-data"
                    ],
                    "ranking_index": [0, 1, 6, 7, 8, 5, 11, 10, 2, 3, 4, 9],
                    "ranking_scores": [0.850, 0.850, 0.227, 0.227, 0.227, 0.260, 0.739, 0.739, 0.316, 0.138, 0.238, 0.256]
                }
            }
        }
        ret = self.evaluation.compare_event(event_data)
        self.assertEqual(ret['comp_ranking_index'], [0, 1, 6, 7, 8, 5, 11, 10, 2, 3, 4, 9])
        self.assertEqual(ret['has_contradiction_on_best'], False)
        self.assertEqual(ret['has_contradiction_on_any'], True)


    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
