# Author : Indeep Singh
# Class : ThroughputTestCase
# Date : 07-July-2020
# Description : This python script is a unit test file to Throughput Evaluation.


import unittest
from unittest.mock import patch

from benchmark_tools.evaluation.throughput_evaluation import ThroughputEvaluation


class ThroughputTestCase(unittest.TestCase):

    def setUp(self):
        self.evaluation = ThroughputEvaluation(
            logging_level='ERROR',
            threshold_functions={},
            jaeger_api_host='jaeger_api_host'
        )

    @patch('benchmark_tools.evaluation.throughput_evaluation.ThroughputEvaluation.order_traces')
    @patch('benchmark_tools.evaluation.throughput_evaluation.ThroughputEvaluation.get_traces')
    @patch('benchmark_tools.evaluation.throughput_evaluation.ThroughputEvaluation.calculate_throughput_metrics')
    @patch('benchmark_tools.evaluation.throughput_evaluation.ThroughputEvaluation.verify_thresholds')
    def test_run_should_call_necessary_functions(self, mocked_threshold, mocked_calculate, mocked_get, mocked_order):
        self.evaluation.run()
        self.assertTrue(mocked_threshold.called)
        self.assertTrue(mocked_calculate.called)
        self.assertTrue(mocked_get.called)
        self.assertTrue(mocked_order.called)

    @patch('benchmark_tools.evaluation.throughput_evaluation.ThroughputEvaluation.get_initial_time')
    @patch('benchmark_tools.evaluation.throughput_evaluation.ThroughputEvaluation.get_end_time')
    def test_calculate_throughput_metrics_should_return_correctly(self, mocked_end, mocked_init):
        traces = [{}, {}]
        mocked_init.return_value = 1000000
        mocked_end.return_value = 11000000
        ret = self.evaluation.calculate_throughput_metrics(traces)

        expected_ret = {
            'throughput_fps': 0.2,
        }

        self.assertDictEqual(ret, expected_ret)

    def test_get_initial_time(self):
        start_time1_a = 10**6
        start_time1_b = start_time1_a + 1000

        start_time2_a = start_time1_b + 2000
        start_time2_b = start_time2_a + 1000
        end_time = start_time2_b + 2000

        traces = [
            {
                'spans': [
                    {'startTime': start_time1_a, 'duration': 1000},  # start time x
                    {'some_other_span'},
                    {'some_other_span'},
                    {'some_other_span'},
                    {'startTime': start_time1_b, 'duration': 2000},  # start time x+10.002 seconds
                ]
            },
            {
                'spans': [
                    {'startTime': start_time2_a, 'duration': 1000},  # start time x
                    {'some_other_span'},
                    {'some_other_span'},
                    {'some_other_span'},
                    {'startTime': start_time2_b, 'duration': 2000},  # start time x+10.002 seconds
                ]
            }
        ]
        ret = self.evaluation.get_initial_time(traces)
        self.assertEqual(ret, start_time1_a)

    def test_get_end_time(self):
        start_time1_a = 10**6
        start_time1_b = start_time1_a + 1000

        start_time2_a = start_time1_b + 2000
        start_time2_b = start_time2_a + 1000
        end_time = start_time2_b + 2000

        traces = [
            {
                'spans': [
                    {'startTime': start_time1_a, 'duration': 1000},  # start time x
                    {'some_other_span'},
                    {'some_other_span'},
                    {'some_other_span'},
                    {'startTime': start_time1_b, 'duration': 2000},  # start time x+10.002 seconds
                ]
            },
            {
                'spans': [
                    {'startTime': start_time2_a, 'duration': 1000},  # start time x
                    {'some_other_span'},
                    {'some_other_span'},
                    {'some_other_span'},
                    {'startTime': start_time2_b, 'duration': 2000},  # start time x+10.002 seconds
                ]
            }
        ]
        ret = self.evaluation.get_end_time(traces)
        self.assertEqual(ret, end_time)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
