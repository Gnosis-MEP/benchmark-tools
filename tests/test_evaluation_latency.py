import unittest
from unittest.mock import patch

from benchmark_tools.evaluation.latency_evaluation import LatencyEvaluation


class EvaluationTestCase(unittest.TestCase):

    def setUp(self):
        self.evaluation = LatencyEvaluation(
            logging_level='ERROR',
            threshold_functions={},
            jaeger_api_host='jaeger_api_host'
        )

    @patch('benchmark_tools.evaluation.latency_evaluation.LatencyEvaluation.get_traces')
    @patch('benchmark_tools.evaluation.latency_evaluation.LatencyEvaluation.calculate_latency_metrics')
    @patch('benchmark_tools.evaluation.latency_evaluation.LatencyEvaluation.verify_thresholds')
    def test_run_should_call_necessary_functions(self, mocked_threshold, mocked_calculate, mocked_get):
        self.evaluation.run()
        self.assertTrue(mocked_threshold.called)
        self.assertTrue(mocked_calculate.called)
        self.assertTrue(mocked_get.called)

    @patch('benchmark_tools.evaluation.latency_evaluation.LatencyEvaluation.get_trace_latency')
    @patch('benchmark_tools.evaluation.latency_evaluation.LatencyEvaluation.calculate_average')
    @patch('statistics.stdev')
    def test_calculate_latency_metrics_should_return_correctly(self, mocked_std, mocked_avg, mocked_get):
        traces = [{}, {}]
        mocked_avg.return_value = 1
        mocked_std.return_value = 2
        ret = self.evaluation.calculate_latency_metrics(traces)
        self.assertTrue(mocked_std.called)
        self.assertTrue(mocked_avg.called)
        self.assertTrue(mocked_get.called)

        expected_ret = {
            'data_points': 2,
            'latency_avg': 1,
            'latency_std': 2
        }

        self.assertDictEqual(ret, expected_ret)

    def test_calculate_average_should_return_correctly(self):
        values = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
        ret = self.evaluation.calculate_average(values)
        expected_ret = 4.5
        self.assertEqual(ret, expected_ret)

    def test_get_trace_latency(self):
        trace = {
            'spans': [
                {'startTime': int('1571981717759324'), 'duration': 1000},  # start time x
                {'some_other_span'},
                {'some_other_span'},
                {'some_other_span'},
                {'startTime': int('1571981727759324'), 'duration': 2000},  # start time x+10.002 seconds
            ]
        }
        ret = self.evaluation.get_trace_latency(trace)
        expected_ret = 10.002
        self.assertEqual(ret, expected_ret)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
