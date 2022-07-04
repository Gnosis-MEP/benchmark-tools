import unittest
from unittest.mock import patch

from benchmark_tools.evaluation.scheduler_load_shedding_evaluation import SchedulerLoadSheddingEvaluation


class SchedulerLoadSheddingTestCase(unittest.TestCase):

    def setUp(self):
        self.evaluation = SchedulerLoadSheddingEvaluation(
            logging_level='ERROR',
            threshold_functions={},
            jaeger_api_host='jaeger_api_host'
        )

    @patch('benchmark_tools.evaluation.scheduler_load_shedding_evaluation.SchedulerLoadSheddingEvaluation.get_traces')
    @patch('benchmark_tools.evaluation.scheduler_load_shedding_evaluation.SchedulerLoadSheddingEvaluation.calculate_load_shedding_rate')
    @patch('benchmark_tools.evaluation.scheduler_load_shedding_evaluation.SchedulerLoadSheddingEvaluation.verify_thresholds')
    def test_run_should_call_necessary_functions(self, mocked_threshold, mocked_calculate, mocked_get):
        self.evaluation.run()
        self.assertTrue(mocked_threshold.called)
        self.assertTrue(mocked_calculate.called)
        self.assertTrue(mocked_get.called)

    @patch('benchmark_tools.evaluation.scheduler_load_shedding_evaluation.SchedulerLoadSheddingEvaluation.get_total_load_shedding')
    def test_calculate_load_shedding_rate_should_return_correctly(self, mocked_get):

        traces = ['fake trace', 'fake LS trace']
        mocked_get.return_value = 1
        ret = self.evaluation.calculate_load_shedding_rate(traces)
        self.assertTrue(mocked_get.called)

        expected_ret = {
            'data_points': 2,
            'load_shedding_rate': 0.5
        }

        self.assertDictEqual(ret, expected_ret)


    def test_has_load_shedding_checks_for_operation_in_trace_spans_correctly(self):
        trace1 = {
            "spans": [
                {
                    "operationName": "process_data_event",
                },
                {
                    "operationName": "log_event_load_shedding",
                },
            ]
        }
        trace2 =  {
            "spans": [
                {
                    "operationName": "process_data_event",
                },
                {
                    "operationName": "otherOperation",
                },
            ]
        }
        ret = self.evaluation.has_load_shedding(trace1)
        self.assertTrue(ret)

        ret2 = self.evaluation.has_load_shedding(trace2)
        self.assertFalse(ret2)

#     def test_calculate_average_should_return_correctly(self):
#         values = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0]
#         ret = self.evaluation.calculate_average(values)
#         expected_ret = 4.5
#         self.assertEqual(ret, expected_ret)

#     def test_get_trace_latency(self):
#         trace = {
#             'spans': [
#                 {'startTime': int('1571981717759324'), 'duration': 1000},  # start time x
#                 {'some_other_span'},
#                 {'some_other_span'},
#                 {'some_other_span'},
#                 {'startTime': int('1571981727759324'), 'duration': 2000},  # start time x+10.002 seconds
#             ]
#         }
#         ret = self.evaluation.get_trace_latency(trace)
#         expected_ret = 10.002
#         self.assertEqual(ret, expected_ret)

    def tearDown(self):
        pass


if __name__ == '__main__':
    unittest.main()
