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

    @patch('benchmark_tools.evaluation.throughput_evaluation.ThroughputEvaluation.get_traces')
    @patch('benchmark_tools.evaluation.throughput_evaluation.ThroughputEvaluation.calculate_latency_metrics')
    @patch('benchmark_tools.evaluation.throughput_evaluation.ThroughputEvaluation.calculate_throughput')
    @patch('benchmark_tools.evaluation.throughput_evaluation.ThroughputEvaluation.verify_thresholds')
    def test_run_should_call_necessary_functions(self, mocked_threshold, mocked_throughput, mocked_calculate, mocked_get):
        self.evaluation.run()
        self.assertTrue(mocked_threshold.called)
        self.assertTrue(mocked_throughput.called)
        self.assertTrue(mocked_calculate.called)
        self.assertTrue(mocked_get.called)


    def test_throughput_should_return_correctly(self):
        avg_latency = 0.03
        ret = self.evaluation.calculate_throughput(avg_latency)
        expected_ret = (1/avg_latency)
        self.assertEquals(ret, expected_ret)

if __name__ == '__main__':
    unittest.main()