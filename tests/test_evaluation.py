import unittest

from benchmark_tools.evaluation import evaluate


class EvaluationTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_prepare_benchmark_results(self):
        target_system = {3: 4}
        metrics = {
            'latency':
            {
                'avg': 'lambda x: x < 10'
            }
        }

        res = evaluate(target_system, metrics)
        expected = {'latency_avg': {'value': 0, 'threshold': 'lambda x: x < 10', 'passed': True}}
        self.assertDictEqual(res, expected)

    def tearDown(self):
        # os.close(self.db_fd)
        # os.unlink(controller.app.config['DATABASE'])
        pass


if __name__ == '__main__':
    unittest.main()
