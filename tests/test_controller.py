import unittest
from unittest.mock import patch

from benchmark_tools.controller import controller


class ControllerTestCase(unittest.TestCase):

    def setUp(self):
        pass

    @patch('benchmark_tools.controller.controller.send_results_to_webhook')
    @patch('benchmark_tools.controller.controller.start_benchmark')
    @patch('benchmark_tools.controller.controller.prepare_benchmark_output')
    def test_run_benchmark_send_results_to_webhook(self, prepare_output_mocked, mocked_start_benchmark, mocked_send_results):
        benchmark = {1: 2}
        target_system = {3: 4}
        result_webhook = 'https://example.com/set_result/123-abc'
        # mocked_start_benchmark.return_value = {'some': 'result'}
        prepare_output_mocked.return_value = {'some': 'result'}
        controller.run_benchmark(benchmark, target_system, result_webhook)
        mocked_send_results.assert_called_once_with({'some': 'result'}, 'https://example.com/set_result/123-abc')

    def test_prepare_benchmark_results(self):
        benchmark = {1: 2}
        target_system = {3: 4}
        benchmark_results = {'evaluation_a': 'result'}
        run_id = '123-abc'
        confs_id = '789-abc'
        res = controller.prepare_benchmark_output(run_id, benchmark, target_system, confs_id, benchmark_results)
        expected = {
            'evaluations': {'evaluation_a': 'result'},
            'configs': {
                'confs_id': '789-abc',
                'benchmark': {1: 2},
                'target_system': {3: 4}
            },
        'run_id': '123-abc'
        }
        self.assertDictEqual(res, expected)

    def tearDown(self):
        # os.close(self.db_fd)
        # os.unlink(controller.app.config['DATABASE'])
        pass


if __name__ == '__main__':
    unittest.main()
