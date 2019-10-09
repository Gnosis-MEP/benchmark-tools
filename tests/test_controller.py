import unittest
# from unittest.mock import patch

from benchmark_tools.controller import controller


class ControllerTestCase(unittest.TestCase):

    def setUp(self):
        pass

    def test_run_benchmark(self):
        benchmark = {1: 2}
        target_system = {3: 4}

        res = controller.run_benchmark(benchmark, target_system)
        for k in ['results', 'configs', 'run_id']:
            self.assertIn(k, res)
        for k in ['benchmark', 'target_system', 'confs_id']:
            self.assertIn(k, res['configs'])

        self.assertDictEqual(benchmark, res['configs']['benchmark'])
        self.assertDictEqual(target_system, res['configs']['target_system'])

    def tearDown(self):
        # os.close(self.db_fd)
        # os.unlink(controller.app.config['DATABASE'])
        pass


if __name__ == '__main__':
    unittest.main()
