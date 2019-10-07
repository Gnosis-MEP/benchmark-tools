import os
import tempfile
import unittest
# from unittest.mock import patch

from benchmark_tools.controller import controller


class ControllerTestCase(unittest.TestCase):

    def setUp(self):
        # self.db_fd, controller.app.config['DATABASE'] = tempfile.mkstemp()

        controller.app.config['TESTING'] = True
        controller.app.config['WTF_CSRF_ENABLED'] = False
        controller.app.config['DEBUG'] = False
        controller.app.testing = True
        self.client = controller.app.test_client()
        # with controller.app.app_context():
        #     pass

    def test_main_page(self):
        response = self.client.get('/', follow_redirects=True)
        self.assertEqual(response.status_code, 200)

    def test_run_benchmark(self):

        payload = {
            'benchmark': 1,
            'target_system': 2
        }

        ret = self.client.post(
            '/api/v1.0/run_benchmark',
            json=payload,
            follow_redirects=True,

        )
        self.assertEqual(ret.status_code, 200)
        for k in ['results', 'configs', 'run_id']:
            self.assertIn(k, ret.json)
        for k in ['benchmark', 'target_system', 'confs_id']:
            self.assertIn(k, ret.json['configs'])

        self.assertEqual(payload['benchmark'], ret.json['configs']['benchmark'])
        self.assertEqual(payload['target_system'], ret.json['configs']['target_system'])

    def tearDown(self):
        # os.close(self.db_fd)
        # os.unlink(controller.app.config['DATABASE'])
        pass


if __name__ == '__main__':
    unittest.main()
