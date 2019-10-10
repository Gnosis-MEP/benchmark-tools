import os

from decouple import config

SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SOURCE_DIR)

LOGGING_LEVEL = config('LOGGING_LEVEL', default='DEBUG')

BENCHMARK_JSON_CONFIG_PATH = config('BENCHMARK_JSON_CONFIG_PATH', default=os.path.join(PROJECT_ROOT, 'bm.json'))
TARGET_SYSTEM_JSON_CONFIG_PATH = config('TARGET_SYSTEM_JSON_CONFIG_PATH', default=os.path.join(PROJECT_ROOT, 'ts.json'))
RESULT_WEBHOOK_URL = config('BASE_RESULT_WEBHOOK_URL', default='http://localhost:5000/set_result/123-abc')
