#!/usr/bin/env python
import time
import sys
import json
import uuid
import hashlib

import requests

from benchmark_tools.conf import (
    BENCHMARK_JSON_CONFIG_PATH,
    TARGET_SYSTEM_JSON_CONFIG_PATH,
    RESULT_WEBHOOK_URL,
)


# this is just a mocked method for running the benchmark
def start_benchmark(benchmark, target_system):
    time.sleep(5)
    return {'latency_avg': 1}


def make_confs_id(benchmark, target_system):
    stringify_confs = f'{benchmark}:{target_system}'.encode('utf-8')
    confs_id = hashlib.md5(stringify_confs).hexdigest()
    return confs_id


def send_results_to_webhook(results, result_webhook):
    res = requests.post(url=result_webhook, json=results)
    return res


def prepare_benchmark_output(run_id, benchmark, target_system, confs_id, benchmark_results):
    results = {
        'results': benchmark_results,
        'configs': {
            'confs_id': confs_id,
            'benchmark': benchmark,
            'target_system': target_system
        },
        'run_id': run_id
    }
    return results


def run_benchmark(benchmark, target_system, result_webhook):
    confs_id = make_confs_id(benchmark, target_system)
    run_id = str(uuid.uuid4())
    benchmark_results = start_benchmark(benchmark, target_system)
    results = prepare_benchmark_output(run_id, benchmark, target_system, confs_id, benchmark_results)
    # results = {
    #     'results': benchmark_results,
    #     'configs': {
    #         'confs_id': confs_id,
    #         'benchmark': benchmark,
    #         'target_system': target_system
    #     },
    #     'run_id': run_id
    # }

    return send_results_to_webhook(results, result_webhook)


def get_configs_from_file(file_path):
    data = {}
    with open(file_path, 'r') as f:
        data = json.load(f)
    return data


def get_configs(benchmark_file_path, target_system_file_path):
    benchmark = get_configs_from_file(benchmark_file_path)
    target_system = get_configs_from_file(target_system_file_path)
    return {'benchmark': benchmark, 'target_system': target_system}


def prepare_and_run_benchmark(benchmark_file_path, target_system_file_path, result_webhook):
    configs = get_configs(benchmark_file_path, target_system_file_path)
    configs['result_webhook'] = result_webhook
    ret = run_benchmark(**configs)
    try:
        return ret.json()
    except:
        return {}


if __name__ == '__main__':
    if len(sys.argv) != 4:
        benchmark_file_path = BENCHMARK_JSON_CONFIG_PATH
        target_system_file_path = TARGET_SYSTEM_JSON_CONFIG_PATH
        result_webhook = RESULT_WEBHOOK_URL
    else:
        print('Replacing env vars with arguments')
        benchmark_file_path = sys.argv[1]
        target_system_file_path = sys.argv[2]
        result_webhook = sys.argv[3]
    print(prepare_and_run_benchmark(benchmark_file_path, target_system_file_path, result_webhook))
