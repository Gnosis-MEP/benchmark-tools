#!/usr/bin/env python
import uuid
import hashlib
import requests


# this is just a mocked method for running the benchmark
def start_benchmark(benchmark, target_system):
    return {'latency_avg': 1}


def make_confs_id(benchmark, target_system):
    stringify_confs = f'{benchmark}:{target_system}'.encode('utf-8')
    confs_id = hashlib.md5(stringify_confs).hexdigest()
    return confs_id


def send_results_to_webhook(results, result_webhook):
    res = requests.post(url=result_webhook, json=results)
    return res


def run_benchmark(benchmark, target_system, result_webhook):
    confs_id = make_confs_id(benchmark, target_system)
    run_id = str(uuid.uuid4())
    benchmark_results = start_benchmark(benchmark, target_system)
    results = {
        'results': benchmark_results,
        'configs': {
            'confs_id': confs_id,
            'benchmark': benchmark,
            'target_system': target_system
        },
        'run_id': run_id
    }

    return send_results_to_webhook(results, result_webhook)
