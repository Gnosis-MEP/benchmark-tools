#!/usr/bin/env python
import functools
import hashlib
import importlib
import json
import sys

import requests


def replace_args_kwargs_vals_with_target_systems_confs(value, benchmark):
    return value


def get_tasks(benchmark):
    # 'benchmark_tools.task_generator.'
    for task in benchmark.get('tasks', []):
        task_python_path = task.get('module')
        task_module = importlib.import_module(task_python_path)
        task_args = task.get('args', [])
        task_kwargs = task.get('kwargs', {})
        # task_actions = task.get('actions', [])
        # task_kwargs['actions'] = task_actions
        task_run_function = getattr(task_module, 'run')
        task_function_prepared = functools.partial(task_run_function, *task_args, **task_kwargs)
        # task['function'] = task_function_prepared
        yield (task, task_function_prepared)


def get_evaluations(benchmark):
    # 'benchmark_tools.task_generator.'
    for evaluation in benchmark.get('evaluations', []):
        evaluation_python_path = evaluation.get('module')
        evaluation_module = importlib.import_module(evaluation_python_path)
        evaluation_args = evaluation.get('args', [])
        evaluation_kwargs = evaluation.get('kwargs', {})
        # threshold_functions = evaluation.get('threshold_functions', {})
        # task_kwargs['threshold_functions'] = threshold_functions
        evaluation_run_function = getattr(evaluation_module, 'run')
        evaluation_function_prepared = functools.partial(
            evaluation_run_function, *evaluation_args, **evaluation_kwargs)
        # evaluation['function'] = evaluation_function_prepared
        yield (evaluation, evaluation_function_prepared)


def run_tasks(benchmark, target_system):
    for task_data, task in get_tasks(benchmark):
        print(f'Running task: {task_data["module"]}')
        task()


def run_evaluations(benchmark, target_system):
    evaluations_result = {'passed': True}
    for evaluation_data, evaluation in get_evaluations(benchmark):
        print(f'Running evaluation: {evaluation_data["module"]}')
        try:
            result = evaluation()
        except Exception as e:
            result = {'passed': False, 'error': str(e)}
        evaluations_result[evaluation_data['module']] = result
        if result['passed'] is False:
            evaluations_result['passed'] = False
    return evaluations_result


# this is just a mocked method for running the benchmark
def start_benchmark(benchmark, target_system):
    run_tasks(benchmark, target_system)
    evaluation = run_evaluations(benchmark, target_system)
    return evaluation


def make_confs_id(benchmark, target_system):
    stringify_confs = f'{benchmark}:{target_system}'.encode('utf-8')
    confs_id = hashlib.md5(stringify_confs).hexdigest()
    return confs_id


def send_results_to_webhook(results, result_webhook):
    res = requests.post(url=result_webhook, json=results)
    return res


def save_results_to_disk(results, result_webhook):
    file_path = result_webhook.split('file://')[-1]
    with open(file_path, 'w') as f:
        json.dump(results, f, indent=4)

    return file_path


def prepare_benchmark_output(run_id, benchmark, target_system, confs_id, benchmark_results):
    results = {
        'evaluations': benchmark_results,
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
    run_id = result_webhook.split('/')[-1]

    print('Running benchmark...')
    benchmark_results = start_benchmark(benchmark, target_system)
    print('Finished benchmark.')
    print('preparing output')
    results = prepare_benchmark_output(run_id, benchmark, target_system, confs_id, benchmark_results)
    if 'file://' in result_webhook:
        print(f'Saving results to disk instead of posting it to url.')
        return save_results_to_disk(results, result_webhook)
    else:
        print(f'Sending results to webhook: {results} ->{result_webhook}')
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


def prepare_and_run_benchmark(configs):
    print(f'Using this configs to run the benchmark: {configs}')
    ret = run_benchmark(**configs)
    if isinstance(ret, str):
        return ret
    try:
        data = ret.json()
        print('Got a json return')
        return data
    except Exception as e:
        print(e)
        return {}


if __name__ == '__main__':

    if sys.stdin.isatty():
        # if len(sys.argv) != 3:
        #     benchmark_file_path = BENCHMARK_JSON_CONFIG_PATH
        #     target_system_file_path = TARGET_SYSTEM_JSON_CONFIG_PATH
        #     result_webhook = RESULT_WEBHOOK_URL
        # else:
        #     print('Replacing env vars with arguments')
        #     benchmark_file_path = sys.argv[1]
        #     target_system_file_path = sys.argv[2]
        #     result_webhook = sys.argv[3]
        print("ignoring...")
    else:
        configs = json.load(sys.stdin)
    print(f'Using this arguments: {configs}')
    print(prepare_and_run_benchmark(configs))
