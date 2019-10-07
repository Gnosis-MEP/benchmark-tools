#!/usr/bin/env python
import uuid
import hashlib

from flask import Flask, request, jsonify, make_response, abort

app = Flask(__name__)


# this is just a mocked method for running the benchmark
def start_benchmark(benchmark, target_system):
    return {'latency_avg': 1}


def make_confs_id(benchmark, target_system):
    stringify_confs = f'{benchmark}:{target_system}'.encode('utf-8')
    confs_id = hashlib.md5(stringify_confs).hexdigest()
    return confs_id


@app.route('/api/v1.0/run_benchmark', methods=['post'])
def run_benchmark():
    if not request.json:
        abort(400)

    benchmark = request.json.get('benchmark')
    target_system = request.json.get('target_system')
    confs_id = make_confs_id(benchmark, target_system)
    run_id = str(uuid.uuid4())
    benchmark_results = start_benchmark(benchmark, target_system)
    return make_response(jsonify({
        'results': benchmark_results,
        'configs': {
            'confs_id': confs_id,
            'benchmark': benchmark,
            'target_system': target_system
        },
        'run_id': run_id
    }), 200)


@app.route('/', methods=['get'])
def get_index():
    return make_response(jsonify({}), 200)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
