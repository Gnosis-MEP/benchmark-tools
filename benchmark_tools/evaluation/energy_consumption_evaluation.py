import csv
import datetime
import functools
import json
import statistics


import requests

from benchmark_tools.evaluation.base import BaseEvaluation


class EnergyConsumptionEvaluation(BaseEvaluation):

    ENERGY_GRID_WEBSERVICE_GET_ENERGY_ENDPOINT = (
        '/api/get-energy?starttimestamp={start_timestamp}&endtimestamp={end_timestamp}&device={device_id}'
    )
    JAEGER_TRACES_URL_FORMAT = (
        'api/traces?'
        'limit=700000000&lookback=10h&maxDuration&minDuration&'
        'service={service}&operation={operation}'
    )
    JAEGER_TAGS_URL_FORMAT = '&tags={tags}'
    JAEGER_TRACES_URL_WITH_TAGS_FORMAT = JAEGER_TRACES_URL_FORMAT + JAEGER_TAGS_URL_FORMAT

    def __init__(self, *args, **kwargs):
        super(EnergyConsumptionEvaluation, self).__init__(*args, **kwargs)
        self.jaeger_api_host = kwargs.get('jaeger_api_host')

        self.jaeger_traces_configs = kwargs.get('jaeger_traces_configs', None)
        if self.jaeger_traces_configs is None:
            self.jaeger_traces_configs = {
                'start': {
                    'service': 'ClientManager',
                    'operation': 'process_action',
                    'tags': {"process-action-name": "addQuery"}
                },
                'end': {
                    'service': 'ClientManager',
                    'operation': 'process_action',
                    'tags': {"process-action-name": "delQuery"}
                }
            }
        self.energy_grid_api_host = kwargs.get('energy_grid_api_host')
        self.start_time = kwargs['start_time']
        self.end_time = kwargs['end_time']
        self.save_readings_on = kwargs.get('save_readings_on', None)
        self.energy_device_id = kwargs['energy_device_id']
        if ';' in self.energy_device_id:
            self.energy_device_id_list = self.energy_device_id.split(';')
            self.energy_device_id = self.energy_device_id[0]
        else:
            self.energy_device_id_list = [self.energy_device_id]
        if self.save_readings_on is not None:
            self.save_readings_on_list = [
                self.save_readings_on.format(device_id) for device_id in self.energy_device_id_list
            ]

        self.setup()

    def setup(self):
        if isinstance(self.start_time, str):
            if self.start_time.lower() == 'jaeger':
                self.start_time = self.get_jaeger_timestamp(first=True)
            else:
                self.start_time = float(self.start_time)

        if self.end_time is None:
            self.end_time = 'now'
        if isinstance(self.end_time, str):
            if self.end_time.lower() == 'now':
                self.end_time = datetime.datetime.now().timestamp()
            elif self.end_time.lower() == 'jaeger':
                self.end_time = self.get_jaeger_timestamp(first=False)
            else:
                self.end_time = float(self.end_time)

    def get_jaeger_timestamp(self, first=True):
        configs_key = 'start' if first else 'end'
        service = self.jaeger_traces_configs.get(configs_key).get('service')
        operation = self.jaeger_traces_configs.get(configs_key).get('operation')
        tags = self.jaeger_traces_configs.get(configs_key).get('tags')

        self.logger.info(
            f'Geting event trace ts (start={first})'
            f' from Jaeger "{self.jaeger_api_host}" first "{operation}" on service "{service}"'
        )
        if tags is None:
            end_point = self.JAEGER_TRACES_URL_FORMAT.format(
                service=service, operation=operation)
        else:
            tags_json = json.dumps(tags)
            end_point = self.JAEGER_TRACES_URL_WITH_TAGS_FORMAT.format(
                service=service, operation=operation, tags=tags_json)

        traces_url = f'{self.jaeger_api_host}/{end_point}'
        req = requests.get(traces_url)
        traces = req.json()['data']
        ordered_traces = self.order_traces(traces)
        trace_timestamp = None
        if first:
            start_timestamp = ordered_traces[0]['spans'][0]['startTime'] / 10**6
            trace_timestamp = start_timestamp - 1
        else:
            end_span = ordered_traces[-1]['spans'][-1]
            end_timestamp = (end_span['startTime'] + end_span['duration']) / 10**6
            trace_timestamp = end_timestamp + 1
        return trace_timestamp

    def get_readings_from_webservice(self, start_timestamp, end_timestamp, device_id):
        end_point_url = f'{self.energy_grid_api_host}{self.ENERGY_GRID_WEBSERVICE_GET_ENERGY_ENDPOINT}'.format(
            start_timestamp=start_timestamp,
            end_timestamp=end_timestamp,
            device_id=device_id
        )
        req = requests.get(end_point_url)
        readings = req.json()['readings']
        return readings

    def get_energy_readings(self):
        return self.get_readings_from_webservice(self.start_time, self.end_time, self.energy_device_id)

    def save_readings(self, readings, device_save_readings_on):
        self.logger.info(f'Saving energy readings on: {device_save_readings_on}')
        with open(device_save_readings_on, 'w') as f:
            json.dump(readings, f, indent=4)

    def calculate_average(self, values):
        return functools.reduce(lambda a, b: a + b, values) / len(values)

    def metris_avg_and_std(self, metric_name, values):
        avg = self.calculate_average(values) if len(values) > 1 else values[0]
        std = statistics.stdev(values) if len(values) > 1 else 0
        return {
            f'{metric_name}_avg': avg,
            f'{metric_name}_std': std,
        }

    def calculate_metrics(self, readings, device_id):
        voltages = []
        freqs = []
        real_energies = []
        total_consumption = 0
        for reading in readings:
            voltages.append(reading['voltage'])
            freqs.append(reading['frequency'])
            real_energies.append(reading['real_energy'])
            total_consumption += 10 * reading['real_energy']

        results = {}
        results.update(self.metris_avg_and_std(f'id_{device_id}-voltage', voltages))
        results.update(self.metris_avg_and_std(f'id_{device_id}-frequency', freqs))
        results.update(self.metris_avg_and_std(f'id_{device_id}-real_energy', real_energies))
        results.update({f'id_{device_id}-data_points': len(real_energies)})
        results.update({f'id_{device_id}-total_consumption': total_consumption})
        return results

    def order_traces(self, traces):
        ordered_traces = sorted(traces, key=lambda t: t['spans'][0]['startTime'])
        return ordered_traces

    def run(self):
        start_datetime = datetime.datetime.fromtimestamp(self.start_time)
        end_datetime = datetime.datetime.fromtimestamp(self.end_time)
        results = {}
        for device_index, device_id in enumerate(self.energy_device_id_list):
            device_id = int(device_id)
            self.energy_device_id = device_id
            self.logger.debug(
                f'Evaluation for Energy Usage of device {device_id}'
                f' from {start_datetime}({self.start_time})'
                f' to {end_datetime}({self.end_time}) is running...'
            )
            readings = self.get_energy_readings()
            self.logger.debug(f'Total Energy Consumption values to be analysed: {len(readings)}')
            if self.save_readings_on is not None:
                device_save_readings_on = self.save_readings_on_list[device_index]
                self.save_readings(readings, device_save_readings_on)
            results.update(self.calculate_metrics(readings, device_id))
        return self.verify_thresholds(results)


def run(
        energy_grid_api_host,
        start_time,
        energy_device_id,
        threshold_functions,
        logging_level,
        save_readings_on=None,
        end_time=None,
        jaeger_api_host=None,
        jaeger_traces_configs=None):

    evaluation = EnergyConsumptionEvaluation(
        jaeger_api_host=jaeger_api_host,
        jaeger_traces_configs=jaeger_traces_configs,
        energy_grid_api_host=energy_grid_api_host,
        start_time=start_time,
        end_time=end_time,
        energy_device_id=energy_device_id,
        save_readings_on=save_readings_on,
        threshold_functions=threshold_functions,
        logging_level=logging_level
    )
    return evaluation.run()


if __name__ == '__main__':

    # 4424 big gpu
    # 1507 jetson
    # other?
    kwargs = {
        "energy_grid_api_host": "http://localhost:5000",
        "jaeger_api_host": "http://localhost:16686",

        "jaeger_traces_configs": {
            'start': {
                'service': 'ClientManager',
                'operation': 'process_action',
                'tags': {"process-action-name": "addQuery"}
            },
            'end': {
                'service': 'ClientManager',
                'operation': 'process_action',
                'tags': {"process-action-name": "delQuery"}
            }
        },

        "start_time": "jaeger",
        "end_time": "jaeger",
        # "energy_device_id": "1507",4424
        "save_readings_on": 'energy_readings_{}.json',
        "energy_device_id": "4424;1507",
        "threshold_functions": {
            ".*": "lambda x: x < 300",
            "real_energy_avg": "lambda x: x < 75",
        },
        "logging_level": "DEBUG"
    }
    import json
    print(json.dumps(run(**kwargs), indent=4))
