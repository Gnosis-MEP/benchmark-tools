import csv
import datetime
import functools
import statistics


import requests

from benchmark_tools.evaluation.base import BaseEvaluation


class EnergyConsumptionEvaluation(BaseEvaluation):

    ENERGY_GRID_WEBSERVICE_GET_ENERGY_ENDPOINT = (
        '/api/get-energy?starttimestamp={start_timestamp}&endtimestamp={end_timestamp}&device={device_id}'
    )
    JAEGER_TRACES_URL_FORMAT = (
        'api/traces?'
        'limit=700000000&lookback=8h&maxDuration&minDuration&'
        'service={service}&operation={operation}'
    )

    def __init__(self, *args, **kwargs):
        super(EnergyConsumptionEvaluation, self).__init__(*args, **kwargs)
        self.jaeger_api_host = kwargs.get('jaeger_api_host')
        self.energy_grid_api_host = kwargs.get('energy_grid_api_host')
        self.start_time = kwargs['start_time']
        self.end_time = kwargs['end_time']
        self.energy_device_id = kwargs['energy_device_id']
        self.setup()

    def setup(self):
        if isinstance(self.start_time, str):
            if self.start_time.lower() == 'jaeger':
                self.start_time = self.get_jaeger_first_timestamp()
            else:
                self.start_time = float(self.start_time)

        if self.end_time is None:
            self.end_time = 'now'
        if isinstance(self.end_time, str):
            if self.end_time.lower() == 'now':
                self.end_time = datetime.datetime.now().timestamp()
            else:
                self.end_time = float(self.end_time)

    def get_jaeger_first_timestamp(self):
        service = 'ClientManager'
        operation = 'process_action'
        self.logger.info(
            f'Geting start_time from Jaeger "{self.jaeger_api_host}" first "{operation}" on service "{service}"'
        )
        end_point = self.JAEGER_TRACES_URL_FORMAT.format(service=service, operation=operation)
        traces_url = f'{self.jaeger_api_host}/{end_point}'
        req = requests.get(traces_url)
        traces = req.json()['data']
        start_timestamp = traces[0]['spans'][0]['startTime'] / 10**6
        # shifts to one second earlier
        earlier_shifted_start_timestamp = start_timestamp - 1
        return earlier_shifted_start_timestamp

    def get_readings_from_filepath(self, energy_readings_path):
        readings = []
        found_starting_point = False
        with open(energy_readings_path, 'r') as csv_file:
            reader = csv.reader(csv_file)
            next(reader)  # Skip header
            for i, reading in enumerate(reader):
                timestamp = float(reading[0])
                device_id = int(reading[3])
                if device_id != self.energy_device_id:
                    continue

                # Find experiment starting point
                if timestamp < self.start_time:
                    continue
                else:
                    if found_starting_point is False:
                        found_starting_point = True
                        curr_datetime = datetime.datetime.fromtimestamp(timestamp)
                        back_start_datetime = datetime.datetime.fromtimestamp(self.start_time)

                        self.logger.debug(f'Found experiment start point on energy consumption data.')
                        self.logger.debug(f'Current Energy Reading datetime: {curr_datetime}')
                        self.logger.debug(f'Experiment Starting time:        {back_start_datetime}')

                # Find experiment ending point
                if timestamp > self.end_time:
                    curr_datetime = datetime.datetime.fromtimestamp(timestamp)
                    back_end_datetime = datetime.datetime.fromtimestamp(self.end_time)

                    self.logger.debug(f'Found experiment end point on energy consumption data.')
                    self.logger.debug(f'Current Energy Reading datetime: {curr_datetime}')
                    self.logger.debug(f'Experiment Ending time:        {back_end_datetime}')

                    break
                voltage = int(reading[6])
                freq = float(reading[7])
                real_energy = int(reading[9])
                reading = {
                    'device_id': device_id,
                    'freq': freq,
                    'real_energy': real_energy,
                    'voltage': voltage,
                }
                readings.append(reading)
        return readings

    def get_traces_per_service(self, service):
        end_point = self.JAEGER_TRACES_URL_FORMAT.format(service=service)
        traces_url = f'{self.jaeger_api_host}/{end_point}'
        req = requests.get(traces_url)
        traces = req.json()['data']
        return traces

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
        # return self.get_readings_from_filepath(energy_readings_path)

    def calculate_average(self, values):
        return functools.reduce(lambda a, b: a + b, values) / len(values)

    def metris_avg_and_std(self, metric_name, values):
        return {
            f'{metric_name}_avg': self.calculate_average(values),
            f'{metric_name}_std': statistics.stdev(values),
        }

    def calculate_metrics(self, readings):
        voltages = []
        freqs = []
        real_energies = []
        for reading in readings:
            voltages.append(reading['voltage'])
            freqs.append(reading['frequency'])
            real_energies.append(reading['real_energy'])
        results = {}
        results.update(self.metris_avg_and_std('voltage', voltages))
        results.update(self.metris_avg_and_std('frequency', freqs))
        results.update(self.metris_avg_and_std('real_energy', real_energies))
        return results

    def run(self):
        start_datetime = datetime.datetime.fromtimestamp(self.start_time)
        end_datetime = datetime.datetime.fromtimestamp(self.end_time)
        self.logger.debug(
            f'Evaluation for Energy Usage of device {self.energy_device_id}'
            f' from {start_datetime}({self.start_time})'
            f' to {end_datetime}({self.end_time}) is running...'
        )
        readings = self.get_energy_readings()
        self.logger.debug(f'Total Energy Consumption values to be analysed: {len(readings)}')
        results = self.calculate_metrics(readings)
        return self.verify_thresholds(results)


def run(energy_grid_api_host, start_time, energy_device_id, threshold_functions, logging_level, end_time=None, jaeger_api_host=None):
    evaluation = EnergyConsumptionEvaluation(
        jaeger_api_host=jaeger_api_host,
        energy_grid_api_host=energy_grid_api_host,
        start_time=start_time,
        end_time=end_time,
        energy_device_id=int(energy_device_id),
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
        "start_time": "jaeger",
        # "energy_device_id": "1507",4424
        "energy_device_id": "4424",
        "threshold_functions": {
            ".*": "lambda x: x < 300",
            "real_energy_avg": "lambda x: x < 75",
        },
        "logging_level": "DEBUG"
    }
    import json
    print(json.dumps(run(**kwargs), indent=4))
