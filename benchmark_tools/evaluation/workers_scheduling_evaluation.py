import os
import json

import requests
import pandas as pd

from benchmark_tools.evaluation.base import BaseEvaluation


class WorkersSchedulingEvaluation(BaseEvaluation):

    W_TO_KW = 1 / 1000
    KW_TO_KWH = 1 / 3600

    JAEGER_TRACES_URL_FORMAT = (
        'api/traces?'
        'limit=700000000&lookback=6h&maxDuration&minDuration&'
        'operation={operation}&service={service}'
    )

    def __init__(self, *args, **kwargs):
        super(WorkersSchedulingEvaluation, self).__init__(*args, **kwargs)
        self.jaeger_api_host = kwargs['jaeger_api_host']
        self.output_path = kwargs['output_path']
        self.workers_configuration_profile = kwargs['workers_configuration_profile']

        self.workers_service_types = kwargs['workers_service_types']
        self.pre_consume_stream_process_name = kwargs['pre_consume_stream_process_name']
        self.consume_stream_process_name = kwargs['consume_stream_process_name']
        self.experiment_time = kwargs['experiment_time']
        self.khw_to_coe_rate = kwargs['khw_to_coe_rate']
        if self.khw_to_coe_rate is None:
            self.khw_to_coe_rate = 0.954
        self.energy_cost = kwargs['energy_cost']
        if self.energy_cost is None:
            self.energy_cost = 19.2 / 100

        # self.workers_service_types = ['ObjectDetection']
        # self.pre_consume_stream_process_name = 'serialize_and_write_event_with_trace'
        # self.consume_stream_process_name = 'consume_stream'
        # self.experiment_time = 90
        # self.khw_to_coe_rate = 0.954
        # self.energy_cost = 19.2 / 100

        self.output_events_csv_file = os.path.join(self.output_path, f'events_results.csv')
        self.output_non_proc_events_json_file = os.path.join(self.output_path, f'non_processed_events.json')

        self.total_hours_in_year = 8760
        self.total_traces = 0
        self.non_proccessed_traces_by_workers = {k: [] for k in self.workers_configuration_profile.keys()}
        self.base_results_df = None

    def get_traces(self):
        end_point = self.JAEGER_TRACES_URL_FORMAT.format(
            operation='process_data_event', service='Scheduler')
        traces_url = f'{self.jaeger_api_host}/{end_point}'
        req = requests.get(traces_url)
        traces = req.json()['data']
        return traces

    def get_trace_processes_to_name_map(self, trace):
        return {
            proc: p_data['serviceName']
            for proc, p_data in trace['processes'].items()
        }

    def get_span_worker_stream_destination_from_tags(self, span):
        worker_stream_key = None
        for tag in span['tags']:
            if tag['key'] == 'message_bus.destination':
                worker_stream_key = tag['value']
                break
        return worker_stream_key

    def get_trace_latency(self, init_time, worker_start_time, worker_duration):
        total_trace_time_mm_sec = (worker_start_time + worker_duration) - init_time
        total_trace_time_sec = total_trace_time_mm_sec / 10**6
        return total_trace_time_sec

    def get_consumer_stream_event_details(self, trace):
        proc_names = self.get_trace_processes_to_name_map(trace)
        ordered_spans = sorted(trace['spans'], key=lambda s: s['startTime'])

        worker_stream_key = None
        init_time = ordered_spans[0]['startTime']
        scheduled_time = None
        worker_start_time = None
        worker_duration = None
        worker_finished_process = False
        for span in ordered_spans:
            if proc_names[span['processID']] == 'Scheduler':
                if span['operationName'] == self.pre_consume_stream_process_name:
                    worker_stream_key = self.get_span_worker_stream_destination_from_tags(span)
                    scheduled_time = span['startTime'] + span['duration']
            elif proc_names[span['processID']] in self.workers_service_types:
                if span['operationName'] == self.consume_stream_process_name:
                    worker_start_time = span['startTime']
                    worker_duration = span['duration']
                    worker_finished_process = True
                    # can stop loop since we reached the last span we need
                    break

        event_processing_details = None
        if worker_stream_key:
            if worker_finished_process:
                event_processing_details = {
                    'worker_stream_key': worker_stream_key,
                    'init_time': init_time,
                    'worker_start_time': worker_start_time,
                    'worker_duration': worker_duration,
                    'worker_finished_process': worker_finished_process,
                    'scheduled_time': scheduled_time,
                }
            else:
                # non-processed scheduled events
                non_processed_event_details = {
                    'worker_stream_key': worker_stream_key,
                    'init_time': init_time,
                    'scheduled_time': scheduled_time,
                }
                self.non_proccessed_traces_by_workers[worker_stream_key].append(non_processed_event_details)

        return event_processing_details


    def calculate_results_for_event_details(self, event_details):
        worker_stream_key = event_details['worker_stream_key']
        w_throughput = self.workers_configuration_profile[worker_stream_key]['throughput']
        w_accuracy = self.workers_configuration_profile[worker_stream_key]['accuracy']
        w_energy_consumption = self.workers_configuration_profile[worker_stream_key]['energy_consumption']
        w_energy_consumption_standby = self.workers_configuration_profile[worker_stream_key]['energy_consumption_standby']

        w_energy_consumption_kw = w_energy_consumption * self.W_TO_KW

        accuracy = w_accuracy

        processing_time_sec = event_details['worker_duration'] / 10**6
        throughput = 1 / processing_time_sec

        latency = self.get_trace_latency(
            event_details['init_time'], event_details['worker_start_time'], event_details['worker_duration'])

        energy_consumption_w_s = processing_time_sec * w_energy_consumption
        energy_consumption_w_h = energy_consumption_w_s / 3600

        # df_line['energy_consumption_kwh'] = df_line['worker_energy_usage_kw'] * df_line['total_processing_time_hour']
        # df_line['energy_consumption_joules'] = df_line['worker_energy_usage'] * df_line['total_processing_time_sec']
        # df_line['energy_consumption_coe'] = df_line['energy_consumption_kwh'] * KHW_TO_COE_RATE
        results = {
            'worker_stream_key': worker_stream_key,
            'w_throughput': w_throughput,
            'w_energy_consumption': w_energy_consumption,
            'w_energy_consumption_standby': w_energy_consumption_standby,
            'w_energy_consumption_kw': w_energy_consumption_kw,
            'accuracy': accuracy,
            'throughput': throughput,
            'latency': latency,
            'energy_consumption_w_s': energy_consumption_w_s,
            'energy_consumption_w_h': energy_consumption_w_h,
            'processing_time_sec': processing_time_sec,
        }
        return results

    def get_base_data_results_frame(self, traces):
        base_results_df = pd.DataFrame({
            'worker_stream_key': [],
            'w_throughput': [],
            'w_energy_consumption': [],
            'w_energy_consumption_standby': [],
            'w_energy_consumption_kw': [],
            'accuracy': [],
            'throughput': [],
            'latency': [],
            'energy_consumption_w_s': [],
            'energy_consumption_w_h': [],
            'processing_time_sec': []
        })
        for trace in traces:
            event_details = self.get_consumer_stream_event_details(trace)
            if event_details is not None:
                event_results = self.calculate_results_for_event_details(event_details)
                base_results_df = base_results_df.append(event_results, ignore_index=True)
        return base_results_df

    def get_total_energy_kwh(self, base_results_df, standby=False):
        total_energy = 0
        if standby:
            standby_kw_values = base_results_df.groupby(['worker_stream_key']).mean()['w_energy_consumption_standby'] / 1000
            standby_time_hour = (
                self.experiment_time - base_results_df.groupby(['worker_stream_key']).sum()['processing_time_sec']
            ) / 3600

            standby_kwh_values = standby_time_hour * standby_kw_values
            total_energy = standby_kwh_values.sum()
        else:
            energy_kw_values = base_results_df.groupby(['worker_stream_key']).mean()['w_energy_consumption'] / 1000
            proc_time_hour = base_results_df.groupby(['worker_stream_key']).sum()['processing_time_sec'] / 3600
            energy_kwh_values = proc_time_hour * energy_kw_values
            total_energy = energy_kwh_values.sum()
        return total_energy

    def extrapolate_one_year(self, total_kwh):
        experiment_time_hours = self.experiment_time / 3600
        energy_multiply = self.total_hours_in_year / experiment_time_hours
        total_energy_year = total_kwh * energy_multiply
        return total_energy_year

    def get_final_results(self, base_results_df):
        total_processing_energy_kwh = self.get_total_energy_kwh(base_results_df)
        total_standby_energy_kwh = self.get_total_energy_kwh(base_results_df, standby=True)
        total_energy_kwh = total_processing_energy_kwh + total_standby_energy_kwh
        one_year_kwh = self.extrapolate_one_year(total_energy_kwh)
        accuracy_avg = base_results_df.mean()['accuracy']
        accuracy_std = base_results_df.std()['accuracy']
        latency_avg = base_results_df.mean()['latency']
        latency_std = base_results_df.std()['latency']
        total_processed = len(base_results_df.index)
        sys_throughput = total_processed / self.experiment_time
        final_results = {
            'sys_throughput': sys_throughput,
            'total_events': self.total_traces,
            'total_processed': total_processed,
            'total_processing_energy_kwh': total_processing_energy_kwh,
            'total_standby_energy_kwh': total_standby_energy_kwh,
            'total_energy_kwh': total_energy_kwh,
            'one_year_kwh': one_year_kwh,
            'accuracy_avg': accuracy_avg,
            'accuracy_std': accuracy_std,
            'latency_avg': latency_avg,
            'latency_std': latency_std,
        }
        return final_results


    def calculate_results(self, traces):
        self.base_results_df = self.get_base_data_results_frame(traces)
        final_results = self.get_final_results(self.base_results_df)
        return final_results

    def save_intermediary_data(self):
        # self.output_events_csv_file = os.path.join(self.output_path, f'events_results.csv')
        # self.output_non_proc_events_json_file = os.path.join(self.output_path, f'non_processed_events.json')
        self.base_results_df.to_csv(self.output_events_csv_file, index=False)
        with open(self.output_non_proc_events_json_file, 'w') as f:
            json.dump(self.non_proccessed_traces_by_workers, f, indent=4)


    def run(self):
        self.logger.debug('Evaluation for Scheduler results for list of workers...')
        traces = self.get_traces()
        self.total_traces = len(traces)
        results = self.calculate_results(traces)
        self.save_intermediary_data()
        return self.verify_thresholds(results)


def run(jaeger_api_host, output_path, workers_configuration_profile, workers_service_types,
        pre_consume_stream_process_name, consume_stream_process_name, experiment_time,
        threshold_functions, logging_level,
        khw_to_coe_rate=None, energy_cost=None):
        # "output_path": "./outputs",
        # "workers_configuration_profile": {
        #     'worker-000-data': {
        #         "throughput": 1.33,
        #         "accuracy": 21,
        #         "energy_consumption": 6.6,
        #     },
        #     'worker-001-data': {
        #         "throughput": 4.43,
        #         "accuracy": 21,
        #         "energy_consumption": 8.3,
        #     },
        # },
        # "workers_service_types": ['ObjectDetection'],
        # "pre_consume_stream_process_name": 'serialize_and_write_event_with_trace',
        # "consume_stream_process_name": 'consume_stream',
        # "experiment_time": 90,
        # "khw_to_coe_rate": 0.954,
        # "energy_cost": 0.192,
    evaluation = WorkersSchedulingEvaluation(
        jaeger_api_host=jaeger_api_host,
        output_path=output_path,
        workers_configuration_profile=workers_configuration_profile,
        workers_service_types=workers_service_types,
        pre_consume_stream_process_name=pre_consume_stream_process_name,
        consume_stream_process_name=consume_stream_process_name,
        experiment_time=experiment_time,
        khw_to_coe_rate=khw_to_coe_rate,
        energy_cost=energy_cost,
        threshold_functions=threshold_functions,
        logging_level=logging_level
    )
    return evaluation.run()


if __name__ == '__main__':
    kwargs = {
        "jaeger_api_host": "http://localhost:16686",
        "output_path": "./outputs",
        "workers_configuration_profile": {
            'object-detection-ssd-gpu-data': {
                "throughput": 34.33,
                "accuracy": 0.21,
                "energy_consumption": 163,
                "energy_consumption_standby": 70,
            },
            'object-detection-ssd-data': {
                "throughput": 45.6,
                "accuracy": 0.21,
                "energy_consumption": 188,
                "energy_consumption_standby": 70,
            },
        },
        "workers_service_types": ['ObjectDetectionService'],
        "pre_consume_stream_process_name": 'serialize_and_write_event_with_trace',
        "consume_stream_process_name": 'process_data_event', #consume_stream
        "experiment_time": 30,
        "khw_to_coe_rate": 0.954,
        "energy_cost": 0.192,
        "threshold_functions": {
        },
        "logging_level": "DEBUG"
    }
    print(run(**kwargs))
