import json
import copy
import os
import random

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

        self.apply_worker_config_variation = kwargs['apply_worker_config_variation']
        if self.apply_worker_config_variation is None:
            self.apply_worker_config_variation = False

        self.output_events_csv_file = os.path.join(self.output_path, f'events_results.csv')
        self.output_non_proc_events_csv_file = os.path.join(self.output_path, f'non_proc_events_results.csv')
        self.output_all_events_csv_file = os.path.join(self.output_path, f'all_events_results.csv')
        self.output_non_proc_events_json_file = os.path.join(self.output_path, f'non_processed_events.json')

        self.total_hours_in_year = 8760
        self.total_traces = 0
        self.non_proccessed_traces_by_workers = {k: [] for k in self.workers_configuration_profile.keys()}
        self.base_results_df = None
        self.non_proc_base_results_df = None
        self.merged_df = None
        self.max_workers_end_time = None
        self.non_proc_exp_time = 0
        self.standby_kw = 0
        self.calculate_total_standby_kw_sum()

        self.init_exp_timestamp_sec = None
        self.end_proc_exp_timestamp_sec = None
        self.proc_exp_time = None
        self.end_non_proc_exp_timestamp_sec = None
        self.extended_exp_time = None

    def _get_base_results_df_dict(self):
        return copy.deepcopy({
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
            'processing_time_sec': [],
            'worker_finished_process': [],
            'worker_end_time_sec': [],
            'scheduled_time_sec': [],
        })

    def calculate_total_standby_kw_sum(self):
        self.standby_kw = 0
        for worker_stream_key, worker_profile in self.workers_configuration_profile.items():
            self.standby_kw += worker_profile['energy_consumption_standby']
        self.standby_kw = self.standby_kw / 1000

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
        worker_end_time = None
        worker_end_time_sec = None
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
                    worker_end_time = (worker_start_time + worker_duration)
                    worker_end_time_sec = worker_end_time / 10**6
                    worker_finished_process = True
                    # can stop loop since we reached the last span we need
                    break

        event_processing_details = None
        if worker_stream_key:
            event_processing_details = {
                'worker_stream_key': worker_stream_key,
                'init_time': init_time,
                'worker_start_time': worker_start_time,
                'worker_end_time': worker_end_time,
                'worker_end_time_sec': worker_end_time_sec,
                'worker_duration': worker_duration,
                'worker_finished_process': worker_finished_process,
                'scheduled_time': scheduled_time,
            }
            if not worker_finished_process:
                self.non_proccessed_traces_by_workers[worker_stream_key].append(event_processing_details)

        return event_processing_details

    def get_delta_variation(self):
        return random.randint(-100, 100) / 100

    def calculate_results_for_event_details(self, event_details):
        worker_stream_key = event_details['worker_stream_key']
        w_throughput = self.workers_configuration_profile[worker_stream_key]['throughput']
        w_accuracy = self.workers_configuration_profile[worker_stream_key]['accuracy']
        w_energy_consumption = self.workers_configuration_profile[worker_stream_key]['energy_consumption']
        w_energy_consumption_standby = self.workers_configuration_profile[worker_stream_key]['energy_consumption_standby']

        scheduled_time_sec =  event_details['scheduled_time'] / 10**6
        if self.apply_worker_config_variation:
            delta_variation = self.get_delta_variation()
            energy_consumption_variation = self.workers_configuration_profile[worker_stream_key]['energy_consumption_std'] * delta_variation
            w_energy_consumption += energy_consumption_variation


        w_energy_consumption_kw = w_energy_consumption * self.W_TO_KW

        accuracy = w_accuracy

        processing_time_sec = event_details['worker_duration'] / 10**6
        throughput = 1 / processing_time_sec

        latency = self.get_trace_latency(
            event_details['init_time'], event_details['worker_start_time'], event_details['worker_duration'])

        energy_consumption_w_s = processing_time_sec * w_energy_consumption
        energy_consumption_w_h = energy_consumption_w_s / 3600

        worker_finished_process = event_details['worker_finished_process']
        worker_end_time_sec = event_details['worker_end_time_sec']
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
            'worker_finished_process': worker_finished_process,
            'worker_end_time_sec': worker_end_time_sec,
            'scheduled_time_sec': scheduled_time_sec
        }
        return results

    def get_base_results_data_frame(self, traces):
        base_results_df = pd.DataFrame(self._get_base_results_df_dict())
        for trace in traces:
            event_details = self.get_consumer_stream_event_details(trace)
            if event_details is not None and event_details['worker_finished_process']:
                event_results = self.calculate_results_for_event_details(event_details)
                base_results_df = base_results_df.append(event_results, ignore_index=True)

        return base_results_df

    # def get_total_energy_kwh(self, base_results_df, standby=False, override_time=None):
    #     total_time = self.experiment_time
    #     if override_time is not None:
    #         total_time = override_time

    #     total_energy = 0
    #     if standby:
    #         used_workers_standby_kw_values = base_results_df.groupby(
    #             ['worker_stream_key']
    #         ).mean()['w_energy_consumption_standby'] / 1000
    #         non_used_workers_standby_kw_sum = 0
    #         for worker_stream_key, worker_profile in self.workers_configuration_profile.items():
    #             if worker_stream_key not in used_workers_standby_kw_values.index:
    #                 non_used_workers_standby_kw_sum += worker_profile['energy_consumption_standby']
    #         non_used_workers_standby_kw_sum = non_used_workers_standby_kw_sum / 1000
    #         non_used_workers_standby_kwh = non_used_workers_standby_kw_sum * total_time / 3600

    #         used_workers_standby_time_hour = (
    #             total_time - base_results_df.groupby(['worker_stream_key']).sum()['processing_time_sec']
    #         ) / 3600

    #         used_workers_standby_kwh_values = used_workers_standby_time_hour * used_workers_standby_kw_values
    #         total_energy = used_workers_standby_kwh_values.sum() + non_used_workers_standby_kwh

    #     else:
    #         energy_kws_values = base_results_df.groupby(['worker_stream_key']).sum()['energy_consumption_w_s'] / 1000
    #         # proc_time_hour = base_results_df.groupby(['worker_stream_key']).sum()['processing_time_sec'] / 3600
    #         energy_kwh_values = energy_kws_values / 3600
    #         total_energy = energy_kwh_values.sum()
    #     return total_energy

    # def extrapolate_one_year(self, total_kwh, total_time=None):
    #     experiment_time_hours = total_time / 3600
    #     energy_multiply = self.total_hours_in_year / experiment_time_hours
    #     total_energy_year = total_kwh * energy_multiply
    #     return total_energy_year

    def calculate_estimates_for_non_processed(self, event_details, previous_queued_event):
        worker_stream_key = event_details['worker_stream_key']
        w_throughput = self.workers_configuration_profile[worker_stream_key]['throughput']
        if self.apply_worker_config_variation:
            delta_variation = self.get_delta_variation()
            throughput_variation = self.workers_configuration_profile[worker_stream_key]['throughput_std'] * delta_variation
            w_throughput += throughput_variation

        processing_time = (10**6) / w_throughput

        # the pc process will imediatly go after the previous, not exactly what happens, should probably add more
        # but it is something (based on one event that I've measured)
        worker_start_time = previous_queued_event['worker_end_time'] + 4

        worker_end_time = worker_start_time + processing_time
        worker_end_time_sec = worker_end_time / 10**6

        event_details['worker_start_time'] = worker_start_time
        event_details['worker_duration'] = processing_time
        event_details['worker_end_time'] = worker_end_time
        event_details['worker_end_time_sec'] = worker_end_time_sec
        return event_details

    def get_non_processes_results_data_frame(self):
        non_proc_results_df = pd.DataFrame(self._get_base_results_df_dict())

        for worker_key, w_events in self.non_proccessed_traces_by_workers.items():
            previous_queued_event = {
                'worker_end_time': self.base_results_df[
                    self.base_results_df['worker_stream_key'] == worker_key
                ].max()['worker_end_time_sec'] * (10**6)
            }
            sorted_events = sorted(w_events, key=lambda e: e['scheduled_time'])
            for event_details in sorted_events:
                event_details = self.calculate_estimates_for_non_processed(event_details, previous_queued_event)
                event_row = self.calculate_results_for_event_details(event_details)
                non_proc_results_df = non_proc_results_df.append(event_row, ignore_index=True)
                previous_queued_event = event_details

        # self.max_workers_end_time = non_proc_results_df.max()['worker_end_time_sec']
        # last_scheduled_time_sec = non_proc_results_df.max()['scheduled_time_sec']
        # self.non_proc_exp_time = self.max_workers_end_time - last_scheduled_time_sec
        # self.ext_experiment_time = self.experiment_time + self.non_proc_exp_time
        return non_proc_results_df

    # def get_non_proc_final_results(self, base_results_accs, base_results_lats, total_processed, non_proc_results_df):
    #     import ipdb; ipdb.set_trace()
    #     total_non_proc_processing_energy_kwh = self.get_total_energy_kwh(
    #         non_proc_results_df, override_time=self.non_proc_exp_time)
    #     total_non_proc_standby_energy_kwh = self.get_total_energy_kwh(
    #         non_proc_results_df, standby=True, override_time=self.non_proc_exp_time)

    #     total_non_proc_energy_kwh = total_non_proc_processing_energy_kwh + total_non_proc_standby_energy_kwh

    #     # total_ext_energy_kwh = total_energy_kwh + total_non_proc_energy_kwh
    #     # one_year_ext_kwh = self.extrapolate_one_year(total_ext_energy_kwh, override_time=self.ext_experiment_time)
    #     accuracy_nonproc_avg = non_proc_results_df['accuracy'].mean()
    #     accuracy_nonproc_std = non_proc_results_df['accuracy'].std()
    #     accuracy_ext_avg = base_results_accs.append(non_proc_results_df['accuracy']).mean()
    #     accuracy_ext_std = base_results_accs.append(non_proc_results_df['accuracy']).std()

    #     latency_nonproc_std = non_proc_results_df['latency'].std()
    #     latency_nonproc_avg = non_proc_results_df['latency'].mean()
    #     latency_ext_avg = base_results_lats.append(non_proc_results_df['latency']).mean()
    #     latency_ext_std = base_results_lats.append(non_proc_results_df['latency']).std()

    #     total_nonprocessed = len(non_proc_results_df.index)
    #     sys_nonproc_throughput = total_nonprocessed / self.non_proc_exp_time
    #     total_ext_events = total_processed + total_nonprocessed
    #     # sys_ext_throughput = total_ext_events / self.ext_experiment_time

    #     final_non_proc_results = {
    #         'sys_nonproc_throughput': sys_nonproc_throughput,
    #         # 'sys_ext_throughput': sys_ext_throughput,
    #         'total_nonprocessed': total_nonprocessed,
    #         'total_ext_events': total_ext_events,
    #         'total_non_proc_processing_energy_kwh': total_non_proc_processing_energy_kwh,
    #         'total_non_proc_standby_energy_kwh': total_non_proc_standby_energy_kwh,
    #         'total_non_proc_energy_kwh': total_non_proc_energy_kwh,
    #         # 'total_ext_energy_kwh': total_ext_energy_kwh,
    #         # 'one_year_ext_kwh': one_year_ext_kwh,
    #         'accuracy_nonproc_avg': accuracy_nonproc_avg,
    #         'accuracy_nonproc_std': accuracy_nonproc_std,
    #         'accuracy_ext_avg': accuracy_ext_avg,
    #         'accuracy_ext_std': accuracy_ext_std,
    #         'latency_nonproc_std': latency_nonproc_std,
    #         'latency_nonproc_avg': latency_nonproc_avg,
    #         'latency_ext_avg': latency_ext_avg,
    #         'latency_ext_std': latency_ext_std,
    #         'non_proc_experiment_time': self.non_proc_exp_time,
    #     }
    #     return final_non_proc_results

    def get_total_energy_kwh(self, results_df, total_time, standby=False):
        total_energy = 0
        if standby:
            used_workers_standby_kw_values = results_df.groupby(
                ['worker_stream_key']
            ).mean()['w_energy_consumption_standby'] / 1000

            non_used_workers_standby_kw_sum = 0
            for worker_stream_key, worker_profile in self.workers_configuration_profile.items():
                if worker_stream_key not in used_workers_standby_kw_values.index:
                    non_used_workers_standby_kw_sum += worker_profile['energy_consumption_standby']
            non_used_workers_standby_kw_sum = non_used_workers_standby_kw_sum / 1000
            non_used_workers_standby_kwh = non_used_workers_standby_kw_sum * total_time / 3600

            used_workers_standby_time_hour = (
                total_time - results_df.groupby(['worker_stream_key']).sum()['processing_time_sec']
            ) / 3600

            used_workers_standby_kwh_values = used_workers_standby_time_hour * used_workers_standby_kw_values
            total_energy = used_workers_standby_kwh_values.sum() + non_used_workers_standby_kwh
        else:
            energy_kws_values = results_df.groupby(['worker_stream_key']).sum()['energy_consumption_w_s'] / 1000
            energy_kwh_values = energy_kws_values / 3600
            total_energy = energy_kwh_values.sum()
        return total_energy


    def get_final_results(self, base_results_df, non_proc_results_df):
        self.merged_df = base_results_df.append(non_proc_results_df)

        processed_only = self.merged_df[self.merged_df['worker_finished_process'] == True]
        proc_processing_energy_kwh = self.get_total_energy_kwh(processed_only, total_time=self.proc_exp_time)
        proc_standby_energy_kwh = self.get_total_energy_kwh(processed_only, total_time=self.proc_exp_time, standby=True)

        accuracy_avg = processed_only.mean()['accuracy']
        accuracy_std = processed_only.std()['accuracy']
        latency_avg = processed_only.mean()['latency']
        latency_std = processed_only.std()['latency']
        total_processed = len(base_results_df.index)
        sys_throughput = total_processed / self.experiment_time

        ext_accuracy_avg = self.merged_df.mean()['accuracy']
        ext_accuracy_std = self.merged_df.std()['accuracy']
        ext_latency_avg = self.merged_df.mean()['latency']
        ext_latency_std = self.merged_df.std()['latency']
        ext_total_processed = len(self.merged_df.index)
        ext_sys_throughput = total_processed / self.extended_exp_time

        total_processing_energy_kwh = self.get_total_energy_kwh(self.merged_df, total_time=self.extended_exp_time)
        total_standby_energy_kwh = self.get_total_energy_kwh(self.merged_df, total_time=self.extended_exp_time, standby=True)

        total_energy_kwh = total_processing_energy_kwh + total_standby_energy_kwh

        final_results = {
            'total_traces': self.total_traces,
            'proc_processing_energy_kwh': proc_processing_energy_kwh,
            'proc_standby_energy_kwh': proc_standby_energy_kwh,
            'accuracy_avg': accuracy_avg,
            'accuracy_std': accuracy_std,
            'latency_avg': latency_avg,
            'latency_std': latency_std,
            'total_processed': total_processed,
            'sys_throughput': sys_throughput,
            'total_processing_energy_kwh': total_processing_energy_kwh,
            'total_standby_energy_kwh': total_standby_energy_kwh,
            'ext_accuracy_avg': ext_accuracy_avg,
            'ext_accuracy_std': ext_accuracy_std,
            'ext_latency_avg': ext_latency_avg,
            'ext_latency_std': ext_latency_std,
            'ext_total_processed': ext_total_processed,
            'ext_sys_throughput': ext_sys_throughput,
            'total_energy_kwh': total_energy_kwh,
            'proc_exp_time': self.proc_exp_time,
            'extended_exp_time': self.extended_exp_time,
            'standby_kw': self.standby_kw,
        }

        return final_results

    def calculate_experiment_times(self):
        self.init_exp_timestamp_sec = self.base_results_df.min()['scheduled_time_sec']

        self.end_proc_exp_timestamp_sec = self.base_results_df.max()['worker_end_time_sec']
        self.proc_exp_time = self.end_proc_exp_timestamp_sec - self.init_exp_timestamp_sec

        self.end_non_proc_exp_timestamp_sec = self.non_proc_base_results_df.max()['worker_end_time_sec']
        self.extended_exp_time = self.end_non_proc_exp_timestamp_sec - self.init_exp_timestamp_sec

    def calculate_results(self, traces):
        self.base_results_df = self.get_base_results_data_frame(traces)
        self.non_proc_base_results_df = self.get_non_processes_results_data_frame()
        self.calculate_experiment_times()
        final_results = self.get_final_results(self.base_results_df, self.non_proc_base_results_df)
        return final_results

    def save_intermediary_data(self):
        self.base_results_df.to_csv(self.output_events_csv_file, index=False)
        self.non_proc_base_results_df.to_csv(self.output_non_proc_events_csv_file, index=False)
        self.merged_df.to_csv(self.output_all_events_csv_file, index=False)

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
        khw_to_coe_rate=None, energy_cost=None, apply_worker_config_variation=False):
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
        apply_worker_config_variation=apply_worker_config_variation,
        threshold_functions=threshold_functions,
        logging_level=logging_level
    )
    return evaluation.run()


if __name__ == '__main__':
    kwargs = {
        "jaeger_api_host": "http://172.17.0.1:16686",
        "output_path": "./outputs",
        "workers_configuration_profile": {
            "worker-000-data": {
                "throughput": 1.33,
                "throughput_std": 0.1,
                "accuracy": 21,
                "energy_consumption": 6.6,
                "energy_consumption_std": 0.4,
                "energy_consumption_standby": 2,
                "energy_consumption_standby_std": 0
            },
            "worker-001-data": {
                "throughput": 4.43,
                "throughput_std": 0.2,
                "accuracy": 21,
                "energy_consumption": 8.3,
                "energy_consumption_std": 0.5,
                "energy_consumption_standby": 2,
                "energy_consumption_standby_std": 0
            },
            "worker-002-data": {
                "throughput": 0.67,
                "throughput_std": 0.1,
                "accuracy": 28,
                "energy_consumption": 8.6,
                "energy_consumption_std": 1.1,
                "energy_consumption_standby": 2,
                "energy_consumption_standby_std": 0
            },
            "worker-003-data": {
                "throughput": 0.42,
                "throughput_std": 0,
                "accuracy": 28,
                "energy_consumption": 12,
                "energy_consumption_std": 0.1,
                "energy_consumption_standby": 2,
                "energy_consumption_standby_std": 0
            },
            "worker-004-data": {
                "throughput": 0.14,
                "throughput_std": 0,
                "accuracy": 37,
                "energy_consumption": 14.3,
                "energy_consumption_std": 0.6,
                "energy_consumption_standby": 2,
                "energy_consumption_standby_std": 0
            },
            "worker-005-data": {
                "throughput": 0.02,
                "throughput_std": 0,
                "accuracy": 37,
                "energy_consumption": 12.0,
                "energy_consumption_std": 0.4,
                "energy_consumption_standby": 2,
                "energy_consumption_standby_std": 0
            },
            "worker-006-data": {
                "throughput": 34.33,
                "throughput_std": 1.1,
                "accuracy": 21,
                "energy_consumption": 163.8,
                "energy_consumption_std": 19.9,
                "energy_consumption_standby": 72.1,
                "energy_consumption_standby_std": 0.3
            },
            "worker-007-data": {
                "throughput": 45.6,
                "throughput_std": 2.5,
                "accuracy": 21,
                "energy_consumption": 188,
                "energy_consumption_std": 6.7,
                "energy_consumption_standby": 72.1,
                "energy_consumption_standby_std": 0.3
            },
            "worker-009-data": {
                "throughput": 2.89,
                "throughput_std": 0.1,
                "accuracy": 28,
                "energy_consumption": 230,
                "energy_consumption_std": 6.9,
                "energy_consumption_standby": 72.1,
                "energy_consumption_standby_std": 0.3
            },
            "worker-010-data": {
                "throughput": 2.28,
                "throughput_std": 0.43,
                "accuracy": 37,
                "energy_consumption": 303.8,
                "energy_consumption_std": 17.5,
                "energy_consumption_standby": 72.1,
                "energy_consumption_standby_std": 0.3
            }
        },
        "workers_service_types": [
            "MockedStreamConsumer"
        ],
        "pre_consume_stream_process_name": "serialize_and_write_event_with_trace",
        "consume_stream_process_name": "consume_stream",
        "experiment_time": 90,
        "khw_to_coe_rate": 0.432727121,
        "energy_cost": 0.192,
        "apply_worker_config_variation": True,
        "threshold_functions": {},
        "logging_level": "ERROR"
    }
    print(json.dumps(run(**kwargs), indent=4))




