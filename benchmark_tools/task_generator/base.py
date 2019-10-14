import time
from benchmark_tools.task_generator.read_queries import get_actions_from_json_file


class BaseTask():

    def __init__(self, *args, **kwargs):
        self.actions_json_file_path = kwargs['actions_json_file_path']
        self.actions = get_actions_from_json_file(self.actions_json_file_path)
        self.logging_level = kwargs['logging_level']

    def process_action(self, action_data):
        action = action_data.get('action', '')
        if action == 'task_gen_wait_for':
            sleep_time = action_data.get('sleep_time', 0)
            time.sleep(sleep_time)
            return True
        return False

    def execute_actions(self):
        for action_data in self.actions:
            self.process_action(action_data)
