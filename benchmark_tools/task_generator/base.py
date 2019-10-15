import time
from benchmark_tools.logging import setup_logging


class BaseTask():

    def __init__(self, *args, **kwargs):
        self.logging_level = kwargs['logging_level']
        self.logger = setup_logging(self.__class__.__name__, self.logging_level)
        self.actions = kwargs['actions']
        # self.actions = self.get_actions_from_json_file(self.actions_json_file_path)

    # def get_actions_from_json_file(self, actions_json_file_path):
    #     actions = {}
    #     with open(actions_json_file_path, 'r') as f:
    #         actions = json.load(f)
    #     return actions

    def process_action(self, action_data):
        action = action_data.get('action', '')
        self.logger.info(f'Processing action "{action}" with data "{action_data}"')
        if action == 'task_gen_wait_for':
            sleep_time = float(action_data.get('sleep_time', 0))
            self.logger.debug(f'Sleeping for {sleep_time} seconds...')
            time.sleep(sleep_time)
            return True
        return False

    def execute_actions(self):
        self.logger.info('Executing actions...')
        for action_data in self.actions:
            self.process_action(action_data)
