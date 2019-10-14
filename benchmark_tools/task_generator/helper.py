import json


def get_actions_from_json_file(actions_json_file_path):
    actions = {}
    with open(actions_json_file_path, 'r') as f:
        actions = json.load(f)
    # stages = ['beginning', 'middle', 'end']

    # for stage in stages:
    #     assert stage in actions, f'Missing "{stage}" stage in actions json file {actions_json_file_path}'

    return actions

