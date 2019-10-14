import json

"""

# this is VEQL query interface where subscribers can write there queries
# the format is <Subscriber_id || Query_id || Query, Action>
# presently the action can be either 'addQuery' to the service or delete it i.e. 'delQuery' in future add 'updateQuery'

1||2||select object_detection from publisher 1 where (object1.label = 'car' AND object1.attribute = 'red') AND (object2.label = 'person')  within TIMEFRAMEWINDOW(10) withconfidence >50 within SLIDINGWINDOW(10)||addQuery
2||1||select object_detection from publisher 3 where (object1.label = 'dog') AND (object2.label = 'car') within TIMEFRAMEWINDOW(10) withconfidence >50 ||addQuery
3||1||select object_detection from publisher 1 where (object.label = 'car') within TIMEFRAMEWINDOW(10) withconfidence >50 ||addQuery

"""
# query_str = "select object_detection from publisher 1 where (object1.label = 'car' AND object1.attribute = 'red') AND (object2.label = 'person')  within TIMEFRAMEWINDOW(10) withconfidence >50 within SLIDINGWINDOW(10)"


def get_query_from_json_file(actions_file_path):
    queries = []
    with open(query_json_file_path, 'r') as f:
        queries = json.load(f)
    for query_data in queries:

        for key in ['subscriber_id', 'query_id', 'query_str', 'action'] or key == wait_key:
            assert key in query_data, f'Missing "{key}" in "{query_data}" from "{query_json_file_path}" file'
        yield query_data
