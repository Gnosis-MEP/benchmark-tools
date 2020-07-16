# Benchmark Tools
Tools for benchmarking.
It has a controller that executes a list of **tasks** followed by the execution of a list of **evaluations**, then the results are logged in the terminal and send as a POST request to the `result_webhook` url (the URL endpoint for the benchmark execution on the Benchmark Platform Controller webservice).

Each task has it's own use, characteristics and parameters to run it. Each **task** execute a list of **actions**.

Each **evaluation** analyses a list of **metrics** and validades their values agains on respective **threshold functions**

One can also use this as a stand-alone tool, by checking output in the terminal, and ignoring the connection error when the results are sent to a non-existent `result_webhook`.


## Configuring
All the configuration is done in a json file.
The important thing is that it should follow the same structure of the `configs.json` file.


## Running
Once finished this configuration file (or the `configs.json`), it should be `cat` and piped to the controller module.
This can be done in different ways: using a local python installation through pipenv or docker-compose.
### Locally
`cat configs.json | python benchmark_tools/controller/controller.py`

### Docker-compose

`cat configs.json | docker-compose run --rm benchmark-tools`


# Tasks

## Base
Base task class used by all tasks.
### Actions
#### task_gen_wait_for
This is an generic action available for all tasks. It's used to wait a fixed number of seconds (`sleep_time`).

## Task_wait_event_trace_timeout
This task provide a non static wait time that's is based in the last timestamp of the event traces from the service and operation defined.

### Kwargs

 * jaeger_api_host: Target system jaeger address and port
 * logging_level: Logging level
 * actions: List of actions to execute.

### Actions
#### wait_timeout_event_trace
Runs a while loop were it waits `wait_retry_time` seconds before checking if the timestamp of the last event trace of the `service` with `operation` has reached the `event_timeout` limit of seconds.
`forced_stop_timeout_limit` is the timeout for the whole action without considering the event trace, this is to avoid a forever running action.

If either `event_timeout` or `forced_stop_timeout_limit` are reached then the loop exits and the action is done.

This allows for example, to wait until 10 seconds have passed since the last frame was published in the system.


## Task_add_publisher
This task is used to register a publisher.
### Kwargs

 * redis_address: Target system Redis address
 * redis_port: Target system redis port
 * input_cmd_stream_key: Target system input command stream (eg: the ClientManager command stream)
 * logging_level: Logging level
 * actions: List of actions to execute.

### Actions
It's `actions` parameters compose the event message that are sent to the `input_cmd_stream_key` stream.


## Task_add_queries
This task is used to register a publisher.
### Kwargs

 * redis_address: Target system Redis address
 * redis_port: Target system redis port
 * input_cmd_stream_key: Target system input command stream (eg: the ClientManager command stream)
 * logging_level: Logging level
 * actions: List of actions to execute.

### Actions
#### addQuery
Send the addQuery action content translated to the equivalent event message to the `input_cmd_stream_key` stream.
#### delQuery
Send the delQuery action content translated to the equivalent event message to the `input_cmd_stream_key` stream.


## Task_export_jaeger_json
This task is used to export jaeger event traces as json files.
### Kwargs

 * jaeger_api_host: Target system jaeger address and port
 * logging_level: Logging level
 * actions: List of actions to execute.

### Actions
#### exportTraces
Gets the all the event traces of a given `service` and `operation` and exports it to a relative directory using `output_path`.
The output event traces json file is named as `{service}-{operation}.json`.
This file can later be uploaded in Jaeger UI to view the exported event traces.


# Evaluations

## Energy_consumption_evaluation
Evaluates the total energy consumption during a time frame of a given energy device through the use of the Energy Grid webservice api.
### Kwargs
 * energy_grid_api_host: Energy grid webservice address and port.
 * start_time: Initial timestamp (float) or the string "jaeger" (Uses jaeger to get the timestamp of the first event in the ClientManager)
 * end_time: Final timestamp (float) or the string "now" (uses current timestamp) or the string "jaeger" (Uses jaeger to get the timestamp of the last event in the ClientManager)
 * jaeger_api_host: (Optional) Target system jaeger address and port. Used required when `jaeger` is defined as a timestamp source.
 * energy_device_id: Energy device ID (4424 for Dedicated Server, 1507 for Jetson, 7246 for Raspberry Pi)
 * logging_level: Logging level
 * threshold_functions: Dictionary defining threshold functions for each metric name (or regexp of metric name).

### Metrics

* real_energy_(avg|std): Average and standard deviation for energy consumption in watts
* voltage_(avg|std): Average and standard deviation for the voltage
* frequency_(avg|std): Average and standard deviation for the frequency

## Latency_evaluation
Evaluates the end-to-end latency of the system by checking the event traces that are outputed to the user, that is: leave the Forwarder.
### Kwargs
 * jaeger_api_host: Target system jaeger address and port.
 * logging_level: Logging level
 * threshold_functions: Dictionary defining threshold functions for each metric name (or regexp of metric name).

### Metrics

* latency_(avg|std): Average and standard deviation for end-to-end latency in seconds.


## Throughput_evaluation
Evaluates the end-to-end throughput of the system by checking the event traces that are outputed to the user, that is: leave the Forwarder.
This does not consider the events that are dropped.
It calculates the ent-to-end throughput by dividing 1 for the end-to-end latency of the system.
### Kwargs
 * jaeger_api_host: Target system jaeger address and port.
 * logging_level: Logging level
 * threshold_functions: Dictionary defining threshold functions for each metric name (or regexp of metric name).

### Metrics

* throughput_avg: Average for end-to-end throughput in Frames Per Second (FPS).


## Per_service_speed_evaluation
Evaluates the speed (in seconds) of all event traces operations for the a given list of services (or all services).
### Kwargs
 * jaeger_api_host: Target system jaeger address and port.
 * services: list of services names (as their appear in Jaeger) or the string "all" to get all services
 * logging_level: Logging level
 * threshold_functions: Dictionary defining threshold functions for each metric name (or regexp of metric name).

### Metrics
This depends on the services listed on the evaluation, and their respective operations available.
It will calculate the average and standard deviation speed for all of the service's operations in the services list.
The metrics names defined as follows:

* \{service\}\_\{operation\}\_(avg|std): Average and standard deviation speed in seconds for a given `service` and `operation`.

