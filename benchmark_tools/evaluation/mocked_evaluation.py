import time
from benchmark_tools.evaluation.base import BaseEvaluation


class MockedEvaluation(BaseEvaluation):

    def some_long_calculation_method(self):
        self.logger.debug('Running a very long method...')
        time.sleep(5)
        metric = 'latency_avg'
        value = 0
        return {
            metric: value
        }

    def run(self):
        result = self.some_long_calculation_method()
        return self.verify_thresholds(result)


def run(some, other, threshold_functions, logging_level):
    evaluation = MockedEvaluation(some=some, other=other,
                                  threshold_functions=threshold_functions, logging_level=logging_level)
    return evaluation.run()


if __name__ == '__main__':
    kwargs = {
        "some": "thing",
        "other": "arguments",
        "threshold_functions": {
            "latency_avg": "lambda x: x < 10"
        },
        "logging_level": "DEBUG"
    }
    print(run(**kwargs))
