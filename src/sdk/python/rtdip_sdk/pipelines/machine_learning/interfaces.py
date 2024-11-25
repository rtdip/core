from abc import abstractmethod

from great_expectations.compatibility.pyspark import DataFrame

from ..interfaces import PipelineComponentBaseInterface


class MachineLearningInterface(PipelineComponentBaseInterface):
    @abstractmethod
    def __init__(self, df: DataFrame):
        pass

    @abstractmethod
    def train(self):
        return self

    @abstractmethod
    def predict(self, *args, **kwargs) -> DataFrame:
        pass
