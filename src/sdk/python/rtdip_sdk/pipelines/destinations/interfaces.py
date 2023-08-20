from abc import abstractmethod
from ..interfaces import PipelineComponentBaseInterface


class DestinationInterface(PipelineComponentBaseInterface):
    @abstractmethod
    def pre_write_validation(self) -> bool:
        pass

    @abstractmethod
    def post_write_validation(self) -> bool:
        pass

    @abstractmethod
    def write_batch(self):
        pass

    @abstractmethod
    def write_stream(self):
        pass
