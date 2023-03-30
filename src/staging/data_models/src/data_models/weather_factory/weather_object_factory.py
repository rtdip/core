from abc import ABCMeta, abstractmethod


class WeatherObjectFactory(metaclass=ABCMeta):
    @staticmethod
    @abstractmethod
    def create_object(self):
        "Creates an Instance of the Weather Object"






