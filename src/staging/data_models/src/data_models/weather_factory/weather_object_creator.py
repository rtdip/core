from data_models.Constants import weather_constants
from data_models.logging import logconf
from data_models.weather.AtmosphericG215minForecastV1 import AtmosphericG215minForecastV1

namespace_str: str = 'WeatherObjectCreator'
logger = logconf.get_logger(namespace_str)


class WeatherObjectCreator:
    @staticmethod
    def create_object(**kwargs):
        version_str: str = kwargs[weather_constants.version]
        if version_str == weather_constants.AtmosphericG215minForecastV1:
            instance_obj = AtmosphericG215minForecastV1()
            return instance_obj.create_object(**kwargs)
        else:
            raise 'Version Not Implemented [{}]'.format(version_str)

    @staticmethod
    def from_json(**kwargs):
        msg_str: str = 'to_json() method not implemented'
        logger.debug(msg_str)
        raise msg_str

    @staticmethod
    def to_json():
        msg_str: str = 'to_json() method not implemented'
        logger.debug(msg_str)
        raise msg_str
