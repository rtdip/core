#!/usr/bin/env python
from data_models.weather_factory.weather_object_creator import WeatherObjectCreator
from data_models.Constants import weather_constants
from data_models.logging import logconf
from data_models.utils import utils
import unittest

namespace_str: str = 'test_create_weather_objects'
logger = logconf.get_logger(namespace_str)


class TestCreateWeatherObjects(unittest.TestCase):

    def setUp(self):
        pass

    def test_create_AtmosphericG215minForecastV1(self):
        # Random values. Not value/type checking. 
        # Will check if this value is set properly in the object attributes
        temp_value_int: int = utils.generate_random_int_number(0, 100)

        # Create the object via the factory
        a_g2_15min_for_v1_instance = WeatherObjectCreator.create_object(
            version=weather_constants.AtmosphericG215minForecastV1,
            clas=utils.generate_random_alpha_num_string(),
            clds=utils.generate_random_int_number(0, 100),
            day_ind=utils.generate_random_alpha_num_string(),
            dewpt=utils.generate_random_int_number(0, 100),
            dow=utils.generate_random_alpha_num_string(),
            expire_time_gmt=utils.get_utc_timestamp(),
            fcst_valid=utils.get_utc_timestamp(),
            fcst_valid_local=utils.get_datetime_from_utc_timestamp(utils.get_utc_timestamp()),
            feels_like=utils.generate_random_int_number(0, 100),
            golf_category=utils.generate_random_alpha_num_string(),
            golf_index=utils.generate_random_int_number(0, 100),
            gust=utils.generate_random_int_number(0, 100),
            hi=utils.generate_random_int_number(0, 100),
            icon_code=utils.generate_random_int_number(0, 100),
            icon_extd=utils.generate_random_int_number(0, 100),
            mslp=utils.generate_random_int_number(0, 100) / 100,
            num=utils.generate_random_int_number(0, 100),
            phrase_12char=utils.generate_random_alpha_num_string(),
            phrase_22char=utils.generate_random_alpha_num_string(),
            phrase_32char=utils.generate_random_alpha_num_string(),
            pop=utils.generate_random_alpha_num_string(),
            precip_type=utils.generate_random_alpha_num_string(),
            qpf=utils.generate_random_int_number(0, 100) / 100,
            rh=utils.generate_random_int_number(0, 100),
            severity=utils.generate_random_int_number(0, 100),
            snow_qpf=utils.generate_random_int_number(0, 100) / 100,
            subphrase_pt1=utils.generate_random_alpha_num_string(),
            subphrase_pt2=utils.generate_random_alpha_num_string(),
            subphrase_pt3=utils.generate_random_alpha_num_string(),
            temp=temp_value_int,
            uv_desc=utils.generate_random_alpha_num_string(),
            uv_index=utils.generate_random_alpha_num_string(),
            uv_index_raw=utils.generate_random_alpha_num_string(),
            uv_warning=utils.generate_random_alpha_num_string(),
            vis=utils.generate_random_int_number(0, 100) / 100,
            wc=utils.generate_random_int_number(0, 100),
            wdir=utils.generate_random_int_number(0, 100),
            wdir_cardinal=utils.generate_random_alpha_num_string(),
            wspd=utils.generate_random_int_number(0, 100),
            wxman=utils.generate_random_alpha_num_string())

        logger.debug('Version: {}'.format(a_g2_15min_for_v1_instance.version))

        self.assertTrue(a_g2_15min_for_v1_instance.temp == temp_value_int)

    def tearDown(self):
        pass


def tests():
    test_suite = unittest.TestSuite()
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestCreateWeatherObjects))
    return test_suite


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(tests())
