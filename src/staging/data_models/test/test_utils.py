#!/usr/bin/env python
from data_models.logging import logconf
from data_models.series import SeriesType
from data_models.utils import utils
from data_models.usage import Usage
from datetime import timezone
from dateutil import tz
import random
import uuid

import datetime

import unittest

namespace_str: str = 'test_utils'
logger = logconf.get_logger(namespace_str)


class TestUtils(unittest.TestCase):

    def test_timeseries_15min(self):
        now_utc_datetime: datetime.datetime = datetime.datetime.now(timezone.utc)
        now_utc_datetime_plus = now_utc_datetime
        # Generate 96 Intervals of 15 min in 24h
        interval_minutes_int = 15
        intervals_in_one_day_int: int = int((24 * 60) / interval_minutes_int)
        arr_list: list = [1 for i in range(intervals_in_one_day_int)]
        interval_list: list = list()
        for i in range(intervals_in_one_day_int):
            now_utc_datetime_plus: datetime.datetime = now_utc_datetime_plus + \
                                                       datetime.timedelta(minutes=interval_minutes_int)
            interval_int: int = utils.get_interval(SeriesType.SeriesType.minutes_15, now_utc_datetime_plus)
            arr_list[interval_int] = 0
            interval_list.append('interval_' + str(interval_int).zfill(2))
        self.assertTrue(sum(arr_list) == 0)
        interval_sorted_list = sorted(interval_list)
        self.assertTrue('00' in interval_sorted_list[0])

    def test_timeseries_1h(self):
        now_utc_datetime: datetime.datetime = datetime.datetime.now(timezone.utc)
        now_utc_datetime_plus = now_utc_datetime
        # Generate 24 Intervals of 1h in 24h
        interval_minutes_int = 60
        intervals_in_one_day_int: int = 24
        interval_list: list = [1 for i in range(intervals_in_one_day_int)]
        interval_dict: dict = dict()

        for i in range(intervals_in_one_day_int):
            now_utc_datetime_plus: datetime.datetime = now_utc_datetime_plus + \
                                                       datetime.timedelta(minutes=interval_minutes_int)
            interval_int: int = utils.get_interval(SeriesType.SeriesType.hour, now_utc_datetime_plus)
            interval_list[interval_int] = 0
            key_str: str = 'interval_' + str(interval_int).zfill(2)
            interval_dict[key_str] = now_utc_datetime_plus
        self.assertTrue(sum(interval_list) == 0)
        interval_sorted_key_list: list = sorted(interval_dict)
        logger.debug(interval_dict)
        for key_str in interval_sorted_key_list:
            logger.debug('[{}]//[{}]'.format(key_str, interval_dict[key_str]))
        self.assertTrue('00' in interval_sorted_key_list[0])

    def test_timeseries_not_implemented(self):
        now_utc_datetime: datetime.datetime = datetime.datetime.now(timezone.utc)
        logger.debug('Now in UTC: {}'.format(now_utc_datetime))
        now_utc_datetime_plus = now_utc_datetime

        try:
            utils.get_interval(SeriesType.SeriesType.test, now_utc_datetime_plus)
            self.fail()
        except SystemError as ex:
            logger.info('Exception catched: {}'.format(ex))

    def test_get_intervals(self):
        # TODO Implement method first
        date_time_str = '12/03/23 00:00:00'
        test_datetime: datetime = datetime.datetime.strptime(date_time_str, '%d/%m/%y %H:%M:%S').date()
        logger.debug('Test datetime: {}'.format(test_datetime))
        # Hourly # TODO method not implemented to support different types of intervals
        result_list: list = utils.get_intervals(SeriesType.SeriesType.hour, test_datetime,  tz.tzlocal())
        logger.debug(len(result_list))



def tests():
    test_suite = unittest.TestSuite()
    test_suite.addTests(unittest.TestLoader().loadTestsFromTestCase(TestUtils))
    return test_suite


if __name__ == "__main__":
    unittest.TextTestRunner(verbosity=2).run(tests())
