# Copyright 2022 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

sys.path.insert(0, ".")

from src.sdk.python.rtdip_sdk.data_models.timeseries import SeriesType
from src.sdk.python.rtdip_sdk.data_models.utils import timeseries_utils
from datetime import timezone
import datetime
import logging
import pytest


def test_timeseries_15min():
    now_utc_datetime: datetime.datetime = datetime.datetime.now(timezone.utc)
    now_utc_datetime_plus = now_utc_datetime
    # Generate 96 Intervals of 15 min in 24h
    interval_minutes_int = 15
    intervals_in_one_day_int: int = int((24 * 60) / interval_minutes_int)
    arr_list: list = [1 for i in range(intervals_in_one_day_int)]  # NOSONAR
    interval_list: list = list()
    for i in range(intervals_in_one_day_int):  # NOSONAR
        now_utc_datetime_plus: datetime.datetime = (
            now_utc_datetime_plus + datetime.timedelta(minutes=interval_minutes_int)
        )
        interval_int: int = timeseries_utils.get_interval(
            SeriesType.Minutes15, now_utc_datetime_plus
        )
        arr_list[interval_int] = 0
        interval_list.append("interval_" + str(interval_int).zfill(2))
    assert sum(arr_list) == 0
    interval_sorted_list = sorted(interval_list)
    assert "00" in interval_sorted_list[0]


def test_timeseries_1h():
    now_utc_datetime: datetime.datetime = datetime.datetime.now(timezone.utc)
    now_utc_datetime_plus = now_utc_datetime
    # Generate 24 Intervals of 1h in 24h
    interval_minutes_int = 60
    intervals_in_one_day_int: int = 24
    interval_list: list = [1 for i in range(intervals_in_one_day_int)]  # NOSONAR
    interval_dict: dict = dict()

    for i in range(intervals_in_one_day_int):  # NOSONAR
        now_utc_datetime_plus: datetime.datetime = (
            now_utc_datetime_plus + datetime.timedelta(minutes=interval_minutes_int)
        )
        interval_int: int = timeseries_utils.get_interval(
            SeriesType.Hour, now_utc_datetime_plus
        )
        interval_list[interval_int] = 0
        key_str: str = "interval_" + str(interval_int).zfill(2)
        interval_dict[key_str] = now_utc_datetime_plus
    assert sum(interval_list) == 0
    interval_sorted_key_list: list = sorted(interval_dict)
    logging.debug(interval_dict)
    for key_str in interval_sorted_key_list:
        logging.debug("[{}]//[{}]".format(key_str, interval_dict[key_str]))
    assert "00" in interval_sorted_key_list[0]


def test_timeseries_not_implemented():
    now_utc_datetime: datetime.datetime = datetime.datetime.now(timezone.utc)
    logging.debug("Now in UTC: {}".format(now_utc_datetime))
    now_utc_datetime_plus = now_utc_datetime

    try:
        timeseries_utils.get_interval(SeriesType.Test, now_utc_datetime_plus)
        pytest.fail()
    except SystemError as ex:
        logging.info("Exception catched: {}".format(ex))
