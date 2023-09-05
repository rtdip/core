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

from ..timeseries import SeriesType
from datetime import date, timezone
from dateutil import tz
import datetime
import secrets
import logging
import string
import random
import logging


type_checks = [
    # (Type, Test)
    (int, int),
    (float, float),
    (date, lambda value: datetime.datetime.strptime(value, "%Y-%m-%d")),
    (date, lambda value: datetime.datetime.strptime(value, "%Y/%m/%d")),
    (date, lambda value: datetime.datetime.strptime(value, "%d/%m/%Y")),
    (
        datetime.datetime,
        lambda value: datetime.datetime.strptime(value, "%Y-%m-%d %H:%M:%S.%f"),
    ),
]


def get_interval(series_type_st: SeriesType, timestamp_datetime: datetime):
    if series_type_st == SeriesType.Minutes15:
        minute_of_the_day_int = get_minute_of_the_day(timestamp_datetime)
        return int(minute_of_the_day_int / 15)
    elif series_type_st == SeriesType.Hour:
        minute_of_the_day_int = get_minute_of_the_day(timestamp_datetime)
        return int(minute_of_the_day_int / 60)
    else:
        error_msg_str: str = "Not implemented for: {}".format(series_type_st)
        raise SystemError(error_msg_str)


def get_utc() -> datetime:
    return datetime.now(timezone.utc)


def get_utc_timestamp() -> float:
    return datetime.datetime.now(timezone.utc).timestamp()


def get_datetime_from_utc_timestamp(timestamp_float: float) -> datetime:
    return datetime.datetime.fromtimestamp(timestamp_float)


def get_minute_of_the_day(timestamp_datetime: datetime):
    hour_int: int = timestamp_datetime.hour
    minute_int: int = timestamp_datetime.minute
    minute_of_the_day_int: int = hour_int * 60 + minute_int
    return minute_of_the_day_int


def generate_random_alpha_num_string(length: int = 8) -> str:
    letters_and_numbers: str = (
        string.ascii_lowercase + string.ascii_uppercase + string.digits
    )
    return "".join(
        secrets.choice(letters_and_numbers) for i in range(length)  # NOSONAR
    )


def generate_random_int_number(min_value: int, max_value: int) -> int:
    return random.randint(min_value, max_value)  # NOSONAR


def get_utc_epoch_timestamp() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def infer_type(value):
    for detected_type, check_if in type_checks:
        try:
            result = check_if(value)
            logging.debug("Result: %s ", result)
            return detected_type
        except ValueError as e:
            logging.exception(e)
            continue
    # We could not find a match.  Default to str type
    return str
