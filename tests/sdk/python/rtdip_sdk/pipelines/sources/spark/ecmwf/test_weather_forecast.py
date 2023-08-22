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

import json
import pandas as pd
import numpy as np
import os

from pyspark.sql import SparkSession

from src.sdk.python.rtdip_sdk.pipelines.sources.spark.ecmwf.weather_forecast import (
    SparkECMWFWeatherForecastSource,
)


from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.weather import (
    WEATHER_FORECAST_SCHEMA,
)
from pytest_mock import MockerFixture
from pyspark.sql import SparkSession

from unittest.mock import Mock, patch

date_start = "2020-10-01 00:00:00"
date_end = "2020-10-02 00:00:00"
save_path = "/path/to/save"
ecmwf_class = "od"
stream = "oper"
expver = "1"
leveltype = "sfc"
ec_vars = ["10u", "10v"]
np.array(ec_vars)
forecast_area = ([73.5, -27, 33, 45],)  # N/W/S/E
ecmwf_api_key = "1234567890"
ecmwf_api_email = "john.smith@email.com"


def test_get_lead_time(spark_session: SparkSession):
    ws = SparkECMWFWeatherForecastSource(
        spark_session,
        date_start=date_start,
        date_end=date_end,
        save_path=save_path,
        ecmwf_class=ecmwf_class,
        stream=stream,
        expver=expver,
        leveltype=leveltype,
        ec_vars=ec_vars,
        forecast_area=forecast_area,
        ecmwf_api_key=ecmwf_api_key,
        ecmwf_api_email=ecmwf_api_email,
    )
    lead_times = ws._get_lead_time()
    expected_lead_times = [
        0,
        1,
        2,
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        17,
        18,
        19,
        20,
        21,
        22,
        23,
        24,
        25,
        26,
        27,
        28,
        29,
        30,
        31,
        32,
        33,
        34,
        35,
        36,
        37,
        38,
        39,
        40,
        41,
        42,
        43,
        44,
        45,
        46,
        47,
        48,
        49,
        50,
        51,
        52,
        53,
        54,
        55,
        56,
        57,
        58,
        59,
        60,
        61,
        62,
        63,
        64,
        65,
        66,
        67,
        68,
        69,
        70,
        71,
        72,
        73,
        74,
        75,
        76,
        77,
        78,
        79,
        80,
        81,
        82,
        83,
        84,
        85,
        86,
        87,
        88,
        89,
        90,
        93,
        96,
        99,
        102,
        105,
        108,
        111,
        114,
        117,
        120,
        123,
        126,
        129,
        132,
        135,
        138,
        141,
        144,
        150,
        156,
        162,
        168,
        174,
        180,
        186,
        192,
        198,
        204,
        210,
        216,
        222,
        228,
        234,
        240,
    ]
    assert isinstance(lead_times, list)
    assert expected_lead_times == lead_times


def test_get_api_params(spark_session: SparkSession):
    ws = SparkECMWFWeatherForecastSource(
        spark_session,
        date_start=date_start,
        date_end=date_end,
        save_path=save_path,
        ecmwf_class=ecmwf_class,
        stream=stream,
        expver=expver,
        leveltype=leveltype,
        ec_vars=ec_vars,
        forecast_area=forecast_area,
        ecmwf_api_key=ecmwf_api_key,
        ecmwf_api_email=ecmwf_api_email,
    )
    lead_times = ws._get_lead_time()
    params = ws._get_api_params(lead_times)
    expected_params = {
        "class": ecmwf_class,
        "stream": stream,
        "expver": expver,
        "levtype": leveltype,
        "type": "fc",
        "param": ec_vars,
        "step": lead_times,
        "area": forecast_area,
        "grid": [0.1, 0.1],
    }
    assert expected_params == params


def test_read_batch(spark_session: SparkSession):
    weather_source = SparkECMWFWeatherForecastSource(
        spark_session,
        date_start=date_start,
        date_end=date_end,
        save_path=save_path,
        ecmwf_class=ecmwf_class,
        stream=stream,
        expver=expver,
        leveltype=leveltype,
        ec_vars=ec_vars,
        forecast_area=forecast_area,
        ecmwf_api_key=ecmwf_api_key,
        ecmwf_api_email=ecmwf_api_email,
    )
    result = weather_source.read_batch()
    assert isinstance(result, object)
