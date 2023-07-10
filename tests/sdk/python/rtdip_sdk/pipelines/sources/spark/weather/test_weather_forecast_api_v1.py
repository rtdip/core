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

import json
import pandas as pd

from src.sdk.python.rtdip_sdk.pipelines.sources.spark.weather import WeatherForecastAPIV1Source
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.weather import WEATHER_FORECAST_SCHEMA
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

configuration = {
    "lat": "32.3667",
    "lon": "-95.4",
    "api_key": "AA",
    "language": "en-US",
    "units": "e"
}

raw_api_response = {
    "metadata": {
        "language": "en-US",
        "transaction_id": "1686945524067:722833eedd2545334406be2bd368acdf",
        "version": "1",
        "latitude": 32.36,
        "longitude": -95.4,
        "units": "e",
        "expire_time_gmt": 1686945840,
        "status_code": 200
    },
    "forecasts": [
        {
            "class": "fod_short_range_hourly",
            "expire_time_gmt": 1686945840,
            "fcst_valid": 1686945600,
            "fcst_valid_local": "2023-06-16T15:00:00-0500",
            "num": 1,
            "day_ind": "D",
            "temp": 91,
            "dewpt": 77,
            "hi": 105,
            "wc": 91,
            "feels_like": 105,
            "icon_extd": 2800,
            "wxman": "wx1230",
            "icon_code": 28,
            "dow": "Friday",
            "phrase_12char": "M Cloudy",
            "phrase_22char": "Mostly Cloudy",
            "phrase_32char": "Mostly Cloudy",
            "subphrase_pt1": "Mostly",
            "subphrase_pt2": "Cloudy",
            "subphrase_pt3": "",
            "pop": 15,
            "precip_type": "rain",
            "qpf": 0.0,
            "snow_qpf": 0.0,
            "rh": 64,
            "wspd": 6,
            "wdir": 233,
            "wdir_cardinal": "SW",
            "gust": None,
            "clds": 79,
            "vis": 10.0,
            "mslp": 29.77,
            "uv_index_raw": 5.65,
            "uv_index": 6,
            "uv_warning": 0,
            "uv_desc": "High",
            "golf_index": 8,
            "golf_category": "Very Good",
            "severity": 1
        },
        {
            "class": "fod_short_range_hourly",
            "expire_time_gmt": 1686945840,
            "fcst_valid": 1686949200,
            "fcst_valid_local": "2023-06-16T16:00:00-0500",
            "num": 2,
            "day_ind": "D",
            "temp": 91,
            "dewpt": 77,
            "hi": 105,
            "wc": 91,
            "feels_like": 105,
            "icon_extd": 2800,
            "wxman": "wx1230",
            "icon_code": 28,
            "dow": "Friday",
            "phrase_12char": "M Cloudy",
            "phrase_22char": "Mostly Cloudy",
            "phrase_32char": "Mostly Cloudy",
            "subphrase_pt1": "Mostly",
            "subphrase_pt2": "Cloudy",
            "subphrase_pt3": "",
            "pop": 15,
            "precip_type": "rain",
            "qpf": 0.0,
            "snow_qpf": 0.0,
            "rh": 63,
            "wspd": 5,
            "wdir": 235,
            "wdir_cardinal": "SW",
            "gust": None,
            "clds": 69,
            "vis": 10.0,
            "mslp": 29.76,
            "uv_index_raw": 5.08,
            "uv_index": 5,
            "uv_warning": 0,
            "uv_desc": "Moderate",
            "golf_index": 8,
            "golf_category": "Very Good",
            "severity": 1
        }
    ]
}

expected_json = {"CLASS": {"0": "fod_short_range_hourly", "1": "fod_short_range_hourly"}, "CLDS": {"0": 79, "1": 69},
                 "DAY_IND": {"0": "D", "1": "D"}, "DEWPT": {"0": 77, "1": 77}, "DOW": {"0": "Friday", "1": "Friday"},
                 "EXPIRE_TIME_GMT": {"0": 1686945840, "1": 1686945840},
                 "FCST_VALID": {"0": 1686945600, "1": 1686949200},
                 "FCST_VALID_LOCAL": {"0": "2023-06-16T15:00:00-0500", "1": "2023-06-16T16:00:00-0500"},
                 "FEELS_LIKE": {"0": 105, "1": 105}, "GOLF_CATEGORY": {"0": "Very Good", "1": "Very Good"},
                 "GOLF_INDEX": {"0": 8.0, "1": 8.0}, "GUST": {"0": None, "1": None}, "HI": {"0": 105, "1": 105},
                 "ICON_CODE": {"0": 28, "1": 28}, "ICON_EXTD": {"0": 2800, "1": 2800}, "MSLP": {"0": 29.77, "1": 29.76},
                 "NUM": {"0": 1, "1": 2}, "PHRASE_12CHAR": {"0": "M Cloudy", "1": "M Cloudy"},
                 "PHRASE_22CHAR": {"0": "Mostly Cloudy", "1": "Mostly Cloudy"},
                 "PHRASE_32CHAR": {"0": "Mostly Cloudy", "1": "Mostly Cloudy"}, "POP": {"0": "15", "1": "15"},
                 "PRECIP_TYPE": {"0": "rain", "1": "rain"}, "QPF": {"0": 0.0, "1": 0.0}, "RH": {"0": 64, "1": 63},
                 "SEVERITY": {"0": 1, "1": 1}, "SNOW_QPF": {"0": 0.0, "1": 0.0},
                 "SUBPHRASE_PT1": {"0": "Mostly", "1": "Mostly"}, "SUBPHRASE_PT2": {"0": "Cloudy", "1": "Cloudy"},
                 "SUBPHRASE_PT3": {"0": "", "1": ""}, "TEMP": {"0": 91, "1": 91},
                 "UV_DESC": {"0": "High", "1": "Moderate"},
                 "UV_INDEX": {"0": 6, "1": 5}, "UV_INDEX_RAW": {"0": 5.65, "1": 5.08}, "UV_WARNING": {"0": 0, "1": 0},
                 "VIS": {"0": 10.0, "1": 10.0}, "WC": {"0": 91, "1": 91}, "WDIR": {"0": 233, "1": 235},
                 "WDIR_CARDINAL": {"0": "SW", "1": "SW"}, "WSPD": {"0": 6, "1": 5},
                 "WXMAN": {"0": "wx1230", "1": "wx1230"}}


def get_api_response() -> str:
    return json.dumps(raw_api_response)


def test_weather_forecast_api_v1_read_setup(spark_session: SparkSession):
    weather_source = WeatherForecastAPIV1Source(spark_session, configuration)

    assert weather_source.system_type().value == 2
    assert weather_source.libraries() == Libraries(maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[])

    assert isinstance(weather_source.settings(), dict)

    assert sorted(weather_source.required_options) == ["api_key", "lat", "lon"]
    assert weather_source.weather_url == "https://api.weather.com/v1/geocode/"
    assert weather_source.pre_read_validation()


def test_weather_forecast_api_v1_params(spark_session: SparkSession):
    weather_source = WeatherForecastAPIV1Source(spark_session, configuration)

    assert weather_source.units == "e"
    assert weather_source.api_key == "AA"
    assert weather_source.language == "en-US"


def test_weather_forecast_api_v1_read_batch(spark_session: SparkSession, mocker: MockerFixture):
    weather_source = WeatherForecastAPIV1Source(spark_session, configuration)

    sample_bytes = bytes(get_api_response().encode("utf-8"))

    class MyResponse:
        content = sample_bytes
        status_code = 200

    def get_response(url: str, params: dict):
        assert url == "https://api.weather.com/v1/geocode/32.3667/-95.4/forecast/hourly/360hour.json"
        assert params == {'apiKey': 'AA', 'units': 'e', 'language': 'en-US'}
        return MyResponse()

    mocker.patch("requests.get", side_effect=get_response)

    df = weather_source.read_batch()

    assert df.count() == 2
    assert isinstance(df, DataFrame)
    assert str(df.schema) == str(WEATHER_FORECAST_SCHEMA)

    pdf = df.toPandas()
    expected_df = pd.DataFrame(expected_json)
    assert str(pdf.to_json()) == str(expected_df.to_json())
