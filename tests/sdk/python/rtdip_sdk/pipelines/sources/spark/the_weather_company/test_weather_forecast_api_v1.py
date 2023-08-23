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

from src.sdk.python.rtdip_sdk.pipelines.sources.spark.the_weather_company import (
    SparkWeatherCompanyForecastAPIV1Source,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.weather import (
    WEATHER_FORECAST_SCHEMA,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

configuration = {
    "lat": "32.3667",
    "lon": "-95.4",
    "api_key": "AA",
    "language": "en-US",
    "units": "e",
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
        "status_code": 200,
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
            "severity": 1,
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
            "severity": 1,
        },
    ],
}


expected_json = {
    "Latitude": {"0": 32.3667, "1": 32.3667},
    "Longitude": {"0": -95.4, "1": -95.4},
    "Class": {"0": "fod_short_range_hourly", "1": "fod_short_range_hourly"},
    "ExpireTimeGmt": {"0": 1686945840, "1": 1686945840},
    "FcstValid": {"0": 1686945600, "1": 1686949200},
    "FcstValidLocal": {
        "0": "2023-06-16T15:00:00-0500",
        "1": "2023-06-16T16:00:00-0500",
    },
    "Num": {"0": 1, "1": 2},
    "DayInd": {"0": "D", "1": "D"},
    "Temp": {"0": 91, "1": 91},
    "Dewpt": {"0": 77, "1": 77},
    "Hi": {"0": 105, "1": 105},
    "Wc": {"0": 91, "1": 91},
    "FeelsLike": {"0": 105, "1": 105},
    "IconExtd": {"0": 2800, "1": 2800},
    "Wxman": {"0": "wx1230", "1": "wx1230"},
    "IconCode": {"0": 28, "1": 28},
    "Dow": {"0": "Friday", "1": "Friday"},
    "Phrase12Char": {"0": "M Cloudy", "1": "M Cloudy"},
    "Phrase22Char": {"0": "Mostly Cloudy", "1": "Mostly Cloudy"},
    "Phrase32Char": {"0": "Mostly Cloudy", "1": "Mostly Cloudy"},
    "SubphrasePt1": {"0": "Mostly", "1": "Mostly"},
    "SubphrasePt2": {"0": "Cloudy", "1": "Cloudy"},
    "SubphrasePt3": {"0": "", "1": ""},
    "Pop": {"0": "15", "1": "15"},
    "PrecipType": {"0": "rain", "1": "rain"},
    "Qpf": {"0": 0.0, "1": 0.0},
    "SnowQpf": {"0": 0.0, "1": 0.0},
    "Rh": {"0": 64, "1": 63},
    "Wspd": {"0": 6, "1": 5},
    "Wdir": {"0": 233, "1": 235},
    "WdirCardinal": {"0": "SW", "1": "SW"},
    "Gust": {"0": None, "1": None},
    "Clds": {"0": 79, "1": 69},
    "Vis": {"0": 10.0, "1": 10.0},
    "Mslp": {"0": 29.77, "1": 29.76},
    "UvIndexRaw": {"0": 5.65, "1": 5.08},
    "UvIndex": {"0": 6, "1": 5},
    "UvWarning": {"0": 0, "1": 0},
    "UvDesc": {"0": "High", "1": "Moderate"},
    "GolfIndex": {"0": 8.0, "1": 8.0},
    "GolfCategory": {"0": "Very Good", "1": "Very Good"},
    "Severity": {"0": 1, "1": 1},
}


def get_api_response() -> str:
    return json.dumps(raw_api_response)


def test_weather_forecast_api_v1_read_setup(spark_session: SparkSession):
    weather_source = SparkWeatherCompanyForecastAPIV1Source(
        spark_session, configuration
    )

    assert weather_source.system_type().value == 2
    assert weather_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )

    assert isinstance(weather_source.settings(), dict)

    assert sorted(weather_source.required_options) == ["api_key", "lat", "lon"]
    assert weather_source.weather_url == "https://api.weather.com/v1/geocode/"
    assert weather_source.pre_read_validation()


def test_weather_forecast_api_v1_params(spark_session: SparkSession):
    weather_source = SparkWeatherCompanyForecastAPIV1Source(
        spark_session, configuration
    )

    assert weather_source.units == "e"
    assert weather_source.api_key == "AA"
    assert weather_source.language == "en-US"


def test_weather_forecast_api_v1_read_batch(
    spark_session: SparkSession, mocker: MockerFixture
):
    weather_source = SparkWeatherCompanyForecastAPIV1Source(
        spark_session, configuration
    )

    sample_bytes = bytes(get_api_response().encode("utf-8"))

    class MyResponse:
        content = sample_bytes
        status_code = 200

    def get_response(url: str, params: dict):
        assert (
            url
            == "https://api.weather.com/v1/geocode/32.3667/-95.4/forecast/hourly/360hour.json"
        )
        assert params == {"apiKey": "AA", "units": "e", "language": "en-US"}
        return MyResponse()

    mocker.patch("requests.get", side_effect=get_response)

    df = weather_source.read_batch()

    assert df.count() == 2
    assert isinstance(df, DataFrame)
    assert str(df.schema) == str(WEATHER_FORECAST_SCHEMA)

    pdf = df.toPandas()
    expected_df = pd.DataFrame(expected_json)
    assert str(pdf.to_json()) == str(expected_df.to_json())
