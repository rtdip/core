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
from io import StringIO

import pandas as pd
from requests import HTTPError

sys.path.insert(0, ".")
import pytest
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso import PJMDailyLoadISOSource
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.iso import PJM_SCHEMA
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

iso_configuration = {"load_type": "forecast", "api_key": "SAMPLE"}

raw_api_forecast_response = (
    "evaluated_at_datetime_utc,evaluated_at_datetime_ept,forecast_datetime_beginning_utc,"
    "forecast_datetime_beginning_ept,forecast_datetime_ending_utc,forecast_datetime_ending_ept,forecast_area,"
    "forecast_load_mw\n"
    "7/9/2023 8:17:11 PM,7/9/2023 4:17:11 PM,7/9/2023 4:00:00 AM,7/9/2023 12:00:00 AM,7/9/2023 5:00:00 AM,"
    "7/9/2023 1:00:00 AM,AE/MIDATL,14\n"
    "7/9/2023 8:17:11 PM,7/9/2023 4:17:11 PM,7/9/2023 5:00:00 AM,7/9/2023 1:00:00 AM,7/9/2023 6:00:00 AM,"
    "7/9/2023 2:00:00 AM,AE/MIDATL,104\n"
    "7/9/2023 8:17:11 PM,7/9/2023 4:17:11 PM,7/9/2023 6:00:00 AM,7/9/2023 2:00:00 AM,7/9/2023 7:00:00 AM,"
    "7/9/2023 3:00:00 AM,AE/MIDATL,144\n"
    "7/9/2023 8:17:11 PM,7/9/2023 4:17:11 PM,7/9/2023 7:00:00 AM,7/9/2023 3:00:00 AM,7/9/2023 8:00:00 AM,"
    "7/9/2023 4:00:00 AM,AE/MIDATL,123\n"
    "7/9/2023 8:17:11 PM,7/9/2023 4:17:11 PM,7/9/2023 8:00:00 AM,7/9/2023 4:00:00 AM,7/9/2023 9:00:00 AM,"
    "7/9/2023 5:00:00 AM,AE/MIDATL,23\n"
)

raw_api_actual_response = (
    "datetime_beginning_utc,datetime_beginning_ept,datetime_ending_utc,datetime_ending_ept,generated_at_ept,area,"
    "area_load_forecast,actual_load,dispatch_rate\n"
    "7/8/2023 4:00:00 AM,7/8/2023 12:00:00 AM,7/8/2023 5:00:00 AM,7/8/2023 1:00:00 AM,7/9/2023 8:00:24 AM,AEP,"
    "14.00,14.51,2.61\n"
    "7/8/2023 4:00:00 AM,7/8/2023 12:00:00 AM,7/8/2023 5:00:00 AM,7/8/2023 1:00:00 AM,7/9/2023 8:00:24 AM,AP,54.00,"
    "53.24,2.55\n"
    "7/8/2023 4:00:00 AM,7/8/2023 12:00:00 AM,7/8/2023 5:00:00 AM,7/8/2023 1:00:00 AM,7/9/2023 8:00:24 AM,ATSI,"
    "70.00,69.94,2.42\n"
    "7/8/2023 4:00:00 AM,7/8/2023 12:00:00 AM,7/8/2023 5:00:00 AM,7/8/2023 1:00:00 AM,7/9/2023 8:00:24 AM,ComEd,"
    "10.00,10.06,9.80\n"
)

expected_forecast_data = (
    "StartTime,EndTime,Zone,Load\n"
    "2023-07-09 04:00:00,2023-07-09 05:00:00,AE/MIDATL,14.0\n"
    "2023-07-09 05:00:00,2023-07-09 06:00:00,AE/MIDATL,104.0\n"
    "2023-07-09 06:00:00,2023-07-09 07:00:00,AE/MIDATL,144.0\n"
    "2023-07-09 07:00:00,2023-07-09 08:00:00,AE/MIDATL,123.0\n"
    "2023-07-09 08:00:00,2023-07-09 09:00:00,AE/MIDATL,23.0\n"
)

expected_actual_data = (
    "StartTime,EndTime,Zone,Load\n"
    "2023-07-08 04:00:00,2023-07-08 05:00:00,AEP,14.51\n"
    "2023-07-08 04:00:00,2023-07-08 05:00:00,AP,53.24\n"
    "2023-07-08 04:00:00,2023-07-08 05:00:00,ATSI,69.94\n"
    "2023-07-08 04:00:00,2023-07-08 05:00:00,ComEd,10.06\n"
)

patch_module_name = "requests.get"


def test_pjm_daily_load_iso_read_setup(spark_session: SparkSession):
    iso_source = PJMDailyLoadISOSource(spark_session, iso_configuration)

    assert iso_source.system_type().value == 2
    assert iso_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )

    assert isinstance(iso_source.settings(), dict)

    assert sorted(iso_source.required_options) == ["api_key", "load_type"]
    assert iso_source.pre_read_validation()


# def test_pjm_daily_load_iso_read_batch_forecast(spark_session: SparkSession, mocker: MockerFixture):
#     iso_source = PJMDailyLoadISOSource(spark_session, {**iso_configuration, "load_type": "forecast"})

#     sample_bytes = bytes(raw_api_forecast_response.encode("utf-8"))

#     class MyResponse:
#         content = sample_bytes
#         status_code = 200

#     def get_response(url: str, headers: dict):
#         assert url.startswith("https://api.pjm.com/api/v1/")
#         assert headers == {'Ocp-Apim-Subscription-Key': 'SAMPLE'}
#         return MyResponse()

#     mocker.patch(patch_module_name, side_effect=get_response)

#     df = iso_source.read_batch()

#     assert df.count() == 5
#     assert isinstance(df, DataFrame)
#     assert str(df.schema) == str(PJM_SCHEMA)

#     expected_df_spark = spark_session.createDataFrame(
#         pd.read_csv(StringIO(expected_forecast_data), parse_dates=["StartTime", "EndTime"]),
#         schema=PJM_SCHEMA)

#     cols = df.columns
#     assert df.orderBy(cols).collect() == expected_df_spark.orderBy(cols).collect()


# def test_pjm_daily_load_iso_read_batch_actual(spark_session: SparkSession, mocker: MockerFixture):
#     iso_source = PJMDailyLoadISOSource(spark_session, {**iso_configuration, "load_type": "actual"})

#     sample_bytes = bytes(raw_api_actual_response.encode("utf-8"))

#     class MyResponse:
#         content = sample_bytes
#         status_code = 200

#     def get_response(url: str, headers: dict):
#         assert url.startswith("https://api.pjm.com/api/v1/")
#         assert headers == {'Ocp-Apim-Subscription-Key': 'SAMPLE'}
#         return MyResponse()

#     mocker.patch(patch_module_name, side_effect=get_response)

#     df = iso_source.read_batch()

#     assert df.count() == 4
#     assert isinstance(df, DataFrame)
#     assert str(df.schema) == str(PJM_SCHEMA)

#     expected_df_spark = spark_session.createDataFrame(
#         pd.read_csv(StringIO(expected_actual_data), parse_dates=["StartTime", "EndTime"]),
#         schema=PJM_SCHEMA)

#     cols = df.columns
#     assert df.orderBy(cols).collect() == expected_df_spark.orderBy(cols).collect()


def test_pjm_daily_load_iso_iso_fetch_url_fails(
    spark_session: SparkSession, mocker: MockerFixture
):
    base_iso_source = PJMDailyLoadISOSource(spark_session, iso_configuration)
    sample_bytes = bytes("Unknown Error".encode("utf-8"))

    class MyResponse:
        content = sample_bytes
        status_code = 401

    mock_res = MyResponse()
    mocker.patch(patch_module_name, side_effect=lambda url, headers: mock_res)

    with pytest.raises(HTTPError) as exc_info:
        base_iso_source.read_batch()

    expected = (
        "Unable to access URL `https://api.pjm.com/api/v1/`."
        " Received status code 401 with message b'Unknown Error'"
    )
    assert str(exc_info.value) == expected


def test_pjm_daily_load_iso_invalid_load_type(spark_session: SparkSession):
    with pytest.raises(ValueError) as exc_info:
        iso_source = PJMDailyLoadISOSource(
            spark_session, {**iso_configuration, "load_type": "both"}
        )
        iso_source.pre_read_validation()

    assert (
        str(exc_info.value)
        == "Invalid load_type `both` given. Supported values are ['actual', 'forecast']."
    )
