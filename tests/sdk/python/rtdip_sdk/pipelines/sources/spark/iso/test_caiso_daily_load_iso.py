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
import os
import sys
from io import StringIO

import pandas as pd
from requests import HTTPError

sys.path.insert(0, ".")
import pytest
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso import CAISODailyLoadISOSource
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.iso import CAISO_SCHEMA
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

dir_path = os.path.dirname(os.path.realpath(__file__))
area_filter = "TacAreaName = 'AVA'"

iso_configuration = {
    "load_types": ["Total Actual Hourly Integrated Load"],
    "date": "2023-08-13"
}

expected_forecast_data = (
    """StartTime,EndTime,LoadType,OprDt,OprHr,OprInterval,MarketRunId,TacAreaName,Label,XmlDataItem,Pos,Load,ExecutionType,Group
    2023-08-13T01:00:00,2023-08-13T02:00:00,2,2023-08-12,19,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1501.95,2DA,1
    2023-08-13T06:00:00,2023-08-13T07:00:00,2,2023-08-12,24,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1064.66,2DA,1
    2023-08-13T03:00:00,2023-08-13T04:00:00,2,2023-08-12,21,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1366.63,2DA,1
    2023-08-13T02:00:00,2023-08-13T03:00:00,2,2023-08-12,20,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1432.65,2DA,1
    2023-08-13T05:00:00,2023-08-13T06:00:00,2,2023-08-12,23,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1171.19,2DA,1
    2023-08-13T04:00:00,2023-08-13T05:00:00,2,2023-08-12,22,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1282.28,2DA,1
    2023-08-13T00:00:00,2023-08-13T01:00:00,2,2023-08-12,18,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1534.18,2DA,1
    2023-08-13T20:00:00,2023-08-13T21:00:00,2,2023-08-13,14,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1488.17,2DA,208
    2023-08-13T07:00:00,2023-08-13T08:00:00,2,2023-08-13,1,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,983.61,2DA,208
    2023-08-13T10:00:00,2023-08-13T11:00:00,2,2023-08-13,4,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,863.85,2DA,208
    2023-08-13T19:00:00,2023-08-13T20:00:00,2,2023-08-13,13,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1410.13,2DA,208
    2023-08-13T14:00:00,2023-08-13T15:00:00,2,2023-08-13,8,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,931.06,2DA,208
    2023-08-13T08:00:00,2023-08-13T09:00:00,2,2023-08-13,2,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,925.66,2DA,208
    2023-08-13T22:00:00,2023-08-13T23:00:00,2,2023-08-13,16,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1619.68,2DA,208
    2023-08-13T17:00:00,2023-08-13T18:00:00,2,2023-08-13,11,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1237.26,2DA,208
    2023-08-13T15:00:00,2023-08-13T16:00:00,2,2023-08-13,9,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1047.49,2DA,208
    2023-08-13T09:00:00,2023-08-13T10:00:00,2,2023-08-13,3,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,886.21,2DA,208
    2023-08-13T16:00:00,2023-08-13T17:00:00,2,2023-08-13,10,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1151.96,2DA,208
    2023-08-13T21:00:00,2023-08-13T22:00:00,2,2023-08-13,15,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1557.72,2DA,208
    2023-08-13T11:00:00,2023-08-13T12:00:00,2,2023-08-13,5,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,857.6,2DA,208
    2023-08-13T12:00:00,2023-08-13T13:00:00,2,2023-08-13,6,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,855.02,2DA,208
    2023-08-13T13:00:00,2023-08-13T14:00:00,2,2023-08-13,7,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,857.43,2DA,208
    2023-08-13T18:00:00,2023-08-13T19:00:00,2,2023-08-13,12,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1323.88,2DA,208
    2023-08-13T23:00:00,2023-08-14T00:00:00,2,2023-08-13,17,0,2DA,AVA,Demand Forecast 2-Day Ahead,SYS_FCST_2DA_MW,1.8,1665.44,2DA,208
    """
)

expected_actual_data = (
    """StartTime,EndTime,LoadType,OprDt,OprHr,OprInterval,MarketRunId,TacAreaName,Label,XmlDataItem,Pos,Load,ExecutionType,Group
    2023-08-13T02:00:00,2023-08-13T03:00:00,0,2023-08-12,20,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1465.0,ACTUAL,69
    2023-08-13T00:00:00,2023-08-13T01:00:00,0,2023-08-12,18,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1564.0,ACTUAL,69
    2023-08-13T03:00:00,2023-08-13T04:00:00,0,2023-08-12,21,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1382.0,ACTUAL,69
    2023-08-13T04:00:00,2023-08-13T05:00:00,0,2023-08-12,22,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1286.0,ACTUAL,69
    2023-08-13T01:00:00,2023-08-13T02:00:00,0,2023-08-12,19,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1534.0,ACTUAL,69
    2023-08-13T06:00:00,2023-08-13T07:00:00,0,2023-08-12,24,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1065.0,ACTUAL,69
    2023-08-13T05:00:00,2023-08-13T06:00:00,0,2023-08-12,23,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1172.0,ACTUAL,69
    2023-08-13T14:00:00,2023-08-13T15:00:00,0,2023-08-13,8,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1001.0,ACTUAL,276
    2023-08-13T23:00:00,2023-08-14T00:00:00,0,2023-08-13,17,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1736.0,ACTUAL,276
    2023-08-13T12:00:00,2023-08-13T13:00:00,0,2023-08-13,6,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,889.0,ACTUAL,276
    2023-08-13T09:00:00,2023-08-13T10:00:00,0,2023-08-13,3,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,899.0,ACTUAL,276
    2023-08-13T19:00:00,2023-08-13T20:00:00,0,2023-08-13,13,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1460.0,ACTUAL,276
    2023-08-13T17:00:00,2023-08-13T18:00:00,0,2023-08-13,11,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1278.0,ACTUAL,276
    2023-08-13T08:00:00,2023-08-13T09:00:00,0,2023-08-13,2,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,933.0,ACTUAL,276
    2023-08-13T11:00:00,2023-08-13T12:00:00,0,2023-08-13,5,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,878.0,ACTUAL,276
    2023-08-13T10:00:00,2023-08-13T11:00:00,0,2023-08-13,4,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,878.0,ACTUAL,276
    2023-08-13T21:00:00,2023-08-13T22:00:00,0,2023-08-13,15,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1614.0,ACTUAL,276
    2023-08-13T20:00:00,2023-08-13T21:00:00,0,2023-08-13,14,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1541.0,ACTUAL,276
    2023-08-13T16:00:00,2023-08-13T17:00:00,0,2023-08-13,10,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1192.0,ACTUAL,276
    2023-08-13T07:00:00,2023-08-13T08:00:00,0,2023-08-13,1,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,985.0,ACTUAL,276
    2023-08-13T13:00:00,2023-08-13T14:00:00,0,2023-08-13,7,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,924.0,ACTUAL,276
    2023-08-13T18:00:00,2023-08-13T19:00:00,0,2023-08-13,12,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1369.0,ACTUAL,276
    2023-08-13T15:00:00,2023-08-13T16:00:00,0,2023-08-13,9,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1099.0,ACTUAL,276
    2023-08-13T22:00:00,2023-08-13T23:00:00,0,2023-08-13,16,0,ACTUAL,AVA,Total Actual Hourly Integrated Load,SYS_FCST_ACT_MW,3.8,1680.0,ACTUAL,276
    """
)

patch_module_name = "requests.get"


def test_caiso_daily_load_iso_read_setup(spark_session: SparkSession):
    iso_source = CAISODailyLoadISOSource(spark_session, iso_configuration)

    assert iso_source.system_type().value == 2
    assert iso_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )

    assert isinstance(iso_source.settings(), dict)

    assert sorted(iso_source.required_options) == ["date", "load_types"]
    assert iso_source.pre_read_validation()


def test_caiso_daily_load_iso_read_batch_actual(spark_session: SparkSession, mocker: MockerFixture):
    iso_source = CAISODailyLoadISOSource(spark_session, {**iso_configuration,
                                                         "load_types": ["Total Actual Hourly Integrated Load"]})

    with open(f"{dir_path}/data/caiso_daily_load_sample1.zip", "rb") as file:
        sample_bytes = file.read()

    class MyResponse:
        content = sample_bytes
        status_code = 200

    def get_response(url: str):
        assert url.startswith("http://oasis.caiso.com/oasisapi/SingleZip")
        return MyResponse()

    mocker.patch(patch_module_name, side_effect=get_response)

    df = iso_source.read_batch()
    df = df.filter(area_filter)

    assert df.count() == 24
    assert isinstance(df, DataFrame)
    assert str(df.schema) == str(CAISO_SCHEMA)

    expected_df_spark = spark_session.createDataFrame(
        pd.read_csv(StringIO(expected_actual_data), parse_dates=["StartTime", "EndTime"]),
        schema=CAISO_SCHEMA)

    cols = df.columns
    assert df.orderBy(cols).collect() == expected_df_spark.orderBy(cols).collect()


def test_caiso_daily_load_iso_read_batch_forecast(spark_session: SparkSession, mocker: MockerFixture):
    iso_source = CAISODailyLoadISOSource(spark_session, {**iso_configuration,
                                                         "load_types": ["Demand Forecast 2-Day Ahead"]})

    with open(f"{dir_path}/data/caiso_daily_load_sample1.zip", "rb") as file:
        sample_bytes = file.read()

    class MyResponse:
        content = sample_bytes
        status_code = 200

    def get_response(url: str):
        assert url.startswith("http://oasis.caiso.com/oasisapi/SingleZip")
        return MyResponse()

    mocker.patch(patch_module_name, side_effect=get_response)

    df = iso_source.read_batch()
    df = df.filter(area_filter)

    assert df.count() == 24
    assert isinstance(df, DataFrame)
    assert str(df.schema) == str(CAISO_SCHEMA)

    expected_df_spark = spark_session.createDataFrame(
        pd.read_csv(StringIO(expected_forecast_data), parse_dates=["StartTime", "EndTime"]),
        schema=CAISO_SCHEMA)

    cols = df.columns
    assert df.orderBy(cols).collect() == expected_df_spark.orderBy(cols).collect()


def test_caiso_daily_load_iso_iso_fetch_url_fails(
        spark_session: SparkSession, mocker: MockerFixture
):
    base_iso_source = CAISODailyLoadISOSource(spark_session, iso_configuration)
    sample_bytes = bytes("Unknown Error".encode("utf-8"))

    class MyResponse:
        content = sample_bytes
        status_code = 401

    mock_res = MyResponse()
    mocker.patch(patch_module_name, side_effect=lambda url: mock_res)

    with pytest.raises(HTTPError) as exc_info:
        base_iso_source.read_batch()
    expected = ("Unable to access URL `http://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=SLD_FCST"
                "&version=1&startdatetime=20230813T00:00-0000&enddatetime=20230814T00:00-0000`. Received status code "
                "401 with message b'Unknown Error'")
    assert str(exc_info.value) == expected


def test_caiso_daily_load_iso_iso_empty_content(
        spark_session: SparkSession, mocker: MockerFixture
):
    base_iso_source = CAISODailyLoadISOSource(spark_session, iso_configuration)
    sample_bytes = b""

    class MyResponse:
        content = sample_bytes
        status_code = 200

    mock_res = MyResponse()
    mocker.patch(patch_module_name, side_effect=lambda url: mock_res)

    with pytest.raises(Exception) as exc_info:
        base_iso_source.read_batch()
    expected = "Empty Response was returned"
    assert str(exc_info.value) == expected


def test_caiso_daily_load_iso_iso_no_data_in_zip(
        spark_session: SparkSession, mocker: MockerFixture
):
    base_iso_source = CAISODailyLoadISOSource(spark_session, iso_configuration)

    with open(f"{dir_path}/data/caiso_daily_load_sample2.zip", "rb") as file:
        sample_bytes = file.read()

    class MyResponse:
        content = sample_bytes
        status_code = 200

    mock_res = MyResponse()
    mocker.patch(patch_module_name, side_effect=lambda url: mock_res)

    with pytest.raises(Exception) as exc_info:
        base_iso_source.read_batch()
    expected = "No data was found in the specified interval"
    assert str(exc_info.value) == expected


def test_caiso_daily_load_iso_iso_invalid_date(spark_session: SparkSession):
    with pytest.raises(ValueError) as exc_info:
        iso_source = CAISODailyLoadISOSource(
            spark_session, {**iso_configuration, "date": "2023/11/01"}
        )
        iso_source.pre_read_validation()

    expected = "Unable to parse date. Please specify in %Y-%m-%d format."
    assert str(exc_info.value) == expected
