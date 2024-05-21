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
from datetime import datetime, timedelta, timezone
import sys
from io import StringIO

import pandas as pd
from requests import HTTPError

sys.path.insert(0, ".")
import pytest
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso import (
    PJMHistoricalLoadISOSource,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.iso import PJM_SCHEMA
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pyspark.sql import DataFrame, SparkSession
from pytest_mock import MockerFixture

iso_configuration = {
    "start_date": "2021-12-09",
    "end_date": "2022-06-10",
    "api_key": "SAMPLE",
}

raw_api_response = (
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

expected_data = (
    "StartTime,EndTime,Zone,Load\n"
    "2023-07-08 04:00:00,2023-07-08 05:00:00,AEP,14.51\n"
    "2023-07-08 04:00:00,2023-07-08 05:00:00,AP,53.24\n"
    "2023-07-08 04:00:00,2023-07-08 05:00:00,ATSI,69.94\n"
    "2023-07-08 04:00:00,2023-07-08 05:00:00,ComEd,10.06\n"
    "2023-07-08 04:00:00,2023-07-08 05:00:00,AEP,14.51\n"
    "2023-07-08 04:00:00,2023-07-08 05:00:00,AP,53.24\n"
    "2023-07-08 04:00:00,2023-07-08 05:00:00,ATSI,69.94\n"
    "2023-07-08 04:00:00,2023-07-08 05:00:00,ComEd,10.06\n"
)


def test_pjm_historical_load_iso_read_setup(spark_session: SparkSession):
    iso_source = PJMHistoricalLoadISOSource(spark_session, iso_configuration)

    assert iso_source.system_type().value == 2
    assert iso_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )

    assert isinstance(iso_source.settings(), dict)

    assert sorted(iso_source.required_options) == ["api_key", "end_date", "start_date"]
    assert iso_source.pre_read_validation()


# def test_pjm_historical_load_iso_read_batch_actual(spark_session: SparkSession, mocker: MockerFixture):
#     iso_source = PJMHistoricalLoadISOSource(spark_session, {**iso_configuration, "load_type": "actual"})

#     sample_bytes = bytes(raw_api_response.encode("utf-8"))
#     expected_urls = ["https://api.pjm.com/api/v1/ops_sum_prev_period?startRow=1&datetime_beginning_ept=2021-12-09 "
#                      "00:00to2022-04-07 23:00&format=csv&download=true",
#                      "https://api.pjm.com/api/v1/ops_sum_prev_period?startRow=1&datetime_beginning_ept=2022-04-08 "
#                      "00:00to2022-06-10 23:00&format=csv&download=true"]

#     class MyResponse:
#         content = sample_bytes
#         status_code = 200
#         url_index = 0

#     def get_response(url: str, headers: dict):
#         assert url == expected_urls[MyResponse.url_index]
#         MyResponse.url_index += 1
#         return MyResponse()

#     mocker.patch("requests.get", side_effect=get_response)

#     df = iso_source.read_batch()

#     assert df.count() == 8
#     assert isinstance(df, DataFrame)
#     assert str(df.schema) == str(PJM_SCHEMA)

#     expected_df_spark = spark_session.createDataFrame(
#         pd.read_csv(StringIO(expected_data), parse_dates=["StartTime", "EndTime"]),
#         schema=PJM_SCHEMA)

#     cols = df.columns
#     assert df.orderBy(cols).collect() == expected_df_spark.orderBy(cols).collect()


def test_miso_historical_load_iso_invalid_dates(spark_session: SparkSession):
    with pytest.raises(ValueError) as exc_info:
        iso_source = PJMHistoricalLoadISOSource(
            spark_session, {**iso_configuration, "start_date": "2021/01/01"}
        )
        iso_source.pre_read_validation()

    assert (
        str(exc_info.value)
        == "Unable to parse Start date. Please specify in %Y-%m-%d format."
    )

    with pytest.raises(ValueError) as exc_info:
        iso_source = PJMHistoricalLoadISOSource(
            spark_session, {**iso_configuration, "end_date": "20230501"}
        )
        iso_source.pre_read_validation()

    assert (
        str(exc_info.value)
        == "Unable to parse End date. Please specify in %Y-%m-%d format."
    )

    with pytest.raises(ValueError) as exc_info:
        iso_source = PJMHistoricalLoadISOSource(
            spark_session,
            {**iso_configuration, "start_date": "2023-03-01", "end_date": "2023-02-01"},
        )
        iso_source.pre_read_validation()

    assert str(exc_info.value) == "Start date can't be ahead of End date."

    future_date = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y-%m-%d")

    with pytest.raises(ValueError) as exc_info:
        iso_source = PJMHistoricalLoadISOSource(
            spark_session,
            {**iso_configuration, "start_date": future_date, "end_date": future_date},
        )
        iso_source.pre_read_validation()

    assert str(exc_info.value) == "Start date can't be in future."

    with pytest.raises(ValueError) as exc_info:
        iso_source = PJMHistoricalLoadISOSource(
            spark_session,
            {**iso_configuration, "start_date": "2023-01-01", "end_date": future_date},
        )
        iso_source.pre_read_validation()

    assert str(exc_info.value) == "End date can't be in future."


def test_miso_historical_load_iso_invalid_sleep_duration(spark_session: SparkSession):
    with pytest.raises(ValueError) as exc_info:
        iso_source = PJMHistoricalLoadISOSource(
            spark_session, {**iso_configuration, "sleep_duration": -10}
        )
        iso_source.pre_read_validation()
    assert str(exc_info.value) == "Sleep duration can't be negative."


def test_miso_historical_load_iso_invalid_request_count(spark_session: SparkSession):
    with pytest.raises(ValueError) as exc_info:
        iso_source = PJMHistoricalLoadISOSource(
            spark_session, {**iso_configuration, "request_count": -20}
        )
        iso_source.pre_read_validation()
    assert str(exc_info.value) == "Request count can't be negative."


def test_miso_historical_load_iso_invalid_query_batch_days(spark_session: SparkSession):
    with pytest.raises(ValueError) as exc_info:
        iso_source = PJMHistoricalLoadISOSource(
            spark_session, {**iso_configuration, "query_batch_days": -3}
        )
        iso_source.pre_read_validation()
    assert str(exc_info.value) == "Query batch days count can't be negative."
