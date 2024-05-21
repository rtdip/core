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

sys.path.insert(0, ".")
import pytest
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso import MISODailyLoadISOSource
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.iso import MISO_SCHEMA
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import date_format
from pytest_mock import MockerFixture

iso_configuration = {"load_type": "actual", "date": "20230501"}


def get_expected_vals(incr: float = 0.01):
    return [round((i * 10) + incr, 2) for i in range(1, 25)]


def get_miso_raw_df(*args, **kwargs) -> pd.DataFrame:
    """
    To generate raw miso Dataframe as we don't want to hit actual MISO API during the testing.
    Returns: pd.DataFrame

    """
    raw_api_response = (
        "Market Day,HourEnding,LRZ1 MTLF (MWh),LRZ1 ActualLoad (MWh),LRZ2_7 MTLF (MWh),"
        "LRZ2_7 ActualLoad (MWh),LRZ3_5 MTLF (MWh),LRZ3_5 ActualLoad (MWh),LRZ4 MTLF (MWh),"
        "LRZ4 ActualLoad (MWh),LRZ6 MTLF (MWh),LRZ6 ActualLoad (MWh),LRZ8_9_10 MTLF (MWh),"
        "LRZ8_9_10 ActualLoad (MWh),MISO MTLF (MWh),MISO ActualLoad (MWh)\n"
        '"April 30, 2023",,,,,,,,,,,,,,,\n'
        "10.01,10.05,10.01\n"
    )

    data_row_str = "2023-04-30 00:00:00,{hour},{val}\n"

    for i in range(1, 25):
        data_row_val = data_row_str.format(
            hour=i, val=(",".join([str(i * 10 + 0.05) + "," + str(i * 10 + 0.01)] * 7))
        )
        raw_api_response = raw_api_response + data_row_val

    raw_df = pd.read_csv(StringIO(raw_api_response)).reset_index(drop=True)

    return raw_df


def test_miso_daily_load_iso_read_setup(spark_session: SparkSession):
    iso_source = MISODailyLoadISOSource(spark_session, iso_configuration)

    assert iso_source.system_type().value == 2
    assert iso_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )

    assert isinstance(iso_source.settings(), dict)

    assert sorted(iso_source.required_options) == ["date", "load_type"]
    assert iso_source.pre_read_validation()


# def test_miso_daily_load_iso_read_batch_actual(spark_session: SparkSession, mocker: MockerFixture):
#     iso_source = MISODailyLoadISOSource(spark_session, {**iso_configuration, "load_type": "actual"})
#     mocker.patch("pandas.read_excel", side_effect=get_miso_raw_df)

#     df = iso_source.read_batch()

#     assert df.count() == 24
#     assert isinstance(df, DataFrame)
#     assert str(df.schema) == str(MISO_SCHEMA)

#     pdf = df.withColumn("DateTime", date_format("DateTime", "yyyy-MM-dd HH:mm:ss")).toPandas()
#     expected_str = str(get_expected_vals())

#     assert str(pdf['Lrz1'].to_list()) == expected_str
#     assert str(pdf['Lrz2_7'].to_list()) == expected_str
#     assert str(pdf['Lrz3_5'].to_list()) == expected_str
#     assert str(pdf['Lrz4'].to_list()) == expected_str
#     assert str(pdf['Lrz6'].to_list()) == expected_str
#     assert str(pdf['Lrz8_9_10'].to_list()) == expected_str
#     assert str(pdf['Miso'].to_list()) == expected_str


# def test_miso_daily_load_iso_read_batch_forecast(spark_session: SparkSession, mocker: MockerFixture):
#     iso_source = MISODailyLoadISOSource(spark_session, {**iso_configuration, "load_type": "forecast"})
#     mocker.patch("pandas.read_excel", side_effect=get_miso_raw_df)

#     df = iso_source.read_batch()

#     assert df.count() == 24
#     assert isinstance(df, DataFrame)
#     assert str(df.schema) == str(MISO_SCHEMA)

#     pdf = df.withColumn("DateTime", date_format("DateTime", "yyyy-MM-dd HH:mm:ss")).toPandas()
#     expected_str = str(get_expected_vals(incr=0.05))

#     assert str(pdf['Lrz1'].to_list()) == expected_str
#     assert str(pdf['Lrz2_7'].to_list()) == expected_str
#     assert str(pdf['Lrz3_5'].to_list()) == expected_str
#     assert str(pdf['Lrz4'].to_list()) == expected_str
#     assert str(pdf['Lrz6'].to_list()) == expected_str
#     assert str(pdf['Lrz8_9_10'].to_list()) == expected_str
#     assert str(pdf['Miso'].to_list()) == expected_str


def test_miso_daily_load_iso_invalid_load_type(spark_session: SparkSession):
    with pytest.raises(ValueError) as exc_info:
        iso_source = MISODailyLoadISOSource(
            spark_session, {**iso_configuration, "load_type": "both"}
        )
        iso_source.pre_read_validation()

    assert (
        str(exc_info.value)
        == "Invalid load_type `both` given. Supported values are ['actual', 'forecast']."
    )


def test_miso_daily_load_iso_invalid_date_format(spark_session: SparkSession):
    with pytest.raises(ValueError) as exc_info:
        iso_source = MISODailyLoadISOSource(
            spark_session, {**iso_configuration, "date": "2023-01-01"}
        )
        iso_source.pre_read_validation()

    assert (
        str(exc_info.value)
        == "Unable to parse Date. Please specify in YYYYMMDD format."
    )


def test_miso_daily_load_iso_invalid_date(spark_session: SparkSession):
    future_date = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y%m%d")

    with pytest.raises(ValueError) as exc_info:
        iso_source = MISODailyLoadISOSource(
            spark_session, {**iso_configuration, "date": future_date}
        )
        iso_source.pre_read_validation()

    assert str(exc_info.value) == "Query date can't be in future."
