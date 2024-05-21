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

from io import StringIO
from datetime import datetime, timedelta, timezone
import pandas as pd

import pytest
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.iso import (
    MISOHistoricalLoadISOSource,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.iso import MISO_SCHEMA
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import date_format
from pytest_mock import MockerFixture

iso_configuration = {"start_date": "20220401", "end_date": "20220410"}


def get_expected_vals(incr: float = 0.01):
    return [round((i * 10) + incr, 2) for i in range(1, 25)]


def get_miso_raw_df(*args, **kwargs) -> pd.DataFrame:
    """
    To generate raw miso Dataframe as we don't want to hit actual MISO API during the testing.
    Returns: pd.DataFrame
    """
    raw_api_response = (
        "MarketDay,HourEnding,LoadResource Zone,MTLF (MWh),ActualLoad (MWh)\n"
    )

    zones = ["Lrz1", "Lrz2_7", "Lrz3_5", "Lrz4", "Lrz6", "Lrz8_9_10", "Miso"]

    data_row_str = "{date},{hour},{zone},{val}\n"

    dates = pd.date_range("2022-04-01", "2022-04-10", freq="D", inclusive="both")

    for i in range(1, 25):
        for date in dates:
            for zone in zones:
                data_row_val = data_row_str.format(
                    date=date.strftime("%m/%d/%Y"),
                    hour=i,
                    zone=zone,
                    val=(
                        str(i * 10 + 0.05)
                        + ","
                        + (str(i * 10 + 0.01) if date != dates[-1] else "")
                    ),
                )
                raw_api_response = raw_api_response + data_row_val

    return pd.read_csv(StringIO(raw_api_response)).reset_index(drop=True)


def test_miso_historical_load_iso_read_setup(spark_session: SparkSession):
    iso_source = MISOHistoricalLoadISOSource(spark_session, iso_configuration)

    assert iso_source.system_type().value == 2
    assert iso_source.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )

    assert isinstance(iso_source.settings(), dict)

    assert sorted(iso_source.required_options) == ["end_date", "start_date"]
    assert iso_source.pre_read_validation()


# def test_miso_historical_load_iso_read_batch(spark_session: SparkSession, mocker: MockerFixture):
#     iso_source = MISOHistoricalLoadISOSource(spark_session, {**iso_configuration})
#     mocker.patch("pandas.read_excel", side_effect=get_miso_raw_df)

#     df = iso_source.read_batch()

#     assert df.count() == 240
#     assert isinstance(df, DataFrame)
#     assert str(df.schema) == str(MISO_SCHEMA)

#     pdf = df.withColumn("DateTime", date_format("DateTime", "yyyy-MM-dd HH:mm:ss")).toPandas()
#     expected_str = str((get_expected_vals() * 9) + get_expected_vals(incr=0.05))

#     assert str(pdf['Lrz1'].to_list()) == expected_str
#     assert str(pdf['Lrz2_7'].to_list()) == expected_str
#     assert str(pdf['Lrz3_5'].to_list()) == expected_str
#     assert str(pdf['Lrz4'].to_list()) == expected_str
#     assert str(pdf['Lrz6'].to_list()) == expected_str
#     assert str(pdf['Lrz8_9_10'].to_list()) == expected_str
#     assert str(pdf['Miso'].to_list()) == expected_str


# def test_miso_historical_load_iso_read_batch_no_fill(spark_session: SparkSession, mocker: MockerFixture):
#     iso_source = MISOHistoricalLoadISOSource(spark_session, {**iso_configuration, "fill_missing": "false"})
#     mocker.patch("pandas.read_excel", side_effect=get_miso_raw_df)

#     df = iso_source.read_batch()

#     assert df.count() == 216
#     assert isinstance(df, DataFrame)
#     assert str(df.schema) == str(MISO_SCHEMA)

#     pdf = df.withColumn("DateTime", date_format("DateTime", "yyyy-MM-dd HH:mm:ss")).toPandas()
#     expected_str = str(get_expected_vals() * 9)

#     assert str(pdf['Lrz1'].to_list()) == expected_str
#     assert str(pdf['Lrz2_7'].to_list()) == expected_str
#     assert str(pdf['Lrz3_5'].to_list()) == expected_str
#     assert str(pdf['Lrz4'].to_list()) == expected_str
#     assert str(pdf['Lrz6'].to_list()) == expected_str
#     assert str(pdf['Lrz8_9_10'].to_list()) == expected_str
#     assert str(pdf['Miso'].to_list()) == expected_str


def test_miso_historical_load_iso_invalid_dates(spark_session: SparkSession):
    with pytest.raises(ValueError) as exc_info:
        iso_source = MISOHistoricalLoadISOSource(
            spark_session, {**iso_configuration, "start_date": "2023-01-01"}
        )
        iso_source.pre_read_validation()

    assert (
        str(exc_info.value)
        == "Unable to parse Start date. Please specify in YYYYMMDD format."
    )

    with pytest.raises(ValueError) as exc_info:
        iso_source = MISOHistoricalLoadISOSource(
            spark_session, {**iso_configuration, "end_date": "2023-05-01"}
        )
        iso_source.pre_read_validation()

    assert (
        str(exc_info.value)
        == "Unable to parse End date. Please specify in YYYYMMDD format."
    )

    with pytest.raises(ValueError) as exc_info:
        iso_source = MISOHistoricalLoadISOSource(
            spark_session,
            {**iso_configuration, "start_date": "20230301", "end_date": "20230201"},
        )
        iso_source.pre_read_validation()

    assert str(exc_info.value) == "Start date can't be ahead of End date."

    future_date = (datetime.now(timezone.utc) + timedelta(days=10)).strftime("%Y%m%d")

    with pytest.raises(ValueError) as exc_info:
        iso_source = MISOHistoricalLoadISOSource(
            spark_session,
            {**iso_configuration, "start_date": future_date, "end_date": future_date},
        )
        iso_source.pre_read_validation()

    assert str(exc_info.value) == "Start date can't be in future."
