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

from src.sdk.python.rtdip_sdk.connectors import SparkConnection, SparkCursor
import pandas as pd
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture
import pytest


def test_connection_cursor(spark_session: SparkSession, mocker: MockerFixture):
    spark_cursor = mocker.spy(SparkConnection, "cursor")

    mocked_connection = SparkConnection(spark=spark_session)
    result = mocked_connection.cursor()

    assert isinstance(result, object)
    spark_cursor.assert_called_once()


def test_cursor_execute(spark_session: SparkSession, mocker: MockerFixture):
    spark_execute = mocker.spy(SparkCursor, "execute")

    cursor = SparkCursor(SparkConnection(spark=spark_session).connection)
    cursor.execute("select 1 as value")

    spark_execute.assert_called_with(mocker.ANY, query="select 1 as value")


def test_cursor_fetch_all(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch.object(
        SparkCursor,
        "fetch_all",
        return_value=pd.DataFrame(
            [
                {
                    "column_name_1": "1",
                    "column_name_2": "2",
                    "column_name_3": "3",
                    "column_name_4": "4",
                }
            ]
        ),
    )

    cursor = SparkCursor(SparkConnection(spark=spark_session).connection)
    cursor.execute("select 1 as value union select 2 as value")
    result = cursor.fetch_all()

    assert isinstance(result, pd.DataFrame)


def test_cursor_close(spark_session: SparkSession, mocker: MockerFixture):
    spy_close = mocker.spy(SparkCursor, "close")

    cursor = SparkCursor(SparkConnection(spark=spark_session).connection)
    cursor.close()

    spy_close.assert_called_once()


def test_connection_close_fails(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch.object(SparkConnection, "close", side_effect=Exception)

    mocked_connection = SparkConnection(spark=spark_session)

    with pytest.raises(Exception):
        mocked_connection.close()


def test_connection_cursor_fails(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch.object(SparkConnection, "cursor", side_effect=Exception)

    mocked_connection = SparkConnection(spark=spark_session)

    with pytest.raises(Exception):
        mocked_connection.cursor()


def test_cursor_execute_fails(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch.object(SparkCursor, "execute", side_effect=Exception)

    mocked_cursor = SparkCursor(SparkConnection(spark=spark_session).connection)

    with pytest.raises(Exception):
        mocked_cursor.execute("test")


def test_cursor_fetch_all_fails(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch.object(SparkCursor, "fetch_all", side_effect=Exception)

    mocked_cursor = SparkCursor(SparkConnection(spark=spark_session).connection)

    with pytest.raises(Exception):
        mocked_cursor.fetch_all()


def test_cursor_close_fails(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch.object(SparkCursor, "close", side_effect=Exception)

    mocked_cursor = SparkCursor(SparkConnection(spark=spark_session).connection)

    with pytest.raises(Exception):
        mocked_cursor.close()
