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

sys.path.insert(0, ".")
import pandas as pd
import pyarrow as pa
import pytest
from pytest_mock import MockerFixture
from tests.sdk.python.rtdip_sdk.connectors.odbc.test_db_sql_connector import (
    MockedDBConnection,
    MockedCursor,
)
from src.sdk.python.rtdip_sdk.connectors import DatabricksSQLConnection
from src.sdk.python.rtdip_sdk.queries.weather.raw import get_grid as raw_grid
from src.sdk.python.rtdip_sdk.queries.weather.raw import get_point as raw_point

SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "mock_databricks_token"
DATABRICKS_SQL_CONNECT = "databricks.sql.connect"
DATABRICKS_SQL_CONNECT_CURSOR = "databricks.sql.connect.cursor"
INTERPOLATION_METHOD = "test/test/test"
MOCKED_QUERY_POINT = "SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(`EventTime`, 'yyyy-MM-dd HH:mm:ss.SSS')), \"+0000\") AS `EventTime`, `TagName`,  `Status`,  `Value` FROM `mocked-forecast`.`weather`.`mocked-region_weather_mocked-data-security-level_events_mocked-data-type` WHERE `EventTime` BETWEEN to_timestamp(\"2011-01-01T00:00:00+00:00\") AND to_timestamp(\"2011-01-02T23:59:59+00:00\") AND `Latitude` == '0' AND `Longitude` == '0' ORDER BY `TagName`, `EventTime` "
MOCKED_QUERY_GRID = "SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(`EventTime`, 'yyyy-MM-dd HH:mm:ss.SSS')), \"+0000\") AS `EventTime`, `TagName`,  `Status`,  `Value` FROM `mocked-forecast`.`weather`.`mocked-region_weather_mocked-data-security-level_events_mocked-data-type` WHERE `EventTime` BETWEEN to_timestamp(\"2011-01-01T00:00:00+00:00\") AND to_timestamp(\"2011-01-02T23:59:59+00:00\") AND `Latitude` > '0' AND `Latitude` < '0.1' AND `Longitude` > '0' AND`Longitude` < '0.1' ORDER BY `TagName`, `EventTime` "
MOCKED_QUERY_OFFSET_LIMIT = "LIMIT 10 OFFSET 10 "
MOCKED_PARAMETER_DICT_GRID = {
    "forecast": "mocked-forecast",
    "region": "mocked-region",
    "data_security_level": "mocked-data-security-level",
    "data_type": "mocked-data-type",
    "start_date": "2011-01-01",
    "end_date": "2011-01-02",
    "include_bad_data": True,
    "min_lat": 0,
    "max_lat": 0.1,
    "min_lon": 0,
    "max_lon": 0.1,
}

MOCKED_PARAMETER_DICT_POINT = {
    "forecast": "mocked-forecast",
    "region": "mocked-region",
    "data_security_level": "mocked-data-security-level",
    "data_type": "mocked-data-type",
    "start_date": "2011-01-01",
    "end_date": "2011-01-02",
    "include_bad_data": True,
    "lat": 0,
    "lon": 0,
}


def test_raw_point(mocker: MockerFixture):
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_connection_close = mocker.spy(MockedDBConnection, "close")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(
        MockedCursor,
        "fetchall_arrow",
        return_value=pa.Table.from_pandas(
            pd.DataFrame(
                data={
                    "EventTime": [pd.to_datetime("2022-01-01 00:10:00+00:00")],
                    "Status": ["Good"],
                    "Value": [177.09220],
                }
            )
        ),
    )
    mocked_close = mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    actual = raw_point(mocked_connection, MOCKED_PARAMETER_DICT_POINT)

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_QUERY_POINT)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_raw_point_fails(mocker: MockerFixture):
    mocker.spy(MockedDBConnection, "cursor")
    mocker.spy(MockedDBConnection, "close")
    mocker.spy(MockedCursor, "execute")
    mocker.patch.object(MockedCursor, "fetchall_arrow", side_effect=Exception)
    mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    with pytest.raises(Exception):
        raw_point(mocked_connection, MOCKED_PARAMETER_DICT_POINT)


def test_raw_grid(mocker: MockerFixture):
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_connection_close = mocker.spy(MockedDBConnection, "close")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(
        MockedCursor,
        "fetchall_arrow",
        return_value=pa.Table.from_pandas(
            pd.DataFrame(
                data={
                    "EventTime": [pd.to_datetime("2022-01-01 00:10:00+00:00")],
                    "Status": ["Good"],
                    "Value": [177.09220],
                }
            )
        ),
    )
    mocked_close = mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    actual = raw_grid(mocked_connection, MOCKED_PARAMETER_DICT_GRID)

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_QUERY_GRID)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_raw_grid_fails(mocker: MockerFixture):
    mocker.spy(MockedDBConnection, "cursor")
    mocker.spy(MockedDBConnection, "close")
    mocker.spy(MockedCursor, "execute")
    mocker.patch.object(MockedCursor, "fetchall_arrow", side_effect=Exception)
    mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    with pytest.raises(Exception):
        raw_grid(mocked_connection, MOCKED_PARAMETER_DICT_GRID)
