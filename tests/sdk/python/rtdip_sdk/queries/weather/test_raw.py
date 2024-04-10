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
from src.sdk.python.rtdip_sdk.queries.weather.raw import (
    get_grid as raw_grid,
)
from src.sdk.python.rtdip_sdk.queries.weather.raw import (
    get_point as raw_point,
)

SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "mock_databricks_token"
DATABRICKS_SQL_CONNECT = "databricks.sql.connect"
DATABRICKS_SQL_CONNECT_CURSOR = "databricks.sql.connect.cursor"
INTERPOLATION_METHOD = "test/test/test"
MOCKED_QUERY_GRID = 'SELECT * FROM `forecast`.`weather`.`mock_region_mock_security_events_mock_data_type` WHERE (`EventTime` BETWEEN to_timestamp("2024-01-01") AND to_timestamp("2024-01-03")) AND (`EnqueuedTime` BETWEEN to_timestamp("2023-12-28") AND to_timestamp("2023-12-31")) AND `Latitude` > 36 AND `Latitude` < 38 AND `Longitude` > -109.1 AND `Longitude` < -107.1 ORDER BY `TagName` '
MOCKED_QUERY_POINT = 'SELECT * FROM `forecast`.`weather`.`mock_region_mock_security_events_mock_data_type` WHERE (`EventTime` BETWEEN to_timestamp("2024-01-01") AND to_timestamp("2024-01-03")) AND (`EnqueuedTime` BETWEEN to_timestamp("2023-12-28") AND to_timestamp("2023-12-31")) AND `Latitude` == 37 AND `Longitude` == -108.1 ORDER BY `TagName` '
MOCKED_QUERY_OFFSET_LIMIT = "LIMIT 10 OFFSET 10 "


MOCKED_PARAMETER_DICT_GRID = {
    "forecast": "forecast",
    "forecast_type": "weather",
    "region": "mock_region",
    "data_security_level": "mock_security",
    "data_type": "mock_data_type",
    "min_lat": 36,
    "max_lat": 38,
    "min_lon": -109.1,
    "max_lon": -107.1,
    "start_date": "2024-01-01",
    "end_date": "2024-01-03",
    "forecast_run_start_date": "2023-12-28",
    "forecast_run_end_date": "2023-12-31",
    "timestamp_column": "EventTime",
    "forecast_run_timestamp_column": "EnqueuedTime",
}

MOCKED_PARAMETER_DICT_POINT = {
    "forecast": "forecast",
    "forecast_type": "weather",
    "region": "mock_region",
    "data_security_level": "mock_security",
    "data_type": "mock_data_type",
    "lat": 37,
    "lon": -108.1,
    "start_date": "2024-01-01",
    "end_date": "2024-01-03",
    "forecast_run_start_date": "2023-12-28",
    "forecast_run_end_date": "2023-12-31",
    "timestamp_column": "EventTime",
    "forecast_run_timestamp_column": "EnqueuedTime",
}

MOCKED_PARAMETER_DICT_GRID_SOURCE = {
    "source": "forecast`.`weather`.`mock_region_mock_security_events_mock_data_type",
    "min_lat": 36,
    "max_lat": 38,
    "min_lon": -109.1,
    "max_lon": -107.1,
    "start_date": "2024-01-01",
    "end_date": "2024-01-03",
    "forecast_run_start_date": "2023-12-28",
    "forecast_run_end_date": "2023-12-31",
    "timestamp_column": "EventTime",
    "forecast_run_timestamp_column": "EnqueuedTime",
}

MOCKED_PARAMETER_DICT_POINT_SOURCE = {
    "source": "forecast`.`weather`.`mock_region_mock_security_events_mock_data_type",
    "lat": 37,
    "lon": -108.1,
    "start_date": "2024-01-01",
    "end_date": "2024-01-03",
    "forecast_run_start_date": "2023-12-28",
    "forecast_run_end_date": "2023-12-31",
    "timestamp_column": "EventTime",
    "forecast_run_timestamp_column": "EnqueuedTime",
}


def test_raw_grid_(mocker: MockerFixture):
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_connection_close = mocker.spy(MockedDBConnection, "close")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(
        MockedCursor,
        "fetchmany_arrow",
        return_value=pa.Table.from_pandas(
            pd.DataFrame(data={"m": [1], "n": [2], "o": [3], "p": [4]})
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


def test_raw_grid_source(mocker: MockerFixture):
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_connection_close = mocker.spy(MockedDBConnection, "close")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(
        MockedCursor,
        "fetchmany_arrow",
        return_value=pa.Table.from_pandas(
            pd.DataFrame(data={"a": [1], "d": [2], "e": [3], "r": [4]})
        ),
    )
    mocked_close = mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    actual = raw_grid(mocked_connection, MOCKED_PARAMETER_DICT_GRID_SOURCE)

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
    mocker.patch.object(MockedCursor, "fetchmany_arrow", side_effect=Exception)
    mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    with pytest.raises(Exception):
        raw_grid(mocked_connection, MOCKED_PARAMETER_DICT_GRID)


def test_raw_point(mocker: MockerFixture):
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_connection_close = mocker.spy(MockedDBConnection, "close")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(
        MockedCursor,
        "fetchmany_arrow",
        return_value=pa.Table.from_pandas(
            pd.DataFrame(data={"q": [1], "r": [2], "s": [3], "t": [4]})
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


def test_raw_point_source(mocker: MockerFixture):
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_connection_close = mocker.spy(MockedDBConnection, "close")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(
        MockedCursor,
        "fetchmany_arrow",
        return_value=pa.Table.from_pandas(
            pd.DataFrame(data={"u": [1], "v": [2], "w": [3], "x": [4]})
        ),
    )
    mocked_close = mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    actual = raw_point(mocked_connection, MOCKED_PARAMETER_DICT_POINT_SOURCE)

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
    mocker.patch.object(MockedCursor, "fetchmany_arrow", side_effect=Exception)
    mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    with pytest.raises(Exception):
        raw_point(mocked_connection, MOCKED_PARAMETER_DICT_POINT)
