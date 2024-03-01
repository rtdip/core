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

from src.sdk.python.rtdip_sdk.connectors import (
    DatabricksSQLConnection,
    DatabricksSQLCursor,
)
import pandas as pd
import pyarrow as pa
from pytest_mock import MockerFixture
import pytest
import pyarrow as pa

SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "mock_databricks_token"
DATABRICKS_SQL_CONNECT = "databricks.sql.connect"
DATABRICKS_SQL_CONNECT_CURSOR = "databricks.sql.connect.cursor"


class MockedDBConnection:
    def __init__(self):
        self.open = True

    def close(self) -> None:
        return None

    def cursor(self) -> object:
        return MockedCursor()


class MockedCursor:
    def __init__(self):
        self.description = [("EventTime",), ("TagName",), ("Status",), ("Value",)]

    def execute(self, query) -> None:
        return None

    def fetchall(self) -> list:
        return list

    def fetchmany_arrow(self) -> list:
        return pa.Table

    def fetchall_arrow(self) -> list:
        return pa.Table

    def close(self) -> None:
        return None


def test_connection_close(mocker: MockerFixture):
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())
    mocked_close = mocker.spy(MockedDBConnection, "close")

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )
    mocked_connection.close()

    mocked_close.assert_called_once()


def test_connection_cursor(mocker: MockerFixture):
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )
    result = mocked_connection.cursor()

    assert isinstance(result, object)
    mocked_cursor.assert_called_once()


def test_cursor_execute(mocker: MockerFixture):
    mocked_execute = mocker.spy(MockedCursor, "execute")

    mocked_cursor = DatabricksSQLCursor(MockedCursor())
    mocked_cursor.execute("test")

    mocked_execute.assert_called_with(mocker.ANY, query="test")


def test_cursor_fetch_all(mocker: MockerFixture):
    mocker.patch.object(
        MockedCursor,
        "fetchmany_arrow",
        return_value=pa.Table.from_pandas(
            pd.DataFrame(
                data={
                    "EventTime": [pd.to_datetime("2022-01-01 00:10:00+00:00")],
                    "TagName": ["MOCKED-TAGNAME"],
                    "Status": ["Good"],
                    "Value": [177.09220],
                }
            )
        ),
    )

    mocked_cursor = DatabricksSQLCursor(MockedCursor())
    result = mocked_cursor.fetch_all()

    assert isinstance(result, pd.DataFrame)


def test_cursor_close(mocker: MockerFixture):
    mocked_close = mocker.spy(MockedCursor, "close")

    mocked_cursor = DatabricksSQLCursor(MockedCursor())
    mocked_cursor.close()

    mocked_close.assert_called_once()


def test_connection_close_fails(mocker: MockerFixture):
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())
    mocker.patch.object(MockedDBConnection, "close", side_effect=Exception)

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    with pytest.raises(Exception):
        mocked_connection.close()


def test_connection_cursor_fails(mocker: MockerFixture):
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())
    mocker.patch.object(MockedDBConnection, "cursor", side_effect=Exception)

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    with pytest.raises(Exception):
        assert mocked_connection.cursor()


def test_cursor_execute_fails(mocker: MockerFixture):
    mocker.patch.object(MockedCursor, "execute", side_effect=Exception)

    mocked_cursor = DatabricksSQLCursor(MockedCursor())

    with pytest.raises(Exception):
        assert mocked_cursor.execute("test")


def test_cursor_fetch_all_fails(mocker: MockerFixture):
    mocker.patch.object(MockedCursor, "fetchall", side_effect=Exception)

    mocked_cursor = DatabricksSQLCursor(MockedCursor())

    with pytest.raises(Exception):
        assert mocked_cursor.fetch_all()


def test_cursor_close_fails(mocker: MockerFixture):
    mocker.patch.object(MockedCursor, "close", side_effect=Exception)

    mocked_cursor = DatabricksSQLCursor(MockedCursor())

    with pytest.raises(Exception):
        assert mocked_cursor.close()
