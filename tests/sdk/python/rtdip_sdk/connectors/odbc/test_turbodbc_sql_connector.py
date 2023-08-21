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

from src.sdk.python.rtdip_sdk.connectors import TURBODBCSQLConnection, TURBODBCSQLCursor
from pytest_mock import MockerFixture
import pytest
import pandas as pd
import pyarrow as pa

HOST_NAME = "myHostName"
HTTP_PATH = "myServerAddress"
ACCESS_TOKEN = "myToken"
TURBODBC_CONNECT = (
    "src.sdk.python.rtdip_sdk.connectors.odbc.turbodbc_sql_connector.connect"
)
TURBODBC_CONNECT_CURSOR = (
    "src.sdk.python.rtdip_sdk.connectors.odbc.turbodbc_sql_connector.connect.cursor"
)


class MockedTURBODBCConnection:
    def close(self) -> None:
        return None

    def cursor(self) -> object:
        return MockedTURBODBCCursor()


class MockedTURBODBCCursor:
    def execute(self, query) -> None:
        return None

    def fetchall(self) -> list:
        return list

    def fetchallarrow(self) -> list:
        return pa.Table

    def close(self) -> None:
        return None


def test_connection_close(mocker: MockerFixture):
    mocker.patch(TURBODBC_CONNECT, return_value=MockedTURBODBCConnection())
    mocked_close = mocker.spy(MockedTURBODBCConnection, "close")

    mocked_connection = TURBODBCSQLConnection(HOST_NAME, HTTP_PATH, ACCESS_TOKEN)
    mocked_connection.close()

    mocked_close.assert_called_once()


def test_connection_cursor(mocker: MockerFixture):
    mocker.patch(TURBODBC_CONNECT, return_value=MockedTURBODBCConnection())
    mocked_cursor = mocker.spy(MockedTURBODBCConnection, "cursor")

    mocked_connection = TURBODBCSQLConnection(HOST_NAME, HTTP_PATH, ACCESS_TOKEN)
    result = mocked_connection.cursor()

    assert isinstance(result, object)
    mocked_cursor.assert_called_once()


def test_cursor_execute(mocker: MockerFixture):
    mocked_execute = mocker.spy(MockedTURBODBCCursor, "execute")

    mocked_cursor = TURBODBCSQLCursor(MockedTURBODBCCursor())
    mocked_cursor.execute("test")

    mocked_execute.assert_called_with(mocker.ANY, query="test")


def test_cursor_fetch_all(mocker: MockerFixture):
    mocker.patch.object(
        MockedTURBODBCCursor,
        "fetchallarrow",
        return_value=pa.Table.from_pylist(
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

    foo = MockedTURBODBCCursor()
    foo.description = (
        ("column_name_1", 0, 1, 2),
        ("column_name_2", 2, 3, 4),
        ("column_name_3", 4, 5, 6),
        ("column_name_4", 6, 7, 8),
    )

    mocked_cursor = TURBODBCSQLCursor(foo)
    result = mocked_cursor.fetch_all()

    assert isinstance(result, pd.DataFrame)
    assert len(result.columns) == 4


def test_cursor_close(mocker: MockerFixture):
    mocked_close = mocker.spy(MockedTURBODBCCursor, "close")

    mocked_cursor = TURBODBCSQLCursor(MockedTURBODBCCursor())
    mocked_cursor.close()

    mocked_close.assert_called_once()


def test_connection_close_fails(mocker: MockerFixture):
    mocker.patch(TURBODBC_CONNECT, return_value=MockedTURBODBCConnection())
    mocker.patch.object(MockedTURBODBCConnection, "close", side_effect=Exception)

    mocked_connection = TURBODBCSQLConnection(HOST_NAME, HTTP_PATH, ACCESS_TOKEN)

    with pytest.raises(Exception):
        assert mocked_connection.close()


def test_connection_cursor_fails(mocker: MockerFixture):
    mocker.patch(TURBODBC_CONNECT, return_value=MockedTURBODBCConnection())
    mocker.patch.object(MockedTURBODBCConnection, "cursor", side_effect=Exception)

    mocked_connection = TURBODBCSQLConnection(HOST_NAME, HTTP_PATH, ACCESS_TOKEN)

    with pytest.raises(Exception):
        assert mocked_connection.cursor()


def test_cursor_execute_fails(mocker: MockerFixture):
    mocker.patch.object(MockedTURBODBCCursor, "execute", side_effect=Exception)

    mocked_cursor = TURBODBCSQLCursor(MockedTURBODBCCursor())

    with pytest.raises(Exception):
        assert mocked_cursor.execute("test")


def test_cursor_fetch_all_fails(mocker: MockerFixture):
    mocker.patch.object(MockedTURBODBCCursor, "fetchall", side_effect=Exception)

    mocked_cursor = TURBODBCSQLCursor(MockedTURBODBCCursor())

    with pytest.raises(Exception):
        assert mocked_cursor.fetch_all()


def test_cursor_close_fails(mocker: MockerFixture):
    mocker.patch.object(MockedTURBODBCCursor, "close", side_effect=Exception)

    mocked_cursor = TURBODBCSQLCursor(MockedTURBODBCCursor())

    with pytest.raises(Exception):
        assert mocked_cursor.close()
