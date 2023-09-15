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

from src.sdk.python.rtdip_sdk.connectors import PYODBCSQLConnection
from src.sdk.python.rtdip_sdk.connectors import PYODBCSQLCursor
import pandas as pd
from pytest_mock import MockerFixture
import pytest

DRIVER_PATH = "sql/driver/mock-path"
SERVER_HOSTNAME = "mock.cloud.databricks.com"
ACCESS_TOKEN = "mock_databricks_token"
HTTP_PATH = "sql/mock/mock-test"

PYODBC_CONNECT = "pyodbc.connect"
PYODBC_CONNECT_CURSOR = "pyodbc.connect.cursor"
CURSOR_DESCIPTION = "cursor.description"


class PMockedDBConnection:
    def __init__(self) -> None:
        self.open = True

    def close(self) -> None:
        return None

    def cursor(self) -> object:
        return PMockedCursor()


class PMockedCursor:
    def execute(self, query) -> None:
        return None

    def fetchall(self) -> pd.DataFrame:
        return pd.DataFrame

    def close(self) -> None:
        return None


def test_connection_close(mocker: MockerFixture):
    mocker.patch(PYODBC_CONNECT, return_value=PMockedDBConnection())
    mocked_close = mocker.spy(PMockedDBConnection, "close")

    pmocked_connection = PYODBCSQLConnection(
        DRIVER_PATH, SERVER_HOSTNAME, ACCESS_TOKEN, HTTP_PATH
    )
    pmocked_connection.close()

    mocked_close.assert_called_once()


def test_connection_cursor(mocker: MockerFixture):
    mocker.patch(PYODBC_CONNECT, return_value=PMockedDBConnection())
    pmocked_cursor = mocker.spy(PMockedDBConnection, "cursor")

    pmocked_connection = PYODBCSQLConnection(
        DRIVER_PATH, SERVER_HOSTNAME, ACCESS_TOKEN, HTTP_PATH
    )
    result = pmocked_connection.cursor()

    assert isinstance(result, object)
    pmocked_cursor.assert_called_once()


def test_cursor_execute(mocker: MockerFixture):
    mocked_execute = mocker.spy(PMockedCursor, "execute")

    pmocked_cursor = PYODBCSQLCursor(PMockedCursor())
    pmocked_cursor.execute("test")

    mocked_execute.assert_called_with(mocker.ANY, query="test")


def test_cursor_fetch_all(mocker: MockerFixture):
    mocker.patch.object(PMockedCursor, "fetchall", return_value=[("1", "2", "3", "4")])

    foo = PMockedCursor()
    foo.description = (
        ("column_name_1", 0, 1, 2),
        ("column_name_2", 2, 3, 4),
        ("column_name_3", 4, 5, 6),
        ("column_name_4", 6, 7, 8),
    )

    pmocked_cursor = PYODBCSQLCursor(foo)
    result = pmocked_cursor.fetch_all()

    assert isinstance(result, pd.DataFrame)
    assert len(result.columns) == 4


def test_cursor_close(mocker: MockerFixture):
    mocked_close = mocker.spy(PMockedCursor, "close")

    pmocked_cursor = PYODBCSQLCursor(PMockedCursor())
    pmocked_cursor.close()

    mocked_close.assert_called_once()


def test_connection_close_fails(mocker: MockerFixture):
    mocker.patch(PYODBC_CONNECT, return_value=PMockedDBConnection)
    mocker.patch.object(PMockedDBConnection, "close", side_effect=Exception)

    mocked_connection = PYODBCSQLConnection(
        DRIVER_PATH, SERVER_HOSTNAME, ACCESS_TOKEN, HTTP_PATH
    )

    with pytest.raises(Exception):
        mocked_connection.close()


def test_connection_cursor_fails(mocker: MockerFixture):
    mocker.patch(PYODBC_CONNECT, return_value=PMockedDBConnection())
    mocker.patch.object(PMockedDBConnection, "cursor", side_effect=Exception)

    mocked_connection = PYODBCSQLConnection(
        DRIVER_PATH, SERVER_HOSTNAME, ACCESS_TOKEN, HTTP_PATH
    )

    with pytest.raises(Exception):
        mocked_connection.cursor()


def test_cursor_execute_fails(mocker: MockerFixture):
    mocker.patch.object(PMockedCursor, "execute", side_effect=Exception)

    mocked_cursor = PYODBCSQLCursor(PMockedCursor())

    with pytest.raises(Exception):
        assert mocked_cursor.execute("test")


def test_cursor_fetch_all_fails(mocker: MockerFixture):
    mocker.patch.object(PMockedCursor, "fetchall", side_effect=Exception)

    mocked_cursor = PYODBCSQLCursor(PMockedCursor())

    with pytest.raises(Exception):
        assert mocked_cursor.fetch_all()


def test_cursor_close_fails(mocker: MockerFixture):
    mocker.patch.object(PMockedCursor, "close", side_effect=Exception)

    mocked_cursor = PYODBCSQLCursor(PMockedCursor())

    with pytest.raises(Exception):
        assert mocked_cursor.close()
