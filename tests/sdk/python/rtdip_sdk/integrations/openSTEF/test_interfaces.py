# Copyright 2023 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys

sys.path.insert(0, ".")
import pandas as pd
import pytest
import sqlalchemy
from src.sdk.python.rtdip_sdk.integrations.openstef.interfaces import _DataInterface
from openstef_dbc import Singleton
from pytest_mock import MockerFixture
from pydantic.v1 import BaseSettings
from typing import Union


QUERY_EXECUTION_ERROR_MSG = "Error occured during executing query"
SAMPLE_SQL_QUERY = "SELECT * FROM test_table"
PANDAS_DATAFRAME_PATH = "pandas.DataFrame"
CREATE_ENGINE_PATH = "sqlalchemy.engine.create_engine"


class MockedCursor:
    def __init__(self, rowcount):
        self.rowcount = rowcount

    def cursor(self):
        pass  # do nothing

    def fetchall(self):
        pass  # do nothing

    def keys(self):
        pass  # do nothing

    def close(self):
        pass  # do nothing


class MockedConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # do nothing

    def execute(self, *args, **kwargs):
        return MockedCursor(rowcount=3)

    def close(self):
        pass  # do nothing


class MockedEngine:
    def connect(self):
        return MockedConnection()


class Settings(BaseSettings):
    pcdm_host: str = "host"
    pcdm_token: str = "token"
    pcdm_port: int = 443
    pcdm_http_path: str = "http_path"
    pcdm_catalog: str = "rtdip"
    pcdm_schema: str = "openstef"
    db_host: str = "host"
    db_token: str = "token"
    db_port: int = 443
    db_http_path: str = "http_path"
    db_catalog: str = "rtdip"
    db_schema: str = "sensors"
    proxies: Union[dict[str, str], None] = None


config = Settings()


@pytest.fixture(autouse=True)
def reset_data_interface():
    # Resets the singleton instance
    _DataInterface._instance = None
    Singleton._instances = {}
    yield


def test_create_mysql_engine(mocker: MockerFixture):
    mocked_engine = mocker.patch(CREATE_ENGINE_PATH, return_value=MockedEngine())

    _DataInterface(config)

    assert mocked_engine.call_count == 2


def test_create_mysql_engine_fails(mocker: MockerFixture, caplog):
    mocker.patch(CREATE_ENGINE_PATH, side_effect=Exception)

    with pytest.raises(Exception):
        _DataInterface(config)

    assert "Could not connect to Databricks database" in caplog.text


def test_exec_influx_query(mocker: MockerFixture):
    mocker.patch(CREATE_ENGINE_PATH, return_value=MockedEngine())

    mock_df = pd.DataFrame(
        {
            "_time": [
                "2023-08-29T08:00:00+01:00",
                "2023-08-29T12:00:00+01:00",
                "2023-08-29T16:00:00+01:00",
            ],
            "_value": ["1", "2", "data"],
        }
    )

    mocked_df = mocker.patch(PANDAS_DATAFRAME_PATH, return_value=mock_df)

    interface = _DataInterface(config)

    flux_query = """
    from(bucket: "test/bucket" )   
	|> range(start: 2023-08-29T00:00:00Z, stop: 2023-08-30T00:00:00Z) 
	|> filter(fn: (r) => r._measurement == "sjv")
    """

    interface.exec_influx_query(flux_query, {})

    mocked_df.assert_called_once()


def test_exec_influx_query_multi(mocker: MockerFixture):
    mocker.patch(CREATE_ENGINE_PATH, return_value=MockedEngine())

    mock_df = pd.DataFrame(
        {
            "_time": [
                "2023-08-29T08:00:00+01:00",
                "2023-08-29T12:00:00+01:00",
                "2023-08-29T16:00:00+01:00",
            ],
            "_value": ["1", "2", "data"],
        }
    )

    mocked_df = mocker.patch(PANDAS_DATAFRAME_PATH, return_value=mock_df)

    interface = _DataInterface(config)

    flux_query = """
    data = from(bucket: "test/bucket" )   
        |> range(start: 2023-08-29T00:00:00Z, stop: 2023-08-30T00:00:00Z) 
        |> filter(fn: (r) => r._measurement == "sjv")

    data
        |> group() |> aggregateWindow(every: 15m, fn: sum)
        |> yield(name: "test_1")

    data
        |> group() |> aggregateWindow(every: 15m, fn: count)
        |> yield(name: "test_2")
    """

    result = interface.exec_influx_query(flux_query, {})

    mocked_df.assert_called()
    assert isinstance(result, list)


def test_exec_influx_query_fails(mocker: MockerFixture, caplog):
    mocked_connection = mocker.MagicMock()
    mocked_connection.__enter__.return_value = mocked_connection
    mocked_connection.execute.side_effect = Exception()

    mocked_engine = mocker.MagicMock()
    mocked_engine.connect.return_value = mocked_connection

    mocker.patch(CREATE_ENGINE_PATH, return_value=mocked_engine)

    interface = _DataInterface(config)

    flux_query = """
    from(bucket: "test/bucket" )   
	|> range(start: 2023-08-29T00:00:00Z, stop: 2023-08-30T00:00:00Z) 
	|> filter(fn: (r) => r._measurement == "sjv")
    """

    with pytest.raises(Exception):
        interface.exec_influx_query(flux_query, {})

    escaped_query = flux_query.replace("\n", "\\n").replace("\t", "\\t")

    assert QUERY_EXECUTION_ERROR_MSG in caplog.text
    assert escaped_query in caplog.text


def test_exec_influx_write(mocker: MockerFixture):
    mocked_to_sql = mocker.patch.object(pd.DataFrame, "to_sql", return_value=None)

    dates = [
        "2023-10-01T12:00:00",
        "2023-10-02T12:00:00",
        "2023-10-03T12:00:00",
    ]
    date_idx = pd.to_datetime(dates)

    expected_data = pd.DataFrame(
        {"test": ["1", "2", "data"], "test2": ["1", "2", "data"]}, index=date_idx
    )
    interface = _DataInterface(config)
    sql_query = interface.exec_influx_write(
        df=expected_data,
        database="database",
        measurement="measurement",
        tag_columns=["test"],
    )

    mocked_to_sql.assert_called_once()
    assert sql_query is True


def test_exec_influx_write_fails(mocker: MockerFixture, caplog):
    mocker.patch.object(pd.DataFrame, "to_sql", side_effect=Exception)

    dates = [
        "2023-10-01T12:00:00",
        "2023-10-02T12:00:00",
        "2023-10-03T12:00:00",
    ]
    date_idx = pd.to_datetime(dates)

    expected_data = pd.DataFrame(
        {"test": ["1", "2", "data"], "test2": ["1", "2", "data"]}, index=date_idx
    )
    interface = _DataInterface(config)

    with pytest.raises(Exception):
        interface.exec_influx_write(
            df=expected_data,
            database="database",
            measurement="measurement",
            tag_columns=["test"],
        )

    assert "Exception occured during writing to Databricks database" in caplog.text


def test_exec_sql_query(mocker: MockerFixture):
    mocker.patch(CREATE_ENGINE_PATH, return_value=MockedEngine())
    mocked_df = mocker.patch(PANDAS_DATAFRAME_PATH, return_value=None)

    interface = _DataInterface(config)
    sql_query = interface.exec_sql_query("SELECT * FROM test_table", {})

    mocked_df.assert_called_once()
    assert sql_query is None


def test_exec_sql_query_operational_fails(mocker: MockerFixture, caplog):
    mocked_connection = mocker.MagicMock()
    mocked_connection.__enter__.return_value = mocked_connection
    mocked_connection.execute.side_effect = sqlalchemy.exc.OperationalError(
        None, None, "Lost connection to Databricks database"
    )

    mocked_engine = mocker.MagicMock()
    mocked_engine.connect.return_value = mocked_connection

    mocker.patch(CREATE_ENGINE_PATH, return_value=mocked_engine)

    interface = _DataInterface(config)

    with pytest.raises(sqlalchemy.exc.OperationalError):
        interface.exec_sql_query(SAMPLE_SQL_QUERY, {})

    assert "Lost connection to Databricks database" in caplog.text
    assert SAMPLE_SQL_QUERY not in caplog.text


def test_exec_sql_query_programming_fails(mocker: MockerFixture, caplog):
    mocked_connection = mocker.MagicMock()
    mocked_connection.__enter__.return_value = mocked_connection
    mocked_connection.execute.side_effect = sqlalchemy.exc.ProgrammingError(
        None, None, QUERY_EXECUTION_ERROR_MSG
    )

    mocked_engine = mocker.MagicMock()
    mocked_engine.connect.return_value = mocked_connection

    mocker.patch(CREATE_ENGINE_PATH, return_value=mocked_engine)

    interface = _DataInterface(config)

    with pytest.raises(sqlalchemy.exc.ProgrammingError):
        interface.exec_sql_query(SAMPLE_SQL_QUERY, {})

    assert QUERY_EXECUTION_ERROR_MSG in caplog.text
    assert SAMPLE_SQL_QUERY in caplog.text


def test_exec_sql_query_database_fails(mocker: MockerFixture, caplog):
    mocked_connection = mocker.MagicMock()
    mocked_connection.__enter__.return_value = mocked_connection
    mocked_connection.execute.side_effect = sqlalchemy.exc.DatabaseError(
        None, None, "Can't connect to Databricks database"
    )

    mocked_engine = mocker.MagicMock()
    mocked_engine.connect.return_value = mocked_connection

    mocker.patch(CREATE_ENGINE_PATH, return_value=mocked_engine)

    interface = _DataInterface(config)

    with pytest.raises(sqlalchemy.exc.DatabaseError):
        interface.exec_sql_query(SAMPLE_SQL_QUERY, {})

    assert "Can't connect to Databricks database" in caplog.text
    assert SAMPLE_SQL_QUERY not in caplog.text


def test_exec_sql_write(mocker: MockerFixture):
    mocker.patch(CREATE_ENGINE_PATH, return_value=MockedEngine())

    interface = _DataInterface(config)

    sql_write = interface.exec_sql_write("INSERT INTO test_table VALUES (1, 'test')")

    assert sql_write is None


def test_exec_sql_write_fails(mocker: MockerFixture, caplog):
    mocked_connection = mocker.MagicMock()
    mocked_connection.__enter__.return_value = mocked_connection
    mocked_connection.execute.side_effect = Exception()

    mocked_engine = mocker.MagicMock()
    mocked_engine.connect.return_value = mocked_connection

    mocker.patch(CREATE_ENGINE_PATH, return_value=mocked_engine)

    interface = _DataInterface(config)

    query = "INSERT INTO test_table VALUES (1, 'test')"

    with pytest.raises(Exception):
        interface.exec_sql_write(query)

    assert QUERY_EXECUTION_ERROR_MSG in caplog.text
    assert query in caplog.text


def test_exec_sql_dataframe_write(mocker: MockerFixture):
    mocked_to_sql = mocker.patch.object(pd.DataFrame, "to_sql", return_value=None)

    mock_df = pd.DataFrame({"test": ["1", "2", "data"]})
    interface = _DataInterface(config)
    sql_write = interface.exec_sql_dataframe_write(mock_df, "test_table")

    mocked_to_sql.assert_called_once()
    assert sql_write is None


def test_exec_sql_dataframe_write_fails(mocker: MockerFixture):
    mocker.patch.object(pd.DataFrame, "to_sql", side_effect=Exception)

    interface = _DataInterface(config)
    mock_df = pd.DataFrame({"test": ["1", "2", "data"]})

    with pytest.raises(Exception):
        interface.exec_sql_dataframe_write(mock_df, "test_table")


def test_check_mysql_available(mocker: MockerFixture):
    mock_df = pd.DataFrame({"Database": ["1", "2", "data"]})
    mocker.patch(CREATE_ENGINE_PATH, return_value=MockedEngine())
    mocker.patch(PANDAS_DATAFRAME_PATH, return_value=mock_df)

    interface = _DataInterface(config)
    interface.check_mysql_available()
