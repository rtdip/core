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
from pytest_mock import MockerFixture
from pydantic.v1 import BaseSettings
from typing import Union


query_error = "Error occured during executing query"
test_query = "SELECT * FROM test_table"


class MockedResult:
    def __init__(self, rowcount):
        self.rowcount = rowcount


class MockedConnection:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass  # do nothing

    def execute(self, *args, **kwargs):
        return MockedResult(rowcount=3)


class MockedEngine:
    def connect(self):
        return MockedConnection()


class Settings(BaseSettings):
    api_username: str = "None"
    api_password: str = "None"
    api_admin_username: str = "None"
    api_admin_password: str = "None"
    api_url: str = "None"
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


def test_exec_influx_one_query(mocker: MockerFixture):
    df = pd.DataFrame(
        {
            "_time": [
                "2023-08-29T08:00:00+01:00",
                "2023-08-29T12:00:00+01:00",
                "2023-08-29T16:00:00+01:00",
            ],
            "_value": ["1", "2", "data"],
        }
    )
    mocked_read_sql = mocker.patch.object(pd, "read_sql", return_value=df)

    interface = _DataInterface(config)

    flux_query = """
    from(bucket: "test/bucket" )   
	|> range(start: 2023-08-29T00:00:00Z, stop: 2023-08-30T00:00:00Z) 
	|> filter(fn: (r) => r._measurement == "sjv")
    """

    interface.exec_influx_query(flux_query, {})

    mocked_read_sql.assert_called_once()


def test_exec_influx_multi_query(mocker: MockerFixture):
    df = pd.DataFrame(
        {
            "_time": [
                "2023-08-29T08:00:00+01:00",
                "2023-08-29T12:00:00+01:00",
                "2023-08-29T16:00:00+01:00",
            ],
            "_value": ["1", "2", "data"],
        }
    )
    mocked_read_sql = mocker.patch.object(pd, "read_sql", return_value=df)

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

    interface.exec_influx_query(flux_query, {})

    mocked_read_sql.assert_called()
    assert mocked_read_sql.call_count == 2


def test_exec_influx_query_fails(mocker: MockerFixture, caplog):
    mocker.patch.object(pd, "read_sql", side_effect=Exception)

    interface = _DataInterface(config)

    flux_query = """
    from(bucket: "test/bucket" )   
	|> range(start: 2023-08-29T00:00:00Z, stop: 2023-08-30T00:00:00Z) 
	|> filter(fn: (r) => r._measurement == "sjv")
    """

    with pytest.raises(Exception):
        interface.exec_influx_query(flux_query, {})

    escaped_query = flux_query.replace("\n", "\\n").replace("\t", "\\t")

    assert query_error in caplog.text
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
    mocked_read_sql = mocker.patch.object(pd, "read_sql", return_value=None)

    interface = _DataInterface(config)
    sql_query = interface.exec_sql_query("SELECT * FROM test_table", {})

    mocked_read_sql.assert_called_once()
    assert sql_query is None


def test_exec_sql_query_operational_fails(mocker: MockerFixture, caplog):
    interface = _DataInterface(config)

    mocker.patch.object(
        pd,
        "read_sql",
        side_effect=sqlalchemy.exc.OperationalError(
            None, None, "Lost connection to Databricks database"
        ),
    )

    with pytest.raises(sqlalchemy.exc.OperationalError):
        interface.exec_sql_query(test_query, {})

    assert "Lost connection to Databricks database" in caplog.text
    assert test_query not in caplog.text


def test_exec_sql_query_programming_fails(mocker: MockerFixture, caplog):
    interface = _DataInterface(config)

    mocker.patch.object(
        pd,
        "read_sql",
        side_effect=sqlalchemy.exc.ProgrammingError(None, None, query_error),
    )

    with pytest.raises(sqlalchemy.exc.ProgrammingError):
        interface.exec_sql_query(test_query, {})

    assert query_error in caplog.text
    assert test_query in caplog.text


def test_exec_sql_query_database_fails(mocker: MockerFixture, caplog):
    interface = _DataInterface(config)

    mocker.patch.object(
        pd,
        "read_sql",
        side_effect=sqlalchemy.exc.DatabaseError(
            None, None, "Can't connect to Databricks database"
        ),
    )

    with pytest.raises(sqlalchemy.exc.DatabaseError):
        interface.exec_sql_query(test_query, {})

    assert "Can't connect to Databricks database" in caplog.text
    assert test_query not in caplog.text


def test_exec_sql_write(mocker: MockerFixture):
    interface = _DataInterface(config)
    mocker.patch.object(interface, "mysql_engine", new_callable=MockedEngine)

    sql_write = interface.exec_sql_write("INSERT INTO test_table VALUES (1, 'test')")

    assert sql_write is None


def test_exec_sql_write_fails(mocker: MockerFixture, caplog):
    mocker.patch.object(_DataInterface, "_create_mysql_engine", return_value=Exception)

    interface = _DataInterface(config)

    query = "INSERT INTO test_table VALUES (1, 'test')"

    with pytest.raises(Exception):
        interface.exec_sql_write(query)

    assert query_error in caplog.text
    assert query in caplog.text


def test_exec_sql_dataframe_write(mocker: MockerFixture):
    mocked_to_sql = mocker.patch.object(pd.DataFrame, "to_sql", return_value=None)

    expected_data = pd.DataFrame({"test": ["1", "2", "data"]})
    interface = _DataInterface(config)
    sql_write = interface.exec_sql_dataframe_write(expected_data, "test_table")

    mocked_to_sql.assert_called_once()
    assert sql_write is None


def test_exec_sql_dataframe_write_fails(mocker: MockerFixture):
    mocker.patch.object(pd.DataFrame, "to_sql", side_effect=Exception)

    interface = _DataInterface(config)
    expected_data = pd.DataFrame({"test": ["1", "2", "data"]})

    with pytest.raises(Exception):
        interface.exec_sql_dataframe_write(expected_data, "test_table")
