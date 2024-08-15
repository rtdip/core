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
from src.sdk.python.rtdip_sdk.queries.sql.sql_query import SQLQueryBuilder

SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "mock_databricks_token"
DATABRICKS_SQL_CONNECT = "databricks.sql.connect"
DATABRICKS_SQL_CONNECT_CURSOR = "databricks.sql.connect.cursor"
MOCKED_SQL_QUERY = "SELECT * FROM MOCKEDTABLE"


def test_sql_query(mocker: MockerFixture):
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_connection_close = mocker.spy(MockedDBConnection, "close")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(
        MockedCursor,
        "fetchmany_arrow",
        return_value=pa.Table.from_pandas(
            pd.DataFrame(data={"x": [1], "y": [2], "z": [3], "w": [4]})
        ),
    )
    mocked_close = mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    actual = SQLQueryBuilder().get(
        connection=mocked_connection, sql_query=MOCKED_SQL_QUERY
    )

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_SQL_QUERY + " ")
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_sql_query_fail(mocker: MockerFixture):
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
        SQLQueryBuilder().get(mocked_connection, MOCKED_SQL_QUERY)


@pytest.mark.parametrize(
    "parameters, expected",
    [
        (
            {
                "sql_statement": "SELECT EventTime, TagName, Status, Value FROM test_table",
            },
            {"count": 3},
        ),
        # Add more test cases as needed
    ],
)
def test_sql_query(spark_connection, parameters, expected):
    df = SQLQueryBuilder().get(spark_connection, parameters["sql_statement"])
    assert df.columns == ["EventTime", "TagName", "Status", "Value"]
    assert df.count() == expected["count"]
