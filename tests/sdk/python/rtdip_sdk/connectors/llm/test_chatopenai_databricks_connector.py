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
    ChatOpenAIDatabricksConnection,
    ChatOpenAIDatabricksSQLCursor,
)
from pytest_mock import MockerFixture
from langchain import SQLDatabase
import pytest

CATALOG = "res"
SCHEMA = "sensors"
SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "mock_access_token"
OPENAI_API_KEY = "mock_openai_api_key"
LANGCHAIN_CHATOPENAI = "langchain.chat_models.ChatOpenAI"
LANGCHAIN_SQLDATABASE = "langchain.SQLDatabase.from_databricks"
LANGCHAIN_SQLDATABASETOOLKIT = "langchain.agents.agent_toolkits.SQLDatabaseToolkit"
LANGCHAIN_CREATE_SQL_AGENT = "langchain.agents.create_sql_agent"
QUESTION = "what is the answer to the question?"
RESPONSE = "This is the ChatOpenAI response"


class MockDialect:
    name: str = "mock_dialect"


class MockEngine:
    dialect: MockDialect = MockDialect()


class MockSQLDatabase(SQLDatabase):
    _engine = MockEngine()

    def __init__(self, engine):
        self._engine = engine if engine else MockEngine()

    def from_uri(self):
        return self

    def from_databricks(self):
        return self

    def engine(self):
        return self._engine


class MockedCursor:
    def __init__(self):
        self.description = [("EventTime",), ("TagName",), ("Status",), ("Value",)]

    def execute(self, query) -> None:
        return None

    def fetchall(self) -> list:
        return list

    def fetchall_arrow(self) -> str:
        return RESPONSE

    def close(self) -> None:
        return None


class MockedDBConnection:
    def close(self) -> None:
        return None

    def cursor(self) -> object:
        return MockedCursor()

    def run(self, query) -> str:
        return RESPONSE


def test_connection_close(mocker: MockerFixture):
    mocker.patch(LANGCHAIN_CHATOPENAI, return_value=None)
    mocker.patch(
        LANGCHAIN_SQLDATABASE, return_value=MockSQLDatabase(engine=MockEngine())
    )
    mocker.patch(LANGCHAIN_SQLDATABASETOOLKIT, return_value=None)
    mocker.patch(LANGCHAIN_CREATE_SQL_AGENT, return_value=None)
    mock_close = mocker.spy(ChatOpenAIDatabricksConnection, "close")

    mocked_connection = ChatOpenAIDatabricksConnection(
        catalog=CATALOG,
        schema=SCHEMA,
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=ACCESS_TOKEN,
        openai_api_key=OPENAI_API_KEY,
    )
    mocked_connection.close()

    mock_close.assert_called_once()


def test_connection_cursor(mocker: MockerFixture):
    mocker.patch(LANGCHAIN_CHATOPENAI, return_value=None)
    mocker.patch(
        LANGCHAIN_SQLDATABASE, return_value=MockSQLDatabase(engine=MockEngine())
    )
    mocker.patch(LANGCHAIN_SQLDATABASETOOLKIT, return_value=None)
    mocker.patch(LANGCHAIN_CREATE_SQL_AGENT, return_value=None)
    mocked_cursor = mocker.spy(ChatOpenAIDatabricksConnection, "cursor")

    mocked_connection = ChatOpenAIDatabricksConnection(
        catalog=CATALOG,
        schema=SCHEMA,
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=ACCESS_TOKEN,
        openai_api_key=OPENAI_API_KEY,
    )
    result = mocked_connection.cursor()

    assert isinstance(result, object)
    mocked_cursor.assert_called_once()


def test_cursor_execute(mocker: MockerFixture):
    mocked_execute = mocker.spy(ChatOpenAIDatabricksSQLCursor, "execute")

    mocked_cursor = ChatOpenAIDatabricksSQLCursor(MockedDBConnection())
    mocked_cursor.execute(QUESTION)

    mocked_execute.assert_called_with(mocker.ANY, query=QUESTION)


def test_cursor_fetch_all(mocker: MockerFixture):
    mocked_cursor = ChatOpenAIDatabricksSQLCursor(MockedDBConnection())
    mocked_cursor.execute(QUESTION)
    result = mocked_cursor.fetch_all()

    assert result == RESPONSE


def test_cursor_close(mocker: MockerFixture):
    mocker.patch(LANGCHAIN_CHATOPENAI, return_value=None)
    mocker.patch(
        LANGCHAIN_SQLDATABASE, return_value=MockSQLDatabase(engine=MockEngine())
    )
    mocker.patch(LANGCHAIN_SQLDATABASETOOLKIT, return_value=None)
    mocker.patch(LANGCHAIN_CREATE_SQL_AGENT, return_value=None)
    spy_close = mocker.spy(ChatOpenAIDatabricksSQLCursor, "close")

    cursor = ChatOpenAIDatabricksSQLCursor(
        ChatOpenAIDatabricksConnection(
            catalog=CATALOG,
            schema=SCHEMA,
            server_hostname=SERVER_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN,
            openai_api_key=OPENAI_API_KEY,
        ).connection
    )
    cursor.close()

    spy_close.assert_called_once()


def test_connection_close_fails(mocker: MockerFixture):
    mocker.patch(LANGCHAIN_CHATOPENAI, return_value=None)
    mocker.patch(
        LANGCHAIN_SQLDATABASE, return_value=MockSQLDatabase(engine=MockEngine())
    )
    mocker.patch(LANGCHAIN_SQLDATABASETOOLKIT, return_value=None)
    mocker.patch(LANGCHAIN_CREATE_SQL_AGENT, return_value=None)
    mocker.patch.object(ChatOpenAIDatabricksConnection, "close", side_effect=Exception)

    mocked_connection = ChatOpenAIDatabricksConnection(
        catalog=CATALOG,
        schema=SCHEMA,
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=ACCESS_TOKEN,
        openai_api_key=OPENAI_API_KEY,
    )

    with pytest.raises(Exception):
        mocked_connection.close()


def test_connection_cursor_fails(mocker: MockerFixture):
    mocker.patch(LANGCHAIN_CHATOPENAI, return_value=None)
    mocker.patch(
        LANGCHAIN_SQLDATABASE, return_value=MockSQLDatabase(engine=MockEngine())
    )
    mocker.patch(LANGCHAIN_SQLDATABASETOOLKIT, return_value=None)
    mocker.patch(LANGCHAIN_CREATE_SQL_AGENT, return_value=None)
    mocker.patch.object(ChatOpenAIDatabricksConnection, "cursor", side_effect=Exception)

    mocked_connection = ChatOpenAIDatabricksConnection(
        catalog=CATALOG,
        schema=SCHEMA,
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        access_token=ACCESS_TOKEN,
        openai_api_key=OPENAI_API_KEY,
    )

    with pytest.raises(Exception):
        mocked_connection.cursor()


def test_cursor_execute_fails(mocker: MockerFixture):
    mocker.patch(LANGCHAIN_CHATOPENAI, return_value=None)
    mocker.patch(
        LANGCHAIN_SQLDATABASE, return_value=MockSQLDatabase(engine=MockEngine())
    )
    mocker.patch(LANGCHAIN_SQLDATABASETOOLKIT, return_value=None)
    mocker.patch(LANGCHAIN_CREATE_SQL_AGENT, return_value=None)
    mocker.patch.object(ChatOpenAIDatabricksSQLCursor, "execute", side_effect=Exception)

    mocked_cursor = ChatOpenAIDatabricksSQLCursor(
        ChatOpenAIDatabricksConnection(
            catalog=CATALOG,
            schema=SCHEMA,
            server_hostname=SERVER_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN,
            openai_api_key=OPENAI_API_KEY,
        ).connection
    )

    with pytest.raises(Exception):
        mocked_cursor.execute("test")


def test_cursor_fetch_all_fails(mocker: MockerFixture):
    mocker.patch(LANGCHAIN_CHATOPENAI, return_value=None)
    mocker.patch(
        LANGCHAIN_SQLDATABASE, return_value=MockSQLDatabase(engine=MockEngine())
    )
    mocker.patch(LANGCHAIN_SQLDATABASETOOLKIT, return_value=None)
    mocker.patch(LANGCHAIN_CREATE_SQL_AGENT, return_value=None)
    mocker.patch.object(
        ChatOpenAIDatabricksSQLCursor, "fetch_all", side_effect=Exception
    )

    mocked_cursor = ChatOpenAIDatabricksSQLCursor(
        ChatOpenAIDatabricksConnection(
            catalog=CATALOG,
            schema=SCHEMA,
            server_hostname=SERVER_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN,
            openai_api_key=OPENAI_API_KEY,
        ).connection
    )

    with pytest.raises(Exception):
        mocked_cursor.fetch_all()


def test_cursor_close_fails(mocker: MockerFixture):
    mocker.patch(LANGCHAIN_CHATOPENAI, return_value=None)
    mocker.patch(
        LANGCHAIN_SQLDATABASE, return_value=MockSQLDatabase(engine=MockEngine())
    )
    mocker.patch(LANGCHAIN_SQLDATABASETOOLKIT, return_value=None)
    mocker.patch(LANGCHAIN_CREATE_SQL_AGENT, return_value=None)
    mocker.patch.object(ChatOpenAIDatabricksSQLCursor, "close", side_effect=Exception)

    mocked_cursor = ChatOpenAIDatabricksSQLCursor(
        ChatOpenAIDatabricksConnection(
            catalog=CATALOG,
            schema=SCHEMA,
            server_hostname=SERVER_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=ACCESS_TOKEN,
            openai_api_key=OPENAI_API_KEY,
        ).connection
    )

    with pytest.raises(Exception):
        mocked_cursor.close()
