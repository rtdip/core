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

from src.sdk.python.rtdip_sdk.connectors import ChatOpenAIDatabricksConnection, ChatOpenAIDatabricksSQLCursor
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session
import pandas as pd
from pyspark.sql import SparkSession
from pytest_mock import MockerFixture
from langchain import SQLDatabase
import pytest

# CATALOG = "test_catalog"
# SCHEMA = "test_schema"
# SERVER_HOSTNAME = "mock.cloud.databricks.com"
# HTTP_PATH = "sql/mock/mock-test"
# ACCESS_TOKEN = "mock_databricks_token"
# OPENAI_API_KEY = "mock_openai_api_key"

CATALOG = "res"
SCHEMA = "sensors"
SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "dapib6c74eda8dc3c2016bbea96c0db1a26c"
OPENAI_API_KEY = "mock_openai_api_key"

# llm = ChatOpenAI(temperature=0, model_name="gpt-3.5-turbo", openai_api_key="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")
# db = SQLDatabase.from_databricks(
#     catalog='res', 
#     schema='sensors', 
#     api_token='dapib6c74eda8dc3c2016bbea96c0db1a26c', 
#     host='adb-3073476248944970.10.azuredatabricks.net', 
#     warehouse_id='6b6f8386146bbfd4',
#     sample_rows_in_table_info=50
# )


class MockSQLDatabase(SQLDatabase):
    _engine = {"dialect": {"name": "mock_dialect"}}
    def __init__(self, engine):
        self._engine = engine if engine else {"dialect": {"name": "mock_dialect"}}
    def from_uri(self):
        return self   
    def from_databricks(self):
        return self
    def engine(self):
        return self._engine

def test_langchain_sql_agent():
    connection = ChatOpenAIDatabricksConnection(
        catalog="res",
        schema="sensors",
        server_hostname="adb-3073476248944970.10.azuredatabricks.net",
        http_path="/sql/1.0/warehouses/6b6f8386146bbfd4",
        access_token="dapib6c74eda8dc3c2016bbea96c0db1a26c",
        openai_api_key="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",
        openai_model="gpt-3.5-turbo-0301",
        sample_rows_in_table_info=50,
        verbose_logging=True
    )

    cursor = connection.cursor()
    cursor.execute("What was the average actual power generated for turbine 1 at pottendijk on 11 April?")
    result = cursor.fetch_all()
    print(result)

def test_connection_close(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch("langchain.chat_models.ChatOpenAI", return_value = None)
    mocker.patch("langchain.SQLDatabase.from_databricks", return_value = MockSQLDatabase(engine={"dialect": {"name": "mock_dialect"}}))
    mocker.patch("langchain.agents.agent_toolkits.SQLDatabaseToolkit", return_value = None)
    mocker.patch("langchain.agents.create_sql_agent", return_value = None)
    spark_close = mocker.spy(ChatOpenAIDatabricksConnection, "close")

    mocked_connection = ChatOpenAIDatabricksConnection(catalog=CATALOG, schema=SCHEMA, server_hostname=SERVER_HOSTNAME, http_path=HTTP_PATH, access_token=ACCESS_TOKEN, openai_api_key=OPENAI_API_KEY)
    mocked_connection.close()

    spark_close.assert_called_once()

def test_connection_cursor(spark_session: SparkSession, mocker: MockerFixture):
    spark_cursor = mocker.spy(ChatOpenAIDatabricksConnection, "cursor")

    mocked_connection = ChatOpenAIDatabricksConnection(spark=spark_session)
    result = mocked_connection.cursor()

    assert isinstance(result, object)
    spark_cursor.assert_called_once()

def test_cursor_execute(spark_session: SparkSession, mocker: MockerFixture):
    spark_execute = mocker.spy(ChatOpenAIDatabricksSQLCursor, "execute")

    cursor = ChatOpenAIDatabricksSQLCursor(ChatOpenAIDatabricksConnection(spark=spark_session).connection)
    cursor.execute("select 1 as value")

    spark_execute.assert_called_with(mocker.ANY, query="select 1 as value")

def test_cursor_fetch_all(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch.object(ChatOpenAIDatabricksSQLCursor, "fetch_all", return_value=pd.DataFrame([{"column_name_1": "1", "column_name_2": "2", "column_name_3": "3", "column_name_4": "4"}]))

    cursor = ChatOpenAIDatabricksSQLCursor(ChatOpenAIDatabricksConnection(spark=spark_session).connection)
    cursor.execute("select 1 as value union select 2 as value")
    result = cursor.fetch_all()

    assert isinstance(result, pd.DataFrame)

def test_cursor_close(spark_session: SparkSession, mocker: MockerFixture):
    spy_close = mocker.spy(ChatOpenAIDatabricksSQLCursor, "close")

    cursor = ChatOpenAIDatabricksSQLCursor(ChatOpenAIDatabricksConnection(spark=spark_session).connection)
    cursor.close()

    spy_close.assert_called_once()

def test_connection_close_fails(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch.object(ChatOpenAIDatabricksConnection, "close", side_effect = Exception)

    mocked_connection = ChatOpenAIDatabricksConnection(spark=spark_session)
    
    with pytest.raises(Exception):
        mocked_connection.close()

def test_connection_cursor_fails(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch.object(ChatOpenAIDatabricksConnection, "cursor", side_effect = Exception)

    mocked_connection = ChatOpenAIDatabricksConnection(spark=spark_session)

    with pytest.raises(Exception):
        assert mocked_connection.cursor()

def test_cursor_execute_fails(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch.object(ChatOpenAIDatabricksSQLCursor, "execute", side_effect = Exception)

    mocked_cursor = ChatOpenAIDatabricksSQLCursor(ChatOpenAIDatabricksConnection(spark=spark_session).connection)

    with pytest.raises(Exception):
        assert mocked_cursor.execute("test")

def test_cursor_fetch_all_fails(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch.object(ChatOpenAIDatabricksSQLCursor, "fetch_all", side_effect = Exception)

    mocked_cursor = ChatOpenAIDatabricksSQLCursor(ChatOpenAIDatabricksConnection(spark=spark_session).connection)

    with pytest.raises(Exception):
        assert mocked_cursor.fetch_all()

def test_cursor_close_fails(spark_session: SparkSession, mocker: MockerFixture):
    mocker.patch.object(ChatOpenAIDatabricksSQLCursor, "close", side_effect = Exception)

    mocked_cursor = ChatOpenAIDatabricksSQLCursor(ChatOpenAIDatabricksConnection(spark=spark_session).connection)

    with pytest.raises(Exception):
        assert mocked_cursor.close()