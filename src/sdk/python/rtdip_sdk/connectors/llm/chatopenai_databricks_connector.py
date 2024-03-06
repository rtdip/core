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

from langchain_community.chat_models import ChatOpenAI
from langchain_community.utilities.sql_database import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent
from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain.agents.agent_types import AgentType
import logging

from ..._sdk_utils.compare_versions import _package_version_meets_minimum
from ..connection_interface import ConnectionInterface
from ..cursor_interface import CursorInterface


class ChatOpenAIDatabricksConnection(ConnectionInterface):
    """
    The Chat Open AI(Chat GPT) Databricks LLM Connector enables you to connect to a Databricks SQL Warehouse and use the Chat Open AI(Chat GPT) LLM to generate SQL queries.

    The connection class represents a connection to a database and uses the Databricks SQL Connector API's for Python to interact with cluster/jobs and langchain to connect to Chat Open AI(Chat GPT) LLM.
    To find details for SQL warehouses server_hostname and http_path location to the SQL Warehouse tab in the documentation.

    Args:
        catalog: Catalog name in Databricks
        schema: Schema name in Databricks
        server_hostname: Server hostname for the cluster or SQL Warehouse
        http_path: Http path for the cluster or SQL Warehouse
        access_token: Azure AD or Databricks PAT token
        openai_api_key: OpenAI API key
        openai_model: OpenAI model name
        sample_rows_in_table_info: Number of rows to sample when getting table information
        verbose_logging: Whether to log verbose messages
    """

    def __init__(
        self,
        catalog: str,
        schema: str,
        server_hostname: str,
        http_path: str,
        access_token: str,
        openai_api_key: str,
        openai_model: str = "gpt-4",
        sample_rows_in_table_info: int = 3,
        verbose_logging: bool = False,
    ) -> None:
        _package_version_meets_minimum("langchain", "0.0.196")
        # connect to llm
        llm = ChatOpenAI(
            temperature=0, model_name=openai_model, openai_api_key=openai_api_key
        )

        # connect to Databricks
        db = SQLDatabase.from_databricks(
            catalog=catalog,
            schema=schema,
            api_token=access_token,
            host=server_hostname,
            warehouse_id=http_path.split("/")[-1],
            sample_rows_in_table_info=sample_rows_in_table_info,
        )

        prefix = """
    ...
    Always adhere to the format and don't return empty names or half responses.
    """
        toolkit = SQLDatabaseToolkit(db=db, llm=llm)

        model_agent_type = AgentType.ZERO_SHOT_REACT_DESCRIPTION
        if "0613" in openai_model:
            model_agent_type = AgentType.OPENAI_FUNCTIONS

        self.connection = create_sql_agent(
            llm=llm,
            prefix=prefix,
            toolkit=toolkit,
            verbose=verbose_logging,
            agent_type=model_agent_type,
        )

    def close(self) -> None:
        """Closes connection to database."""
        pass

    def cursor(self) -> object:
        """
        Initiates the cursor and returns it.

        Returns:
          ChatOpenAIDatabricksSQLCursor: Object to represent a connection to Databricks and Open AI with methods to interact with clusters/jobs and ChatGPT.
        """
        try:
            return ChatOpenAIDatabricksSQLCursor(self.connection)
        except Exception as e:
            logging.exception("error with cursor object")
            raise e

    def run(self, query: str) -> str:
        """
        Runs a query on the ChatGPT and the Databricks Cluster or SQL Warehouse.

        Returns:
            str: response from ChatGPT and the Databricks Cluster or SQL Warehouse
        """
        cursor = self.cursor()
        cursor.execute(query)
        return cursor.fetch_all()


class ChatOpenAIDatabricksSQLCursor(CursorInterface):
    """
    Object to represent a connection to Databricks and Open AI with methods to interact with clusters/jobs and ChatGPT.

    Args:
        cursor: controls execution of commands on cluster or SQL Warehouse
    """

    response = None

    def __init__(self, cursor: object) -> None:
        self.cursor = cursor

    def execute(self, query: str) -> None:
        """
        Prepares and runs a database query.

        Args:
            query: sql query to execute on ChatGPT and the Databricks Cluster or SQL Warehouse
        """
        try:
            self.response = self.cursor.run(query)
        except Exception as e:
            logging.exception("error while executing the query")
            raise e

    def fetch_all(
        self,
    ) -> list:
        """
        Gets all rows of a query.

        Returns:
            list: list of results
        """
        try:
            return self.response
        except Exception as e:
            logging.exception("error while fetching the rows of a query")
            raise e

    def close(self) -> None:
        """Closes the cursor."""
        pass
