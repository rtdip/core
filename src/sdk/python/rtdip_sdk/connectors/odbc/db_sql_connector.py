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

from typing import Union
from databricks import sql
import pyarrow as pa
from ..connection_interface import ConnectionInterface
from ..cursor_interface import CursorInterface
from ..models import ConnectionReturnType
import logging


class DatabricksSQLConnection(ConnectionInterface):
    """
    The Databricks SQL Connector for Python is a Python library that allows you to use Python code to run SQL commands on Databricks clusters and Databricks SQL warehouses.

    The connection class represents a connection to a database and uses the Databricks SQL Connector API's for Python to interact with cluster/jobs.
    To find details for SQL warehouses server_hostname and http_path location to the SQL Warehouse tab in the documentation.

    Args:
        server_hostname: Server hostname for the cluster or SQL Warehouse
        http_path: Http path for the cluster or SQL Warehouse
        access_token: Azure AD or Databricks PAT token
    """

    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        access_token: str,
        return_type=ConnectionReturnType.Pandas,
    ) -> None:
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token
        self.return_type = return_type
        # call auth method
        self.connection = self._connect()

    def _connect(self):
        """Connects to the endpoint"""
        try:
            return sql.connect(
                server_hostname=self.server_hostname,
                http_path=self.http_path,
                access_token=self.access_token,
                _user_agent_entry="RTDIP",
            )
        except Exception as e:
            logging.exception("error while connecting to the endpoint")
            raise e

    def close(self) -> None:
        """Closes connection to database."""
        try:
            self.connection.close()
        except Exception as e:
            logging.exception("error while closing connection")
            raise e

    def cursor(self) -> object:
        """
        Initiates the cursor and returns it.

        Returns:
          DatabricksSQLCursor: Object to represent a databricks workspace with methods to interact with clusters/jobs.
        """
        try:
            if self.connection.open == False:
                self.connection = self._connect()
            return DatabricksSQLCursor(self.connection.cursor(), self.return_type)
        except Exception as e:
            logging.exception("error with cursor object")
            raise e


class DatabricksSQLCursor(CursorInterface):
    """
    Object to represent a databricks workspace with methods to interact with clusters/jobs.

    Args:
        cursor: controls execution of commands on cluster or SQL Warehouse
    """

    def __init__(self, cursor: object, return_type=ConnectionReturnType.Pandas) -> None:
        self.cursor = cursor
        self.return_type = return_type

    def execute(self, query: str) -> None:
        """
        Prepares and runs a database query.

        Args:
            query: sql query to execute on the cluster or SQL Warehouse
        """
        try:
            self.cursor.execute(query)
        except Exception as e:
            logging.exception("error while executing the query")
            raise e

    def fetch_all(self, fetch_size=5_000_000) -> Union[list, dict]:
        """
        Gets all rows of a query.

        Returns:
            list: list of results
        """
        try:
            get_next_result = True
            results = None if self.return_type == ConnectionReturnType.String else []
            count = 0
            sample_row = None
            while get_next_result:
                result = self.cursor.fetchmany_arrow(fetch_size)
                count += result.num_rows
                if self.return_type == ConnectionReturnType.List:
                    column_list = []
                    for column in result.columns:
                        column_list.append(column.to_pylist())
                    results.extend(zip(*column_list))
                elif self.return_type == ConnectionReturnType.String:
                    column_list = []
                    for column in result.columns:
                        column_list.append(column.to_pylist())
                    rows = [str(item[0]) for item in zip(*column_list)]
                    if len(rows) > 0:
                        sample_row = rows[0]
                    strings = ",".join(rows)
                    if results is None:
                        results = strings
                    else:
                        results = ",".join([results, strings])
                else:
                    results.append(result)
                if result.num_rows < fetch_size:
                    get_next_result = False

            if self.return_type == ConnectionReturnType.Pandas:
                pyarrow_table = pa.concat_tables(results)
                return pyarrow_table.to_pandas()
            elif self.return_type == ConnectionReturnType.Pyarrow:
                pyarrow_table = pa.concat_tables(results)
                return pyarrow_table
            elif self.return_type == ConnectionReturnType.List:
                return results
            elif self.return_type == ConnectionReturnType.String:
                return {
                    "data": results,
                    "sample_row": sample_row,
                    "count": count,
                }
        except Exception as e:
            logging.exception("error while fetching the rows of a query")
            raise e

    def close(self) -> None:
        """Closes the cursor."""
        try:
            self.cursor.close()
        except Exception as e:
            logging.exception("error while closing the cursor")
            raise e
