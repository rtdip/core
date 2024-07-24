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

from turbodbc import connect, make_options, Megabytes
import pandas as pd
from ..._sdk_utils.compare_versions import _package_version_meets_minimum
from ..connection_interface import ConnectionInterface
from ..cursor_interface import CursorInterface
from ..models import ConnectionReturnType
import logging
import os


class TURBODBCSQLConnection(ConnectionInterface):
    """
    Turbodbc is a python module used to access relational databases through an ODBC interface. It will allow a user to connect to databricks clusters or sql warehouses.

    Turbodbc offers built-in NumPy support allowing it to be much faster for processing compared to other connectors.
    To find details for SQL warehouses server_hostname and http_path location to the SQL Warehouse tab in the documentation.

    Args:
        server_hostname: hostname for the cluster or SQL Warehouse
        http_path: Http path for the cluster or SQL Warehouse
        access_token: Azure AD Token

    Note:
        More fields such as driver can be configured upon extension.
    """

    def __init__(
        self,
        server_hostname: str,
        http_path: str,
        access_token: str,
        return_type=ConnectionReturnType.Pandas,
    ) -> None:
        _package_version_meets_minimum("turbodbc", "4.0.0")
        self.server_hostname = server_hostname
        self.http_path = http_path
        self.access_token = access_token
        self.return_type = return_type
        # call auth method
        self.connection = self._connect()
        self.open = True

    def _connect(self):
        """Connects to the endpoint"""
        try:
            options = make_options(
                autocommit=True, read_buffer_size=Megabytes(100), use_async_io=True
            )

            return connect(
                Driver="Simba Spark ODBC Driver",
                Host=self.server_hostname,
                Port=443,
                SparkServerType=3,
                ThriftTransport=2,
                SSL=1,
                AuthMech=11,
                Auth_AccessToken=self.access_token,
                Auth_Flow=0,
                HTTPPath=self.http_path,
                UseNativeQuery=1,
                FastSQLPrepare=1,
                ApplyFastSQLPrepareToAllQueries=1,
                DisableLimitZero=1,
                EnableAsyncExec=1,
                RowsFetchedPerBlock=os.getenv("RTDIP_ODBC_ROW_BLOCK_SIZE", 500000),
                UserAgentEntry="RTDIP",
                turbodbc_options=options,
            )

        except Exception as e:
            logging.exception("error while connecting to the endpoint")
            raise e

    def close(self) -> None:
        """Closes connection to database."""
        try:
            self.connection.close()
            self.open = False
        except Exception as e:
            logging.exception("error while closing the connection")
            raise e

    def cursor(self) -> object:
        """
        Intiates the cursor and returns it.

        Returns:
          TURBODBCSQLCursor: Object to represent a databricks workspace with methods to interact with clusters/jobs.
        """
        try:
            if self.open == False:
                self.connection = self._connect()
            return TURBODBCSQLCursor(
                self.connection.cursor(), return_type=self.return_type
            )
        except Exception as e:
            logging.exception("error with cursor object")
            raise e


class TURBODBCSQLCursor(CursorInterface):
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

    def fetch_all(self) -> list:
        """
        Gets all rows of a query.

        Returns:
            list: list of results
        """
        try:
            result = self.cursor.fetchallarrow()
            if self.return_type == ConnectionReturnType.Pyarrow:
                return result
            elif self.return_type == ConnectionReturnType.Pandas:
                return result.to_pandas()
        except Exception as e:
            logging.exception("error while fetching the rows from the query")
            raise e

    def close(self) -> None:
        """Closes the cursor."""
        try:
            self.cursor.close()
        except Exception as e:
            logging.exception("error while closing the cursor")
            raise e
