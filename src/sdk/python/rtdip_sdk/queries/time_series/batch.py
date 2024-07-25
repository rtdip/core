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
from typing import List
import logging
import pandas as pd
from ._time_series_query_builder import _query_builder
from ...connectors.odbc.db_sql_connector import DatabricksSQLConnection
from concurrent.futures import *


def get(
    connection: object, request_list: List[dict], threadpool_max_workers=1
) -> List[pd.DataFrame]:
    """
    A function to return back raw data by querying databricks SQL Warehouse using a connection specified by the user.

    The available connectors by RTDIP are Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect.

    The available authentication methods are Certificate Authentication, Client Secret Authentication or Default Authentication. See documentation.

    Args:
        connection: Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect)
        request_list: A list of dictionaries, each contaiing the type of request and a dictionary of parameters.

    Returns:
        DataFrame: A list of dataframes of timeseries data.

    """
    try:
        results = []

        # Get connection parameters and close, as each thread will create new connection
        server_hostname = connection.server_hostname
        http_path = connection.http_path
        access_token = connection.access_token
        connection.close()

        def execute_request(connection_params, request):
            # Create connection and cursor
            connection = DatabricksSQLConnection(*connection_params)
            cursor = connection.cursor()

            # Build query with query builder
            query = _query_builder(request["parameters_dict"], request["type"])

            # Execute query
            try:
                cursor.execute(query)
                df = cursor.fetch_all()
                return df
            except Exception as e:
                logging.exception("error returning dataframe")
                raise e
            finally:
                # Close cursor and connection at end
                cursor.close()
                connection.close()

        with ThreadPoolExecutor(max_workers=threadpool_max_workers) as executor:
            # Package up connection params into tuple
            connection_params = (server_hostname, http_path, access_token)

            # Execute queries with threadpool - map preserves order
            results = executor.map(
                lambda arguments: execute_request(*arguments),
                [(connection_params, request) for request in request_list],
            )

        return results

    except Exception as e:
        logging.exception("error with batch function")
        raise e
