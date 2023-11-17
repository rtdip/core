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

import logging
import pandas as pd
from ...connectors.connection_interface import ConnectionInterface


class SQLQueryBuilder:
    """
    A builder for developing RTDIP queries using any delta table
    """

    sql_query: dict
    connection: ConnectionInterface

    def get(self, connection=object, sql_query=str) -> pd.DataFrame:
        """
        A function to return back raw data by querying databricks SQL Warehouse using a connection specified by the user.

        The available connectors by RTDIP are Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect.

        The available authentication methods are Certificate Authentication, Client Secret Authentication or Default Authentication. See documentation.

        This function requires the user to input a dictionary of parameters. (See Attributes table below)

        Args:
            connection (obj): Connection chosen by the user (Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect)
            sql_query (str): A string of the SQL query to be executed.

        Returns:
            DataFrame: A dataframe of raw timeseries data.
        """
        try:
            try:
                cursor = connection.cursor()
                cursor.execute(sql_query)
                df = cursor.fetch_all()
                cursor.close()
                connection.close()
                return df
            except Exception as e:
                logging.exception("Error returning dataframe")
                raise e

        except Exception as e:
            logging.exception("error with sql query")
            raise e
