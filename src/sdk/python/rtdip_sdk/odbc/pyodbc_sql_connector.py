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

import pyodbc
import pandas as pd
from .connection_interface import ConnectionInterface
from .cursor_interface import CursorInterface
import logging

class PYODBCSQLConnection(ConnectionInterface):
  """
    PYODBC is an open source python module which allows access to ODBC databases. 
    This allows the user to connect through ODBC to data in azure databricks clusters or sql warehouses.
    
    Uses the databricks API's (2.0) to connect to the sql server.
    
    Args:
        driver_path: Driver installed to work with PYODBC
        server_hostname: Server hostname for the cluster or SQL Warehouse
        http_path: Http path for the cluster or SQL Warehouse
        access_token: Azure AD Token

    Note 1:
        More fields can be configured here in the connection ie PORT, Schema, etc.
        
    Note 2:
        When using Unix, Linux or Mac OS brew installation of PYODBC is required for connection.
  """
  def __init__(self, driver_path: str, server_hostname: str, http_path: str, access_token: str) -> None:

    self.connection = pyodbc.connect('Driver=' + driver_path +';' +
                                    'HOST=' + server_hostname + ';' +
                                    'PORT=443;' +
                                    'Schema=default;' +
                                    'SparkServerType=3;' +
                                    'AuthMech=11;' +
                                    'UID=token;' +
                                    #'PWD=' + access_token+ ";" +
                                    'Auth_AccessToken='+ access_token +';'
                                    'ThriftTransport=2;' +
                                    'SSL=1;' +
                                    'HTTPPath=' + http_path,
                                    autocommit=True)

  def close(self) -> None:
    """Closes connection to database."""
    try:
      self.connection.close()
    except Exception as e:
      logging.exception('error while closing the connection')
      raise e

  def cursor(self) -> object:
    """
    Intiates the cursor and returns it.

    Returns:
      PYODBCSQLCursor: Object to represent a databricks workspace with methods to interact with clusters/jobs.
    """
    try:
      return PYODBCSQLCursor(self.connection.cursor())
    except Exception as e:
      logging.exception('error with cursor object')
      raise e

 
class PYODBCSQLCursor(CursorInterface):
  """
  Object to represent a databricks workspace with methods to interact with clusters/jobs.

  Args:
      cursor: controls execution of commands on cluster or SQL Warehouse
  """
  def __init__(self, cursor: object) -> None:
    self.cursor = cursor

  def execute(self, query: str) -> None:
    """
    Prepares and runs a database query.

    Args:
        query: sql query to execute on the cluster or SQL Warehouse
    """
    try:
      self.cursor.execute(query)
      
    except Exception as e:
      logging.exception('error while executing the query')
      raise e

  def fetch_all(self) -> list: 
    """
    Gets all rows of a query.
    
    Returns:
        list: list of results
    """
    try:
      result = self.cursor.fetchall()
      cols = [column[0] for column in self.cursor.description]
      result = [list(x) for x in result]
      df = pd.DataFrame(result)
      df.columns = cols
      return df
    except Exception as e:
      logging.exception('error while fetching rows from the query')
      raise e

  def close(self) -> None: 
    """Closes the cursor."""
    try:
      self.cursor.close()
    except Exception as e:
      logging.exception('error while closing the cursor')
      raise e