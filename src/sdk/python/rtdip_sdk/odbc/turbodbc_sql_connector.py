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
from .connection_interface import ConnectionInterface
from .cursor_interface import CursorInterface
import logging

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
  def __init__(self, server_hostname: str, http_path: str, access_token: str) -> None:
    options = make_options(
        autocommit=True, 
        read_buffer_size=Megabytes(100),
        use_async_io=True)
    self.connection = connect(Driver="Simba Spark ODBC Driver",
                              Server=server_hostname,
                              HOST=server_hostname,
                              PORT=443,
                              SparkServerType=3,
                              Schema="default",
                              ThriftTransport=2,
                              SSL=1,
                              AuthMech=11,
                              Auth_AccessToken=access_token,
                              Auth_Flow=0,
                              HTTPPath=http_path,
                              UseNativeQuery=1,
                              FastSQLPrepare=1,
                              ApplyFastSQLPrepareToAllQueries=1,
                              DisableLimitZero=1,                      
                              EnableAsyncExec=1,
                              turbodbc_options=options)

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
      TURBODBCSQLCursor: Object to represent a databricks workspace with methods to interact with clusters/jobs.
    """
    try:
      return TURBODBCSQLCursor(self.connection.cursor())
    except Exception as e:
      logging.exception('error with cursor object')
      raise e


class TURBODBCSQLCursor(CursorInterface):
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
      df = pd.DataFrame(result)
      df.columns = cols
      return df
    except Exception as e:
      logging.exception('error while fetching the rows from the query')
      raise e

  def close(self) -> None: 
    """Closes the cursor."""
    try:
      self.cursor.close()
    except Exception as e:
      logging.exception('error while closing the cursor')
      raise e