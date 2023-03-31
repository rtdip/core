# Connectors

RTDIP SDK provides functionality to connect to and query its data using connectors. Below is a list of the available connectors.

## ODBC

### Databricks SQL Connector

Enables connectivity to Databricks using the [Databricks SQL Connector](https://pypi.org/project/databricks-sql-connector/) which does not require any ODBC installation. 

For more information refer to this [documentation](https://docs.databricks.com/dev-tools/python-sql-connector.html) and for the specific implementation within the RTDIP SDK, refer to this [link](../code-reference/query/db-sql-connector.md)

#### Example

```python
from rtdip_sdk.odbc import db_sql_connector

server_hostname = "server_hostname"
http_path = "http_path"
access_token = "token"

connection = db_sql_connector.DatabricksSQLConnection(server_hostname, http_path, access_token)
```

Replace **server_hostname**, **http_path** and **access_token** with your own information.

### PYODBC SQL Connector

[PYDOBC](https://pypi.org/project/pyodbc/) is a popular python package for querying data using ODBC. Refer to their [documentation](https://github.com/mkleehammer/pyodbc/wiki) for more information about pyodbc and how you can leverage it in your code.

View information about how pyodbc is implemented in the RTDIP SDK [here.](../code-reference/query/pyodbc-sql-connector.md)

#### Example

```python
from rtdip_sdk.odbc import pyodbc_sql_connector

server_hostname = "server_hostname"
http_path = "http_path"
access_token = "token"
driver_path = "/Library/simba/spark/lib/libsparkodbc_sbu.dylib"

connection = pyodbc_sql_connector.PYODBCSQLConnection(driver_path, sever_hostname, http_path, access_token)
```

Replace **server_hostname**, **http_path** and **access_token** with your own information.

### TURBODBC SQL Connector 

Turbodbc is a powerful python ODBC package that has advanced options for querying performance. Find out more about installing it on your operation system and what Turbodbc can do [here](https://turbodbc.readthedocs.io/en/latest/) and refer to this [documentation](../code-reference/query/turbodbc-sql-connector.md) for more information about how it is implemented in the RTDIP SDK.

#### Example
```python
from rtdip_sdk.odbc import turbodbc_sql_connector

server_hostname = "server_hostname"
http_path = "http_path"
access_token = "token"

connection = turbodbc_sql_connector.TURBODBCSQLConnection(server_hostname, http_path, access_token)
```

Replace **server_hostname**, **http_path** and **access_token** with your own information.

