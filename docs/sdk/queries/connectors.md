# Connectors

RTDIP SDK provides functionality to connect to and query its data using connectors. Below is a list of the available connectors.

## ODBC

### Databricks SQL Connector

Enables connectivity to Databricks using the [Databricks SQL Connector](https://pypi.org/project/databricks-sql-connector/) which does not require any ODBC installation. 

For more information refer to this [documentation](https://docs.databricks.com/dev-tools/python-sql-connector.html) and for the specific implementation within the RTDIP SDK, refer to this [link](../code-reference/query/db-sql-connector.md)

```python
from rtdip_sdk.connectors import DatabricksSQLConnection

server_hostname = "server_hostname"
http_path = "http_path"
access_token = "token"

connection = DatabricksSQLConnection(server_hostname, http_path, access_token)
```

Replace **server_hostname**, **http_path** and **access_token** with your own information.

### PYODBC SQL Connector

[PYDOBC](https://pypi.org/project/pyodbc/) is a popular python package for querying data using ODBC. Refer to their [documentation](https://github.com/mkleehammer/pyodbc/wiki) for more information about pyodbc and how you can leverage it in your code.

View information about how pyodbc is implemented in the RTDIP SDK [here.](../code-reference/query/pyodbc-sql-connector.md)

```python
from rtdip_sdk.connectors import PYODBCSQLConnection

server_hostname = "server_hostname"
http_path = "http_path"
access_token = "token"
driver_path = "/Library/simba/spark/lib/libsparkodbc_sbu.dylib"

connection = PYODBCSQLConnection(driver_path, sever_hostname, http_path, access_token)
```

Replace **server_hostname**, **http_path** and **access_token** with your own information.

### TURBODBC SQL Connector 

Turbodbc is a powerful python ODBC package that has advanced options for querying performance. Find out more about installing it on your operation system and what Turbodbc can do [here](https://turbodbc.readthedocs.io/en/latest/) and refer to this [documentation](../code-reference/query/turbodbc-sql-connector.md) for more information about how it is implemented in the RTDIP SDK.

```python
from rtdip_sdk.connectors import TURBODBCSQLConnection

server_hostname = "server_hostname"
http_path = "http_path"
access_token = "token"

connection = TURBODBCSQLConnection(server_hostname, http_path, access_token)
```

Replace **server_hostname**, **http_path** and **access_token** with your own information.

## Spark 

### Spark Connector 

The Spark Connector enables querying of data using a Spark Session. This is useful for querying local instances of Spark or Delta. However, the most useful application of this connector is to leverage [Spark Connect](https://spark.apache.org/docs/latest/spark-connect-overview.html) to enable connecting to a remote Spark Cluster to provide the compute for the query being run from a local machine.

```python
from rtdip_sdk.connectors import SparkConnection

spark_server = "spark_server"
access_token = "my_token"

spark_remote = "sc://{}:443;token={}".format(spark_server, access_token)
connection = SparkConnection(spark_remote=spark_remote)
```

Replace the **access_token** with your own information.