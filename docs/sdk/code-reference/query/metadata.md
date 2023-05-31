# Metadata Function
::: src.sdk.python.rtdip_sdk.queries.metadata

## Example
```python
from rtdip_sdk.authentication.azure import DefaultAuth
from rtdip_sdk.connectors import DatabricksSQLConnection
from rtdip_sdk.queries import metadata

auth = DefaultAuth().authenticate()
token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

parameters = {
    "business_unit": "Business Unit",
    "region": "Region", 
    "asset": "Asset Name", 
    "data_security_level": "Security Level",
    "tag_names": ["tag_1", "tag_2"], #list of tags
}
x = metadata.get(connection, parameters)
print(x)
```

This example is using [```DefaultAuth()```](../authentication/azure.md) and [```DatabricksSQLConnection()```](db-sql-connector.md) to authenticate and connect. You can find other ways to authenticate [here](../authentication/azure.md). The alternative built in connection methods are either by [```PYODBCSQLConnection()```](pyodbc-sql-connector.md), [```TURBODBCSQLConnection()```](turbodbc-sql-connector.md) or [```SparkConnection()```](spark-connector.md).

!!! note "Note"
    </b>```server_hostname``` and ```http_path``` can be found on the [SQL Warehouses Page](../../queries/databricks/sql-warehouses.md). <br />
