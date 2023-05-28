# Time Weighted Average
::: src.sdk.python.rtdip_sdk.queries.time_series.time_weighted_average

## Example

```python
from rtdip_sdk.authentication.azure import DefaultAuth
from rtdip_sdk.connectors import DatabricksSQLConnection
from rtdip_sdk.queries import time_weighted_average

auth = DefaultAuth().authenticate()
token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

parameters = {
    "business_unit": "Business Unit",
    "region": "Region", 
    "asset": "Asset Name", 
    "data_security_level": "Security Level", 
    "data_type": "float", #options:["float", "double", "integer", "string"]
    "tag_names": ["tag_1", "tag_2"], #list of tags
    "start_date": "2023-01-01", #start_date can be a date in the format "YYYY-MM-DD" or a datetime in the format "YYYY-MM-DDTHH:MM:SS" or specify the timezone offset in the format "YYYY-MM-DDTHH:MM:SS+zz:zz"
    "end_date": "2023-01-31", #end_date can be a date in the format "YYYY-MM-DD" or a datetime in the format "YYYY-MM-DDTHH:MM:SS" or specify the timezone offset in the format "YYYY-MM-DDTHH:MM:SS+zz:zz"
    "window_size_mins": 15, #numeric input
    "window_length": 20, #numeric input
    "include_bad_data": True, #options: [True, False]
    "step": True
}
x = time_weighted_average.get(connection, parameters)
print(x)
```

This example is using [```DefaultAuth()```](../authentication/azure.md) and [```DatabricksSQLConnection()```](db-sql-connector.md) to authenticate and connect. You can find other ways to authenticate [here](../authentication/azure.md). The alternative built in connection methods are either by [```PYODBCSQLConnection()```](pyodbc-sql-connector.md), [```TURBODBCSQLConnection()```](turbodbc-sql-connector.md) or [```SparkConnection()```](spark-connector.md).

!!! note "Note"
    </b>```server_hostname``` and ```http_path``` can be found on the [SQL Warehouses Page](../../queries/databricks/sql-warehouses.md). <br />