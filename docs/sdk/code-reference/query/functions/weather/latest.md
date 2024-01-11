# Weather Latest Function
::: src.sdk.python.rtdip_sdk.queries.weather.latest

## Examples get_point

```python
from rtdip_sdk.authentication.azure import DefaultAuth
from rtdip_sdk.queries.weather.latest import get_point
from rtdip_sdk.connectors import DatabricksSQLConnection

auth = DefaultAuth().authenticate()
token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

params = {
    "source": "forecast`.`weather`.`mock_region_mock_security_events_mock_data_type",
    "lat": 1.1,
    "lon": 1.1,
}

x = get_point(connection, params)

print(x)
```

```python
from rtdip_sdk.authentication.azure import DefaultAuth
from rtdip_sdk.queries.weather.latest import get_point
from rtdip_sdk.connectors import DatabricksSQLConnection

auth = DefaultAuth().authenticate()
token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

params = {
    "forecast": "forecast",
    "forecast_type": "weather",
    "region": "mock_region",
    "data_security_level": "mock_security",
    "data_type": "mock_data_type",
    "lat": 1.1,
    "lon": 1.1,
}

x = get_point(connection, params)

print(x)
```

The above examples use two different routes to select the table, one directly uses a table name provided the other builds the table name from the variables.

These examples are using [```DefaultAuth()```](../../../authentication/azure.md) and [```DatabricksSQLConnection()```](../../connectors/db-sql-connector.md) to authenticate and connect. You can find other ways to authenticate [here](../../../authentication/azure.md). The alternative built in connection methods are either by [```PYODBCSQLConnection()```](../../connectors/pyodbc-sql-connector.md), [```TURBODBCSQLConnection()```](../../connectors/turbodbc-sql-connector.md) or [```SparkConnection()```](../../connectors/spark-connector.md).

!!! note "Note"
    </b>```server_hostname``` and ```http_path``` can be found on the [SQL Warehouses Page](../../../../queries/databricks/sql-warehouses.md). <br />

    
## Examples get_grid

```python
from rtdip_sdk.authentication.azure import DefaultAuth
from rtdip_sdk.queries.weather.latest import get_grid
from rtdip_sdk.connectors import DatabricksSQLConnection

auth = DefaultAuth().authenticate()
token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

params = {
    "source": "forecast`.`weather`.`mock_region_mock_security_events_mock_data_type",
    "min_lat": 36,
    "max_lat": 38,
    "min_lon": -109.1,
    "max_lon": -107.1,
}

x = get_grid(connection, params)

print(x)
```

```python
from rtdip_sdk.authentication.azure import DefaultAuth
from rtdip_sdk.queries.weather.latest import get_point
from rtdip_sdk.connectors import DatabricksSQLConnection

auth = DefaultAuth().authenticate()
token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

params = {
    "forecast": "forecast",
    "forecast_type": "weather",
    "region": "mock_region",
    "data_security_level": "mock_security",
    "data_type": "mock_data_type",
    "min_lat": 36,
    "max_lat": 38,
    "min_lon": -109.1,
    "max_lon": -107.1,
}

x = get_grid(connection, params)

print(x)
```

The above examples use two different routes to select the table, one directly uses a table name provided the other builds the table name from the variables.

These examples are using [```DefaultAuth()```](../../../authentication/azure.md) and [```DatabricksSQLConnection()```](../../connectors/db-sql-connector.md) to authenticate and connect. You can find other ways to authenticate [here](../../../authentication/azure.md). The alternative built in connection methods are either by [```PYODBCSQLConnection()```](../../connectors/pyodbc-sql-connector.md), [```TURBODBCSQLConnection()```](../../connectors/turbodbc-sql-connector.md) or [```SparkConnection()```](../../connectors/spark-connector.md).

!!! note "Note"
    </b>```server_hostname``` and ```http_path``` can be found on the [SQL Warehouses Page](../../../../queries/databricks/sql-warehouses.md). <br />