# Weather Query Builder
::: src.sdk.python.rtdip_sdk.queries.weather.weather_query_builder

## Examples 

```python
from rtdip_sdk.queries.weather.weather_query_builder import (
    WeatherQueryBuilder,
)
from rtdip_sdk.authentication.azure import DefaultAuth
from rtdip_sdk.connectors import DatabricksSQLConnection


auth = DefaultAuth().authenticate()
token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", token)

table = "weather.forecast.table"


data = (
        WeatherQueryBuilder()
        .connect(connection)
        .source(table, status_column=None)
        .raw_point(
            start_date="2021-01-01",
            end_date="2021-01-02",
            forecast_run_start_date="2021-01-01",
            forecast_run_end_date="2021-01-02",
            lat=0.1,
            lon=0.1,
        )
    )

data = (
        WeatherQueryBuilder()
        .connect(connection)
        .source(table, status_column=None)
        .latest_point(
            lat=0.1,
            lon=0.1,
        )
    )

data = (
        WeatherQueryBuilder()
        .connect(connection)
        .source(table, status_column=None)
        .raw_grid(
            start_date="2021-01-01",
            end_date="2021-01-02",
            forecast_run_start_date="2021-01-01",
            forecast_run_end_date="2021-01-02",
            min_lat=0.1,
            max_lat=0.1,
            min_lon=0.1,
            max_lon=0.1,
        )
    )

data = (
        WeatherQueryBuilder()
        .connect(connection)
        .source(table, status_column=None)
        .latest_grid(
            min_lat=0.1,
            max_lat=0.1,
            min_lon=0.1,
            max_lon=0.1,
        )
    )

```

The above example shows the 4 ways the Weather Query Builder can be use to get_point, get_grid, latest_point and latest_grid.

These examples are using [```DefaultAuth()```](../../../authentication/azure.md) and [```DatabricksSQLConnection()```](../../connectors/db-sql-connector.md) to authenticate and connect. You can find other ways to authenticate [here](../../../authentication/azure.md). The alternative built in connection methods are either by [```PYODBCSQLConnection()```](../../connectors/pyodbc-sql-connector.md), [```TURBODBCSQLConnection()```](../../connectors/turbodbc-sql-connector.md) or [```SparkConnection()```](../../connectors/spark-connector.md).

!!! note "Note"
    </b>```server_hostname``` and ```http_path``` can be found on the [SQL Warehouses Page](../../../../queries/databricks/sql-warehouses.md). <br />