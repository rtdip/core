# Interpolate Function
::: src.sdk.python.rtdip_sdk.queries.time_series.interpolate

## Example
```python
--8<-- "https://raw.githubusercontent.com/rtdip/samples/main/queries/TimeSeriesQueryBuilder/Interpolate/interpolate.py"
```

This example is using [```DefaultAuth()```](../../../authentication/azure.md) and [```DatabricksSQLConnection()```](../../connectors/db-sql-connector.md) to authenticate and connect. You can find other ways to authenticate [here](../../../authentication/azure.md). The alternative built in connection methods are either by [```PYODBCSQLConnection()```](../../connectors/pyodbc-sql-connector.md), [```TURBODBCSQLConnection()```](../../connectors/turbodbc-sql-connector.md) or [```SparkConnection()```](../../connectors/spark-connector.md).

!!! note "Note"
    See [Samples Repository](https://github.com/rtdip/samples/tree/main/queries) for full list of examples.

!!! note "Note"
    </b>```server_hostname``` and ```http_path``` can be found on the [SQL Warehouses Page](../../../../queries/databricks/sql-warehouses.md). <br />
