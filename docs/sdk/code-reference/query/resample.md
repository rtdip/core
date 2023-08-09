# Resample Function
::: src.sdk.python.rtdip_sdk.queries.time_series.resample

## Example
```python
--8<-- "https://raw.githubusercontent.com/rodalynbarce/samples/feature/dagsterv2/queries/Resample/resample.py"
```

This example is using [```DefaultAuth()```](../authentication/azure.md) and [```DatabricksSQLConnection()```](db-sql-connector.md) to authenticate and connect. You can find other ways to authenticate [here](../authentication/azure.md). The alternative built in connection methods are either by [```PYODBCSQLConnection()```](pyodbc-sql-connector.md), [```TURBODBCSQLConnection()```](turbodbc-sql-connector.md) or [```SparkConnection()```](spark-connector.md).

!!! note "Note"
    See [Samples Repository](https://github.com/rodalynbarce/samples/tree/feature/dagsterv2/queries) for full list of examples.

!!! note "Note"
    </b>```server_hostname``` and ```http_path``` can be found on the [SQL Warehouses Page](../../queries/databricks/sql-warehouses.md). <br />