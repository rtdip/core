# Query Databricks SQL using the RTDIP SDK

This article provides a guide on how to use RTDIP SDK to query data via Databricks SQL. Before getting started, ensure you have installed the [RTDIP Python Package](https://pypi.org/project/rtdip-sdk/) and check the [RTDIP Installation Page](../../../getting-started/installation.md) for all the required prerequisites.


## How to use RTDIP SDK with Databricks SQL

The RTDIP SDK has rich support of querying data using Databricks SQL, such as allowing the user to authenticate, connect and/or use the most commonly requested methods for manipulating time series data accessible via Databricks SQL.

### Authentication

=== "Azure Active Directory"

    Refer to the [Azure Active Directory](../../authentication/azure.md) documentation for further options to perform Azure AD authentication, such as Service Principal authentication using certificates or secrets. Below is an example of performing default authentication that retrieves a token for Azure Databricks. 

    Also refer to the [Code Reference](../../code-reference/authentication/azure.md) for further technical information.

    ```python
    from rtdip_sdk.authentication import authenticate as auth

    authentication = auth.DefaultAuth().authenticate()
    access_token = authentication.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
    ```

    !!! note "Note"
        </b>If you are experiencing any trouble authenticating please see [Troubleshooting - Authentication](troubleshooting.md)<br />

=== "Databricks"

    Refer to the [Databricks](../../authentication/databricks.md) documentation for further information about generating a Databricks PAT Token. Below is an example of performing default authentication that retrieves a token for a Databricks Workspace. 

    Provide your `dbapi.....` token to the `access_token` in the examples below.

    ```python
    access_token = "dbapi.........."
    ```

### Connect to Databricks SQL

The RTDIP SDK offers several ways to connect to a Databricks SQL Warehouse.

=== "Databricks SQL Connector"

    The simplest method to connect to RTDIP and does not require any additional installation steps.

    ```python
    from rtdip_sdk.connectors import DatabricksSQLConnection

    server_hostname = "server_hostname"
    http_path = "http_path"
    access_token = "token"

    connection = DatabricksSQLConnection(server_hostname, http_path, access_token)
    ```

    Replace **server_hostname**, **http_path** and **access_token** with your own information.

    For more information about each of the connection methods, please see [Code Reference](../../code-reference/query/connectors/db-sql-connector.md) and navigate to the required section.

=== "PYODBC"

    A popular library that python developers use for ODBC connectivity but requires more setup steps.

    [ODBC](https://databricks.com/spark/odbc-drivers-download) or [JDBC](https://databricks.com/spark/jdbc-drivers-download) are required to leverage PYODBC. Follow these [instructions](https://docs.databricks.com/integrations/jdbc-odbc-bi.html) to install the drivers in your environment.

    * Microsoft Visual C++ 14.0 or greater is required. Get it from [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)

    * Driver paths can be found on [PYODBC Driver Paths](../../code-reference/query/connectors/pyodbc-sql-connector.md)

    ```python
    from rtdip_sdk.connectors import PYODBCSQLConnection

    server_hostname = "server_hostname"
    http_path = "http_path"
    access_token = "token"
    driver_path = "/Library/simba/spark/lib/libsparkodbc_sbu.dylib"

    connection = PYODBCSQLConnection(driver_path, sever_hostname, http_path, access_token)
    ```

    Replace **server_hostname**, **http_path** and **access_token** with your own information.

    For more information about each of the connection methods, please see [Code Reference](../../code-reference/query/connectors/pyodbc-sql-connector.md) and navigate to the required section.

=== "TURBODBC"
    The RTDIP development team have found this to be the most performant method of connecting to RTDIP leveraging the arrow implementation within Turbodbc to obtain data, but requires a number of additional installation steps to get working on OSX, Linux and Windows

    * [ODBC](https://databricks.com/spark/odbc-drivers-download) or [JDBC](https://databricks.com/spark/jdbc-drivers-download) are required to leverage TURBODBC. Follow these [instructions](https://docs.databricks.com/integrations/jdbc-odbc-bi.html) to install the drivers in your environment.
    * [Boost](https://turbodbc.readthedocs.io/en/latest/pages/getting_started.html) needs to be installed locally to use the [TURBODBC SQL Connector](../../code-reference/query/connectors/turbodbc-sql-connector.md) (<em>Optional</em>)

    ```python
    from rtdip_sdk.connectors import TURBODBCSQLConnection

    server_hostname = "server_hostname"
    http_path = "http_path"
    access_token = "token"

    connection = TURBODBCSQLConnection(server_hostname, http_path, access_token)
    ```

    Replace **server_hostname**, **http_path** and **access_token** with your own information.

    For more information about each of the connection methods, please see [Code Reference](../../code-reference/query/connectors/turbodbc-sql-connector.md) and navigate to the required section.
    

### Functions

Finally, after authenticating and connecting using one of the methods above, you have access to the commonly requested RTDIP functions such as **Resample**, **Interpolate**, **Raw**, **Time Weighted Averages** or **Metadata**. 

1\. To use any of the RTDIP functions, use the commands below.

```python
from rtdip_sdk.queries import resample
from rtdip_sdk.queries import interpolate
from rtdip_sdk.queries import raw
from rtdip_sdk.queries import time_weighted_average
from rtdip_sdk.queries import metadata
```

2\. From functions you can use any of the following methods.

#### Resample
    resample.get(connection, parameters_dict)

#### Interpolate
    interpolate.get(connection, parameters_dict)

#### Raw
    raw.get(connection, parameters_dict)

#### Time Weighted Average
    time_weighted_average.get(connection, parameter_dict)

#### Metadata
    metadata.get(connection, parameter_dict)

For more information about the function parameters see [Code Reference](../../code-reference/query/functions/time_series/resample.md) and navigate through the required function.

### Example

This is a code example of the RTDIP SDK Interpolate function. You will need to replace the parameters with your own requirements and details. If you are unsure on the options please see [Code Reference - Interpolate](../../code-reference/query/functions/time_series/interpolate.md) and navigate to the attributes section. 

```python
from rtdip_sdk.authentication import authenticate as auth
from rtdip_sdk.connectors import DatabricksSQLConnection
from rtdip_sdk.queries import interpolate

authentication = auth.DefaultAuth().authenticate()
access_token = authentication.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", access_token)

parameters = {
    "business_unit": "{business_unit}", 
    "region": "{region}",
    "asset": "{asset}", 
    "data_security_level": "{date_security_level}",
    "data_type": "{data_type}", #options are float, integer, string and double (the majority of data is float)
    "tag_names": ["{tag_name_1}, {tag_name_2}"],
    "start_date": "2022-03-08", #start_date can be a date in the format "YYYY-MM-DD" or a datetime in the format "YYYY-MM-DDTHH:MM:SS"
    "end_date": "2022-03-10", #end_date can be a date in the format "YYYY-MM-DD" or a datetime in the format "YYYY-MM-DDTHH:MM:SS"
    "time_interval_rate": "1", #numeric input
    "time_interval_unit": "hour", #options are second, minute, day or hour
    "agg_method": "first", #options are first, last, avg, min, max
    "interpolation_method": "forward_fill", #options are forward_fill, backward_fill or linear
    "include_bad_data": True #boolean options are True or False
}

result = interpolate.get(connection, parameters)
print(result)
```
!!! note "Note"
    </b>If you are having problems please see [Troubleshooting](../../../sdk/queries/databricks/troubleshooting.md) for more information.<br />

### Conclusion

Congratulations! You have now learnt how to use the RTDIP SDK. Please check back for regular updates and if you would like to contribute, you can open an issue on GitHub. See the [Contributing Guide](https://github.com/rtdip/core/blob/develop/CONTRIBUTING.md) for more help.