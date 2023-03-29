# Getting started with using the RTDIP SDK

This article provides a guide on how to use RTDIP SDK. Before you get started, ensure you have installed the [RTDIP Python Package](https://pypi.org/project/rtdip-sdk/) and check the [RTDIP Installation Page](../getting-started/installation.md) for all the required prerequisites.


## How to use the SDK

The RTDIP SDK has many functionalities, such as allowing the user to authenticate, connect and/or use the most commonly requested methods.

### Authenticate

!!! note "Note"
        </b>If you are using the SDK on databricks please note that DefaultAuth will not work.<br />

1\. Import **rtdip-sdk** authentication methods with the following:

    from rtdip_sdk.authentication import authenticate as auth

2\. Use any of the following authentication methods. Replace **tenant_id** , **client_id**, **certificate_path** or **client_secret** with your own details.

#### Default Authentication
    DefaultAzureCredential = auth.DefaultAuth().authenticate()
    
#### Certificate Authentication
    CertificateCredential = auth.CertificateAuth(tenant_id: str, client_id: str, certificate_path: str).authenticate()

#### Client Secret Authentication
    ClientSecretCredential = auth.ClientSecretAuth(tenant_id: str, client_id: str, client_secret: str).authenticate()

3\. The methods above will return back a Client Object. The following example will show you how to retrieve the access_token from a credential object. The access token will be used in later steps to connect to RTDIP via the three options (Databricks SQL Connect, PYODBC SQL Connect, TURBODBC SQL Connect).
    
    access_token = DefaultAzureCredential.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token

### Sample Authentication Code:

```python
from rtdip_sdk.authentication import authenticate as auth

authentication = auth.DefaultAuth().authenticate()
access_token = authentication.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
```

For more information about each of the authentication methods, see [Code Reference](code-reference/query/authenticate.md) and navigate to the required section.

!!! note "Note"
    </b>If you are experiencing any trouble authenticating please see [Troubleshooting - Authentication](troubleshooting.md)<br />

### Connect to RTDIP

1\. The RTDIP SDK offers several ways to connect to a Databricks SQL Warehouse:

- Databricks SQL Connect: The simplest method to connect to RTDIP and does not require any additional installation steps.
- PYODBC SQL Connect: A popular library that python developers use for ODBC connectivity but requires more setup steps.
- TURBODBC SQL Connect: The RTDIP development team have found this to be the most performant method of connecting to RTDIP leveraging the arrow implementation within Turbodbc to obtain data, but requires a number of addditional installation steps to get working on OSX, Linux and Windows. 
   
    
2\. Replace **server_hostname**, **http_path** and **access_token** with your own details.

#### Databricks SQL Connect

```python
from rtdip_sdk.authentication import authenticate as auth
from rtdip_sdk.odbc import db_sql_connector as d

server_hostname = "server_hostname"
http_path = "http_path"

authentication = auth.DefaultAuth().authenticate()
access_token = authentication.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token

connection = d.DatabricksSQLConnection(server_hostname: str, http_path: str, access_token: str)
```

#### PYODBC SQL Connect

* [ODBC](https://databricks.com/spark/odbc-drivers-download) or [JDBC](https://databricks.com/spark/jdbc-drivers-download) are required to leverage PYODBC. Follow these [instructions](https://docs.databricks.com/integrations/jdbc-odbc-bi.html) to install the drivers in your environment.

* Microsoft Visual C++ 14.0 or greater is required. Get it from [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)

* Driver paths can be found on [PYODBC Driver Paths](code-reference/query/pyodbc-sql-connector.md)

```python
from rtdip_sdk.authentication import authenticate as auth
from rtdip_sdk.odbc import pyodbc_sql_connector as p

server_hostname = "server_hostname"
http_path = "http_path"
driver_path = "/Library/simba/spark/lib/libsparkodbc_sbu.dylib"

authentication = auth.DefaultAuth().authenticate()
access_token = authentication.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token

connection = p.PYODBCSQLConnection(driver_path: str, sever_hostname: str, http_path: str, access_token: str)
```

#### TURBODBC SQL Connect

* [ODBC](https://databricks.com/spark/odbc-drivers-download) or [JDBC](https://databricks.com/spark/jdbc-drivers-download) are required to leverage TURBODBC. Follow these [instructions](https://docs.databricks.com/integrations/jdbc-odbc-bi.html) to install the drivers in your environment.
* [Boost](https://turbodbc.readthedocs.io/en/latest/pages/getting_started.html) needs to be installed locally to use the [TURBODBC SQL Connector](code-reference/query/turbodbc-sql-connector.md) (<em>Optional</em>)

```python
from rtdip_sdk.authentication import authenticate as auth
from rtdip_sdk.odbc import turbodbc_sql_connector as t

server_hostname = "server_hostname"
http_path = "http_path"

authentication = auth.DefaultAuth().authenticate()
access_token = authentication.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token

connection = t.TURBODBCSQLConnection(server_hostname: str, http_path: str, access_token: str)
```

For more information about each of the connection methods, please see [Code Reference](code-reference/query/db-sql-connector.md) and navigate to the required section.

### Functions

Finally, after authenticating and connecting using one of the methods above, you have access to the commonly requested RTDIP functions such as **Resample**, **Interpolate**, **Raw**, **Time Weighted Averages** or **Metadata**. 

1\. To use any of the RTDIP functions, use the commands below.

```python
from rtdip_sdk.functions import resample
from rtdip_sdk.functions import interpolate
from rtdip_sdk.functions import raw
from rtdip_sdk.functions import time_weighted_average
from rtdip_sdk.functions import metadata
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

For more information about the function parameters see [Code Reference](code-reference/query/resample.md) and navigate through the required function.

### Example

This is a code example of the RTDIP SDK Interpolate function. You will need to replace the parameters with your own requirements and details. If you are unsure on the options please see [Code Reference - Interpolate](code-reference/query/interpolate.md) and navigate to the attributes section. 

```python
from rtdip_sdk.authentication import authenticate as auth
from rtdip_sdk.odbc import db_sql_connector as dbc
from rtdip_sdk.functions import interpolate

authentication = auth.DefaultAuth().authenticate()
access_token = authentication.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
connection = dbc.DatabricksSQLConnection("{server_hostname}", "{http_path}", access_token)

dict = {
    "business_unit": "{business_unit}", 
    "region": "{region}",
    "asset": "{asset}", 
    "data_security_level": "{date_security_level}",
    "data_type": "{data_type}", #options are float, integer, string and double (the majority of data is float)
    "tag_names": ["{tag_name_1}, {tag_name_2}"],
    "start_date": "2022-03-08", #start_date can be a date in the format "YYYY-MM-DD" or a datetime in the format "YYYY-MM-DDTHH:MM:SS"
    "end_date": "2022-03-10", #end_date can be a date in the format "YYYY-MM-DD" or a datetime in the format "YYYY-MM-DDTHH:MM:SS"
    "sample_rate": "1", #numeric input
    "sample_unit": "hour", #options are second, minute, day, hour
    "agg_method": "first", #options are first, last, avg, min, max
    "interpolation_method": "forward_fill", #options are forward_fill or backward_fill
    "include_bad_data": True #boolean options are True or False
}

result = interpolate.get(connection, dict)
print(result)
```
!!! note "Note"
    </b>If you are having problems please see [Troubleshooting](troubleshooting.md) for more information.<br />

### Conclusion

Congratulations! You have now learnt how to use the RTDIP SDK. Please check back for regular updates and if you would like to contribute, you can open an issue on GitHub. See the [Contributing Guide](https://github.com/rtdip/core/blob/develop/CONTRIBUTING.md) for more help.