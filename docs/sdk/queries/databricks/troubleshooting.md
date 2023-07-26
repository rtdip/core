# Troubleshooting

### Cannot install pyodbc

Microsoft Visual C++ 14.0 or greater is required to install pyodbc. Get it with [Microsoft C++ Build Tools](https://visualstudio.microsoft.com/visual-cpp-build-tools/)

### Cannot build wheels (Using legacy setup.py)

To install rtdip-sdk using setup.py, you need to have **wheel** installed using the following command:
    
    pip install wheel

### Authentication

For Default Credential authentication, a number of troubleshooting options are available [here](https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/identity/azure-identity/TROUBLESHOOTING.md#troubleshooting-default-azure-credential-authentication-issues).

For Visual Studio Code errors, the version of Azure Account extension is installed(0.9.11) - **To authenticate in Visual Studio Code, ensure version 0.9.11 or earlier of the Azure Account extension is installed. To track progress toward supporting newer extension versions, see [this GitHub issue](https://github.com/Azure/azure-sdk-for-net/issues/27263). Once installed, open the Command Palette and run the Azure: Sign In command**

### Exception has occurred: TypeError 'module' object is not callable

Ensure you are importing and using the RTDIP SDK functions correctly. You will need to give the module a name and reference it when using the function. See below for a code example. 

```python
from rtdip_sdk.authentication import authenticate as auth
from rtdip_sdk.connectors import DatabricksSQLConnection
from rtdip_sdk.queries import interpolate

authentication = auth.DefaultAuth().authenticate()
access_token = authentication.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token
connection = DatabricksSQLConnection("{server_hostname}", "{http_path}", access_token)

dict = {
    "business_unit": "{business_unit}", 
    "region": "{region}", 
    "asset": "{asset}", 
    "data_security_level": "{date_security_level}",
    "data_type": "{data_type}", #options are float, integer, string and double (the majority of data is float)
    "tag_names": ["{tag_name_1}, {tag_name_2}"],
    "start_date": "2022-03-08", #start_date can be a date in the format "YYYY-MM-DD" or a datetime in the format "YYYY-MM-DDTHH:MM:SS"
    "end_date": "2022-03-10", #end_date can be a date in the format "YYYY-MM-DD" or a datetime in the format "YYYY-MM-DDTHH:MM:SS"
    "time_interval_rate": "1", #numeric input
    "time_interval_unit": "hour", #options are second, minute, day, hour
    "agg_method": "first", #options are first, last, avg, min, max
    "interpolation_method": "forward_fill", #options are forward_fill or backward_fill
    "include_bad_data": True #boolean options are True or False
}

result = interpolate.get(connection, dict)
print(result)
```

### Databricks ODBC/JDBC Driver issues

#### General Troubleshooting

Most issues related to the installation or performance of the ODBC/JDBC driver are documented [here.](https://docs.microsoft.com/en-us/azure/databricks/kb/bi/jdbc-odbc-troubleshooting)

#### ODBC with a proxy

Follow this document to use the ODBC driver with a [proxy](https://docs.microsoft.com/en-us/azure/databricks/kb/bi/configure-simba-proxy-windows).  


