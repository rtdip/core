# Databricks 

<!-- --8<-- [start:databrickspat] -->

Databricks supports authentication using Personal Access Tokens (PAT) and information about this authentication method is available [here.](https://docs.databricks.com/dev-tools/api/latest/authentication.html)

## Authentication

To generate a Databricks PAT Token, follow this [guide](https://docs.databricks.com/dev-tools/api/latest/authentication.html#generate-a-personal-access-token) and ensure that the token is stored securely and is never used directly in code.

Your Databricks PAT Token can be used in the RTDIP SDK to authenticate with any Databricks Workspace or Databricks SQL Warehouse and simply provided in the `access_token` fields where tokens are required in the RTDIP SDK.

## Example

Below is an example of using a Databricks PAT Token for authenticating with a Databricks SQL Warehouse.

```python
from rtdip_sdk.connectors import DatabricksSQLConnection

server_hostname = "server_hostname"
http_path = "http_path"
access_token = "dbapi......."

connection = DatabricksSQLConnection(server_hostname, http_path, access_token)
```

Replace **server_hostname**, **http_path** with your own information and specify your Databricks PAT token for the **access_token**. 

<!-- --8<-- [end:databrickspat] -->