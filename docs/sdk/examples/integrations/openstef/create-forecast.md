# Creating a forecast
This page provides a guide on how to create a forecast using RTDIP's Database Connector with OpenSTEF's `create_forecast` task. OpenSTEF's `create_forecast` task gets historic training data, applies features, loads models, makes a prediction and writes it to the database. More information on this task can be found [here](https://openstef.github.io/openstef/openstef.tasks.html#module-openstef.tasks.create_forecast). This pipeline was tested on an M2 Macbook Pro using VS Code in a Python (3.10) environment.

## Prerequisites
The following prerequisites must be met to run this task:

* Ensure the RTDIP SDK has been installed with the `integrations` dependency as follows:
`pip install "rtdip-sdk[integrations]"`.
Further instructions can be found [here](../../../../getting-started/installation.md#installing-the-rtdip-sdk). 

* [Configure your environment](https://docs.databricks.com/en/mlflow/access-hosted-tracking-server.html) to access the MLflow tracking server from outside Databricks. This example assumes you have a profile set up in the `.databrickscfg` file.

## Components
|Name|Description|
|--|--|
|[Database](../../../code-reference/integrations/openstef/database.md)|Provides an interface to get and write data for OpenSTEF pipelines and tasks.|

## Example
Below is an example of how to set up a python file to create a forecast. A `DATABRICKS_WORKSPACE_PATH` environment variable must be set to write experiments to a Databricks workspace.

```python
import os
from openstef.tasks import create_forecast as task
from pydantic.v1 import BaseSettings
from typing import Union
from rtdip_sdk.integrations.openstef.database import DataBase
from rtdip_sdk.authentication.azure import DefaultAuth

auth = DefaultAuth().authenticate()
token = auth.get_token("2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default").token

os.environ[
    "DATABRICKS_WORKSPACE_PATH"
] = "/Users/{username}/path/to/experiment"

class ConfigSettings(BaseSettings):
    pcdm_host: str = "{DATABRICKS-SERVER-HOSTNAME}"
    pcdm_token: str = token
    pcdm_port: int = 443
    pcdm_http_path: str = "{SQL-WAREHOUSE-HTTP-PATH}"
    pcdm_catalog: str = "{YOUR-CATALOG-NAME}"
    pcdm_schema: str = "{YOUR-SCHEMA-NAME}"
    db_host: str = "{DATABRICKS-SERVER-HOSTNAME}"
    db_token: str = token
    db_port: int = 443
    db_http_path: str = "{SQL-WAREHOUSE-HTTP-PATH}"
    db_catalog: str = "{YOUR-CATALOG-NAME}"
    db_schema: str = "{YOUR-SCHEMA-NAME}"
    proxies: Union[dict[str, str], None] = None
    externally_posted_forecasts_pids: list = None
    known_zero_flatliners: list = []
    paths_mlflow_tracking_uri: str = (
        "databricks://<profileName>"
    )
    paths_artifact_folder: str = "/path/to/mlflow/artifacts"

config = ConfigSettings()

def main():
    database = DataBase(config)
    task.main(config=config, database=database)


if __name__ == "__main__":
    main()
```