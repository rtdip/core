# Copyright 2022 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .interfaces import SecretsInterface
from pyspark.sql import SparkSession
from .._pipeline_utils.spark import get_dbutils
from .._pipeline_utils.models import Libraries, SystemType


class DatabricksSecrets(SecretsInterface):
    """
    Retrieves secrets from Databricks Secret Scopes. For more information about Databricks Secret Scopes, see [here.](https://docs.databricks.com/security/secrets/secret-scopes.html)

    Example
    -------
    ```python
    # Reads Secrets from Databricks Secret Scopes

    from rtdip_sdk.pipelines.secrets import DatabricksSecrets
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    get_databricks_secret = DatabricksSecrets(
        spark=spark,
        vault="{NAME-OF-DATABRICKS-SECRET-SCOPE}"
        key="{KEY-NAME-OF-SECRET}",
    )

    get_databricks_secret.get()
    ```

    Parameters:
        spark: Spark Session required to read data from a Delta table
        vault: Name of the Databricks Secret Scope
        key: Name/Key of the secret in the Databricks Secret Scope
    """

    spark: SparkSession
    vault: str
    key: str

    def __init__(self, spark: SparkSession, vault: str, key: str):
        self.spark = spark
        self.vault = vault
        self.key = key

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK on Databricks
        """
        return SystemType.PYSPARK_DATABRICKS

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def get(self):
        """
        Retrieves the secret from the Databricks Secret Scope
        """
        dbutils = get_dbutils(self.spark)
        return dbutils.secrets.get(scope=self.vault, key=self.key)

    def set(self):
        """
        Sets the secret in the Secret Scope
        Raises:
            NotImplementedError: Will be implemented at a later point in time
        """
        return NotImplementedError
