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

import logging
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError

from ..interfaces import UtilitiesInterface
from .configuration import SparkConfigurationUtility
from ..._pipeline_utils.models import Libraries, SystemType


class SparkADLSGen2SPNConnectUtility(UtilitiesInterface):
    """
    Configures Spark to Connect to an ADLS Gen 2 Storage Account using a Service Principal.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.utilities import SparkADLSGen2SPNConnectUtility
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    adls_gen2_connect_utility = SparkADLSGen2SPNConnectUtility(
        spark=spark,
        storage_account="YOUR-STORAGAE-ACCOUNT-NAME",
        tenant_id="YOUR-TENANT-ID",
        client_id="YOUR-CLIENT-ID",
        client_secret="YOUR-CLIENT-SECRET"
    )

    result = adls_gen2_connect_utility.execute()
    ```

    Parameters:
        spark (SparkSession): Spark Session required to read data from cloud storage
        storage_account (str): Name of the ADLS Gen 2 Storage Account
        tenant_id (str): Tenant ID of the Service Principal
        client_id (str): Service Principal Client ID
        client_secret (str): Service Principal Client Secret
    """

    spark: SparkSession
    storage_account: str
    tenant_id: str
    client_id: str
    client_secret: str

    def __init__(
        self,
        spark: SparkSession,
        storage_account: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
    ) -> None:
        self.spark = spark
        self.storage_account = storage_account
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK
        """
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def execute(self) -> bool:
        """Executes spark configuration to connect to an ADLS Gen 2 Storage Account using a service principal"""
        try:
            adls_gen2_config = SparkConfigurationUtility(
                spark=self.spark,
                config={
                    "fs.azure.account.auth.type.{}.dfs.core.windows.net".format(
                        self.storage_account
                    ): "OAuth",
                    "fs.azure.account.oauth.provider.type.{}.dfs.core.windows.net".format(
                        self.storage_account
                    ): "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                    "fs.azure.account.oauth2.client.id.{}.dfs.core.windows.net".format(
                        self.storage_account
                    ): self.client_id,
                    "fs.azure.account.oauth2.client.secret.{}.dfs.core.windows.net".format(
                        self.storage_account
                    ): self.client_secret,
                    "fs.azure.account.oauth2.client.endpoint.{}.dfs.core.windows.net".format(
                        self.storage_account
                    ): "https://login.microsoftonline.com/{}/oauth2/token".format(
                        self.tenant_id
                    ),
                },
            )
            adls_gen2_config.execute()
            return True

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e
