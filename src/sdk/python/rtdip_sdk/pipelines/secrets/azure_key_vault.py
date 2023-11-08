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
from azure.keyvault.secrets import SecretClient
from .._pipeline_utils.models import Libraries, SystemType
from .._pipeline_utils.constants import get_default_package


class AzureKeyVaultSecrets(SecretsInterface):
    """
    Retrieves and creates/updates secrets in Azure Key Vault. For more information about Azure Key Vaults, see [here.](https://learn.microsoft.com/en-gb/azure/key-vault/general/overview)

    Example
    -------
    ```python
    # Retrieves Secrets from Azure Key Vault

    from rtdip_sdk.pipelines.secrets import AzureKeyVaultSecrets

    get_key_vault_secret = AzureKeyVaultSecrets(
        vault="https://{YOUR-KEY-VAULT}.azure.net/",
        key="{KEY}",
        secret=None,
        credential="{CREDENTIAL}",
        kwargs=None
    )

    get_key_vault_secret.get()

    ```
    ```python
    # Creates or Updates Secrets in Azure Key Vault

    from rtdip_sdk.pipelines.secrets import AzureKeyVaultSecrets

    set_key_vault_secret = AzureKeyVaultSecrets(
        vault="https://{YOUR-KEY-VAULT}.azure.net/",
        key="{KEY}",
        secret="{SECRET-TO-BE-SET}",
        credential="{CREDENTIAL}",
        kwargs=None
    )

    set_key_vault_secret.set()
    ```

    Parameters:
        vault (str): Azure Key Vault URL
        key (str): Key for the secret
        secret (str): Secret or Password to be set in the Azure Key Vault
        credential (str): Credential for authenticating with Azure Key Vault
        kwargs (dict): List of additional parameters to be passed when creating a Azure Key Vault Client. Please see [here](https://github.com/Azure/azure-sdk-for-python/tree/main/sdk/keyvault/azure-keyvault-secrets) for more details on parameters that can be provided to the client
    """

    vault: str
    key: str
    secret: str
    credential: str
    kwargs: dict

    def __init__(
        self,
        vault: str,
        key: str,
        secret: str = None,
        credential=None,
        kwargs: dict = None,
    ):
        self.vault = vault
        self.key = key
        self.secret = secret
        self.credential = credential
        self.kwargs = {} if kwargs is None else kwargs
        self.client = self._get_akv_client()

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYTHON
        """
        return SystemType.PYTHON

    @staticmethod
    def libraries():
        libraries = Libraries()
        libraries.add_pypi_library(get_default_package("azure_key_vault_secret"))
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def _get_akv_client(self):
        return SecretClient(
            vault_url="https://{}.vault.azure.net".format(self.vault),
            credential=self.credential,
            **self.kwargs
        )

    def get(self):
        """
        Retrieves the secret from the Azure Key Vault
        """
        response = self.client.get_secret(name=self.key)
        return response.value

    def set(self):
        """
        Creates or updates a secret in the Azure Key Vault
        """
        self.client.set_secret(name=self.key, value=self.secret)
        return True
