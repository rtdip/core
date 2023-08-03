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
import hvac
from .._pipeline_utils.models import Libraries, SystemType
from .._pipeline_utils.constants import get_default_package

class HashiCorpVaultSecrets(SecretsInterface):
    '''
    Reads secrets from a Hashicorp Vault. For more information about Hashicorp Vaults, see [here.](https://developer.hashicorp.com/vault/docs/get-started/developer-qs)

    Args:
        vault (str): Hashicorp Vault URL
        key (str): Name/Key of the secret in the Hashicorp Vault 
        secret (str): Secret or Password to be stored in the Hashicorp Vault
        credential (str): Token for authentication with the Hashicorp Vault
        kwargs (dict): List of additional parameters to be passed when creating a Hashicorp Vault Client. Please see [here](https://hvac.readthedocs.io/en/stable/overview.html#initialize-the-client) for more details on parameters that can be provided to the client
    '''    
    vault: str
    key: str
    secret: str
    credential: str

    def __init__(self, vault: str, key: str, secret: str = None, credential: str = None, kwargs: dict = {}): # NOSONAR
        self.vault = vault
        self.key = key
        self.secret = secret
        self.credential = credential
        self.kwargs = kwargs
        self.client=self._get_hvac_client()

    @staticmethod
    def system_type():
        '''
        Attributes:
            SystemType (Environment): Requires PYTHON
        '''
        return SystemType.PYTHON

    @staticmethod
    def libraries():
        libraries = Libraries()
        libraries.add_pypi_library(get_default_package("hashicorp_vault"))
        return libraries
    
    @staticmethod
    def settings() -> dict:
        return {}
    
    def _get_hvac_client(self):
        return hvac.Client(
            url = self.vault,
            token = self.credential,
            **self.kwargs
        )
    
    def get(self):
        '''
        Retrieves the secret from the Hashicorp Vault
        '''
        response = self.client.secrets.kv.read_secret_version(path=self.key)
        return response['data']['data']['password']
    
    def set(self):
        '''
        Creates or updates a secret in the Hashicorp Vault
        '''      
        self.client.secrets.kv.v2.create_or_update_secret(
            path=self.key,
            secret=dict(password=self.secret),
        )
        return True
