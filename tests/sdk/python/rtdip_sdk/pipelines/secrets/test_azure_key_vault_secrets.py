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

import sys

sys.path.insert(0, ".")
from pytest_mock import MockerFixture
from src.sdk.python.rtdip_sdk.pipelines.secrets.azure_key_vault import (
    AzureKeyVaultSecrets,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    PyPiLibrary,
)


class MockSecretValue:
    value = "test"


def test_azure_key_vault_secret_setup(mocker: MockerFixture):
    azure_key_vault = AzureKeyVaultSecrets(
        vault="akv-vault-url", credential=object(), key="test"
    )
    assert azure_key_vault.system_type().value == 1
    assert azure_key_vault.libraries() == Libraries(
        pypi_libraries=[
            PyPiLibrary(name="azure-keyvault-secrets", version="4.7.0", repo=None)
        ],
        maven_libraries=[],
        pythonwheel_libraries=[],
    )
    assert isinstance(azure_key_vault.settings(), dict)


def test_azure_key_vault_secrets_get(mocker: MockerFixture):
    mocker.patch(
        "azure.keyvault.secrets.SecretClient.get_secret", return_value=MockSecretValue()
    )

    azure_key_vault = AzureKeyVaultSecrets(
        vault="akv-vault-url", credential=object(), key="vault-key"
    )

    result = azure_key_vault.get()
    assert result == "test"


def test_azure_key_vault_secrets_sert(mocker: MockerFixture):
    mocker.patch("azure.keyvault.secrets.SecretClient.set_secret", return_value=True)

    hashicorp_vault = AzureKeyVaultSecrets(
        vault="akv-vault-url",
        credential=object(),
        key="vault-key",
        secret="vault-secret",
    )

    result = hashicorp_vault.set()
    assert result
