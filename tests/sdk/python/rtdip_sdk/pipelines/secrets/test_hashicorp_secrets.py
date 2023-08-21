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
from src.sdk.python.rtdip_sdk.pipelines.secrets.hashicorp_vault import (
    HashiCorpVaultSecrets,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    PyPiLibrary,
)


def test_hashicorp_vault_secret_setup():
    hashicorp_vault_secret = HashiCorpVaultSecrets(
        vault="hashicorp-vault-url", key="vault-key"
    )
    assert hashicorp_vault_secret.system_type().value == 1
    assert hashicorp_vault_secret.libraries() == Libraries(
        pypi_libraries=[PyPiLibrary(name="hvac", version="1.1.0", repo=None)],
        maven_libraries=[],
        pythonwheel_libraries=[],
    )
    assert isinstance(hashicorp_vault_secret.settings(), dict)


def test_hashicorp_vault_secrets_get(mocker: MockerFixture):
    mock_hashicorp_client = mocker.MagicMock()
    mock_hashicorp_client.secrets.kv.read_secret_version.return_value = {
        "data": {"data": {"password": "test"}}
    }  # NOSONAR
    mocker.patch("hvac.Client", return_value=mock_hashicorp_client)

    hashicorp_vault = HashiCorpVaultSecrets(
        vault="hashicorp-vault-url", key="vault-key", secret="vault-secret"
    )

    result = hashicorp_vault.get()
    assert result == "test"


def test_hashicorp_vault_secrets_sert(mocker: MockerFixture):
    mock_hashicorp_client = mocker.MagicMock()
    mock_hashicorp_client.secrets.kv.v2.create_or_update_secret.return_value = True
    mocker.patch("hvac.Client", return_value=mock_hashicorp_client)

    hashicorp_vault = HashiCorpVaultSecrets(
        vault="hashicorp-vault-url", key="vault-key", secret="vault-secret"
    )

    result = hashicorp_vault.set()
    assert result
