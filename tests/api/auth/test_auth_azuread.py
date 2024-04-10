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

from pytest_mock import MockerFixture
from src.api.auth import azuread
from azure.identity import DefaultAzureCredential
from azure.identity import OnBehalfOfCredential
from azure.core.credentials import AccessToken


def test_auth_azuread_no_auth_header(mocker: MockerFixture):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.authentication.azure.DefaultAuth.authenticate",
        return_value=DefaultAzureCredential,
    )
    mocker.patch(
        "azure.identity.DefaultAzureCredential.get_token", return_value=AccessToken
    )
    mocker.patch("azure.core.credentials.AccessToken.token", return_value="token")

    token = azuread.get_azure_ad_token(None)
    assert token.return_value == "token"


def test_auth_azuread_bearer_token(mocker: MockerFixture):
    token = azuread.get_azure_ad_token("Bearer token")
    assert token == "token"


def test_auth_azuread_bearer_token_no_prefix(mocker: MockerFixture):
    token = azuread.get_azure_ad_token("token")
    assert token == "token"
