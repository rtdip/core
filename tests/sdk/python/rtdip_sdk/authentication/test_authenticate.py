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
from src.sdk.python.rtdip_sdk.authentication.azure import (
    CertificateAuth,
    ClientSecretAuth,
    DefaultAuth,
)
import pytest

TENANT_ID = "tenantid123"
CLIENT_ID = "clientid123"
CLIENT_SECRET = "clientsecret123"
CERTIFICATE_PATH = "/test/test-certificate.pem"
STORAGE_ACCOUNT = "teststorageaccount"
FILE_SYSTEM = "test"


class MockedAuthClass:
    def authenticate(self) -> object:
        return object

    mocked_client_secret_auth = ClientSecretAuth(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
    result = mocked_client_secret_auth.authenticate()

    assert isinstance(result, object)
    assert result._tenant_id == "tenantid123"


def test_certificate_auth(mocker: MockerFixture):
    mocker.patch("src.sdk.python.rtdip_sdk.authentication.azure.CertificateCredential")

    mocked_certificate_auth = CertificateAuth(TENANT_ID, CLIENT_ID, CERTIFICATE_PATH)
    result = mocked_certificate_auth.authenticate()

    assert isinstance(result, object)


def test_default_auth():
    mocked_default_auth = DefaultAuth()
    result = mocked_default_auth.authenticate()

    assert isinstance(result, object)


def test_client_secret_auth_fails(mocker: MockerFixture):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.authentication.azure.ClientSecretCredential",
        side_effect=Exception,
    )
    mocked_client_secret_auth = ClientSecretAuth(TENANT_ID, CLIENT_ID, CLIENT_SECRET)

    with pytest.raises(Exception):
        assert mocked_client_secret_auth.authenticate()


def test_certificate_auth_fails(mocker: MockerFixture):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.authentication.azure.CertificateCredential",
        side_effect=Exception,
    )
    mocked_certificate_auth = CertificateAuth(TENANT_ID, CLIENT_ID, CERTIFICATE_PATH)

    with pytest.raises(Exception):
        assert mocked_certificate_auth.authenticate()


def test_default_auth_fails(mocker: MockerFixture):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.authentication.azure.DefaultAzureCredential",
        side_effect=Exception,
    )
    mocked_default_auth = DefaultAuth()

    with pytest.raises(Exception):
        assert mocked_default_auth.authenticate()
