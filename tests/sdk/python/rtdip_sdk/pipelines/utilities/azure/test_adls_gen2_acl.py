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

from src.sdk.python.rtdip_sdk.pipelines.utilities import (
    ADLSGen2DirectoryACLUtility,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.constants import (
    get_default_package,
)
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.azure import (
    MockDataLakeServiceClient,
)


def test_adls_gen2_acl_utility_setup():
    adls_gen2_acl_utility = ADLSGen2DirectoryACLUtility(
        storage_account="test_storage_account",
        container="test_container",
        credential="test_credential",
        directory="/test/directory",
        group_object_id="test_group_object_id",
        folder_permissions="rwx",
    )

    assert adls_gen2_acl_utility.system_type().value == 1
    assert adls_gen2_acl_utility.libraries() == Libraries(
        maven_libraries=[],
        pypi_libraries=[
            get_default_package("azure_adls_gen_2"),
        ],
        pythonwheel_libraries=[],
    )
    assert isinstance(adls_gen2_acl_utility.settings(), dict)


def test_adls_gen2_acl_multi_folder(mocker: MockerFixture):
    adls_gen2_acl_utility = ADLSGen2DirectoryACLUtility(
        storage_account="test_storage_account",
        container="test_container",
        credential="test_credential",
        directory="/test/directory",
        group_object_id="test_group_object_id",
        folder_permissions="rwx",
    )

    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.utilities.azure.adls_gen2_acl.DataLakeServiceClient",
        return_value=MockDataLakeServiceClient(),
    )
    result = adls_gen2_acl_utility.execute()
    assert result


def test_adls_gen2_acl_single_folder(mocker: MockerFixture):
    adls_gen2_acl_utility = ADLSGen2DirectoryACLUtility(
        storage_account="test_storage_account",
        container="test_container",
        credential="test_credential",
        directory="directory",
        group_object_id="test_group_object_id",
        folder_permissions="rwx",
    )

    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.utilities.azure.adls_gen2_acl.DataLakeServiceClient",
        return_value=MockDataLakeServiceClient(),
    )
    result = adls_gen2_acl_utility.execute()
    assert result
