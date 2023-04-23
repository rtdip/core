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
sys.path.insert(0, '.')
from pytest_mock import MockerFixture

from src.sdk.python.rtdip_sdk.pipelines.utilities.azure.adls_gen2_acl import ADLSGen2DirectoryACLUtility
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session
from pyspark.sql import SparkSession

class MockDirectoryClient():

    def get_access_control(self):
        return {
            "acl": "test_acl"
        }

    def set_access_control(self, acl):
        return None

class MockFileSystemClient():

    def create_directory(self, directory: str):
        return None
    
    def get_directory_client(self, path):
        return MockDirectoryClient()

class MockDataLakeServiceClient():

    # def __init__(account_url: str, credential: str):
    #     return None
    
    def get_file_system_client(self, file_system: str):
        return MockFileSystemClient()
        

def test_adls_gen2_acl(mocker: MockerFixture):
    adls_gen2_acl_utility = ADLSGen2DirectoryACLUtility(
        storage_account="test_storage_account",
        container="test_container",
        credential="test_credential",
        directory="/test/directory",
        group_object_id="test_group_object_id",
        folder_permissions="rwx"
    )

    mocker.patch("src.sdk.python.rtdip_sdk.pipelines.utilities.azure.adls_gen2_acl.DataLakeServiceClient", return_value=MockDataLakeServiceClient())
    result = adls_gen2_acl_utility.execute()
    assert result