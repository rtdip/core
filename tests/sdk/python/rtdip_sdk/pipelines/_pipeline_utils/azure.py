# Copyright 2022 RTDIP
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


class MockDirectoryClient:
    def get_access_control(self):
        return {"acl": "group:test_group_object_id:r-x"}

    def set_access_control(self, acl):  # NOSONAR
        return None

    def exists(self):
        return False


class MockFileSystemClient:
    def create_directory(self, directory: str):  # NOSONAR
        return None

    def get_directory_client(self, path):  # NOSONAR
        return MockDirectoryClient()


class MockDataLakeServiceClient:
    def get_file_system_client(self, file_system: str):  # NOSONAR
        return MockFileSystemClient()
