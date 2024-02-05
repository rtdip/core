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
from typing import Dict, Union

from ..interfaces import UtilitiesInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package

from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient
from azure.core.credentials import (
    TokenCredential,
    AzureNamedKeyCredential,
    AzureSasCredential,
)


class ADLSGen2DirectoryACLUtility(UtilitiesInterface):
    """
    Assigns Azure AD Groups to ACLs on directories in an Azure Data Lake Store Gen 2 storage account.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.utilities import ADLSGen2DirectoryACLUtility

    adls_gen2_directory_acl_utility = ADLSGen2DirectoryACLUtility(
        storage_account="YOUR-STORAGAE-ACCOUNT-NAME",
        container="YOUR-ADLS_CONTAINER_NAME",
        credential="YOUR-TOKEN-CREDENTIAL",
        directory="DIRECTORY",
        group_object_id="GROUP-OBJECT",
        folder_permissions="r-x",
        parent_folder_permissions="r-x",
        root_folder_permissions="r-x",
        set_as_default_acl=True,
        create_directory_if_not_exists=True
    )

    result = adls_gen2_directory_acl_utility.execute()
    ```

    Parameters:
        storage_account (str): ADLS Gen 2 Storage Account Name
        container (str): ADLS Gen 2 Container Name
        credential (TokenCredential): Credentials to authenticate with ADLS Gen 2 Storage Account
        directory (str): Directory to be assign ACLS to in an ADLS Gen 2
        group_object_id (str): Azure AD Group Object ID to be assigned to Directory
        folder_permissions (optional, str): Folder Permissions to Assign to directory
        parent_folder_permissions (optional, str): Folder Permissions to Assign to parent directories. Parent Folder ACLs not set if None
        root_folder_permissions (optional, str): Folder Permissions to Assign to root directory. Root Folder ACL not set if None
        set_as_default_acl (bool, optional): Sets the ACL as the default ACL on the folder
        create_directory_if_not_exists (bool, optional): Creates the directory(and Parent Directories) if it does not exist
    """

    storage_account: str
    container: str
    credential: Union[
        str,
        Dict[str, str],
        AzureNamedKeyCredential,
        AzureSasCredential,
        TokenCredential,
        None,
    ]
    directory: str
    group_object_id: str
    folder_permissions: str
    parent_folder_permissions: str
    root_folder_permissions: str
    set_as_default_acl: bool
    create_directory_if_not_exists: bool

    def __init__(
        self,
        storage_account: str,
        container: str,
        credential: Union[
            str,
            Dict[str, str],
            AzureNamedKeyCredential,
            AzureSasCredential,
            TokenCredential,
            None,
        ],
        directory: str,
        group_object_id: str,
        folder_permissions: str = "r-x",
        parent_folder_permissions: Union[str, None] = "r-x",
        root_folder_permissions: Union[str, None] = "r-x",
        set_as_default_acl: bool = True,
        create_directory_if_not_exists: bool = True,
    ) -> None:
        self.storage_account = storage_account
        self.container = container
        self.credential = credential
        self.directory = directory
        self.group_object_id = group_object_id
        self.folder_permissions = folder_permissions
        self.parent_folder_permissions = parent_folder_permissions
        self.root_folder_permissions = root_folder_permissions
        self.set_as_default_acl = set_as_default_acl
        self.create_directory_if_not_exists = create_directory_if_not_exists

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
        libraries.add_pypi_library(get_default_package("azure_adls_gen_2"))
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def _set_acl(
        self,
        file_system_client: FileSystemClient,
        path: str,
        group_object_id: str,
        folder_permissions: str,
        set_as_default_acl: bool,
    ):
        acl_directory_client = file_system_client.get_directory_client(path)

        group_id_acl = "group:{}:{}".format(group_object_id, folder_permissions)
        acl_props = acl_directory_client.get_access_control().get("acl")
        acl_props_list = acl_props.split(",")

        for acl in acl_props_list:
            if group_object_id in acl:
                acl_props_list.remove(acl)

        acl_props_list.append(group_id_acl)
        if set_as_default_acl == True:
            acl_props_list.append("default:{}".format(group_id_acl))

        new_acl_props = ",".join(acl_props_list)
        acl_directory_client.set_access_control(acl=new_acl_props)

    def execute(self) -> bool:
        try:
            # Setup file system client
            service_client = DataLakeServiceClient(
                account_url="{}://{}.dfs.core.windows.net".format(
                    "https", self.storage_account
                ),
                credential=self.credential,
            )
            file_system_client = service_client.get_file_system_client(
                file_system=self.container
            )

            # Create directory if it doesn't already exist
            if self.create_directory_if_not_exists:
                directory_client = file_system_client.get_directory_client(
                    self.directory
                )
                if not directory_client.exists():
                    file_system_client.create_directory(self.directory)

            group_object_id = str(self.group_object_id)
            acl_path = ""
            directory_list = self.directory.split("/")

            # Set Root Folder ACLs if specified
            if self.root_folder_permissions != None:
                self._set_acl(
                    file_system_client,
                    "/",
                    group_object_id,
                    self.root_folder_permissions,
                    False,
                )

            # Set Parent Folders ACLs if specified
            if self.parent_folder_permissions != None:
                for directory in directory_list[:-1]:
                    if directory == "":
                        acl_path = "/"
                        continue
                    elif acl_path == "/":
                        acl_path += directory
                    else:
                        acl_path += "/" + directory

                    self._set_acl(
                        file_system_client,
                        acl_path,
                        group_object_id,
                        self.parent_folder_permissions,
                        False,
                    )

            # Set Folder ACLs
            self._set_acl(
                file_system_client,
                self.directory,
                group_object_id,
                self.folder_permissions,
                self.set_as_default_acl,
            )

            return True

        except Exception as e:
            logging.exception(str(e))
            raise e
