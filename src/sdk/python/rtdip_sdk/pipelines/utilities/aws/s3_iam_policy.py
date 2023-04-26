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
from typing import Dict

from ..interfaces import UtilitiesInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import DEFAULT_PACKAGES

import boto3
import json

# TODO: Test with S3 Buckets

class S3IAMPolicyUtility(UtilitiesInterface):
    '''
    Assigns Azure AD Groups to ACLs on directories in an Azure Data Lake Store Gen 2 storage account

    Args:
        bucket_name (str): S3 Bucket Name
        aws_access_key_id (str): AWS Access Key
        aws_secret_access_key (str): AWS Secret Key
        aws_session_token (str): AWS Session Token
        directory (str): Directory to be assign S3 IAM Policy in an S3 Bucket
        sid (str): S3 Bucket Policy Sid to be updated
        folder_permissions (optional, str): Folder Permissions to Assign to directory 
        parent_folder_permissions (optional, str): Folder Permissions to Assign to parent directories. Parent Folder ACLs not set if None
        root_folder_permissions (optional, str): Folder Permissions to Assign to root directory. Root Folder ACL not set if None
        set_as_default_acl (bool, optional): Sets the ACL as the default ACL on the folder
        create_directory_if_not_exists (bool, optional): Creates the directory(and Parent Directories) if it does not exist
    ''' 
    bucket_name: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str
    sid: str
    directory: str
    folder_permissions: str
    parent_folder_permissions: str
    root_folder_permissions: str
    set_as_default_acl: bool
    create_directory_if_not_exists: bool

    def __init__(self, bucket_name: str, aws_access_key_id: str, aws_secret_access_key: str, aws_session_token: str, directory: str, sid: str, folder_permissions: str = "r-x", parent_folder_permissions: str | None = "r-x", root_folder_permissions: str | None = "r-x", set_as_default_acl: bool = True, create_directory_if_not_exists: bool = True) -> None:
        self.bucket_name = bucket_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.directory = directory
        self.sid = sid
        self.folder_permissions = folder_permissions
        self.parent_folder_permissions = parent_folder_permissions
        self.root_folder_permissions = root_folder_permissions
        self.set_as_default_acl = set_as_default_acl
        self.create_directory_if_not_exists = create_directory_if_not_exists

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
        libraries.add_pypi_library(DEFAULT_PACKAGES["aws_boto3"])
        return libraries
    
    @staticmethod
    def settings() -> dict:
        return {}
    
    def execute(self) -> bool:
        try:
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token
            )

            bucket_list = s3_client.list_buckets()
            bucket_policy = s3_client.get_bucket_policy(Bucket=self.bucket_name)
            bucket_policy = {
                'Version': '2012-10-17',
                'Statement': [{
                    'Sid': 'AddPerm',
                    'Effect': 'Allow',
                    'Principal': '*',
                    'Action': ['s3:GetObject'],
                    'Resource': f'arn:aws:s3:::{self.bucket_name}/{self.directory}/*'
                }]
            }

            return True
        
        except Exception as e:
            logging.exception(str(e))
            raise e
