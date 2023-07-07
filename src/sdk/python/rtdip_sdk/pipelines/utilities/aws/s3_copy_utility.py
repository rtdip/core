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

from botocore.exceptions import ClientError
from boto3.s3.transfer import S3Transfer
from botocore.config import Config
import traceback

import os

import boto3
from boto3.s3.transfer import S3Transfer
from botocore.config import Config
from ..interfaces import UtilitiesInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import DEFAULT_PACKAGES



import uri_utils

import logging

format_str: str = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
date_format_str: str = '%m/%d/%Y %H:%M:%S %Z'
logging.basicConfig(format=format_str,
                    datefmt=date_format_str,
                    level=logging.INFO)

logger = logging.getLogger('S3CopyUtility')


class S3CopyUtility():
    '''
    Copies an object from one S3 location to another

    Args:
        source_uri (str): URI of the source object
        destination_uri (str): URI of the destination object
        source_version_id (optional str): Version ID of the source bucket
        extra_args (optional dict): Extra arguments that can be passed to the client operation. See [here](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer.ALLOWED_DOWNLOAD_ARGS){ target="_blank" } for a list of download arguments
        callback (optional function): Takes a UDF used for tracking the progress of the copy operation
        source_client (optional botocore or boto3 client): A different S3 client to use for the source bucket during the copy operation
        transfer_config (optional class): The transfer configuration used during the copy. See [here](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.TransferConfig){ target="_blank" } for all parameters

    '''
    source_uri: str
    destination_uri: str
    destination_key: str
    extra_args: dict
    callback: str
    source_client: S3Transfer
    transfer_config: Config

    def __init__(self,
                 source_uri: str,
                 destination_uri: str,
                 source_version_id: str = None,
                 extra_args: dict = None, callback=None,
                 source_client: S3Transfer = None, transfer_config: Config = None):
        self.source_uri = source_uri
        self.destination_uri = destination_uri
        self.source_version_id = source_version_id
        self.extra_args = extra_args
        self.callback = callback
        self.source_client = source_client
        self.transfer_config = transfer_config

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

    def execute(self) -> None:

        logger.info('executing')
        logger.info('Source: [{}]'.format(self.source_uri))
        logger.info('Destination: [{}]'.format(self.destination_uri))

        # S3 to S3 Copy
        if self.source_uri.startswith(uri_utils.get_s3_protocol_definition()) \
                and self.destination_uri.startswith(uri_utils.get_s3_protocol_definition()):

            logger.info('S3 to S3 Copy')

            source_domain, source_key = uri_utils.get_domain_key_pair(self.source_uri)
            destination_domain, destination_key = uri_utils.get_domain_key_pair(self.destination_uri)

            logger.debug('Source Domain/Source Key: [{}]/[{}]'.format(source_domain, source_key))
            logger.debug('Destination Domain/Destination Key: [{}]/[{}]'.format(destination_domain, destination_key))

            s3 = boto3.resource('s3')
            copy_source = {
                'Bucket': source_domain,
                'Key': source_key
            }
            if self.source_version_id is not None:
                copy_source['VersionId'] = self.source_version_id

            try:
                s3.meta.client.copy(copy_source, destination_domain, destination_key, self.extra_args, self.callback,
                                    self.source_client, self.transfer_config)

            except ClientError as e:
                traceback.print_exc()
                return False
        # Local File to S3 Copy (Upload)
        elif (os.path.isfile(self.source_uri)) \
                and self.destination_uri.startswith(uri_utils.get_s3_protocol_definition()):

            logger.info('Local to S3 Copy [Upload]')

            s3_client = boto3.client('s3')
            destination_domain, destination_key = uri_utils.get_domain_key_pair(self.destination_uri)
            try:
                response = s3_client.upload_file(self.source_uri, destination_domain, destination_key)
            except ClientError as e:
                traceback.print_exc()
                return False
        # S3 to Local File Copy (Download)
        elif self.source_uri.startswith(uri_utils.get_s3_protocol_definition()) \
                and not self.destination_uri.startswith(uri_utils.get_s3_protocol_definition()):

            logger.info('Local to S3 Copy [Download]')

            try:
                source_domain, source_key = uri_utils.get_domain_key_pair(self.source_uri)
                s3 = boto3.client('s3')
                s3.download_file(source_domain, source_key, self.destination_uri)
            except ClientError as e:
                traceback.print_exc()
                return False
        else:
            logger.info('Not Implemented. From  [{}] \n\t to: [{}]'.format(self.source_uri, self.destination_uri))
        return True


