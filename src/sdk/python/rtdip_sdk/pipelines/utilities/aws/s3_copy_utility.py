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

import os
import logging
import boto3
from boto3.s3.transfer import S3Transfer
from botocore.config import Config
from ..interfaces import UtilitiesInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package
from ....data_models.storage_objects import storage_objects_utils


class S3CopyUtility(UtilitiesInterface):
    """
    Copies an object from S3 to S3, from Local to S3 and S3 to local depending on the source and destination uri.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.utilities import S3CopyUtility

    s3_copy_utility = S3CopyUtility(
        source_uri="YOUR-SOURCE-URI",
        destination_uri="YOUR-DESTINATION-URI",
        source_version_id="YOUR-VERSION-ID",
        extra_args={},
        callback="YOUD-SID",
        source_client="PRINCIPAL",
        transfer_config=["ACTIONS"]
    )

    result = s3_bucket_policy_utility.execute()
    ```

    Parameters:
        source_uri (str): URI of the source object
        destination_uri (str): URI of the destination object
        source_version_id (optional str): Version ID of the source bucket
        extra_args (optional dict): Extra arguments that can be passed to the client operation. See [here](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.S3Transfer.ALLOWED_DOWNLOAD_ARGS){ target="_blank" } for a list of download arguments
        callback (optional function): Takes a UDF used for tracking the progress of the copy operation
        source_client (optional botocore or boto3 client): A different S3 client to use for the source bucket during the copy operation
        transfer_config (optional class): The transfer configuration used during the copy. See [here](https://boto3.amazonaws.com/v1/documentation/api/latest/reference/customizations/s3.html#boto3.s3.transfer.TransferConfig){ target="_blank" } for all parameters

    """

    source_uri: str
    destination_uri: str
    destination_key: str
    extra_args: dict
    callback: str
    source_client: S3Transfer
    transfer_config: Config

    def __init__(
        self,
        source_uri: str,
        destination_uri: str,
        source_version_id: str = None,
        extra_args: dict = None,
        callback=None,
        source_client: S3Transfer = None,
        transfer_config: Config = None,
    ):
        self.source_uri = source_uri
        self.destination_uri = destination_uri
        self.source_version_id = source_version_id
        self.extra_args = extra_args
        self.callback = callback
        self.source_client = source_client
        self.transfer_config = transfer_config

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
        libraries.add_pypi_library(get_default_package("aws_boto3"))
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def execute(self) -> bool:
        # S3 to S3 Copy
        if self.source_uri.startswith(
            storage_objects_utils.S3_SCHEME
        ) and self.destination_uri.startswith(storage_objects_utils.S3_SCHEME):
            schema, source_domain, source_key = storage_objects_utils.validate_uri(
                self.source_uri
            )
            (
                schema,
                destination_domain,
                destination_key,
            ) = storage_objects_utils.validate_uri(self.destination_uri)

            s3 = boto3.resource(schema)
            copy_source = {"Bucket": source_domain, "Key": source_key}
            if self.source_version_id is not None:
                copy_source["VersionId"] = self.source_version_id

            try:
                s3.meta.client.copy(
                    copy_source,
                    destination_domain,
                    destination_key,
                    self.extra_args,
                    self.callback,
                    self.source_client,
                    self.transfer_config,
                )

            except Exception as ex:
                logging.error(ex)
                return False
        # Local File to S3 Copy (Upload)
        elif (os.path.isfile(self.source_uri)) and self.destination_uri.startswith(
            storage_objects_utils.S3_SCHEME
        ):
            (
                schema,
                destination_domain,
                destination_key,
            ) = storage_objects_utils.validate_uri(self.destination_uri)

            s3_client = boto3.client(schema)

            try:
                s3_client.upload_file(
                    self.source_uri, destination_domain, destination_key
                )
            except Exception as ex:
                logging.error(ex)
                return False
        # S3 to Local File Copy (Download)
        elif self.source_uri.startswith(
            storage_objects_utils.S3_SCHEME
        ) and not self.destination_uri.startswith(storage_objects_utils.S3_SCHEME):
            try:
                schema, source_domain, source_key = storage_objects_utils.validate_uri(
                    self.source_uri
                )
                s3 = boto3.client(schema)
                s3.download_file(source_domain, source_key, self.destination_uri)
            except Exception as ex:
                logging.error(ex)
                return False
        else:
            logging.error(
                "Not Implemented. From: %s \n\t to: %s",
                self.source_uri,
                self.destination_uri,
            )
        return True
