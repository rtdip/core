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
from typing import Dict, List

from ..interfaces import UtilitiesInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package

import boto3
import json


class S3BucketPolicyUtility(UtilitiesInterface):
    """
    Assigns an IAM Bucket Policy to an S3 Bucket.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.utilities import S3BucketPolicyUtility

    s3_bucket_policy_utility = S3BucketPolicyUtility(
        bucket_name="YOUR-BUCKET-NAME",
        aws_access_key_id="YOUR-AWS-ACCESS-KEY",
        aws_secret_access_key="YOUR-AWS-SECRET-ACCESS-KEY",
        aws_session_token="YOUR-AWS-SESSION-TOKEN",
        sid="YOUD-SID",
        effect="EFFECT",
        principal="PRINCIPAL",
        action=["ACTIONS"],
        resource=["RESOURCES"]
    )

    result = s3_bucket_policy_utility.execute()
    ```

    Parameters:
        bucket_name (str): S3 Bucket Name
        aws_access_key_id (str): AWS Access Key
        aws_secret_access_key (str): AWS Secret Key
        aws_session_token (str): AWS Session Token
        sid (str): S3 Bucket Policy Sid to be updated
        effect (str): Effect to be applied to the policy
        principal (str): Principal to be applied to Policy
        action (list[str]): List of actions to be applied to the policy
        resource (list[str]): List of resources to be applied to the policy
    """

    bucket_name: str
    aws_access_key_id: str
    aws_secret_access_key: str
    aws_session_token: str
    sid: str
    effect: str
    principal: str
    action: List[str]
    resource: List[str]

    def __init__(
        self,
        bucket_name: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        aws_session_token: str,
        sid: str,
        principal: str,
        effect: str,
        action: List[str],
        resource: List[str],
    ) -> None:
        self.bucket_name = bucket_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.aws_session_token = aws_session_token
        self.sid = sid
        self.effect = effect
        self.principal = principal
        self.action = action
        self.resource = resource

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
        try:
            s3_client = boto3.client(
                "s3",
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
                aws_session_token=self.aws_session_token,
            )

            bucket_policy = s3_client.get_bucket_policy(Bucket=self.bucket_name)

            policy_statement = None
            if "Policy" in bucket_policy and bucket_policy["Policy"] != None:
                policy_statement = json.loads(bucket_policy["Policy"])

            if policy_statement is None:
                policy_statement = {"Version": "2012-10-17", "Statement": []}

            sid_found = False
            for statement in policy_statement["Statement"]:
                if statement["Sid"] == self.sid:
                    sid_found = True
                    statement["Effect"] = self.effect
                    statement["Principal"] = self.principal
                    statement["Action"] = self.action
                    if isinstance(statement["Resource"], list):
                        statement["Resource"] + self.resource
                    else:
                        self.resource.append(statement["Resource"])
                        statement["Resource"] = self.resource
                    statement["Resource"] = list(set(statement["Resource"]))

            if not sid_found:
                policy_statement["Statement"].append(
                    {
                        "Sid": self.sid,
                        "Effect": self.effect,
                        "Principal": self.principal,
                        "Action": self.action,
                        "Resource": self.resource,
                    }
                )

            policy = json.dumps(policy_statement)
            s3_client.put_bucket_policy(Bucket=self.bucket_name, Policy=policy)

            return True

        except Exception as e:
            logging.exception(str(e))
            raise e
