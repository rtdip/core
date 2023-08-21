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
import pytest
from pytest_mock import MockerFixture
import json

from src.sdk.python.rtdip_sdk.pipelines.utilities.aws.s3_bucket_policy import (
    S3BucketPolicyUtility,
)
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.aws import MockS3Client

s3_client_location = (
    "src.sdk.python.rtdip_sdk.pipelines.utilities.aws.s3_bucket_policy.boto3.client"
)
expected_bucket_name = "test_bucket"
expected_bucket_policy = '{"Version": "2012-10-17", "Statement": [{"Sid": "test_sid", "Effect": "Allow", "Principal": "*", "Action": ["s3:GetObject"], "Resource": ["arn:aws:s3:::test_bucket/*"]}]}'


@pytest.fixture
def s3_bucket_policy():
    return S3BucketPolicyUtility(
        bucket_name="test_bucket",
        aws_access_key_id="test_access_key",
        aws_secret_access_key="test_secret_key",
        aws_session_token="test_session_token",
        sid="test_sid",
        effect="Allow",
        principal="*",
        action=["s3:GetObject"],
        resource=["arn:aws:s3:::test_bucket/*"],
    )


def test_basic_s3_bucket_policy(s3_bucket_policy, mocker: MockerFixture):
    mock_s3_client = MockS3Client(
        bucket_policy={"Policy": json.dumps({"Version": "2012-10-17", "Statement": []})}
    )
    mocker.patch(s3_client_location, return_value=mock_s3_client)

    result = s3_bucket_policy.execute()
    assert result
    assert mock_s3_client.put_bucket_name == expected_bucket_name
    assert mock_s3_client.put_bucket_policy == expected_bucket_policy


def test_no_s3_bucket_policy(s3_bucket_policy, mocker: MockerFixture):
    mock_s3_client = MockS3Client(bucket_policy={"Policy": None})
    mocker.patch(s3_client_location, return_value=mock_s3_client)

    result = s3_bucket_policy.execute()
    assert result
    assert mock_s3_client.put_bucket_name == expected_bucket_name
    assert mock_s3_client.put_bucket_policy == expected_bucket_policy


def test_existing_s3_bucket_policy(s3_bucket_policy, mocker: MockerFixture):
    mock_s3_client = MockS3Client(
        bucket_policy={
            "Policy": json.dumps(
                {
                    "Version": "2012-10-17",
                    "Statement": [
                        {
                            "Sid": "test_sid",
                            "Effect": "Allow",
                            "Principal": "*",
                            "Action": ["s3:GetObject"],
                            "Resource": "arn:aws:s3:::test_bucket/*",
                        }
                    ],
                }
            )
        }
    )
    mocker.patch(s3_client_location, return_value=mock_s3_client)

    result = s3_bucket_policy.execute()
    assert result
    assert mock_s3_client.put_bucket_name == expected_bucket_name
    assert mock_s3_client.put_bucket_policy == expected_bucket_policy
