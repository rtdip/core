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
import boto3

from src.sdk.python.rtdip_sdk.pipelines.utilities.aws.s3_iam_policy import S3IAMPolicyUtility

def test_s3_iam_policy():
    
    sso_client = boto3.client("sso", region_name='us-east-1')
    sso_client.list_accounts()
    
    s3_iam_policy = S3IAMPolicyUtility(
 
        directory="test/permissions",
        sid="Global-Read-Basic",
    )

    result = s3_iam_policy.execute()
    assert result