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
import tempfile
import string
import random
import sys
from datetime import datetime
import boto3
from moto import mock_aws


sys.path.insert(0, ".")

from src.sdk.python.rtdip_sdk.pipelines.utilities.aws.s3_copy_utility import (
    S3CopyUtility,
)

from src.sdk.python.rtdip_sdk.data_models.storage_objects import storage_objects_utils


@mock_aws
def test_s3_copy_utility():
    length: int = 1024
    random.seed(datetime.now().timestamp())
    rnd_text: str = "".join(
        random.choice(string.ascii_lowercase) for _ in range(length)  # NOSONAR
    )

    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".txt", delete=False
    ) as rnd_tempfile:
        for i in range(length):
            rnd_tempfile.writelines(f"[{i}] - " + rnd_text)

    rnd_tempfile.close()
    assert os.path.exists(rnd_tempfile.name)

    letters_and_numbers: string = string.ascii_lowercase + string.digits

    rnd_source_domain_name: str = ".".join(
        "".join(random.choice(letters_and_numbers) for _ in range(9))
        for _ in range(3)  # NOSONAR
    )  # NOSONAR
    rnd_destination_domain_name: str = ".".join(
        "".join(random.choice(letters_and_numbers) for _ in range(9))
        for _ in range(3)  # NOSONAR
    )  # NOSONAR
    rnd_keys: str = "".join(
        "".join(random.choice(letters_and_numbers) for _ in range(4))
        for _ in range(3)  # NOSONAR
    )  # NOSONAR
    rnd_object_name: str = (
        "".join(random.choice(letters_and_numbers) for _ in range(9))  # NOSONAR
        + "."
        + "".join(random.choice(string.ascii_lowercase) for _ in range(3))  # NOSONAR
    )

    rnd_full_source_s3_uri: str = storage_objects_utils.to_uri(
        storage_objects_utils.S3_SCHEME,
        rnd_source_domain_name,
        rnd_keys + "/" + rnd_object_name,
    )
    rnd_full_destination_s3_uri: str = storage_objects_utils.to_uri(
        storage_objects_utils.S3_SCHEME,
        rnd_destination_domain_name,
        rnd_keys + "/" + rnd_object_name,
    )

    # Create buckets first (required by moto)
    conn = boto3.resource("s3")
    conn.create_bucket(
        Bucket=rnd_source_domain_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )
    conn.create_bucket(
        Bucket=rnd_destination_domain_name,
        CreateBucketConfiguration={"LocationConstraint": "eu-west-1"},
    )

    # Upload to S3
    s3_copy_utility: S3CopyUtility = S3CopyUtility(
        rnd_tempfile.name, rnd_full_destination_s3_uri
    )
    result: bool = s3_copy_utility.execute()
    assert result is True

    # S3 to S3 Copy
    s3_copy_utility: S3CopyUtility = S3CopyUtility(
        rnd_full_destination_s3_uri, rnd_full_source_s3_uri
    )
    result: bool = s3_copy_utility.execute()
    assert result is True

    # Download from S3
    # Calculate the hash of the file, delete it and download it from S3 and re-compute the hash and check if it is the same
    os.remove(rnd_tempfile.name)
    s3_copy_utility: S3CopyUtility = S3CopyUtility(
        rnd_full_destination_s3_uri, rnd_tempfile.name
    )
    result: bool = s3_copy_utility.execute()
    assert result is True
    assert os.path.exists(rnd_tempfile.name is True)

    # Upload to S3
    # Bucket does not exist (destination)
    rnd_destination_domain_name: str = ".".join(
        "".join(random.choice(letters_and_numbers) for _ in range(9))
        for _ in range(3)  # NOSONAR
    )
    rnd_full_destination_s3_uri: str = storage_objects_utils.to_uri(
        storage_objects_utils.S3_SCHEME,
        rnd_destination_domain_name,
        rnd_keys + "/" + rnd_object_name,  # NOSONAR
    )
    s3_copy_utility: S3CopyUtility = S3CopyUtility(
        rnd_tempfile.name, rnd_full_destination_s3_uri
    )
    result: bool = s3_copy_utility.execute()
    assert result is False

    # Download from S3
    # Bucket does not exist (source)
    os.remove(rnd_tempfile.name)
    rnd_source_domain_name: str = ".".join(
        "".join(random.choice(letters_and_numbers) for _ in range(9))
        for _ in range(3)  # NOSONAR
    )
    rnd_full_source_s3_uri: str = storage_objects_utils.to_uri(
        storage_objects_utils.S3_SCHEME,
        rnd_source_domain_name,
        rnd_keys + "/" + rnd_object_name,
    )
    s3_copy_utility: S3CopyUtility = S3CopyUtility(
        rnd_full_source_s3_uri, rnd_tempfile.name
    )
    result: bool = s3_copy_utility.execute()
    assert result is False

    # S3 to S3
    # Bucket does not exist
    s3_copy_utility: S3CopyUtility = S3CopyUtility(
        rnd_full_source_s3_uri, rnd_full_destination_s3_uri
    )
    result: bool = s3_copy_utility.execute()
    assert result is False
