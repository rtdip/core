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

import random
import string
import sys
from datetime import datetime

sys.path.insert(0, ".")

from src.sdk.python.rtdip_sdk.data_models.storage_objects import storage_objects_utils


def test_validate():
    random.seed(datetime.now().timestamp())  # NOSONAR
    lan: string = string.ascii_lowercase + string.digits
    rnd_domain_name: str = ".".join(
        "".join(random.choice(lan) for _ in range(9)) for _ in range(3)  # NOSONAR
    )

    rnd_keys: str = "".join(
        "".join(random.choice(lan) for _ in range(4)) for _ in range(3)  # NOSONAR
    )

    rnd_object_name: str = (
        "".join(random.choice(lan) for _ in range(9))  # NOSONAR
        + "."
        + "".join(random.choice(string.ascii_lowercase) for _ in range(3))  # NOSONAR
    )  # NOSONAR

    rnd_full_s3_uri: str = storage_objects_utils.to_uri(
        storage_objects_utils.S3_SCHEME,
        rnd_domain_name,
        rnd_keys + "/" + rnd_object_name,
    )

    scheme, domain, path = storage_objects_utils.validate_uri(rnd_full_s3_uri)

    assert scheme == storage_objects_utils.S3_SCHEME
    assert domain == rnd_domain_name

    assert path == "/" + rnd_keys + "/" + rnd_object_name

    rnd_protocol: str = (
        "".join(random.choice(string.ascii_lowercase) for _ in range(5))  # NOSONAR
        + "://"
    )  # NOSONAR
    exception_thrown: bool = False

    try:
        rnd_full_invalid_uri: str = (
            rnd_protocol + rnd_domain_name + "/" + rnd_keys + "/" + rnd_object_name
        )
        storage_objects_utils.validate_uri(rnd_full_invalid_uri)

    except SystemError:
        exception_thrown = True

    assert exception_thrown is True
