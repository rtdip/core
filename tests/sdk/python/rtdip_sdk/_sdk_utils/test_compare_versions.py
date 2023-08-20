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
from src.sdk.python.rtdip_sdk._sdk_utils.compare_versions import (
    _package_version_meets_minimum,
)

DISTRIBUTION_FROM_NAME = "importlib_metadata.Distribution.from_name"


class MockPackageClass:
    version: str


def test_package_package_version_meets_minimum(mocker: MockerFixture):
    mock_package = MockPackageClass()
    mock_package.version = "5.2.0"
    mocker.patch(DISTRIBUTION_FROM_NAME, return_value=mock_package)
    result = _package_version_meets_minimum("test-package-version", "1.0.0")
    assert result == True


def test_package_package_version_prerelease_meets_minimum(mocker: MockerFixture):
    mock_package = MockPackageClass()
    mock_package.version = "5.2.0rc1"
    mocker.patch(DISTRIBUTION_FROM_NAME, return_value=mock_package)
    result = _package_version_meets_minimum("test-package-version", "5.2.0rc1")
    assert result == True


def test_package_version_does_not_meet_minimum(mocker: MockerFixture):
    mock_package = MockPackageClass()
    mock_package.version = "0.9.0"
    mocker.patch(DISTRIBUTION_FROM_NAME, return_value=mock_package)

    with pytest.raises(AssertionError):
        _package_version_meets_minimum("test-package-version", "1.0.0")
