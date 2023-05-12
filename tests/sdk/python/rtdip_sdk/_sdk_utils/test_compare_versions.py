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
import pytest
from pytest_mock import MockerFixture
from src.sdk.python.rtdip_sdk._sdk_utils.compare_versions import _version_meets_minimum

def test_package_version_meets_minimum(mocker: MockerFixture):
    mocker.patch("importlib.metadata.version", return_value="5.2.0")
    result = _version_meets_minimum("test-package-version", "1.0.0")
    assert True

def test_package_version_does_not_meet_minimum(mocker: MockerFixture):
    mocker.patch("importlib.metadata.version", return_value="0.9.0")
    with pytest.raises(AssertionError):
        result = _version_meets_minimum("test-package-version", "1.0.0")
