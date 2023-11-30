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
from pytest_mock import MockerFixture
from src.sdk.python.rtdip_sdk.queries.time_series.raw import get as raw_get
from tests.sdk.python.rtdip_sdk.queries.time_series._test_base import (
    _test_base_succeed,
    _test_base_fails,
)
from tests.sdk.python.rtdip_sdk.queries._test_utils.sdk_test_objects import (
    RAW_MOCKED_QUERY,
    MOCKED_QUERY_OFFSET_LIMIT,
    MOCKED_PARAMETER_DICT,
)

MOCKED_RAW_DICT = MOCKED_PARAMETER_DICT.copy()


def test_raw(mocker: MockerFixture):
    _test_base_succeed(mocker, MOCKED_RAW_DICT, RAW_MOCKED_QUERY, raw_get)


def test_raw_offset_limit(mocker: MockerFixture):
    MOCKED_RAW_DICT["offset"] = 10
    MOCKED_RAW_DICT["limit"] = 10
    _test_base_succeed(
        mocker,
        MOCKED_RAW_DICT,
        RAW_MOCKED_QUERY + MOCKED_QUERY_OFFSET_LIMIT,
        raw_get,
    )


def test_raw_fails(mocker: MockerFixture):
    _test_base_fails(mocker, MOCKED_PARAMETER_DICT, raw_get)
