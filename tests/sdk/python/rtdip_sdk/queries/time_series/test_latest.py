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
from src.sdk.python.rtdip_sdk.queries.time_series.latest import get as latest_raw
from tests.sdk.python.rtdip_sdk.queries.time_series._test_base import (
    _test_base_succeed,
    _test_base_fails,
)
from tests.sdk.python.rtdip_sdk.queries._test_utils.sdk_test_objects import (
    MOCKED_PARAMETER_DICT,
    MOCKED_QUERY_OFFSET_LIMIT,
    LATEST_MOCKED_QUERY,
    LATEST_MOCKED_QUERY_CHECK_TAGS,
    LATEST_MOCKED_QUERY_NO_TAGS,
    LATEST_MOCKED_QUERY_UOM,
)

MOCKED_LATEST_PARAMETER_DICT = MOCKED_PARAMETER_DICT.copy()


def test_latest(mocker: MockerFixture):
    _test_base_succeed(
        mocker, MOCKED_LATEST_PARAMETER_DICT, LATEST_MOCKED_QUERY, latest_raw
    )


def test_latest_check_tags(mocker: MockerFixture):
    MOCKED_LATEST_PARAMETER_DICT["case_insensitivity_tag_search"] = True
    _test_base_succeed(
        mocker, MOCKED_LATEST_PARAMETER_DICT, LATEST_MOCKED_QUERY_CHECK_TAGS, latest_raw
    )


def test_latest_uom(mocker: MockerFixture):
    MOCKED_LATEST_PARAMETER_DICT["case_insensitivity_tag_search"] = False
    MOCKED_LATEST_PARAMETER_DICT["display_uom"] = True
    _test_base_succeed(
        mocker, MOCKED_LATEST_PARAMETER_DICT, LATEST_MOCKED_QUERY_UOM, latest_raw
    )


def test_latest_offset_limit(mocker: MockerFixture):
    MOCKED_LATEST_PARAMETER_DICT["display_uom"] = False
    MOCKED_LATEST_PARAMETER_DICT["offset"] = 10
    MOCKED_LATEST_PARAMETER_DICT["limit"] = 10
    _test_base_succeed(
        mocker,
        MOCKED_LATEST_PARAMETER_DICT,
        LATEST_MOCKED_QUERY + MOCKED_QUERY_OFFSET_LIMIT,
        latest_raw,
    )


def test_no_tag_latest(mocker: MockerFixture):
    MOCKED_LATEST_PARAMETER_DICT.pop("tag_names")
    MOCKED_LATEST_PARAMETER_DICT["offset"] = None
    MOCKED_LATEST_PARAMETER_DICT["limit"] = None
    _test_base_succeed(
        mocker, MOCKED_LATEST_PARAMETER_DICT, LATEST_MOCKED_QUERY_NO_TAGS, latest_raw
    )


def test_latest_fails(mocker: MockerFixture):
    _test_base_fails(mocker, MOCKED_LATEST_PARAMETER_DICT, latest_raw)
