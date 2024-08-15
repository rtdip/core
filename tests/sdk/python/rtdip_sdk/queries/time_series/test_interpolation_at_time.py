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
from src.sdk.python.rtdip_sdk.connectors import DatabricksSQLConnection
from src.sdk.python.rtdip_sdk.queries.time_series.interpolation_at_time import (
    get as interpolation_at_time_get,
)
from tests.sdk.python.rtdip_sdk.queries.time_series._test_base import (
    _test_base_succeed,
    _test_base_fails,
)
from tests.sdk.python.rtdip_sdk.queries._test_utils.sdk_test_objects import (
    MOCKED_PARAMETER_DICT,
    MOCKED_QUERY_OFFSET_LIMIT,
    IAT_MOCKED_QUERY,
    IAT_MOCKED_QUERY_CHECK_TAGS,
    IAT_MOCKED_QUERY_PIVOT,
    IAT_MOCKED_QUERY_UOM,
)

MOCKED_IAT_PARAMETER_DICT = MOCKED_PARAMETER_DICT.copy()
MOCKED_IAT_PARAMETER_DICT["timestamps"] = ["2011-01-01T00:00:00+00:00"]
MOCKED_IAT_PARAMETER_DICT["window_length"] = 1
MOCKED_IAT_PARAMETER_DICT["pivot"] = False


def test_interpolation_at_time(mocker: MockerFixture):
    _test_base_succeed(
        mocker,
        MOCKED_IAT_PARAMETER_DICT,
        IAT_MOCKED_QUERY,
        interpolation_at_time_get,
    )


def test_interpolation_at_time_check_tags(mocker: MockerFixture):
    MOCKED_IAT_PARAMETER_DICT["case_insensitivity_tag_search"] = True
    _test_base_succeed(
        mocker,
        MOCKED_IAT_PARAMETER_DICT,
        IAT_MOCKED_QUERY_CHECK_TAGS,
        interpolation_at_time_get,
    )


def test_interpolation_at_time_pivot(mocker: MockerFixture):
    MOCKED_IAT_PARAMETER_DICT["case_insensitivity_tag_search"] = False
    MOCKED_IAT_PARAMETER_DICT["pivot"] = True
    _test_base_succeed(
        mocker,
        MOCKED_IAT_PARAMETER_DICT,
        IAT_MOCKED_QUERY_PIVOT,
        interpolation_at_time_get,
    )


def test_interpolation_at_time_uom(mocker: MockerFixture):
    MOCKED_IAT_PARAMETER_DICT["pivot"] = False
    MOCKED_IAT_PARAMETER_DICT["display_uom"] = True

    _test_base_succeed(
        mocker,
        MOCKED_IAT_PARAMETER_DICT,
        IAT_MOCKED_QUERY_UOM,
        interpolation_at_time_get,
    )


def test_interpolation_at_time_offset_limit(mocker: MockerFixture):
    MOCKED_IAT_PARAMETER_DICT["display_uom"] = False
    MOCKED_IAT_PARAMETER_DICT["offset"] = 10
    MOCKED_IAT_PARAMETER_DICT["limit"] = 10

    _test_base_succeed(
        mocker,
        MOCKED_IAT_PARAMETER_DICT,
        IAT_MOCKED_QUERY + MOCKED_QUERY_OFFSET_LIMIT,
        interpolation_at_time_get,
    )


def test_interpolation_at_time_fails(mocker: MockerFixture):
    _test_base_fails(mocker, MOCKED_IAT_PARAMETER_DICT, interpolation_at_time_get)


def test_interpolate_at_time_tag_name_not_list_fails(mocker: MockerFixture):
    MOCKED_IAT_PARAMETER_DICT["tag_names"] = "abc"
    _test_base_fails(mocker, MOCKED_IAT_PARAMETER_DICT, interpolation_at_time_get)


def test_interpolate_at_time_timestamps_not_list_fails(mocker: MockerFixture):
    MOCKED_IAT_PARAMETER_DICT["timestamps"] = "2011-01-01T00:00:00+00:00"
    _test_base_fails(mocker, MOCKED_IAT_PARAMETER_DICT, interpolation_at_time_get)
