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
from src.sdk.python.rtdip_sdk.queries.time_series.interpolate import (
    get as interpolate_get,
)
from tests.sdk.python.rtdip_sdk.queries.time_series._test_base import (
    _test_base_succeed,
    _test_base_fails,
)
from tests.sdk.python.rtdip_sdk.queries._test_utils.sdk_test_objects import (
    MOCKED_PARAMETER_DICT,
    INTERPOLATE_MOCKED_QUERY,
    MOCKED_QUERY_OFFSET_LIMIT,
    INTERPOLATE_MOCKED_QUERY_PIVOT,
)

MOCKED_INTERPOLATE_PARAMETER_DICT = MOCKED_PARAMETER_DICT.copy()
MOCKED_INTERPOLATE_PARAMETER_DICT["time_interval_rate"] = "15"
MOCKED_INTERPOLATE_PARAMETER_DICT["time_interval_unit"] = "minute"
MOCKED_INTERPOLATE_PARAMETER_DICT["agg_method"] = "avg"
MOCKED_INTERPOLATE_PARAMETER_DICT["interpolation_method"] = "forward_fill"
MOCKED_INTERPOLATE_PARAMETER_DICT["pivot"] = False


def test_interpolate(mocker: MockerFixture):
    _test_base_succeed(
        mocker,
        MOCKED_INTERPOLATE_PARAMETER_DICT,
        INTERPOLATE_MOCKED_QUERY,
        interpolate_get,
    )


def test_interpolate_sample_rate_unit(mocker: MockerFixture):
    MOCKED_INTERPOLATE_PARAMETER_DICT["sample_rate"] = "15"
    MOCKED_INTERPOLATE_PARAMETER_DICT["sample_unit"] = "minute"
    _test_base_succeed(
        mocker,
        MOCKED_INTERPOLATE_PARAMETER_DICT,
        INTERPOLATE_MOCKED_QUERY,
        interpolate_get,
    )


def test_interpolate_pivot(mocker: MockerFixture):
    MOCKED_INTERPOLATE_PARAMETER_DICT["pivot"] = True
    _test_base_succeed(
        mocker,
        MOCKED_INTERPOLATE_PARAMETER_DICT,
        INTERPOLATE_MOCKED_QUERY_PIVOT,
        interpolate_get,
    )


def test_interpolate_offset_limit(mocker: MockerFixture):
    MOCKED_INTERPOLATE_PARAMETER_DICT["offset"] = 10
    MOCKED_INTERPOLATE_PARAMETER_DICT["limit"] = 10
    MOCKED_INTERPOLATE_PARAMETER_DICT["pivot"] = False

    _test_base_succeed(
        mocker,
        MOCKED_INTERPOLATE_PARAMETER_DICT,
        INTERPOLATE_MOCKED_QUERY + MOCKED_QUERY_OFFSET_LIMIT,
        interpolate_get,
    )


def test_interpolate_fails(mocker: MockerFixture):
    _test_base_fails(mocker, MOCKED_INTERPOLATE_PARAMETER_DICT, interpolate_get)


def test_interpolate_tag_name_not_list_fails(mocker: MockerFixture):
    MOCKED_INTERPOLATE_PARAMETER_DICT["tag_names"] = "abc"
    _test_base_fails(mocker, MOCKED_INTERPOLATE_PARAMETER_DICT, interpolate_get)
