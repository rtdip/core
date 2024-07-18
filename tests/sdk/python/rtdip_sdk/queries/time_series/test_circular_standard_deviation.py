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
from src.sdk.python.rtdip_sdk.queries.time_series.circular_standard_deviation import (
    get as circular_standard_deviation_get,
)
from tests.sdk.python.rtdip_sdk.queries.time_series._test_base import (
    _test_base_succeed,
    _test_base_fails,
)
from tests.sdk.python.rtdip_sdk.queries._test_utils.sdk_test_objects import (
    MOCKED_PARAMETER_DICT,
    MOCKED_QUERY_OFFSET_LIMIT,
    CIRCULAR_SD_MOCKED_QUERY,
    CIRCULAR_SD_MOCKED_QUERY_CHECK_TAGS,
    CIRCULAR_SD_MOCKED_QUERY_PIVOT,
    CIRCULAR_SD_MOCKED_QUERY_UOM,
)

MOCKED_CIRCULAR_SD_PARAMETER_DICT = MOCKED_PARAMETER_DICT.copy()
MOCKED_CIRCULAR_SD_PARAMETER_DICT["time_interval_rate"] = "15"
MOCKED_CIRCULAR_SD_PARAMETER_DICT["time_interval_unit"] = "minute"
MOCKED_CIRCULAR_SD_PARAMETER_DICT["lower_bound"] = 0
MOCKED_CIRCULAR_SD_PARAMETER_DICT["upper_bound"] = 360
MOCKED_CIRCULAR_SD_PARAMETER_DICT["pivot"] = False


def test_circular_standard_deviation(mocker: MockerFixture):
    _test_base_succeed(
        mocker,
        MOCKED_CIRCULAR_SD_PARAMETER_DICT,
        CIRCULAR_SD_MOCKED_QUERY,
        circular_standard_deviation_get,
    )


def test_circular_standard_deviation_check_tags(mocker: MockerFixture):
    MOCKED_CIRCULAR_SD_PARAMETER_DICT["case_insensitivity_tag_search"] = True
    _test_base_succeed(
        mocker,
        MOCKED_CIRCULAR_SD_PARAMETER_DICT,
        CIRCULAR_SD_MOCKED_QUERY_CHECK_TAGS,
        circular_standard_deviation_get,
    )


def test_circular_standard_deviation_pivot(mocker: MockerFixture):
    MOCKED_CIRCULAR_SD_PARAMETER_DICT["case_insensitivity_tag_search"] = False
    MOCKED_CIRCULAR_SD_PARAMETER_DICT["pivot"] = True
    _test_base_succeed(
        mocker,
        MOCKED_CIRCULAR_SD_PARAMETER_DICT,
        CIRCULAR_SD_MOCKED_QUERY_PIVOT,
        circular_standard_deviation_get,
    )


def test_circular_standard_deviation_uom(mocker: MockerFixture):
    MOCKED_CIRCULAR_SD_PARAMETER_DICT["pivot"] = False
    MOCKED_CIRCULAR_SD_PARAMETER_DICT["display_uom"] = True
    _test_base_succeed(
        mocker,
        MOCKED_CIRCULAR_SD_PARAMETER_DICT,
        CIRCULAR_SD_MOCKED_QUERY_UOM,
        circular_standard_deviation_get,
    )


def test_circular_standard_deviation_offset_limit(mocker: MockerFixture):
    MOCKED_CIRCULAR_SD_PARAMETER_DICT["limit"] = 10
    MOCKED_CIRCULAR_SD_PARAMETER_DICT["offset"] = 10
    MOCKED_CIRCULAR_SD_PARAMETER_DICT["display_uom"] = False

    _test_base_succeed(
        mocker,
        MOCKED_CIRCULAR_SD_PARAMETER_DICT,
        CIRCULAR_SD_MOCKED_QUERY + MOCKED_QUERY_OFFSET_LIMIT,
        circular_standard_deviation_get,
    )


def test_circular_standard_deviation_fails(mocker: MockerFixture):
    _test_base_fails(
        mocker, MOCKED_CIRCULAR_SD_PARAMETER_DICT, circular_standard_deviation_get
    )


def test_circular_standard_deviation_tag_name_not_list_fails(mocker: MockerFixture):
    MOCKED_CIRCULAR_SD_PARAMETER_DICT["tag_names"] = "abc"
    _test_base_fails(
        mocker, MOCKED_CIRCULAR_SD_PARAMETER_DICT, circular_standard_deviation_get
    )
