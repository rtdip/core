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
from src.sdk.python.rtdip_sdk.queries.time_series.plot import get as plot_get
from tests.sdk.python.rtdip_sdk.queries.time_series._test_base import (
    _test_base_succeed,
    _test_base_fails,
)
from tests.sdk.python.rtdip_sdk.queries._test_utils.sdk_test_objects import (
    MOCKED_QUERY_OFFSET_LIMIT,
    MOCKED_PARAMETER_DICT,
    PLOT_MOCKED_QUERY,
    PLOT_MOCKED_QUERY_CHECK_TAGS,
    PLOT_MOCKED_QUERY_PIVOT,
    PLOT_MOCKED_QUERY_UOM,
)

MOCKED_PLOT_PARAMETER_DICT = MOCKED_PARAMETER_DICT.copy()
MOCKED_PLOT_PARAMETER_DICT["time_interval_rate"] = "15"
MOCKED_PLOT_PARAMETER_DICT["time_interval_unit"] = "minute"


def test_plot_success(mocker: MockerFixture):
    _test_base_succeed(
        mocker,
        MOCKED_PLOT_PARAMETER_DICT,
        PLOT_MOCKED_QUERY,
        plot_get,
    )


def test_plot_check_tags(mocker: MockerFixture):
    MOCKED_PLOT_PARAMETER_DICT["case_insensitivity_tag_search"] = True
    _test_base_succeed(
        mocker,
        MOCKED_PLOT_PARAMETER_DICT,
        PLOT_MOCKED_QUERY_CHECK_TAGS,
        plot_get,
    )


def test_plot_sample_rate_unit(mocker: MockerFixture):
    MOCKED_PLOT_PARAMETER_DICT["case_insensitivity_tag_search"] = False
    MOCKED_PLOT_PARAMETER_DICT["sample_rate"] = "15"
    MOCKED_PLOT_PARAMETER_DICT["sample_unit"] = "minute"
    _test_base_succeed(
        mocker,
        MOCKED_PLOT_PARAMETER_DICT,
        PLOT_MOCKED_QUERY,
        plot_get,
    )


def test_plot_pivot(mocker: MockerFixture):
    MOCKED_PLOT_PARAMETER_DICT["pivot"] = True

    _test_base_succeed(
        mocker,
        MOCKED_PLOT_PARAMETER_DICT,
        PLOT_MOCKED_QUERY_PIVOT,
        plot_get,
    )


def test_plot_uom(mocker: MockerFixture):
    MOCKED_PLOT_PARAMETER_DICT["pivot"] = False
    MOCKED_PLOT_PARAMETER_DICT["display_uom"] = True

    _test_base_succeed(
        mocker,
        MOCKED_PLOT_PARAMETER_DICT,
        PLOT_MOCKED_QUERY_UOM,
        plot_get,
    )


def test_plot_offset_limit(mocker: MockerFixture):
    MOCKED_PLOT_PARAMETER_DICT["display_uom"] = False
    MOCKED_PLOT_PARAMETER_DICT["offset"] = 10
    MOCKED_PLOT_PARAMETER_DICT["limit"] = 10
    _test_base_succeed(
        mocker,
        MOCKED_PLOT_PARAMETER_DICT,
        (PLOT_MOCKED_QUERY + MOCKED_QUERY_OFFSET_LIMIT),
        plot_get,
    )


def test_plot_fails(mocker: MockerFixture):
    _test_base_fails(mocker, MOCKED_PLOT_PARAMETER_DICT, plot_get)


def test_plot_tag_name_not_list_fails(mocker: MockerFixture):
    MOCKED_PLOT_PARAMETER_DICT["tag_names"] = "abc"
    _test_base_fails(mocker, MOCKED_PLOT_PARAMETER_DICT, plot_get)
