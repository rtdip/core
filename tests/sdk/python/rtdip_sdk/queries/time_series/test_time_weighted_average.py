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
from src.sdk.python.rtdip_sdk.queries.time_series.time_weighted_average import (
    get as time_weighted_average_get,
)
from src.sdk.python.rtdip_sdk.queries.time_series._time_series_query_builder import (
    _time_weighted_average_query,
)
from tests.sdk.python.rtdip_sdk.queries.time_series._test_base import (
    _test_base_succeed,
    _test_base_fails,
)
from tests.sdk.python.rtdip_sdk.queries._test_utils.sdk_test_objects import (
    MOCKED_PARAMETER_DICT,
    TWA_MOCKED_QUERY_METADATA,
)

MOCKED_TWA_PARAMETER_DICT = MOCKED_PARAMETER_DICT.copy()
MOCKED_TWA_PARAMETER_DICT["time_interval_rate"] = "15"
MOCKED_TWA_PARAMETER_DICT["time_interval_unit"] = "minute"
MOCKED_TWA_PARAMETER_DICT["window_length"] = 1
MOCKED_TWA_PARAMETER_DICT["step"] = "false"
MOCKED_TWA_PARAMETER_DICT["pivot"] = False


def expected_twa_query(parameters):
    query_parameters = parameters.copy()
    query_parameters.setdefault("step_was_provided", "step" in query_parameters)
    query_parameters.setdefault("step", "metadata")
    return _time_weighted_average_query(query_parameters)


def test_time_weighted_average(mocker: MockerFixture):
    _test_base_succeed(
        mocker,
        MOCKED_TWA_PARAMETER_DICT,
        expected_twa_query(MOCKED_TWA_PARAMETER_DICT),
        time_weighted_average_get,
    )


def test_time_weighted_average_check_tags(mocker: MockerFixture):
    MOCKED_TWA_PARAMETER_DICT["case_insensitivity_tag_search"] = True

    _test_base_succeed(
        mocker,
        MOCKED_TWA_PARAMETER_DICT,
        expected_twa_query(MOCKED_TWA_PARAMETER_DICT),
        time_weighted_average_get,
    )
    MOCKED_TWA_PARAMETER_DICT["case_insensitivity_tag_search"] = False


def test_time_weighted_average_metadata_step(mocker: MockerFixture):
    MOCKED_TWA_PARAMETER_DICT["step"] = "metadata"

    _test_base_succeed(
        mocker,
        MOCKED_TWA_PARAMETER_DICT,
        expected_twa_query(MOCKED_TWA_PARAMETER_DICT),
        time_weighted_average_get,
    )
    assert "boundary_times AS" in expected_twa_query(MOCKED_TWA_PARAMETER_DICT)


def test_time_weighted_average_legacy_modes_do_not_use_linear_boundary_lookup():
    for step, supplied in (
        ("true", True),
        ("metadata", False),
        ("null", True),
        (None, True),
    ):
        parameters = MOCKED_TWA_PARAMETER_DICT.copy()
        parameters["step"] = step
        parameters["step_was_provided"] = supplied
        query = expected_twa_query(parameters)
        assert "boundary_times AS" not in query
        assert "event_bounds AS" not in query


def test_time_weighted_average_omitted_step_retains_legacy_metadata_query(
    mocker: MockerFixture,
):
    parameters = MOCKED_TWA_PARAMETER_DICT.copy()
    parameters.pop("step", None)
    parameters.pop("step_was_provided", None)

    _test_base_succeed(
        mocker, parameters, TWA_MOCKED_QUERY_METADATA, time_weighted_average_get
    )
    assert parameters["step"] == "metadata"
    assert parameters["step_was_provided"] is False


def test_time_weighted_average_pivot(mocker: MockerFixture):
    MOCKED_TWA_PARAMETER_DICT["step"] = "false"
    MOCKED_TWA_PARAMETER_DICT["pivot"] = True

    _test_base_succeed(
        mocker,
        MOCKED_TWA_PARAMETER_DICT,
        expected_twa_query(MOCKED_TWA_PARAMETER_DICT),
        time_weighted_average_get,
    )


def test_time_weighted_average_uom(mocker: MockerFixture):
    MOCKED_TWA_PARAMETER_DICT["pivot"] = False
    MOCKED_TWA_PARAMETER_DICT["display_uom"] = True

    _test_base_succeed(
        mocker,
        MOCKED_TWA_PARAMETER_DICT,
        expected_twa_query(MOCKED_TWA_PARAMETER_DICT),
        time_weighted_average_get,
    )


def test_time_weighted_average_offset_limit(mocker: MockerFixture):
    MOCKED_TWA_PARAMETER_DICT["offset"] = 10
    MOCKED_TWA_PARAMETER_DICT["limit"] = 10
    MOCKED_TWA_PARAMETER_DICT["display_uom"] = False

    _test_base_succeed(
        mocker,
        MOCKED_TWA_PARAMETER_DICT,
        expected_twa_query(MOCKED_TWA_PARAMETER_DICT),
        time_weighted_average_get,
    )


def test_time_weighted_average_fails(mocker: MockerFixture):
    _test_base_fails(mocker, MOCKED_TWA_PARAMETER_DICT, time_weighted_average_get)


def test_time_weighted_average_tag_name_not_list_fails(mocker: MockerFixture):
    MOCKED_TWA_PARAMETER_DICT["tag_names"] = "abc"
    _test_base_fails(mocker, MOCKED_TWA_PARAMETER_DICT, time_weighted_average_get)
