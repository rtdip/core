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
import pandas as pd

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


def test_time_weighted_average_corrected_ctes_are_closed():
    for step in ("false", "metadata"):
        parameters = MOCKED_TWA_PARAMETER_DICT.copy()
        parameters["step"] = step
        parameters["step_was_provided"] = True
        query = expected_twa_query(parameters)
        assert query.count("twa_calculations AS (SELECT") == 1
        assert query.index("twa_calculations AS (SELECT") > query.index(
            "interpolate AS ("
        )
        assert "), CASE WHEN Step == false THEN" not in query


def test_time_weighted_average_adaptive_sql_has_private_boundary_marker():
    parameters = MOCKED_TWA_PARAMETER_DICT.copy()
    parameters.update(
        {
            "adaptive_linear_search": True,
            "_linear_window_length": 30,
            "step_was_provided": True,
        }
    )
    query = expected_twa_query(parameters)

    assert "AS HasPrevious" in query
    assert "AS HasNext" in query
    assert "boundary_status AS" in query
    assert "_TwaBoundarySearchIncomplete" in query
    assert ")), 30) AND date_add(" in query
    assert "project AS (SELECT `TagName`, `EventTime`, `Value` FROM twa" in query

    parameters["case_insensitivity_tag_search"] = True
    case_insensitive_query = expected_twa_query(parameters)
    assert "requested_twa_tags AS" in case_insensitive_query
    assert "WHERE NOT EXISTS (SELECT 1 FROM raw_events found" in case_insensitive_query


@pytest.mark.parametrize(
    "boundary_flags,expected_windows",
    [([1, 0], [7, 30]), ([1, 1, 1, 1], [7, 30, 180, 366])],
)
def test_time_weighted_average_adaptive_boundary_search(
    mocker: MockerFixture, boundary_flags, expected_windows
):
    parameters = MOCKED_TWA_PARAMETER_DICT.copy()
    parameters.pop("window_length")
    parameters["step_was_provided"] = True
    executed_windows = []

    def build_query(query_parameters, query_type):
        executed_windows.append(query_parameters["_linear_window_length"])
        return str(query_type)

    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.time_series.time_weighted_average._query_builder",
        side_effect=build_query,
    )

    class Cursor:
        def __init__(self, boundary_flag):
            self.boundary_flag = boundary_flag

        def execute(self, query):
            return None

        def fetch_all(self):
            return pd.DataFrame(
                {
                    "Value": [1.0],
                    "_TwaBoundarySearchIncomplete": [self.boundary_flag],
                }
            )

        def close(self):
            return None

    class Connection:
        def __init__(self):
            self.flags = iter(boundary_flags)

        def cursor(self):
            return Cursor(next(self.flags))

        def close(self):
            return None

    result = time_weighted_average_get(Connection(), parameters)

    assert executed_windows == expected_windows
    assert "_TwaBoundarySearchIncomplete" not in result.columns


def test_time_weighted_average_fixed_window_and_legacy_mode_do_not_retry(
    mocker: MockerFixture,
):
    query_parameters = []

    def build_query(parameters, query_type):
        query_parameters.append(parameters.copy())
        return str(query_type)

    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.time_series.time_weighted_average._query_builder",
        side_effect=build_query,
    )

    class Cursor:
        def execute(self, query):
            return None

        def fetch_all(self):
            return pd.DataFrame({"Value": [1.0]})

        def close(self):
            return None

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            return None

    fixed_window = MOCKED_TWA_PARAMETER_DICT.copy()
    fixed_window["window_length"] = 30
    fixed_window["window_length_was_provided"] = True
    time_weighted_average_get(Connection(), fixed_window)

    legacy_metadata = MOCKED_TWA_PARAMETER_DICT.copy()
    legacy_metadata.pop("step", None)
    legacy_metadata.pop("step_was_provided", None)
    legacy_metadata.pop("window_length", None)
    time_weighted_average_get(Connection(), legacy_metadata)

    assert len(query_parameters) == 2
    assert all(not parameters["adaptive_linear_search"] for parameters in query_parameters)


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
