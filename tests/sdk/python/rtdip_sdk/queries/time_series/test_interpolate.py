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
from src.sdk.python.rtdip_sdk.queries.time_series.interpolate import (
    get as interpolate_get,
)
from src.sdk.python.rtdip_sdk.queries.time_series._time_series_query_builder import (
    _interpolation_query,
)
from tests.sdk.python.rtdip_sdk.queries.time_series._test_base import (
    _test_base_succeed,
    _test_base_fails,
)
from tests.sdk.python.rtdip_sdk.queries._test_utils.sdk_test_objects import (
    MOCKED_PARAMETER_DICT,
    INTERPOLATE_MOCKED_QUERY,
    INTERPOLATE_MOCKED_QUERY_CHECK_TAGS,
    MOCKED_QUERY_OFFSET_LIMIT,
    INTERPOLATE_MOCKED_QUERY_PIVOT,
    INTERPOLATE_MOCKED_QUERY_UOM,
)

MOCKED_INTERPOLATE_PARAMETER_DICT = MOCKED_PARAMETER_DICT.copy()
MOCKED_INTERPOLATE_PARAMETER_DICT["time_interval_rate"] = "15"
MOCKED_INTERPOLATE_PARAMETER_DICT["time_interval_unit"] = "minute"
MOCKED_INTERPOLATE_PARAMETER_DICT["pivot"] = False


def test_interpolate(mocker: MockerFixture):
    TEST_PARAMETERS = MOCKED_INTERPOLATE_PARAMETER_DICT.copy()
    TEST_PARAMETERS["display_uom"] = False
    _test_base_succeed(
        mocker,
        TEST_PARAMETERS,
        INTERPOLATE_MOCKED_QUERY,
        interpolate_get,
    )


def test_interpolate_step_modes_query():
    parameters = MOCKED_INTERPOLATE_PARAMETER_DICT.copy()
    legacy_query = _interpolation_query(parameters)
    assert _interpolation_query({**parameters, "step": None}) == legacy_query

    linear_query = _interpolation_query({**parameters, "step": "false"})
    assert "boundary_times AS" in linear_query
    assert "PreviousEventTime" in linear_query
    assert "NextEventTime" in linear_query
    assert "date_sub(to_date(to_timestamp(" in linear_query
    assert ")), 7) AND date_add(" in linear_query

    step_query = _interpolation_query({**parameters, "step": "true"})
    assert "CASE WHEN c.`Prev` IS NOT NULL THEN c.`Prev`.`Value`" in step_query
    assert "boundary_times AS" not in step_query
    assert "date_sub(to_date(to_timestamp(" not in step_query

    metadata_query = _interpolation_query({**parameters, "step": "metadata"})
    assert "IFNULL(m.`Step`, false)" in metadata_query
    assert "metadata_modes WHERE Step = false" in metadata_query
    assert "metadata_modes WHERE Step = true" in metadata_query
    assert "boundary_times AS" in metadata_query
    assert "date_sub(to_date(to_timestamp(" in metadata_query
    assert "`mocked-asset_mocked-data-security-level_metadata` AS m" in metadata_query
    assert "_InterpolateBoundarySearchIncomplete" in linear_query
    assert "_InterpolateBoundarySearchIncomplete" in metadata_query


def test_interpolate_window_length_validation():
    parameters = MOCKED_INTERPOLATE_PARAMETER_DICT.copy()
    for window_length in (0, 366):
        query = _interpolation_query(
            {**parameters, "step": "false", "window_length": window_length}
        )
        assert f")), {window_length}) AND date_add(" in query

    with pytest.raises(ValueError, match="between 0 and 366"):
        _interpolation_query(
            {**parameters, "step": "false", "window_length": 367}
        )


@pytest.mark.parametrize(
    "step,boundary_flags,expected_windows",
    [
        ("false", [1, 0], [7, 30]),
        ("metadata", [1, 1, 1, 1], [7, 30, 180, 366]),
    ],
)
def test_interpolate_adaptive_boundary_search(
    mocker: MockerFixture, step, boundary_flags, expected_windows
):
    parameters = MOCKED_INTERPOLATE_PARAMETER_DICT.copy()
    parameters["step"] = step
    executed_windows = []

    def build_query(query_parameters, query_type):
        executed_windows.append(query_parameters["_linear_window_length"])
        return str(query_type)

    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.time_series.interpolate._query_builder",
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
                    "EventTime": [pd.Timestamp("2025-01-01T00:00:00Z")],
                    "TagName": ["mocked-TAGNAME"],
                    "Value": [1.0],
                    "_InterpolateBoundarySearchIncomplete": [self.boundary_flag],
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

    result = interpolate_get(Connection(), parameters)

    assert executed_windows == expected_windows
    assert "_InterpolateBoundarySearchIncomplete" not in result.columns


def test_interpolate_explicit_window_and_legacy_modes_do_not_retry(
    mocker: MockerFixture,
):
    query_parameters = []

    def build_query(parameters, query_type):
        query_parameters.append(parameters.copy())
        return str(query_type)

    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.time_series.interpolate._query_builder",
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

    explicit_window = MOCKED_INTERPOLATE_PARAMETER_DICT.copy()
    explicit_window.update(
        {"step": "false", "window_length": 30, "window_length_was_provided": True}
    )
    interpolate_get(Connection(), explicit_window)
    legacy = MOCKED_INTERPOLATE_PARAMETER_DICT.copy()
    interpolate_get(Connection(), legacy)

    assert len(query_parameters) == 2
    assert all(not parameters["adaptive_search"] for parameters in query_parameters)


def test_interpolate_null_step_uses_legacy_query(mocker: MockerFixture):
    parameters = MOCKED_INTERPOLATE_PARAMETER_DICT.copy()
    parameters["step"] = "null"
    _test_base_succeed(
        mocker, parameters, INTERPOLATE_MOCKED_QUERY, interpolate_get
    )
    assert parameters["step"] is None


def test_interpolate_uom(mocker: MockerFixture):
    TEST_PARAMETERS = MOCKED_INTERPOLATE_PARAMETER_DICT.copy()
    TEST_PARAMETERS["display_uom"] = True
    _test_base_succeed(
        mocker,
        TEST_PARAMETERS,
        INTERPOLATE_MOCKED_QUERY_UOM,
        interpolate_get,
    )


def test_interpolate_check_tags(mocker: MockerFixture):
    TEST_PARAMETERS = MOCKED_INTERPOLATE_PARAMETER_DICT.copy()
    TEST_PARAMETERS["case_insensitivity_tag_search"] = True
    _test_base_succeed(
        mocker,
        TEST_PARAMETERS,
        INTERPOLATE_MOCKED_QUERY_CHECK_TAGS,
        interpolate_get,
    )


def test_interpolate_sample_rate_unit(mocker: MockerFixture):
    TEST_PARAMETERS = MOCKED_INTERPOLATE_PARAMETER_DICT.copy()
    TEST_PARAMETERS["case_insensitivity_tag_search"] = False
    TEST_PARAMETERS["sample_rate"] = "15"
    TEST_PARAMETERS["sample_unit"] = "minute"
    _test_base_succeed(
        mocker,
        TEST_PARAMETERS,
        INTERPOLATE_MOCKED_QUERY,
        interpolate_get,
    )


def test_interpolate_pivot(mocker: MockerFixture):
    TEST_PARAMETERS = MOCKED_INTERPOLATE_PARAMETER_DICT.copy()
    TEST_PARAMETERS["pivot"] = True
    _test_base_succeed(
        mocker,
        TEST_PARAMETERS,
        INTERPOLATE_MOCKED_QUERY_PIVOT,
        interpolate_get,
    )


def test_interpolate_offset_limit(mocker: MockerFixture):
    TEST_PARAMETERS = MOCKED_INTERPOLATE_PARAMETER_DICT.copy()
    TEST_PARAMETERS["pivot"] = False
    TEST_PARAMETERS["offset"] = 10
    TEST_PARAMETERS["limit"] = 10
    _test_base_succeed(
        mocker,
        TEST_PARAMETERS,
        INTERPOLATE_MOCKED_QUERY + " " + MOCKED_QUERY_OFFSET_LIMIT.strip(),
        interpolate_get,
    )


def test_interpolate_fails(mocker: MockerFixture):
    _test_base_fails(mocker, MOCKED_INTERPOLATE_PARAMETER_DICT, interpolate_get)


def test_interpolate_tag_name_not_list_fails(mocker: MockerFixture):
    TEST_PARAMETERS = MOCKED_INTERPOLATE_PARAMETER_DICT.copy()
    TEST_PARAMETERS["tag_names"] = "abc"
    _test_base_fails(mocker, TEST_PARAMETERS, interpolate_get)
