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
import json
import pandas as pd
import pytest

sys.path.insert(0, ".")
from pytest_mock import MockerFixture
from src.sdk.python.rtdip_sdk.connectors import DatabricksSQLConnection
from src.sdk.python.rtdip_sdk.queries.time_series.time_weighted_average import (
    get as time_weighted_average_get,
)
from src.sdk.python.rtdip_sdk.queries.time_series._time_series_query_builder import (
    _time_weighted_average_query,
)
from src.sdk.python.rtdip_sdk.queries.time_series._boundary_search import (
    extract_boundary_search_status,
)
from tests.sdk.python.rtdip_sdk.queries.time_series._test_base import (
    _test_base_succeed,
    _test_base_fails,
)
from tests.sdk.python.rtdip_sdk.queries._test_utils.sdk_test_objects import (
    MOCKED_PARAMETER_DICT,
)

MOCKED_TWA_PARAMETER_DICT = MOCKED_PARAMETER_DICT.copy()
MOCKED_TWA_PARAMETER_DICT["start_date"] = "2011-01-01T00:00:00+00:00"
MOCKED_TWA_PARAMETER_DICT["end_date"] = "2011-01-02T23:59:59+00:00"
MOCKED_TWA_PARAMETER_DICT["time_interval_rate"] = "15"
MOCKED_TWA_PARAMETER_DICT["time_interval_unit"] = "minute"
MOCKED_TWA_PARAMETER_DICT["time_zone"] = "+0000"
MOCKED_TWA_PARAMETER_DICT["range_join_seconds"] = 900
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


def test_metadata_boundary_probe_qualifies_tag_column():
    from src.sdk.python.rtdip_sdk.queries.time_series.time_weighted_average import (
        _build_twa_boundary_probe,
    )

    parameters = {**MOCKED_TWA_PARAMETER_DICT, "step": "metadata"}
    query = _build_twa_boundary_probe(parameters, 7)
    assert "SELECT requested.`TagName` AS `TagName`" in query
    assert "GROUP BY requested.`TagName`" in query
    assert "IFNULL(m.Step, false) OR" in query
    assert "LEFT JOIN (SELECT *" in query
    json_query = _build_twa_boundary_probe({**parameters, "to_json": True}, 7)
    assert json_query.startswith("SELECT to_json(struct(*)) AS Value FROM (")


def test_twa_incomplete_probe_result_retries():
    from src.sdk.python.rtdip_sdk.queries.time_series.time_weighted_average import (
        _read_probe_status,
    )

    assert _read_probe_status(pd.DataFrame())
    assert _read_probe_status(pd.DataFrame({"HasPrevious": [1]}))
    assert not _read_probe_status(pd.DataFrame({"HasPrevious": [1], "HasNext": [1]}))


def test_linear_twa_includes_raw_events_in_window_integration():
    parameters = {**MOCKED_TWA_PARAMETER_DICT, "step": "false"}
    query = expected_twa_query(parameters)
    assert "UNION ALL SELECT" in query
    assert "FROM raw_events" in query
    assert "window_start" in query
    assert "GROUP BY `TagName`, `window_start`" in query
    assert "ORDER BY `EventTime` DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW" in query
    assert "UNBOUNDED FOLLOWING" not in query


def test_compact_twa_case_status_and_pagination():
    parameters = {
        **MOCKED_TWA_PARAMETER_DICT,
        "step": "false",
        "status_column": None,
        "case_insensitivity_tag_search": True,
        "limit": 2,
        "offset": 1,
        "pivot": False,
        "display_uom": False,
    }
    query = expected_twa_query(parameters)
    assert "UPPER(s.`TagName`) AS `TagName`" in query
    assert "'Good' AS `Status`" in query
    assert "ORDER BY `TagName`, `EventTime` LIMIT 2 OFFSET 1" in query


@pytest.mark.parametrize("mode", ["pivot", "display_uom"])
@pytest.mark.parametrize("step", ["false", "metadata"])
def test_linear_twa_noncompact_includes_raw_events(mode, step):
    parameters = {**MOCKED_TWA_PARAMETER_DICT, "step": step, "step_was_provided": True, mode: True}
    if mode == "display_uom":
        parameters["pivot"] = False
    query = expected_twa_query(parameters)
    assert "FROM raw_events b LEFT ANTI JOIN date_array a" in query


def test_time_weighted_average_adaptive_sql_has_private_boundary_marker():
    parameters = MOCKED_TWA_PARAMETER_DICT.copy()
    parameters.update(
        {
            "step": "metadata",
            "adaptive_linear_search": True,
            "_linear_window_length": 30,
            "linear_window_length": 30,
            "window_length_was_provided": False,
            "step_was_provided": True,
        }
    )
    query = expected_twa_query(parameters)

    assert "source_events AS" in query
    assert "boundary_times AS" in query
    assert "twa_calculations AS" in query
    assert "twa AS (SELECT `TagName`, `window_start` AS `EventTime`," in query
    assert "metadata_mode AS" in query
    assert "(SELECT Step FROM metadata_mode) AS Step" in query
    assert "timestampadd(day, -30" in query
    assert "timestampadd(day, -30" in query
    assert "timestampadd(day, 30" in query
    assert "source_events AS" in query
    assert "boundary_times AS" in query
    assert "twa_calculations AS" in query
    parameters["to_json"] = True
    api_query = expected_twa_query(parameters)
    assert "to_json(struct(*)" in api_query

    parameters["case_insensitivity_tag_search"] = True
    case_insensitive_query = expected_twa_query(parameters)
    assert "metadata" in case_insensitive_query.lower()
    assert "timestampadd(day, -30" in case_insensitive_query
    assert "twa_calculations AS" in case_insensitive_query


@pytest.mark.parametrize(
    "boundary_flags,expected_windows",
    [([1, 0], [7, 30]), ([1, 1, 1, 1], [7, 30, 180, 366])],
)
def test_time_weighted_average_adaptive_boundary_search(
    mocker: MockerFixture, boundary_flags, expected_windows
):
    parameters = MOCKED_TWA_PARAMETER_DICT.copy()
    parameters.pop("window_length")
    parameters.pop("window_length_was_provided", None)
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
        def __init__(self, boundary_flag=None):
            self.boundary_flag = boundary_flag

        def execute(self, query):
            return None

        def fetch_all(self):
            if self.boundary_flag is None:
                return pd.DataFrame({"Value": [1.0]})
            complete = bool(self.boundary_flag == 0)
            return pd.DataFrame(
                {"HasPrevious": [1 if complete else 0], "HasNext": [1 if complete else 0]}
            )

        def close(self):
            return None

    class Connection:
        def __init__(self):
            self.flags = iter(boundary_flags)

        def cursor(self):
            try:
                return Cursor(next(self.flags))
            except StopIteration:
                return Cursor()

        def close(self):
            return None

    result = time_weighted_average_get(Connection(), parameters)

    assert executed_windows == [expected_windows[-1]]
    assert isinstance(result, pd.DataFrame)


def test_time_weighted_average_paginates_combined_tags(mocker: MockerFixture):
    parameters = {
        **MOCKED_TWA_PARAMETER_DICT,
        "tag_names": ["B", "A"],
        "step": "true",
        "limit": 1,
        "offset": 1,
        "display_uom": False,
        "pivot": False,
    }
    executed_parameters = []

    def build_query(query_parameters, query_type):
        executed_parameters.append(query_parameters.copy())
        return query_parameters["tag_names"][0]

    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.time_series.time_weighted_average._query_builder",
        side_effect=build_query,
    )

    class Cursor:
        def execute(self, query):
            self.tag = query

        def fetch_all(self):
            row = json.dumps({"TagName": self.tag, "EventTime": "2025-01-01", "Value": 1})
            return {"data": row, "sample_row": row, "count": 1}

        def close(self):
            pass

    class Connection:
        def cursor(self):
            return Cursor()

        def close(self):
            pass

    result = time_weighted_average_get(Connection(), parameters)
    assert all(item.get("limit") is None and item.get("offset") is None for item in executed_parameters)
    assert result["count"] == 1
    assert json.loads(result["data"])["TagName"] == "B"
    assert json.loads(result["sample_row"])["TagName"] == "B"


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
            row = {"EventTime": "2025-01-01T00:00:00Z", "Value": 1.0}
            serialized = json.dumps(row, separators=(",", ":"))
            return {"data": serialized, "sample_row": serialized, "count": 1}

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
    legacy_metadata.pop("window_length_was_provided", None)
    time_weighted_average_get(Connection(), legacy_metadata)

    assert len(query_parameters) == 2
    assert all(not parameters["adaptive_linear_search"] for parameters in query_parameters)


def test_time_weighted_average_extracts_boundary_status_from_api_result():
    marker = "_TwaBoundarySearchIncomplete"
    envelope = {"data": {"Value": 1.0}, marker: 1}
    serialized = json.dumps(envelope, separators=(",", ":"))
    result = {"data": serialized, "sample_row": serialized, "count": 1}

    cleaned, incomplete = extract_boundary_search_status(result, marker)

    assert incomplete
    assert marker not in json.loads(f"[{cleaned['data']}]")[0]


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
        assert "FULL OUTER JOIN raw_events" in query
        assert "date_trunc('day', to_timestamp(" in query


def test_time_weighted_average_omitted_step_retains_legacy_metadata_query(
    mocker: MockerFixture,
):
    parameters = MOCKED_TWA_PARAMETER_DICT.copy()
    parameters.pop("step", None)
    parameters.pop("step_was_provided", None)

    expected_query = expected_twa_query(parameters)
    _test_base_succeed(
        mocker, parameters, expected_query, time_weighted_average_get
    )
    assert parameters["step"] == "metadata"
    assert parameters["step_was_provided"] is False
    assert "boundary_times AS" not in expected_query


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
