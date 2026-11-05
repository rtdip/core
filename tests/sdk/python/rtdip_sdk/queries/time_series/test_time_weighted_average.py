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
from datetime import datetime

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
from tests.sdk.python.rtdip_sdk.queries.time_series._test_base import (
    _test_base_succeed,
    _test_base_fails,
)
from tests.sdk.python.rtdip_sdk.queries._test_utils.sdk_test_objects import (
    MOCKED_PARAMETER_DICT,
    MOCKED_QUERY_OFFSET_LIMIT,
    TWA_MOCKED_QUERY,
    TWA_MOCKED_QUERY_CHECK_TAGS,
    TWA_MOCKED_QUERY_PIVOT,
    TWA_MOCKED_QUERY_METADATA,
    TWA_MOCKED_QUERY_UOM,
)

MOCKED_TWA_PARAMETER_DICT = MOCKED_PARAMETER_DICT.copy()
MOCKED_TWA_PARAMETER_DICT["time_interval_rate"] = "15"
MOCKED_TWA_PARAMETER_DICT["time_interval_unit"] = "minute"
MOCKED_TWA_PARAMETER_DICT["window_length"] = 1
MOCKED_TWA_PARAMETER_DICT["step"] = "false"
MOCKED_TWA_PARAMETER_DICT["pivot"] = False


def test_time_weighted_average(mocker: MockerFixture):
    _test_base_succeed(
        mocker,
        MOCKED_TWA_PARAMETER_DICT,
        TWA_MOCKED_QUERY,
        time_weighted_average_get,
    )


def test_time_weighted_average_check_tags(mocker: MockerFixture):
    MOCKED_TWA_PARAMETER_DICT["case_insensitivity_tag_search"] = True

    _test_base_succeed(
        mocker,
        MOCKED_TWA_PARAMETER_DICT,
        TWA_MOCKED_QUERY_CHECK_TAGS,
        time_weighted_average_get,
    )
    MOCKED_TWA_PARAMETER_DICT["case_insensitivity_tag_search"] = False


def test_time_weighted_average_metadata_step(mocker: MockerFixture):
    MOCKED_TWA_PARAMETER_DICT["step"] = "metadata"

    _test_base_succeed(
        mocker,
        MOCKED_TWA_PARAMETER_DICT,
        TWA_MOCKED_QUERY_METADATA,
        time_weighted_average_get,
    )


def test_time_weighted_average_step_true(mocker: MockerFixture):
    parameters = MOCKED_TWA_PARAMETER_DICT.copy()
    parameters["step"] = "true"

    _test_base_succeed(
        mocker,
        parameters,
        TWA_MOCKED_QUERY.replace(
            ",fill_step AS (SELECT *, false AS Step FROM fill_value)",
            ",fill_step AS (SELECT *, true AS Step FROM fill_value)",
        ),
        time_weighted_average_get,
    )


def test_time_weighted_average_metadata_pi_query():
    parameters = MOCKED_TWA_PARAMETER_DICT.copy()
    parameters["step"] = "metadata_pi"
    parameters["start_date"] = "2011-01-01T00:00:00+00:00"
    parameters["end_date"] = "2011-01-02T23:59:59+00:00"
    parameters["time_zone"] = "+0000"
    parameters["range_join_seconds"] = 900
    query = _time_weighted_average_query(parameters)

    assert "WITH source_events AS" in query
    assert "boundary_times AS" in query
    assert "Previous_Event" in query
    assert "Next_Event" in query
    assert "FROM event_bounds" in query

    parameters["step"] = "false"
    legacy_query = _time_weighted_average_query(parameters)
    assert "boundary_times AS" not in legacy_query
    assert "FROM fill_step" in legacy_query
    assert "lag(`EventTime`)" in legacy_query


def test_time_weighted_average_pivot(mocker: MockerFixture):
    MOCKED_TWA_PARAMETER_DICT["step"] = "false"
    MOCKED_TWA_PARAMETER_DICT["pivot"] = True

    _test_base_succeed(
        mocker,
        MOCKED_TWA_PARAMETER_DICT,
        TWA_MOCKED_QUERY_PIVOT,
        time_weighted_average_get,
    )


def test_time_weighted_average_uom(mocker: MockerFixture):
    MOCKED_TWA_PARAMETER_DICT["pivot"] = False
    MOCKED_TWA_PARAMETER_DICT["display_uom"] = True

    _test_base_succeed(
        mocker,
        MOCKED_TWA_PARAMETER_DICT,
        TWA_MOCKED_QUERY_UOM,
        time_weighted_average_get,
    )


def test_time_weighted_average_offset_limit(mocker: MockerFixture):
    MOCKED_TWA_PARAMETER_DICT["offset"] = 10
    MOCKED_TWA_PARAMETER_DICT["limit"] = 10
    MOCKED_TWA_PARAMETER_DICT["display_uom"] = False

    _test_base_succeed(
        mocker,
        MOCKED_TWA_PARAMETER_DICT,
        TWA_MOCKED_QUERY + MOCKED_QUERY_OFFSET_LIMIT,
        time_weighted_average_get,
    )


def test_time_weighted_average_fails(mocker: MockerFixture):
    _test_base_fails(mocker, MOCKED_TWA_PARAMETER_DICT, time_weighted_average_get)


def test_time_weighted_average_tag_name_not_list_fails(mocker: MockerFixture):
    MOCKED_TWA_PARAMETER_DICT["tag_names"] = "abc"
    _test_base_fails(mocker, MOCKED_TWA_PARAMETER_DICT, time_weighted_average_get)


def test_time_weighted_average_default_metadata_step(mocker: MockerFixture):
    parameters = MOCKED_TWA_PARAMETER_DICT.copy()
    parameters.update(
        pivot=False,
        display_uom=False,
        tag_names=MOCKED_PARAMETER_DICT["tag_names"],
        limit=None,
        offset=None,
    )
    parameters.pop("step", None)

    _test_base_succeed(
        mocker, parameters, TWA_MOCKED_QUERY_METADATA, time_weighted_average_get
    )
    assert parameters["step"] == "metadata"


@pytest.mark.parametrize("window_length", [0, 1, 7])
@pytest.mark.parametrize(
    "events, linear_values, stepped_values",
    [
        (
            [(datetime(2025, 10, 1), 100.0), (datetime(2025, 10, 5), 500.0)],
            [250.0, 350.0],
            [100.0, 100.0],
        ),
        (
            [
                (datetime(2025, 10, 1), 100.0),
                (datetime(2025, 10, 3, 12), 600.0),
                (datetime(2025, 10, 5), 500.0),
            ],
            [400.0, 1700.0 / 3.0],
            [100.0, 350.0],
        ),
        (
            [
                (datetime(2025, 10, 1), 100.0),
                (datetime(2025, 10, 3), 500.0),
                (datetime(2025, 10, 5), 300.0),
            ],
            [400.0, 450.0],
            [100.0, 500.0],
        ),
        (
            [
                (datetime(2025, 8, 1), 900.0),
                (datetime(2025, 9, 1), 100.0),
                (datetime(2025, 11, 10), 800.0),
                (datetime(2025, 12, 1), 3000.0),
            ],
            [415.0, 425.0],
            [100.0, 100.0],
        ),
    ],
)
def test_time_weighted_average_sparse_events(
    spark_session, window_length, events, linear_values, stepped_values
):
    tag_names = ["linear", "stepped", "null_step", "missing_metadata"]
    spark_session.createDataFrame(
        [
            (timestamp, tag_name, "Good", value)
            for tag_name in tag_names
            for timestamp, value in events
        ],
        "EventTime timestamp, TagName string, Status string, Value double",
    ).createOrReplaceTempView("twa_sparse_events")
    spark_session.createDataFrame(
        [("linear", False), ("stepped", True), ("null_step", None)],
        "TagName string, Step boolean",
    ).createOrReplaceTempView("twa_sparse_metadata")
    parameters = {
        "source": "twa_sparse_events",
        "metadata_source": "twa_sparse_metadata",
        "tag_names": tag_names,
        "start_date": "2025-10-02T00:00:00+00:00",
        "end_date": "2025-10-04T00:00:00+00:00",
        "time_interval_rate": "1",
        "time_interval_unit": "day",
        "step": "metadata_pi",
        "time_zone": "+0000",
        "range_join_seconds": 86400,
        "window_length": window_length,
        "include_bad_data": False,
    }
    try:
        actual = spark_session.sql(
            _time_weighted_average_query(parameters)
        ).collect()
        expected = {
            (tag_name, datetime(2025, 10, day)): values[index]
            for tag_name in tag_names
            for values in [
                stepped_values if tag_name == "stepped"
                else linear_values
            ]
            for index, day in enumerate([2, 3])
        }
        assert len(actual) == len(expected)
        assert {
            (row.TagName, row.EventTime): row.Value for row in actual
        } == pytest.approx(expected)
    finally:
        spark_session.catalog.dropTempView("twa_sparse_events")
        spark_session.catalog.dropTempView("twa_sparse_metadata")


@pytest.mark.parametrize("window_length", [0, 1, 7])
def test_time_weighted_average_pi_october(spark_session, window_length):
    """Reproduce the daily PI averages from the October 2025 comparison workbook."""
    events = [
        ("2025-09-27T05:50:59", 3972.0),
        ("2025-10-01T00:50:49", 3723.0),
        ("2025-10-04T05:50:00", 4041.0),
        ("2025-10-05T05:00:13", 3520.0),
        ("2025-10-10T22:30:14", 4286.0),
        ("2025-10-11T18:30:49", 3994.0),
        ("2025-10-12T21:30:00", 3361.0),
        ("2025-10-14T21:30:23", 3903.0),
        ("2025-10-15T22:45:32", 3668.0),
        ("2025-10-19T00:21:18", 4110.0),
        ("2025-10-21T01:35:23", 4034.0),
        ("2025-10-27T00:30:52", 2918.0),
        ("2025-10-28T21:53:46", 2915.0),
        ("2025-11-03T15:00:00", 3193.0),
    ]
    expected_values = [
        3793.852416191895,
        3892.9868230675697,
        3992.105826989567,
        3768.0042373886422,
        3592.4015547443341,
        3726.1031028220145,
        3859.8046508996949,
        3993.5061989773753,
        4127.2077470550566,
        4237.304335516691,
        3977.1324400721664,
        3495.3975624270565,
        3592.4483604612815,
        3832.4573491729911,
        3729.0123508200368,
        3783.5573437606158,
        3927.6953945332257,
        4066.8215979413544,
        4082.762718271319,
        4043.1771205586679,
        3905.8635256710504,
        3718.4641759745323,
        3531.0648262780137,
        3343.6654765814951,
        3156.2661268849765,
        2973.7203818392386,
        2916.8441000403982,
        2918.1222206849889,
        2955.7638408830285,
        3004.4276683942021,
    ]
    spark_session.createDataFrame(
        [
            (datetime.fromisoformat(timestamp), "pi_tag", "Good", value)
            for timestamp, value in events
        ],
        "EventTime timestamp, TagName string, Status string, Value double",
    ).createOrReplaceTempView("twa_pi_events")
    spark_session.createDataFrame(
        [("pi_tag", False)], "TagName string, Step boolean"
    ).createOrReplaceTempView("twa_pi_metadata")
    parameters = {
        "source": "twa_pi_events",
        "metadata_source": "twa_pi_metadata",
        "tag_names": ["pi_tag"],
        "start_date": "2025-10-01T06:00:00+00:00",
        "end_date": "2025-10-31T06:00:00+00:00",
        "time_interval_rate": "1",
        "time_interval_unit": "day",
        "step": "metadata_pi",
        "time_zone": "+0000",
        "range_join_seconds": 86400,
        "window_length": window_length,
        "include_bad_data": False,
    }
    try:
        actual = spark_session.sql(
            _time_weighted_average_query(parameters)
        ).collect()
        assert [row.EventTime for row in actual] == [
            datetime(2025, 10, day, 6) for day in range(1, 31)
        ]
        assert all(row.TagName == "pi_tag" for row in actual)
        assert [row.Value for row in actual] == pytest.approx(
            expected_values, rel=1e-10, abs=1e-8
        )
    finally:
        spark_session.catalog.dropTempView("twa_pi_events")
        spark_session.catalog.dropTempView("twa_pi_metadata")
