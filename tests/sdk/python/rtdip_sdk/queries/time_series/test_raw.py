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
import pytest
from pytest_mock import MockerFixture
from src.sdk.python.rtdip_sdk.queries.time_series.raw import get as raw_get
from tests.sdk.python.rtdip_sdk.queries.time_series._test_base import (
    _test_base_succeed,
    _test_base_fails,
)
from tests.sdk.python.rtdip_sdk.queries._test_utils.sdk_test_objects import (
    RAW_MOCKED_QUERY,
    RAW_MOCKED_QUERY_CHECK_TAGS,
    MOCKED_QUERY_OFFSET_LIMIT,
    MOCKED_PARAMETER_DICT,
    RAW_MOCKED_QUERY_DISPLAY_UOM,
)

MOCKED_RAW_DICT = MOCKED_PARAMETER_DICT.copy()


def test_raw(mocker: MockerFixture):
    _test_base_succeed(mocker, MOCKED_RAW_DICT, RAW_MOCKED_QUERY, raw_get)


def test_raw_check_tags(mocker: MockerFixture):
    MOCKED_RAW_DICT["case_insensitivity_tag_search"] = True
    _test_base_succeed(mocker, MOCKED_RAW_DICT, RAW_MOCKED_QUERY_CHECK_TAGS, raw_get)


def test_raw_uom(mocker: MockerFixture):
    MOCKED_RAW_DICT["case_insensitivity_tag_search"] = False
    MOCKED_RAW_DICT["display_uom"] = True
    _test_base_succeed(mocker, MOCKED_RAW_DICT, RAW_MOCKED_QUERY_DISPLAY_UOM, raw_get)


def test_raw_offset_limit(mocker: MockerFixture):
    MOCKED_RAW_DICT["case_insensitivity_tag_search"] = False
    MOCKED_RAW_DICT["display_uom"] = False
    MOCKED_RAW_DICT["offset"] = 10
    MOCKED_RAW_DICT["limit"] = 10
    _test_base_succeed(
        mocker,
        MOCKED_RAW_DICT,
        RAW_MOCKED_QUERY + MOCKED_QUERY_OFFSET_LIMIT,
        raw_get,
    )


def test_raw_fails(mocker: MockerFixture):
    _test_base_fails(mocker, MOCKED_PARAMETER_DICT, raw_get)


@pytest.mark.parametrize(
    "parameters, expected",
    [
        (
            {
                "source": "test_table",
                "start_date": "2022-01-01",
                "end_date": "2022-01-01",
                "tag_names": ["TestTag"],
                "include_bad_data": True,
            },
            {"count": 2},
        ),
        (
            {
                "source": "test_table",
                "start_date": "2022-01-01T00:00:00",
                "end_date": "2022-01-01T23:59:59",
                "tag_names": ["TestTag"],
                "include_bad_data": True,
            },
            {"count": 2},
        ),
        # Add more test cases as needed
    ],
)
def test_raw_query(spark_connection, parameters, expected):
    df = raw_get(spark_connection, parameters)
    assert df.columns == ["EventTime", "TagName", "Status", "Value"]
    assert df.count() == expected["count"]
