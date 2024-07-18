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
import pandas as pd
import pyarrow as pa
import pytest
from pytest_mock import MockerFixture
from tests.sdk.python.rtdip_sdk.connectors.odbc.test_db_sql_connector import (
    MockedDBConnection,
    MockedCursor,
)
from src.sdk.python.rtdip_sdk.connectors import DatabricksSQLConnection
from src.sdk.python.rtdip_sdk.queries.time_series.summary import get as summary_get
from tests.sdk.python.rtdip_sdk.queries.time_series._test_base import (
    _test_base_succeed,
    _test_base_fails,
)
from tests.sdk.python.rtdip_sdk.queries._test_utils.sdk_test_objects import (
    MOCKED_QUERY_OFFSET_LIMIT,
    MOCKED_PARAMETER_DICT,
    SUMMARY_MOCKED_QUERY,
    SUMMARY_MOCKED_QUERY_CHECK_TAGS,
    SUMMARY_MOCKED_QUERY_UOM,
)

MOCKED_SUMMARY_DICT = MOCKED_PARAMETER_DICT.copy()


def test_summary_get(mocker: MockerFixture):
    _test_base_succeed(
        mocker,
        MOCKED_SUMMARY_DICT,
        SUMMARY_MOCKED_QUERY,
        summary_get,
    )


def test_summary_get_check_tags(mocker: MockerFixture):
    MOCKED_SUMMARY_DICT["case_insensitivity_tag_search"] = True
    _test_base_succeed(
        mocker,
        MOCKED_SUMMARY_DICT,
        SUMMARY_MOCKED_QUERY_CHECK_TAGS,
        summary_get,
    )


def test_summary_uom(mocker: MockerFixture):
    MOCKED_SUMMARY_DICT["case_insensitivity_tag_search"] = False
    MOCKED_SUMMARY_DICT["display_uom"] = True
    _test_base_succeed(
        mocker,
        MOCKED_SUMMARY_DICT,
        SUMMARY_MOCKED_QUERY_UOM,
        summary_get,
    )


def test_summary_offset_limit(mocker: MockerFixture):
    MOCKED_SUMMARY_DICT["display_uom"] = False
    MOCKED_SUMMARY_DICT["offset"] = 10
    MOCKED_SUMMARY_DICT["limit"] = 10
    _test_base_succeed(
        mocker,
        MOCKED_SUMMARY_DICT,
        SUMMARY_MOCKED_QUERY + MOCKED_QUERY_OFFSET_LIMIT,
        summary_get,
    )


def test_summary_fails(mocker: MockerFixture):
    _test_base_fails(
        mocker,
        MOCKED_PARAMETER_DICT,
        summary_get,
    )
