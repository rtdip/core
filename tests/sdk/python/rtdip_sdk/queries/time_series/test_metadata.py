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
from src.sdk.python.rtdip_sdk.queries.metadata import get as metadata_raw
from tests.sdk.python.rtdip_sdk.queries.time_series._test_base import (
    _test_base_succeed,
    _test_base_fails,
)
from tests.sdk.python.rtdip_sdk.queries._test_utils.sdk_test_objects import (
    MOCKED_PARAMETER_DICT,
    MOCKED_QUERY_OFFSET_LIMIT,
    METADATA_MOCKED_QUERY,
    METADATA_MOCKED_QUERY_PIVOT,
    METADATA_MOCKED_QUERY_NO_TAGS,
)

MOCKED_METADATA_PARAMETER_DICT = MOCKED_PARAMETER_DICT.copy()


def test_metadata(mocker: MockerFixture):
    _test_base_succeed(
        mocker, MOCKED_METADATA_PARAMETER_DICT, METADATA_MOCKED_QUERY, metadata_raw
    )


def test_metadata_offset_limit(mocker: MockerFixture):
    MOCKED_METADATA_PARAMETER_DICT["offset"] = 10
    MOCKED_METADATA_PARAMETER_DICT["limit"] = 10
    _test_base_succeed(
        mocker,
        MOCKED_METADATA_PARAMETER_DICT,
        METADATA_MOCKED_QUERY + MOCKED_QUERY_OFFSET_LIMIT,
        metadata_raw,
    )


def test_no_tag_metadata(mocker: MockerFixture):
    MOCKED_METADATA_PARAMETER_DICT.pop("tag_names")
    _test_base_succeed(
        mocker,
        MOCKED_METADATA_PARAMETER_DICT,
        METADATA_MOCKED_QUERY_NO_TAGS,
        metadata_raw,
    )


def test_metadata_fails(mocker: MockerFixture):
    _test_base_fails(mocker, MOCKED_METADATA_PARAMETER_DICT, metadata_raw)
