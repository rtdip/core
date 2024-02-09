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
from src.sdk.python.rtdip_sdk.queries.metadata import get as metadata_raw
from tests.sdk.python.rtdip_sdk.queries.time_series._test_base import (
    _test_base_succeed,
    _test_base_fails,
)
from tests.sdk.python.rtdip_sdk.queries._test_utils.sdk_test_objects import (
    MOCKED_PARAMETER_DICT,
    MOCKED_QUERY_OFFSET_LIMIT,
    METADATA_MOCKED_QUERY,
    METADATA_MOCKED_QUERY_CHECK_TAGS,
    METADATA_MOCKED_QUERY_NO_TAGS,
)

MOCKED_METADATA_PARAMETER_DICT = MOCKED_PARAMETER_DICT.copy()
MOCKED_METADATA_PARAMETER_DICT.pop("start_date")
MOCKED_METADATA_PARAMETER_DICT.pop("end_date")


def test_metadata(mocker: MockerFixture):
    _test_base_succeed(
        mocker, MOCKED_METADATA_PARAMETER_DICT, METADATA_MOCKED_QUERY, metadata_raw
    )


def test_metadata_check_tags(mocker: MockerFixture):
    MOCKED_METADATA_PARAMETER_DICT["case_insensitivity_tag_search"] = True
    _test_base_succeed(
        mocker,
        MOCKED_METADATA_PARAMETER_DICT,
        METADATA_MOCKED_QUERY_CHECK_TAGS,
        metadata_raw,
    )


def test_metadata_with_no_tag(mocker: MockerFixture):
    MOCKED_METADATA_PARAMETER_DICT["case_insensitivity_tag_search"] = False
    MOCKED_METADATA_PARAMETER_DICT.pop("tag_names")
    _test_base_succeed(
        mocker,
        MOCKED_METADATA_PARAMETER_DICT,
        METADATA_MOCKED_QUERY_NO_TAGS,
        metadata_raw,
    )


def test_metadata_offset_limit(mocker: MockerFixture):
    MOCKED_METADATA_PARAMETER_DICT["offset"] = 10
    MOCKED_METADATA_PARAMETER_DICT["limit"] = 10
    MOCKED_METADATA_PARAMETER_DICT["tag_names"] = ["mocked-TAGNAME"]
    _test_base_succeed(
        mocker,
        MOCKED_METADATA_PARAMETER_DICT,
        "SELECT * FROM `mocked-buiness-unit`.`sensors`.`mocked-asset_mocked-data-security-level_metadata`  WHERE `TagName` IN ('mocked-TAGNAME') ORDER BY `TagName` LIMIT 10 OFFSET 10 ",
        metadata_raw,
    )


def test_metadata_fails(mocker: MockerFixture):
    _test_base_fails(mocker, MOCKED_METADATA_PARAMETER_DICT, metadata_raw)
