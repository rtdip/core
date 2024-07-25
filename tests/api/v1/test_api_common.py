# Copyright 2024 RTDIP
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

import pytest
from unittest.mock import patch
import json
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from src.api.v1.common import (
    lookup_before_get,
    query_mapping_endpoint,
    split_table_name,
    concatenate_dfs_and_order,
    json_response_batch,
)
from src.sdk.python.rtdip_sdk.connectors import DatabricksSQLConnection
from src.sdk.python.rtdip_sdk.queries.time_series import raw

from tests.api.v1.api_test_objects import (
    RAW_MOCKED_PARAMETER_DICT,
    MOCK_MAPPING_ENDPOINT_URL,
    MOCK_TAG_MAPPING_SINGLE,
    MOCK_TAG_MAPPING_MULTIPLE,
    MOCK_TAG_MAPPING_EMPTY,
    mocker_setup,
)

###############################
# Mocker set-ups
###############################
MOCK_METHOD = "src.sdk.python.rtdip_sdk.queries.time_series.raw.get"
MOCK_BATCH_METHOD = "src.sdk.python.rtdip_sdk.queries.time_series.batch.get"


###############################
# Tests for lookup_before_get
###############################
def test_api_lookup_before_get(mocker):
    # parameters dict
    test_parameters = RAW_MOCKED_PARAMETER_DICT
    test_parameters["tag_names"] = ["Tagname1", "Tagname2", "Tagname3"]

    # Mock get, but for each time called provides next result
    mock_get_data = [
        # Two tags in one table
        pd.DataFrame(
            {
                "EventTime": [
                    RAW_MOCKED_PARAMETER_DICT["start_date"],
                    RAW_MOCKED_PARAMETER_DICT["start_date"],
                ],
                "TagName": ["Tagname1", "Tagname2"],
                "Status": ["Good", "Good"],
                "Value": [1.01, 2.02],
            }
        ),
        # One tag in another
        pd.DataFrame(
            {
                "EventTime": [RAW_MOCKED_PARAMETER_DICT["end_date"]],
                "TagName": ["Tagname3"],
                "Status": ["Good"],
                "Value": [3.03],
            }
        ),
    ]

    # Set-up mocker
    mocker = mocker_setup(
        mocker,
        MOCK_METHOD,
        mock_get_data,
        patch_side_effect=mock_get_data,
        tag_mapping_data=MOCK_TAG_MAPPING_MULTIPLE,
    )
    mocker.patch(MOCK_BATCH_METHOD, return_value=mock_get_data)

    # Get result from lookup_before_get function
    connection = DatabricksSQLConnection(
        access_token="token", server_hostname="test", http_path="test"
    )
    actual = lookup_before_get("raw", connection, test_parameters)

    # Define expected result
    expected = pd.DataFrame(
        {
            "EventTime": [
                RAW_MOCKED_PARAMETER_DICT["start_date"],
                RAW_MOCKED_PARAMETER_DICT["start_date"],
                RAW_MOCKED_PARAMETER_DICT["end_date"],
            ],
            "TagName": ["Tagname1", "Tagname2", "Tagname3"],
            "Status": ["Good", "Good", "Good"],
            "Value": [1.01, 2.02, 3.03],
        }
    )

    # Assert equality
    pd.testing.assert_frame_equal(actual, expected, check_dtype=True)


###############################
# Tests for query_mapping_endpoint
###############################


def test_api_common_query_mapping_endpoint(mocker):
    # Set-up mocker
    mocker_setup(
        mocker, MOCK_METHOD, test_data={}, tag_mapping_data=MOCK_TAG_MAPPING_MULTIPLE
    )

    # Run the function
    tags = ["Tagname1", "Tagname2"]
    connection = DatabricksSQLConnection(
        access_token="token", server_hostname="test", http_path="test"
    )
    actual = query_mapping_endpoint(
        tags, MOCK_MAPPING_ENDPOINT_URL, connection=connection
    )

    expected = {
        "rtdip.sensors.asset1_restricted_events_float": ["Tagname1", "Tagname2"],
        "rtdip.sensors.asset2_restricted_events_integer": ["Tagname3"],
    }

    assert actual == expected


###############################
# Tests for splitTablename
###############################
def test_api_common_split_table_name():
    """Tests for splitting table name into dict of business_unit, asset etc"""

    actual_with_expected_format = split_table_name(
        "test.sensors.asset_restricted_events_float"
    )
    expected_with_expected_format = {
        "business_unit": "test",
        "asset": "asset",
        "data_security_level": "restricted",
        "data_type": "float",
    }

    with pytest.raises(Exception) as actual_with_incorrect_format_missing:
        split_table_name("test")

    with pytest.raises(Exception) as actual_with_incorrect_schema:
        split_table_name("test.schema.asset_restricted_events_float")

    expected_with_incorrect_format_message = "Unsupported table name format supplied. Please use the format 'businessunit.schema.asset.datasecurityevel_events_datatype"

    assert actual_with_expected_format == expected_with_expected_format
    assert (
        actual_with_incorrect_format_missing.value.args[0]
        == expected_with_incorrect_format_message
    )
    assert (
        actual_with_incorrect_schema.value.args[0]
        == expected_with_incorrect_format_message
    )


###############################
# Tests for concatenate_dfs_and_order
###############################

test_df1 = pd.DataFrame(
    {
        "EventTime": [
            "01/01/2024 14:00",
            "01/01/2024 15:00",
        ],
        "TagName": ["TestTag1", "TestTag2"],
        "Status": ["Good", "Good"],
        "Value": [1.01, 2.02],
    }
)

test_df2 = pd.DataFrame(
    {
        "EventTime": ["01/01/2024 14:00"],
        "TagName": ["TestTag3"],
        "Status": ["Good"],
        "Value": [3.03],
    }
)

test_df3_pivoted = pd.DataFrame(
    {
        "EventTime": [
            "01/01/2024 14:00",
            "01/01/2024 15:00",
        ],
        "TestTag1": [1.01, 5.05],
        "TestTag2": [2.02, 6.05],
    }
)

test_df4_pivoted = pd.DataFrame(
    {
        "EventTime": ["01/01/2024 14:00", "01/01/2024 15:00"],
        "TestTag3": [4.04, 7.07],
    }
)


def test_api_common_concatenate_dfs_and_order_unpivoted():
    """Tests unpivoted concatenation of dfs"""

    actual = concatenate_dfs_and_order(
        dfs_arr=[test_df1, test_df2],
        tags=["TestTag1", "TestTag2", "TestTag3"],
        pivot=False,
    )

    expected = pd.DataFrame(
        {
            "EventTime": ["01/01/2024 14:00", "01/01/2024 15:00", "01/01/2024 14:00"],
            "TagName": ["TestTag1", "TestTag2", "TestTag3"],
            "Status": ["Good", "Good", "Good"],
            "Value": [1.01, 2.02, 3.03],
        }
    )

    pd.testing.assert_frame_equal(actual, expected, check_dtype=True)


def test_api_common_concatenate_dfs_and_order_pivoted():
    """Tests pivoted concatenation of dfs, which adds columns"""

    actual = concatenate_dfs_and_order(
        dfs_arr=[test_df3_pivoted, test_df4_pivoted],
        tags=["TestTag1", "TestTag2", "TestTag3"],
        pivot=True,
    )

    expected = pd.DataFrame(
        {
            "EventTime": ["01/01/2024 14:00", "01/01/2024 15:00"],
            "TestTag1": [1.01, 5.05],
            "TestTag2": [2.02, 6.05],
            "TestTag3": [4.04, 7.07],
        }
    )

    pd.testing.assert_frame_equal(actual, expected, check_dtype=True)


def test_api_common_concatenate_dfs_and_order_pivoted_ordering():
    """Tests pivoted concatenation of dfs, with specific tag ordering"""

    actual = concatenate_dfs_and_order(
        dfs_arr=[test_df3_pivoted, test_df4_pivoted],
        tags=["TestTag2", "TestTag1", "TestTag3"],
        pivot=True,
    )

    expected = pd.DataFrame(
        {
            "EventTime": ["01/01/2024 14:00", "01/01/2024 15:00"],
            "TestTag2": [2.02, 6.05],
            "TestTag1": [1.01, 5.05],
            "TestTag3": [4.04, 7.07],
        }
    )

    pd.testing.assert_frame_equal(actual, expected, check_dtype=True)


###############################
# Tests for json_response_batch
###############################
def test_api_common_json_response_batch():
    """Tests that should correctly combine list of dfs into a json response"""

    summary_test_data = pd.DataFrame(
        {
            "TagName": ["TestTag"],
            "Count": [10.0],
            "Avg": [5.05],
            "Min": [1.0],
            "Max": [10.0],
            "StDev": [3.02],
            "Sum": [25.0],
            "Var": [0.0],
        }
    )

    raw_test_data = pd.DataFrame(
        {
            "EventTime": ["2024-06-27T15:35", "2024-06-27T15:45"],
            "TagName": ["TestTag", "TestTag"],
            "Status": ["Good", "Good"],
            "Value": [1.01, 5.55],
        }
    )

    actual = json_response_batch([summary_test_data, raw_test_data])

    expected = {
        "data": [
            {
                "schema": {
                    "fields": [
                        {"name": "TagName", "type": "string"},
                        {"name": "Count", "type": "number"},
                        {"name": "Avg", "type": "number"},
                        {"name": "Min", "type": "number"},
                        {"name": "Max", "type": "number"},
                        {"name": "StDev", "type": "number"},
                        {"name": "Sum", "type": "number"},
                        {"name": "Var", "type": "number"},
                    ],
                    "primaryKey": False,
                    "pandas_version": "1.4.0",
                },
                "data": [
                    {
                        "TagName": "TestTag",
                        "Count": 10.0,
                        "Avg": 5.05,
                        "Min": 1.0,
                        "Max": 10.0,
                        "StDev": 3.02,
                        "Sum": 25.0,
                        "Var": 0.0,
                    }
                ],
            },
            {
                "schema": {
                    "fields": [
                        {"name": "EventTime", "type": "string"},
                        {"name": "TagName", "type": "string"},
                        {"name": "Status", "type": "string"},
                        {"name": "Value", "type": "number"},
                    ],
                    "primaryKey": False,
                    "pandas_version": "1.4.0",
                },
                "data": [
                    {
                        "EventTime": "2024-06-27T15:35",
                        "TagName": "TestTag",
                        "Status": "Good",
                        "Value": 1.01,
                    },
                    {
                        "EventTime": "2024-06-27T15:45",
                        "TagName": "TestTag",
                        "Status": "Good",
                        "Value": 5.55,
                    },
                ],
            },
        ]
    }
    assert json.loads(actual.body) == expected
