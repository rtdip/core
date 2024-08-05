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

import os
import json
import pytest
from pytest_mock import MockerFixture
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from tests.api.v1.api_test_objects import (
    BATCH_MOCKED_PARAMETER_DICT,
    BATCH_POST_PAYLOAD_SINGLE_WITH_GET,
    BATCH_POST_PAYLOAD_SINGLE_WITH_POST,
    BATCH_POST_PAYLOAD_SINGLE_WITH_GET_ERROR_DICT,
    BATCH_POST_PAYLOAD_SINGLE_WITH_POST_ERROR_DICT,
    BATCH_POST_PAYLOAD_MULTIPLE,
    mocker_setup,
    TEST_HEADERS,
    BASE_URL,
    MOCK_TAG_MAPPING_SINGLE,
    MOCK_MAPPING_ENDPOINT_URL,
)
from src.api.v1.models import (
    RawResponse,
)
from pandas.io.json import build_table_schema
from httpx import AsyncClient
from src.api.v1 import app
from src.api.v1.common import json_response_batch

MOCK_METHOD = "src.sdk.python.rtdip_sdk.queries.time_series.raw.get"
MOCK_API_NAME = "/api/v1/events/batch"

pytestmark = pytest.mark.anyio


async def test_api_batch_single_get_success(mocker: MockerFixture):
    """
    Case when single get request supplied in array of correct format
    """

    test_data = pd.DataFrame(
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

    # Mock the batch method, which outputs test data in the form of an array of dfs
    mock_method = "src.sdk.python.rtdip_sdk.queries.time_series.batch.get"
    mock_method_return_data = [test_data]
    mocker = mocker_setup(
        mocker,
        mock_method,
        mock_method_return_data,
        tag_mapping_data=MOCK_TAG_MAPPING_SINGLE,
    )
    mocker.patch.dict(
        os.environ, {"DATABRICKS_SERVING_ENDPOINT": MOCK_MAPPING_ENDPOINT_URL}
    )

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        actual = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=BATCH_MOCKED_PARAMETER_DICT,
            json=BATCH_POST_PAYLOAD_SINGLE_WITH_GET,
        )

    # Define full expected structure for one test - for remainder use json_response_batch as already tested in common
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
            }
        ]
    }

    assert actual.json() == expected
    assert actual.status_code == 200


async def test_api_batch_single_post_success(mocker: MockerFixture):
    """
    Case when single post request supplied in array of correct format
    """

    test_data = pd.DataFrame(
        {
            "EventTime": [datetime.now(timezone.utc)],
            "TagName": ["TestTag"],
            "Status": ["Good"],
            "Value": [1.01],
        }
    )

    # Mock the batch method, which outputs test data in the form of an array of dfs
    mock_method = "src.sdk.python.rtdip_sdk.queries.time_series.batch.get"
    mock_method_return_data = [test_data]
    mocker = mocker_setup(
        mocker,
        mock_method,
        mock_method_return_data,
        tag_mapping_data=MOCK_TAG_MAPPING_SINGLE,
    )
    mocker.patch.dict(
        os.environ, {"DATABRICKS_SERVING_ENDPOINT": MOCK_MAPPING_ENDPOINT_URL}
    )

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        actual = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=BATCH_MOCKED_PARAMETER_DICT,
            json=BATCH_POST_PAYLOAD_SINGLE_WITH_POST,
        )

    expected = json.loads(json_response_batch([test_data]).body.decode("utf-8"))

    assert actual.json() == expected
    assert actual.status_code == 200


async def test_api_batch_single_get_unsupported_route_error(mocker: MockerFixture):
    """
    Case when single post request supplied but route not supported
    """

    test_data = pd.DataFrame(
        {
            "EventTime": [datetime.now(timezone.utc)],
            "TagName": ["TestTag"],
            "Status": ["Good"],
            "Value": [1.01],
        }
    )

    # Mock the batch method, which outputs test data in the form of an array of dfs
    mock_method = "src.sdk.python.rtdip_sdk.queries.time_series.batch.get"
    mock_method_return_data = [test_data]
    mocker = mocker_setup(
        mocker,
        mock_method,
        mock_method_return_data,
        tag_mapping_data=MOCK_TAG_MAPPING_SINGLE,
    )
    mocker.patch.dict(
        os.environ, {"DATABRICKS_SERVING_ENDPOINT": MOCK_MAPPING_ENDPOINT_URL}
    )

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        actual = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=BATCH_MOCKED_PARAMETER_DICT,
            json=BATCH_POST_PAYLOAD_SINGLE_WITH_GET_ERROR_DICT,
        )

    expected = {
        "detail": "Unsupported url: Only relative base urls are supported. Please provide any parameters in the params key"
    }

    assert actual.json() == expected
    assert actual.status_code == 400


async def test_api_batch_single_post_missing_body_error(mocker: MockerFixture):
    """
    Case when single post request supplied in array of incorrect format (missing payload)
    """

    test_data = pd.DataFrame(
        {
            "EventTime": [datetime.now(timezone.utc)],
            "TagName": ["TestTag"],
            "Status": ["Good"],
            "Value": [1.01],
        }
    )

    # Mock the batch method, which outputs test data in the form of an array of dfs
    mock_method = "src.sdk.python.rtdip_sdk.queries.time_series.batch.get"
    mock_method_return_data = [test_data]
    mocker = mocker_setup(
        mocker,
        mock_method,
        mock_method_return_data,
        tag_mapping_data=MOCK_TAG_MAPPING_SINGLE,
    )
    mocker.patch.dict(
        os.environ, {"DATABRICKS_SERVING_ENDPOINT": MOCK_MAPPING_ENDPOINT_URL}
    )

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        actual = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=BATCH_MOCKED_PARAMETER_DICT,
            json=BATCH_POST_PAYLOAD_SINGLE_WITH_POST_ERROR_DICT,
        )

    expected = {
        "detail": "Incorrectly formatted request provided: All POST requests require a body"
    }

    assert actual.json() == expected
    assert actual.status_code == 400


async def test_api_batch_multiple_success(mocker: MockerFixture):
    """
    Case when single post request supplied in array of correct format
    """

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
            "EventTime": [datetime.now(timezone.utc)],
            "TagName": ["TestTag"],
            "Status": ["Good"],
            "Value": [1.01],
        }
    )

    # Mock the batch method, which outputs test data in the form of an array of dfs
    mock_method = "src.sdk.python.rtdip_sdk.queries.time_series.batch.get"
    mock_method_return_data = None
    # add side effect since require batch to return different data after each call
    # batch.get return value is array of dfs, so must patch with nested array
    mock_patch_side_effect = [[summary_test_data], [raw_test_data]]
    mocker = mocker_setup(
        mocker,
        mock_method,
        mock_method_return_data,
        patch_side_effect=mock_patch_side_effect,
        tag_mapping_data=MOCK_TAG_MAPPING_SINGLE,
    )
    mocker.patch.dict(
        os.environ, {"DATABRICKS_SERVING_ENDPOINT": MOCK_MAPPING_ENDPOINT_URL}
    )

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        actual = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=BATCH_MOCKED_PARAMETER_DICT,
            json=BATCH_POST_PAYLOAD_MULTIPLE,
        )

    expected = json.loads(
        json_response_batch([summary_test_data, raw_test_data]).body.decode("utf-8")
    )

    assert actual.json() == expected
    assert actual.status_code == 200
