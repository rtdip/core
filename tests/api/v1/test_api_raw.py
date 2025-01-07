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
import pytest
from pytest_mock import MockerFixture
from tests.api.v1.api_test_objects import (
    RAW_MOCKED_PARAMETER_DICT,
    RAW_MOCKED_PARAMETER_ERROR_DICT,
    RAW_POST_MOCKED_PARAMETER_DICT,
    RAW_POST_BODY_MOCKED_PARAMETER_DICT,
    mocker_setup,
    TEST_HEADERS,
    BASE_URL,
    MOCK_TAG_MAPPING_SINGLE,
    MOCK_TAG_MAPPING_EMPTY,
    MOCK_MAPPING_ENDPOINT_URL,
)
from pandas.io.json import build_table_schema
import pandas as pd
from httpx import AsyncClient, ASGITransport
from src.api.v1 import app

MOCK_METHOD = "src.sdk.python.rtdip_sdk.queries.time_series.raw.get"
MOCK_API_NAME = "/api/v1/events/raw"

pytestmark = pytest.mark.anyio


async def test_api_raw_get_success(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_raw"])

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME, headers=TEST_HEADERS, params=RAW_MOCKED_PARAMETER_DICT
        )
    actual = response.text

    assert response.status_code == 200
    assert actual == api_test_data["expected_raw"]


async def test_api_raw_get_validation_error(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_raw"])

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME, headers=TEST_HEADERS, params=RAW_MOCKED_PARAMETER_ERROR_DICT
        )
    actual = response.text

    assert response.status_code == 422
    assert (
        actual
        == '{"detail":[{"type":"missing","loc":["query","start_date"],"msg":"Field required","input":null}]}'
    )


async def test_api_raw_get_error(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(
        mocker,
        MOCK_METHOD,
        api_test_data["mock_data_raw"],
        Exception("Error Connecting to Database"),
    )

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME, headers=TEST_HEADERS, params=RAW_MOCKED_PARAMETER_DICT
        )
    actual = response.text

    assert response.status_code == 400
    assert actual == '{"detail":"Error Connecting to Database"}'


async def test_api_raw_post_success(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_raw"])

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=RAW_POST_MOCKED_PARAMETER_DICT,
            json=RAW_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 200
    assert actual == api_test_data["expected_raw"]


async def test_api_raw_post_validation_error(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_raw"])

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=RAW_MOCKED_PARAMETER_ERROR_DICT,
            json=RAW_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 422
    assert (
        actual
        == '{"detail":[{"type":"missing","loc":["query","start_date"],"msg":"Field required","input":null}]}'
    )


async def test_api_raw_post_error(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(
        mocker,
        MOCK_METHOD,
        api_test_data["mock_data_raw"],
        Exception("Error Connecting to Database"),
    )

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=RAW_MOCKED_PARAMETER_DICT,
            json=RAW_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 400
    assert actual == '{"detail":"Error Connecting to Database"}'


async def test_api_raw_get_lookup_success(mocker: MockerFixture, api_test_data):
    """
    Case when no business_unit, asset etc supplied so instead invokes tag lookup
    """

    test_data = pd.DataFrame(
        {
            "EventTime": [RAW_MOCKED_PARAMETER_DICT["start_date"]],
            "TagName": ["Tagname1"],
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

    # Remove parameters so that runs lookup
    modified_param_dict = RAW_MOCKED_PARAMETER_DICT.copy()
    del modified_param_dict["business_unit"]

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        actual = await ac.get(
            MOCK_API_NAME, headers=TEST_HEADERS, params=modified_param_dict
        )

    expected = test_data.to_json(orient="table", index=False, date_unit="ns")
    expected = (
        expected.rstrip("}") + ',"pagination":{"limit":null,"offset":null,"next":null}}'
    )

    assert actual.text == expected
    assert actual.status_code == 200


async def test_api_raw_post_lookup_success(mocker: MockerFixture):
    """
    Case when no business_unit, asset etc supplied so instead invokes tag lookup
    """

    test_data = pd.DataFrame(
        {
            "EventTime": [RAW_MOCKED_PARAMETER_DICT["start_date"]],
            "TagName": ["Tagname1"],
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

    # Remove parameters so that runs lookup
    modified_param_dict = RAW_POST_MOCKED_PARAMETER_DICT.copy()
    del modified_param_dict["business_unit"]

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        actual = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=modified_param_dict,
            json=RAW_POST_BODY_MOCKED_PARAMETER_DICT,
        )

    expected = test_data.to_json(orient="table", index=False, date_unit="ns")
    expected = (
        expected.rstrip("}") + ',"pagination":{"limit":null,"offset":null,"next":null}}'
    )

    assert actual.text == expected
    assert actual.status_code == 200


async def test_api_raw_get_lookup_no_tag_map_error(mocker: MockerFixture):
    """
    Case when no business_unit, asset etc supplied so instead invokes tag lookup
    AND there is no table associated with the tag which results in error.
    """

    test_data = pd.DataFrame(
        {
            "EventTime": [RAW_MOCKED_PARAMETER_DICT["start_date"]],
            "TagName": ["Tagname1"],
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
        tag_mapping_data=MOCK_TAG_MAPPING_EMPTY,
    )
    mocker.patch.dict(
        os.environ, {"DATABRICKS_SERVING_ENDPOINT": MOCK_MAPPING_ENDPOINT_URL}
    )

    # Remove parameters so that runs lookup, and add tag that does not exist
    modified_param_dict = RAW_MOCKED_PARAMETER_DICT.copy()
    modified_param_dict["tagname"] = ["NonExistentTag"]
    del modified_param_dict["business_unit"]

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        actual = await ac.get(
            MOCK_API_NAME, headers=TEST_HEADERS, params=modified_param_dict
        )

    expected = '{"detail":"One or more tags do not have tables associated with them, the data belongs to a confidential table, or you do not have access. If the tag belongs to a confidential table and you do have access, please supply the business_unit, asset, data_security_level and data_type"}'

    assert actual.text == expected
    assert actual.status_code == 400
