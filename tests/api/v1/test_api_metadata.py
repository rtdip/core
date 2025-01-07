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
import pandas as pd
from tests.api.v1.api_test_objects import (
    METADATA_MOCKED_PARAMETER_DICT,
    METADATA_MOCKED_PARAMETER_ERROR_DICT,
    METADATA_POST_MOCKED_PARAMETER_DICT,
    METADATA_POST_BODY_MOCKED_PARAMETER_DICT,
    mocker_setup,
    TEST_HEADERS,
    BASE_URL,
    MOCK_TAG_MAPPING_SINGLE,
    MOCK_TAG_MAPPING_EMPTY,
    MOCK_MAPPING_ENDPOINT_URL,
)
from httpx import AsyncClient, ASGITransport
from src.api.v1 import app

MOCK_METHOD = "src.sdk.python.rtdip_sdk.queries.metadata.get"
MOCK_API_NAME = "/api/v1/metadata"
TEST_DATA = pd.DataFrame(
    {"TagName": ["TestTag"], "UoM": ["UoM1"], "Description": ["Test Description"]}
)

pytestmark = pytest.mark.anyio


async def test_api_metadata_get_tags_provided_success(
    mocker: MockerFixture, api_test_data
):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_metadata"])

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME, headers=TEST_HEADERS, params=METADATA_MOCKED_PARAMETER_DICT
        )
    actual = response.text

    assert response.status_code == 200
    assert actual == api_test_data["expected_metadata"]


async def test_api_metadata_get_no_tags_provided_success(
    mocker: MockerFixture, api_test_data
):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_metadata"])

    METADATA_MOCKED_PARAMETER_NO_TAG_DICT = METADATA_MOCKED_PARAMETER_DICT.copy()
    METADATA_MOCKED_PARAMETER_NO_TAG_DICT.pop("tag_name")
    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=METADATA_MOCKED_PARAMETER_NO_TAG_DICT,
        )
    actual = response.text

    assert response.status_code == 200
    assert actual == api_test_data["expected_metadata"]


async def test_api_metadata_get_validation_error(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_metadata"])

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=METADATA_MOCKED_PARAMETER_ERROR_DICT,
        )
    actual = response.text

    assert response.status_code == 422
    assert (
        actual
        == '{"detail":[{"type":"missing","loc":["query","business_unit"],"msg":"Field required","input":null}]}'
    )


async def test_api_metadata_get_error(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(
        mocker,
        MOCK_METHOD,
        api_test_data["mock_data_metadata"],
        Exception("Error Connecting to Database"),
    )

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME, headers=TEST_HEADERS, params=METADATA_MOCKED_PARAMETER_DICT
        )
    actual = response.text

    assert response.status_code == 400
    assert actual == '{"detail":"Error Connecting to Database"}'


async def test_api_metadata_post_tags_provided_success(
    mocker: MockerFixture, api_test_data
):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_metadata"])

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=METADATA_POST_MOCKED_PARAMETER_DICT,
            json=METADATA_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 200
    assert actual == api_test_data["expected_metadata"]


async def test_api_metadata_post_no_tags_provided_error(
    mocker: MockerFixture, api_test_data
):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_metadata"])

    METADATA_MOCKED_PARAMETER_NO_TAG_DICT = METADATA_MOCKED_PARAMETER_DICT.copy()
    METADATA_MOCKED_PARAMETER_NO_TAG_DICT.pop("tag_name")
    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=METADATA_MOCKED_PARAMETER_NO_TAG_DICT,
        )
    actual = response.text

    assert response.status_code == 422
    assert (
        actual
        == '{"detail":[{"type":"missing","loc":["body"],"msg":"Field required","input":null}]}'
    )


async def test_api_metadata_post_validation_error(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_metadata"])

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=METADATA_MOCKED_PARAMETER_ERROR_DICT,
            json=METADATA_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 422
    assert (
        actual
        == '{"detail":[{"type":"missing","loc":["query","business_unit"],"msg":"Field required","input":null}]}'
    )


async def test_api_metadata_post_error(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(
        mocker,
        MOCK_METHOD,
        api_test_data["mock_data_metadata"],
        Exception("Error Connecting to Database"),
    )

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=METADATA_MOCKED_PARAMETER_DICT,
            json=METADATA_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 400
    assert actual == '{"detail":"Error Connecting to Database"}'


async def test_api_metadata_get_lookup_success(mocker: MockerFixture):
    """
    Case when no business_unit, asset etc supplied so instead invokes tag lookup
    """

    # Mock the batch method, which outputs test data in the form of an array of dfs
    mock_method = "src.sdk.python.rtdip_sdk.queries.time_series.batch.get"
    mock_method_return_data = [TEST_DATA]
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
    modified_param_dict = METADATA_MOCKED_PARAMETER_DICT.copy()
    del modified_param_dict["business_unit"]

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        actual = await ac.get(
            MOCK_API_NAME, headers=TEST_HEADERS, params=modified_param_dict
        )

    expected = TEST_DATA.to_json(orient="table", index=False, date_unit="ns")
    expected = (
        expected.replace(',"tz":"UTC"', "").rstrip("}")
        + ',"pagination":{"limit":null,"offset":null,"next":null}}'
    )

    assert actual.text == expected
    assert actual.status_code == 200


async def test_api_metadata_post_lookup_success(mocker: MockerFixture):
    """
    Case when no business_unit, asset etc supplied so instead invokes tag lookup
    """

    # Mock the batch method, which outputs test data in the form of an array of dfs
    mock_method = "src.sdk.python.rtdip_sdk.queries.time_series.batch.get"
    mock_method_return_data = [TEST_DATA]
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
    modified_param_dict = METADATA_MOCKED_PARAMETER_DICT.copy()
    del modified_param_dict["business_unit"]

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        actual = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=modified_param_dict,
            json=METADATA_POST_BODY_MOCKED_PARAMETER_DICT,
        )

    expected = TEST_DATA.to_json(orient="table", index=False, date_unit="ns")
    expected = (
        expected.replace(',"tz":"UTC"', "").rstrip("}")
        + ',"pagination":{"limit":null,"offset":null,"next":null}}'
    )

    assert actual.text == expected
    assert actual.status_code == 200


async def test_api_metadata_get_lookup_no_tag_map_error(mocker: MockerFixture):
    """
    Case when no business_unit, asset etc supplied so instead invokes tag lookup
    """

    # Mock the batch method, which outputs test data in the form of an array of dfs
    mock_method = "src.sdk.python.rtdip_sdk.queries.time_series.batch.get"
    mock_method_return_data = [TEST_DATA]
    mocker = mocker_setup(
        mocker,
        mock_method,
        mock_method_return_data,
        tag_mapping_data=MOCK_TAG_MAPPING_EMPTY,
    )
    mocker.patch.dict(
        os.environ, {"DATABRICKS_SERVING_ENDPOINT": MOCK_MAPPING_ENDPOINT_URL}
    )

    # Remove parameters so that runs lookup
    modified_param_dict = METADATA_MOCKED_PARAMETER_DICT.copy()
    modified_param_dict["tagname"] = ["NonExistentTag"]
    del modified_param_dict["business_unit"]

    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        actual = await ac.get(
            MOCK_API_NAME, headers=TEST_HEADERS, params=modified_param_dict
        )

    expected = '{"detail":"One or more tags do not have tables associated with them, the data belongs to a confidential table, or you do not have access. If the tag belongs to a confidential table and you do have access, please supply the business_unit, asset, data_security_level and data_type"}'

    assert actual.text == expected
    assert actual.status_code == 400
