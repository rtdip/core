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

import pytest
from pytest_mock import MockerFixture
import pandas as pd
from tests.api.v1.api_test_objects import METADATA_MOCKED_PARAMETER_DICT, METADATA_MOCKED_PARAMETER_ERROR_DICT, METADATA_POST_MOCKED_PARAMETER_DICT, METADATA_POST_BODY_MOCKED_PARAMETER_DICT, mocker_setup, TEST_HEADERS, BASE_URL
from httpx import AsyncClient
from src.api.v1 import app

MOCK_METHOD = "src.sdk.python.rtdip_sdk.queries.metadata.get"
MOCK_API_NAME = "/api/v1/metadata"
TEST_DATA = pd.DataFrame({"TagName": ["TestTag"], "UoM": ["UoM1"], "Description": ["Test Description"]})

pytestmark = pytest.mark.anyio

async def test_api_metadata_get_tags_provided_success(mocker: MockerFixture):
    mocker = mocker_setup(mocker, MOCK_METHOD, TEST_DATA)
    
    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.get(MOCK_API_NAME, headers=TEST_HEADERS, params=METADATA_MOCKED_PARAMETER_DICT)
    actual = response.text
    expected = TEST_DATA.to_json(orient="table", index=False)  

    assert response.status_code == 200
    assert actual == expected

async def test_api_metadata_get_no_tags_provided_success(mocker: MockerFixture):
    mocker = mocker_setup(mocker, MOCK_METHOD, TEST_DATA)
    
    METADATA_MOCKED_PARAMETER_NO_TAG_DICT = METADATA_MOCKED_PARAMETER_DICT.copy()
    METADATA_MOCKED_PARAMETER_NO_TAG_DICT.pop("tag_name")
    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.get(MOCK_API_NAME, headers=TEST_HEADERS, params=METADATA_MOCKED_PARAMETER_NO_TAG_DICT)
    actual = response.text
    expected = TEST_DATA.to_json(orient="table", index=False)  

    assert response.status_code == 200
    assert actual == expected

async def test_api_metadata_get_validation_error(mocker: MockerFixture):
    mocker = mocker_setup(mocker, MOCK_METHOD, TEST_DATA)  
    
    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.get(MOCK_API_NAME, headers=TEST_HEADERS, params=METADATA_MOCKED_PARAMETER_ERROR_DICT)
    actual = response.text  

    assert response.status_code == 422
    assert actual == '{"detail":[{"loc":["query","business_unit"],"msg":"field required","type":"value_error.missing"}]}'    

async def test_api_raw_get_error(mocker: MockerFixture):
    mocker = mocker_setup(mocker, MOCK_METHOD, TEST_DATA, Exception("Error Connecting to Database"))  
    
    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.get(MOCK_API_NAME, headers=TEST_HEADERS, params=METADATA_MOCKED_PARAMETER_DICT)
    actual = response.text

    assert response.status_code == 400
    assert actual == '{"detail":"Error Connecting to Database"}'

async def test_api_metadata_post_tags_provided_success(mocker: MockerFixture):
    mocker = mocker_setup(mocker, MOCK_METHOD, TEST_DATA)
    
    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.post(MOCK_API_NAME, headers=TEST_HEADERS, params=METADATA_POST_MOCKED_PARAMETER_DICT, json=METADATA_POST_BODY_MOCKED_PARAMETER_DICT)
    actual = response.text
    expected = TEST_DATA.to_json(orient="table", index=False)  

    assert response.status_code == 200
    assert actual == expected

async def test_api_metadata_post_no_tags_provided_error(mocker: MockerFixture):
    mocker = mocker_setup(mocker, MOCK_METHOD, TEST_DATA)
    
    METADATA_MOCKED_PARAMETER_NO_TAG_DICT = METADATA_MOCKED_PARAMETER_DICT.copy()
    METADATA_MOCKED_PARAMETER_NO_TAG_DICT.pop("tag_name")
    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.post(MOCK_API_NAME, headers=TEST_HEADERS, params=METADATA_MOCKED_PARAMETER_NO_TAG_DICT)
    actual = response.text

    assert response.status_code == 422
    assert actual == '{"detail":[{"loc":["body"],"msg":"field required","type":"value_error.missing"}]}'

async def test_api_metadata_post_validation_error(mocker: MockerFixture):
    mocker = mocker_setup(mocker, MOCK_METHOD, TEST_DATA)  
    
    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.post(MOCK_API_NAME, headers=TEST_HEADERS, params=METADATA_MOCKED_PARAMETER_ERROR_DICT, json=METADATA_POST_BODY_MOCKED_PARAMETER_DICT)
    actual = response.text  

    assert response.status_code == 422
    assert actual == '{"detail":[{"loc":["query","business_unit"],"msg":"field required","type":"value_error.missing"}]}'    

async def test_api_raw_post_error(mocker: MockerFixture):
    mocker = mocker_setup(mocker, MOCK_METHOD, TEST_DATA, Exception("Error Connecting to Database"))  
    
    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.post(MOCK_API_NAME, headers=TEST_HEADERS, params=METADATA_MOCKED_PARAMETER_DICT, json=METADATA_POST_BODY_MOCKED_PARAMETER_DICT)
    actual = response.text

    assert response.status_code == 400
    assert actual == '{"detail":"Error Connecting to Database"}'