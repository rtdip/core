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
from tests.api.v1.api_test_objects import (
    SUMMARY_MOCKED_PARAMETER_DICT,
    SUMMARY_MOCKED_PARAMETER_ERROR_DICT,
    SUMMARY_POST_MOCKED_PARAMETER_DICT,
    SUMMARY_POST_BODY_MOCKED_PARAMETER_DICT,
    mocker_setup,
    TEST_HEADERS,
    BASE_URL,
)
from httpx import AsyncClient, ASGITransport
from src.api.v1 import app
import json

MOCK_METHOD = "src.sdk.python.rtdip_sdk.queries.time_series.summary.get"
MOCK_API_NAME = "/api/v1/events/summary"

pytestmark = pytest.mark.anyio


async def test_api_summary_get_success(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_summary"])

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME, headers=TEST_HEADERS, params=SUMMARY_MOCKED_PARAMETER_DICT
        )
    actual = response.text

    assert response.status_code == 200
    assert actual == api_test_data["expected_summary"]


async def test_api_summary_get_validation_error(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_summary"])

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=SUMMARY_MOCKED_PARAMETER_ERROR_DICT,
        )
    actual = response.text

    assert response.status_code == 422
    assert (
        actual
        == '{"detail":[{"type":"missing","loc":["query","start_date"],"msg":"Field required","input":null}]}'
    )


async def test_api_summary_get_error(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(
        mocker,
        MOCK_METHOD,
        api_test_data["mock_data_summary"],
        Exception("Error Connecting to Database"),
    )

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME, headers=TEST_HEADERS, params=SUMMARY_MOCKED_PARAMETER_DICT
        )
    actual = response.text

    assert response.status_code == 400
    assert actual == '{"detail":"Error Connecting to Database"}'


async def test_api_summary_post_success(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_summary"])

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=SUMMARY_POST_MOCKED_PARAMETER_DICT,
            json=SUMMARY_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 200
    assert actual == api_test_data["expected_summary"]


async def test_api_summary_post_validation_error(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_summary"])

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=SUMMARY_MOCKED_PARAMETER_ERROR_DICT,
            json=SUMMARY_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 422
    assert (
        actual
        == '{"detail":[{"type":"missing","loc":["query","start_date"],"msg":"Field required","input":null}]}'
    )


async def test_api_summary_post_error(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(
        mocker,
        MOCK_METHOD,
        api_test_data["mock_data_summary"],
        Exception("Error Connecting to Database"),
    )

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=SUMMARY_MOCKED_PARAMETER_DICT,
            json=SUMMARY_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 400
    assert actual == '{"detail":"Error Connecting to Database"}'
