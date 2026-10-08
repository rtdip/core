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
MOCK_MASKED_API_NAME = "/api/v1/events/rawmasked"

pytestmark = pytest.mark.anyio


async def test_api_raw_get_success(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_raw"])

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME, headers=TEST_HEADERS, params=RAW_MOCKED_PARAMETER_DICT
        )
    actual = response.text

    assert response.status_code == 200
    assert actual == api_test_data["expected_raw"]


async def test_api_raw_get_validation_error(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_raw"])

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
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

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME, headers=TEST_HEADERS, params=RAW_MOCKED_PARAMETER_DICT
        )
    actual = response.text

    assert response.status_code == 400
    assert actual == '{"detail":"Error Connecting to Database"}'


async def test_api_raw_post_success(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_raw"])

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
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

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
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

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=RAW_MOCKED_PARAMETER_DICT,
            json=RAW_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 400
    assert actual == '{"detail":"Error Connecting to Database"}'


@pytest.mark.parametrize("method", ["get", "post"])
async def test_api_raw_masked_masks_bad_values(mocker: MockerFixture, method):
    rows = [
        {
            "EventTime": "2022-01-01T00:00:00.000000000Z",
            "TagName": "TestTag",
            "Status": "Good",
            "Value": 1.5,
        },
        {
            "EventTime": "2022-01-01T01:00:00.000000000Z",
            "TagName": "TestTag",
            "Status": "Bad",
            "Value": 999.0,
        },
    ]
    mock_data = {
        "data": ",".join(json.dumps(row, separators=(",", ":")) for row in rows),
        "count": len(rows),
        "sample_row": json.dumps(rows[0], separators=(",", ":")),
    }
    mocker_setup(mocker, MOCK_METHOD, mock_data)

    request_kwargs = {
        "headers": TEST_HEADERS,
        "params": (
            RAW_MOCKED_PARAMETER_DICT
            if method == "get"
            else RAW_POST_MOCKED_PARAMETER_DICT
        ),
    }
    if method == "post":
        request_kwargs["json"] = RAW_POST_BODY_MOCKED_PARAMETER_DICT

    async with AsyncClient(transport=ASGITransport(app=app), base_url=BASE_URL) as ac:
        response = await getattr(ac, method)(MOCK_MASKED_API_NAME, **request_kwargs)

    assert response.status_code == 200
    assert response.json()["data"] == [rows[0], {**rows[1], "Value": "Bad Data"}]
