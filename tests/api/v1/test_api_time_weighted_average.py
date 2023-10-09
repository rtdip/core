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
from datetime import datetime
from tests.api.v1.api_test_objects import (
    TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT,
    TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_ERROR_DICT,
    TIME_WEIGHTED_AVERAGE_POST_MOCKED_PARAMETER_DICT,
    TIME_WEIGHTED_AVERAGE_POST_BODY_MOCKED_PARAMETER_DICT,
    mocker_setup,
    TEST_HEADERS,
    BASE_URL,
)
from httpx import AsyncClient
from src.api.v1 import app

MOCK_METHOD = "src.sdk.python.rtdip_sdk.queries.time_series.time_weighted_average.get"
MOCK_API_NAME = "/api/v1/events/timeweightedaverage"

pytestmark = pytest.mark.anyio


async def test_api_time_weighted_average_get_success(mocker: MockerFixture):
    test_data = pd.DataFrame(
        {"EventTime": [datetime.utcnow()], "TagName": ["TestTag"], "Value": [1.01]}
    )
    test_data = test_data.set_index("EventTime")
    mocker = mocker_setup(mocker, MOCK_METHOD, test_data)

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT,
        )
    actual = response.text
    test_data = test_data.reset_index()
    expected = test_data.to_json(orient="table", index=False, date_unit="us")

    assert response.status_code == 200
    assert actual == expected


async def test_api_time_weighted_average_get_validation_error(mocker: MockerFixture):
    test_data = pd.DataFrame(
        {"EventTime": [datetime.utcnow()], "TagName": ["TestTag"], "Value": [1.01]}
    )
    mocker = mocker_setup(mocker, MOCK_METHOD, test_data)

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_ERROR_DICT,
        )
    actual = response.text

    assert response.status_code == 422
    assert (
        actual
        == '{"detail":[{"type":"missing","loc":["query","start_date"],"msg":"Field required","input":null,"url":"https://errors.pydantic.dev/2.4/v/missing"}]}'
    )


async def test_api_time_weighted_average_get_error(mocker: MockerFixture):
    test_data = pd.DataFrame(
        {"EventTime": [datetime.utcnow()], "TagName": ["TestTag"], "Value": [1.01]}
    )
    mocker = mocker_setup(
        mocker, MOCK_METHOD, test_data, Exception("Error Connecting to Database")
    )

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 400
    assert actual == '{"detail":"Error Connecting to Database"}'


async def test_api_time_weighted_average_post_success(mocker: MockerFixture):
    test_data = pd.DataFrame(
        {"EventTime": [datetime.utcnow()], "TagName": ["TestTag"], "Value": [1.01]}
    )
    test_data = test_data.set_index("EventTime")
    mocker = mocker_setup(mocker, MOCK_METHOD, test_data)

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=TIME_WEIGHTED_AVERAGE_POST_MOCKED_PARAMETER_DICT,
            json=TIME_WEIGHTED_AVERAGE_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text
    test_data = test_data.reset_index()
    expected = test_data.to_json(orient="table", index=False, date_unit="us")

    assert response.status_code == 200
    assert actual == expected


async def test_api_time_weighted_average_post_validation_error(mocker: MockerFixture):
    test_data = pd.DataFrame(
        {"EventTime": [datetime.utcnow()], "TagName": ["TestTag"], "Value": [1.01]}
    )
    mocker = mocker_setup(mocker, MOCK_METHOD, test_data)

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_ERROR_DICT,
            json=TIME_WEIGHTED_AVERAGE_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 422
    assert (
        actual
        == '{"detail":[{"type":"missing","loc":["query","start_date"],"msg":"Field required","input":null,"url":"https://errors.pydantic.dev/2.4/v/missing"}]}'
    )


async def test_api_time_weighted_average_post_error(mocker: MockerFixture):
    test_data = pd.DataFrame(
        {"EventTime": [datetime.utcnow()], "TagName": ["TestTag"], "Value": [1.01]}
    )
    mocker = mocker_setup(
        mocker, MOCK_METHOD, test_data, Exception("Error Connecting to Database")
    )

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT,
            json=TIME_WEIGHTED_AVERAGE_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 400
    assert actual == '{"detail":"Error Connecting to Database"}'
