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
from datetime import datetime, timezone
from tests.api.v1.api_test_objects import (
    PLOT_MOCKED_PARAMETER_DICT,
    PLOT_MOCKED_PARAMETER_ERROR_DICT,
    PLOT_POST_MOCKED_PARAMETER_DICT,
    PLOT_POST_BODY_MOCKED_PARAMETER_DICT,
    mocker_setup,
    TEST_HEADERS,
    BASE_URL,
)
from httpx import AsyncClient
from src.api.v1 import app

MOCK_METHOD = "src.sdk.python.rtdip_sdk.queries.time_series.plot.get"
MOCK_API_NAME = "/api/v1/events/plot"

pytestmark = pytest.mark.anyio


async def test_api_plot_get_success(mocker: MockerFixture):
    test_data = pd.DataFrame(
        {
            "EventTime": [datetime.now(timezone.utc)],
            "TagName": ["TestTag"],
            "Average": [1.01],
            "Min": [1.01],
            "Max": [1.01],
            "First": [1.01],
            "Last": [1.01],
            "StdDev": [1.01],
        }
    )
    mocker = mocker_setup(mocker, MOCK_METHOD, test_data)

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME, headers=TEST_HEADERS, params=PLOT_MOCKED_PARAMETER_DICT
        )
    actual = response.text
    expected = test_data.to_json(orient="table", index=False, date_unit="ns")
    expected = (
        expected.replace(',"tz":"UTC"', "").rstrip("}")
        + ',"pagination":{"limit":null,"offset":null,"next":null}}'
    )

    assert response.status_code == 200
    assert actual == expected


async def test_api_plot_get_validation_error(mocker: MockerFixture):
    test_data = pd.DataFrame(
        {
            "EventTime": [datetime.now(timezone.utc)],
            "TagName": ["TestTag"],
            "Average": [1.01],
            "Min": [1.01],
            "Max": [1.01],
            "First": [1.01],
            "Last": [1.01],
            "StdDev": [1.01],
        }
    )
    mocker = mocker_setup(mocker, MOCK_METHOD, test_data)

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=PLOT_MOCKED_PARAMETER_ERROR_DICT,
        )
    actual = response.text

    assert response.status_code == 422
    assert (
        actual
        == '{"detail":[{"type":"missing","loc":["query","start_date"],"msg":"Field required","input":null}]}'
    )


async def test_api_pot_get_error(mocker: MockerFixture):
    test_data = pd.DataFrame(
        {
            "EventTime": [datetime.now(timezone.utc)],
            "TagName": ["TestTag"],
            "Average": [1.01],
            "Min": [1.01],
            "Max": [1.01],
            "First": [1.01],
            "Last": [1.01],
            "StdDev": [1.01],
        }
    )
    mocker = mocker_setup(
        mocker, MOCK_METHOD, test_data, Exception("Error Connecting to Database")
    )

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.get(
            MOCK_API_NAME, headers=TEST_HEADERS, params=PLOT_MOCKED_PARAMETER_DICT
        )
    actual = response.text

    assert response.status_code == 400
    assert actual == '{"detail":"Error Connecting to Database"}'


async def test_api_plot_post_success(mocker: MockerFixture):
    test_data = pd.DataFrame(
        {
            "EventTime": [datetime.now(timezone.utc)],
            "TagName": ["TestTag"],
            "Average": [1.01],
            "Min": [1.01],
            "Max": [1.01],
            "First": [1.01],
            "Last": [1.01],
            "StdDev": [1.01],
        }
    )
    mocker = mocker_setup(mocker, MOCK_METHOD, test_data)

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=PLOT_POST_MOCKED_PARAMETER_DICT,
            json=PLOT_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text
    expected = test_data.to_json(orient="table", index=False, date_unit="ns")
    expected = (
        expected.replace(',"tz":"UTC"', "").rstrip("}")
        + ',"pagination":{"limit":null,"offset":null,"next":null}}'
    )

    assert response.status_code == 200
    assert actual == expected


async def test_api_plot_post_validation_error(mocker: MockerFixture):
    test_data = pd.DataFrame(
        {
            "EventTime": [datetime.now(timezone.utc)],
            "TagName": ["TestTag"],
            "Average": [1.01],
            "Min": [1.01],
            "Max": [1.01],
            "First": [1.01],
            "Last": [1.01],
            "StdDev": [1.01],
        }
    )
    mocker = mocker_setup(mocker, MOCK_METHOD, test_data)

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=PLOT_MOCKED_PARAMETER_ERROR_DICT,
            json=PLOT_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 422
    assert (
        actual
        == '{"detail":[{"type":"missing","loc":["query","start_date"],"msg":"Field required","input":null}]}'
    )


async def test_api_plot_post_error(mocker: MockerFixture):
    test_data = pd.DataFrame(
        {
            "EventTime": [datetime.now(timezone.utc)],
            "TagName": ["TestTag"],
            "Average": [1.01],
            "Min": [1.01],
            "Max": [1.01],
            "First": [1.01],
            "Last": [1.01],
            "StdDev": [1.01],
        }
    )
    mocker = mocker_setup(
        mocker, MOCK_METHOD, test_data, Exception("Error Connecting to Database")
    )

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=PLOT_MOCKED_PARAMETER_DICT,
            json=PLOT_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 400
    assert actual == '{"detail":"Error Connecting to Database"}'
