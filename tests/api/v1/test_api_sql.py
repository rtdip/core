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
import numpy as np
from datetime import datetime, timezone
from tests.api.v1.api_test_objects import (
    SQL_POST_MOCKED_PARAMETER_DICT,
    SQL_POST_BODY_MOCKED_PARAMETER_DICT,
    mocker_setup,
    TEST_HEADERS,
    BASE_URL,
)
from pandas.io.json import build_table_schema
from httpx import AsyncClient
from src.api.v1 import app

MOCK_METHOD = "src.sdk.python.rtdip_sdk.queries.sql.sql_query.SQLQueryBuilder.get"
MOCK_API_NAME = "/api/v1/sql/execute"

pytestmark = pytest.mark.anyio


async def test_api_sql_post_success(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_raw"])

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=SQL_POST_MOCKED_PARAMETER_DICT,
            json=SQL_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 200
    assert actual == api_test_data["expected_sql"]


async def test_api_sql_post_validation_error(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(mocker, MOCK_METHOD, api_test_data["mock_data_raw"])

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=SQL_POST_MOCKED_PARAMETER_DICT,
            json={},
        )
    actual = response.text

    assert response.status_code == 422
    assert (
        actual
        == '{"detail":[{"type":"missing","loc":["body","sql_statement"],"msg":"Field required","input":{}}]}'
    )


async def test_api_sql_post_error(mocker: MockerFixture, api_test_data):
    mocker = mocker_setup(
        mocker,
        MOCK_METHOD,
        api_test_data["mock_data_raw"],
        Exception("Error Connecting to Database"),
    )

    async with AsyncClient(app=app, base_url=BASE_URL) as ac:
        response = await ac.post(
            MOCK_API_NAME,
            headers=TEST_HEADERS,
            params=SQL_POST_MOCKED_PARAMETER_DICT,
            json=SQL_POST_BODY_MOCKED_PARAMETER_DICT,
        )
    actual = response.text

    assert response.status_code == 400
    assert actual == '{"detail":"Error Connecting to Database"}'
