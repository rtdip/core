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
from httpx import AsyncClient, ASGITransport
from tests.api.v1.api_test_objects import BASE_URL
from src.api.v1 import app

pytestmark = pytest.mark.anyio


async def test_api_home(mocker: MockerFixture):
    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.get("/")

    assert response.status_code == 307
    assert response.next_request.url == BASE_URL + "/docs"


async def test_api_docs(mocker: MockerFixture):
    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.get("/docs")

    assert response.status_code == 200


async def test_api_redoc(mocker: MockerFixture):
    async with AsyncClient(transport=ASGITransport(app), base_url=BASE_URL) as ac:
        response = await ac.get("/redoc")

    assert response.status_code == 200
