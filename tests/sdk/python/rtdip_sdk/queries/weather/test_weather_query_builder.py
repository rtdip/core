# Copyright 2023 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from src.sdk.python.rtdip_sdk.queries.weather.weather_query_builder import (
    WeatherQueryBuilder,
)
from src.sdk.python.rtdip_sdk.connectors import DatabricksSQLConnection
from src.sdk.python.rtdip_sdk.authentication.azure import DefaultAuth
from pytest_mock import MockerFixture

MOCK_TABLE = "mock_catalog.mock_scema.mock_table"
MOCK_CONNECTION = "mock_connection"


def test_query_builder_raw(mocker: MockerFixture):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.query_builder.raw.get",
        return_value={"test": "data"},
    )

    data = (
        WeatherQueryBuilder()
        .connect(MOCK_CONNECTION)
        .source(MOCK_TABLE, status_column=None)
        .raw(["max"], start_date="2021-01-01", end_date="2021-01-02")
    )
    assert data == {"test": "data"}


def test_query_builder_latest(mocker: MockerFixture):
    mocker.patch(
        "src.sdk.python.rtdip_sdk.queries.query_builder.latest.get",
        return_value={"test": "data"},
    )

    data = (
        WeatherQueryBuilder()
        .connect(MOCK_CONNECTION)
        .source(MOCK_TABLE)
        .latest(tagname_filter=["0"])
    )
    assert data == {"test": "data"}
