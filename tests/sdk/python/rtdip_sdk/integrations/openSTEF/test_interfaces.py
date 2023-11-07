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

import pandas
import sqlalchemy
import pytest
from src.sdk.python.rtdip_sdk.integrations.openSTEF.interfaces import _DataInterface
from pytest_mock import MockerFixture
from pydantic.v1 import BaseSettings
from typing import Union
from unittest.mock import MagicMock

# class MockedConnection:
#     def execute(self, *args, **kwargs):
#         pass  # do nothing

class MockedEngine:
    def connect(self):
        # return MockedConnection()
        pass  # do nothing
    def execute(self, *args, **kwargs):
        pass  # do nothing

class Settings(BaseSettings):
    api_username: str = "None"
    api_password: str = "None"
    api_admin_username: str = "None"
    api_admin_password: str = "None"
    api_url: str = "None"
    pcdm_host: str = "host"
    pcdm_token: str = "token"
    pcdm_port: int = 443
    pcdm_http_path: str = "http_path"
    pcdm_catalog: str = "rtdip"
    pcdm_schema: str = "openstef"
    db_host: str = "host"
    db_token: str = "token"
    db_port: int = 443
    db_http_path: str = "http_path"
    db_catalog: str = "rtdip"
    db_schema: str = "sensors"
    proxies: Union[dict[str, str], None] = None


config = Settings()
interface = _DataInterface(config)


def test_exec_influx_query(mocker: MockerFixture):
    pass


def test_exec_influx_write(mocker: MockerFixture):
    pass


def test_check_influx_available(mocker: MockerFixture):
    pass


def test_exec_sql_query(mocker: MockerFixture):
    pass


def test_exec_sql_write(mocker: MockerFixture):
    mocker.patch.object(interface, '_create_mysql_engine', return_value=MockedEngine())
    mocked_execute = mocker.patch.object(MockedEngine, "execute")

    sql_write = interface.exec_sql_write("INSERT INTO test_table VALUES (1, 'test')")

    mocked_execute.assert_called_once_with("INSERT INTO test_table VALUES (1, 'test')")
    assert sql_write is None


def test_exec_sql_dataframe_write(mocker: MockerFixture):
    mocked_to_sql = mocker.patch.object(pandas.DataFrame, "to_sql", return_value=None)

    expected_data = pandas.DataFrame({"test": ["1", "2", "data"]})
    sql_write = interface.exec_sql_dataframe_write(expected_data, "test_table")

    mocked_to_sql.assert_called_once()
    assert sql_write == None


def test_exec_sql_dataframe_write_fails(mocker: MockerFixture):
    mocker.patch.object(pandas.DataFrame, "to_sql", side_effect=Exception)

    expected_data = pandas.DataFrame({"test": ["1", "2", "data"]})

    with pytest.raises(Exception):
        interface.exec_sql_dataframe_write(expected_data, "test_table")


def test_check_mysql_available(mocker: MockerFixture):
    pass
