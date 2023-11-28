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

import sys

sys.path.insert(0, ".")
import pandas as pd
import pyarrow as pa
import pytest
from pytest_mock import MockerFixture
from tests.sdk.python.rtdip_sdk.connectors.odbc.test_db_sql_connector import (
    MockedDBConnection,
    MockedCursor,
)
from src.sdk.python.rtdip_sdk.connectors import DatabricksSQLConnection
from src.sdk.python.rtdip_sdk.queries.time_series.summary import get as summary_get

SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "mock_databricks_token"
DATABRICKS_SQL_CONNECT = "databricks.sql.connect"
DATABRICKS_SQL_CONNECT_CURSOR = "databricks.sql.connect.cursor"
MOCKED_QUERY = 'SELECT `TagName`, count(`Value`) as Count, CAST(Avg(`Value`) as decimal(10, 2)) as Avg, CAST(Min(`Value`) as decimal(10, 2)) as Min, CAST(Max(`Value`) as decimal(10, 2)) as Max, CAST(stddev(`Value`) as decimal(10, 2)) as StDev, CAST(sum(`Value`) as decimal(10, 2)) as Sum, CAST(variance(`Value`) as decimal(10, 2)) as Var FROM `mocked-buiness-unit`.`sensors`.`mocked-asset_mocked-data-security-level_events_mocked-data-type` WHERE `EventTime` BETWEEN to_timestamp("2011-01-01T00:00:00+00:00") AND to_timestamp("2011-01-02T23:59:59+00:00") AND `TagName` IN (\'MOCKED-TAGNAME\') GROUP BY `TagName` '
MOCKED_QUERY_OFFSET_LIMIT = "LIMIT 10 OFFSET 10 "
MOCKED_PARAMETER_DICT = {
    "business_unit": "mocked-buiness-unit",
    "region": "mocked-region",
    "asset": "mocked-asset",
    "data_security_level": "mocked-data-security-level",
    "data_type": "mocked-data-type",
    "tag_names": ["MOCKED-TAGNAME"],
    "start_date": "2011-01-01",
    "end_date": "2011-01-02",
    "include_bad_data": True,
}

test_data = pa.Table.from_pandas(
    pd.DataFrame(
        data={
            "TagName": ["MOCKED-TAGNAME"],
            "Count": [10.0],
            "Avg": [5.5],
            "Min": [1.0],
            "Max": [10.0],
            "StDev": [3.2],
            "Sum": [25.0],
            "var": [0.0],
        }
    )
)


def test_summary_get(mocker: MockerFixture):
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_connection_close = mocker.spy(MockedDBConnection, "close")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(
        MockedCursor, "fetchall_arrow", return_value=test_data
    )
    mocked_close = mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    actual = summary_get(mocked_connection, MOCKED_PARAMETER_DICT)

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_QUERY)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_raw_offset_limit(mocker: MockerFixture):
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_connection_close = mocker.spy(MockedDBConnection, "close")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(
        MockedCursor, "fetchall_arrow", return_value=test_data
    )
    mocked_close = mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    MOCKED_PARAMETER_OFFSET_LIMIT_DICT = MOCKED_PARAMETER_DICT.copy()
    MOCKED_PARAMETER_OFFSET_LIMIT_DICT["offset"] = 10
    MOCKED_PARAMETER_OFFSET_LIMIT_DICT["limit"] = 10
    actual = summary_get(mocked_connection, MOCKED_PARAMETER_OFFSET_LIMIT_DICT)

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(
        mocker.ANY, query=MOCKED_QUERY + MOCKED_QUERY_OFFSET_LIMIT
    )
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_raw_fails(mocker: MockerFixture):
    mocker.spy(MockedDBConnection, "cursor")
    mocker.spy(MockedDBConnection, "close")
    mocker.spy(MockedCursor, "execute")
    mocker.patch.object(MockedCursor, "fetchall_arrow", side_effect=Exception)
    mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    with pytest.raises(Exception):
        summary_get(mocked_connection, MOCKED_PARAMETER_DICT)
