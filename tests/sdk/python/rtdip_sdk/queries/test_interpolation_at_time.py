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
sys.path.insert(0, '.')
import pandas as pd
import pyarrow as pa
import pytest
from pytest_mock import MockerFixture
from tests.sdk.python.rtdip_sdk.connectors.odbc.test_db_sql_connector import MockedDBConnection, MockedCursor 
from src.sdk.python.rtdip_sdk.connectors import DatabricksSQLConnection
from src.sdk.python.rtdip_sdk.queries.time_series.interpolation_at_time import get as interpolation_at_time_get

SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "mock_databricks_token"
DATABRICKS_SQL_CONNECT = 'databricks.sql.connect'
DATABRICKS_SQL_CONNECT_CURSOR = 'databricks.sql.connect.cursor'
MOCKED_QUERY = 'WITH raw_events AS (SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(EventTime, \'yyyy-MM-dd HH:mm:ss.SSS\')), "+0000") as EventTime, TagName,  Status, Value FROM `mocked-buiness-unit`.`sensors`.`mocked-asset_mocked-data-security-level_events_mocked-data-type` WHERE EventDate BETWEEN  date_sub(to_date(to_timestamp("2011-01-01T00:00:00+00:00")), 1) AND date_add(to_date(to_timestamp("2011-01-01T00:00:00+00:00")), 1)  AND TagName in (\'MOCKED-TAGNAME\')  AND Status = \'Good\' ) , date_array AS (SELECT explode(array( from_utc_timestamp(to_timestamp("2011-01-01T00:00:00+00:00"), "+0000")   )) AS EventTime, explode(array(\'MOCKED-TAGNAME\')) AS TagName) , interpolation_events AS (SELECT coalesce(a.TagName, b.TagName) as TagName, coalesce(a.EventTime, b.EventTime) as EventTime, a.EventTime as Requested_EventTime, b.EventTime as Found_EventTime, b.Status, b.Value FROM date_array a FULL OUTER JOIN  raw_events b ON a.EventTime = b.EventTime AND a.TagName = b.TagName) , interpolation_calculations AS (SELECT *, lag(EventTime) OVER (PARTITION BY TagName ORDER BY EventTime) AS Previous_EventTime, lag(Value) OVER (PARTITION BY TagName ORDER BY EventTime) AS Previous_Value, lead(EventTime) OVER (PARTITION BY TagName ORDER BY EventTime) AS Next_EventTime, lead(Value) OVER (PARTITION BY TagName ORDER BY EventTime) AS Next_Value, CASE WHEN Requested_EventTime = Found_EventTime THEN Value WHEN Next_EventTime IS NULL THEN Previous_Value WHEN Previous_EventTime IS NULL and Next_EventTime IS NULL THEN NULL ELSE Previous_Value + ((Next_Value - Previous_Value) * ((unix_timestamp(EventTime) - unix_timestamp(Previous_EventTime)) / (unix_timestamp(Next_EventTime) - unix_timestamp(Previous_EventTime)))) END AS Interpolated_Value FROM interpolation_events) SELECT TagName, EventTime, Interpolated_Value as Value FROM interpolation_calculations WHERE EventTime in ( from_utc_timestamp(to_timestamp("2011-01-01T00:00:00+00:00"), "+0000")  ) '
MOCKED_PARAMETER_DICT = {
        "business_unit": "mocked-buiness-unit",
        "region": "mocked-region",
        "asset": "mocked-asset",
        "data_security_level": "mocked-data-security-level",
        "data_type": "mocked-data-type",
        "tag_names": ["MOCKED-TAGNAME"],
        "timestamps": ["2011-01-01T00:00:00+00:00"],
        "window_length": 1,
        "include_bad_data": False
        }

def test_interpolation_at_time(mocker: MockerFixture):
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(MockedCursor, "fetchall_arrow", return_value = pa.Table.from_pandas(pd.DataFrame(data={'a': [1], 'b': [2], 'c': [3], 'd': [4]})))
    mocked_close = mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN)

    actual = interpolation_at_time_get(mocked_connection, MOCKED_PARAMETER_DICT)

    mocked_cursor.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_QUERY)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)

def test_interpolation_at_time_fails(mocker: MockerFixture):
    mocker.spy(MockedDBConnection, "cursor")
    mocker.spy(MockedCursor, "execute")
    mocker.patch.object(MockedCursor, "fetchall_arrow", side_effect=Exception)
    mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN)

    with pytest.raises(Exception):
        interpolation_at_time_get(mocked_connection, MOCKED_PARAMETER_DICT)