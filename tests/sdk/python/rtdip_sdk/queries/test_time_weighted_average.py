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
from src.sdk.python.rtdip_sdk.queries.time_series.time_weighted_average import get as time_weighted_average_get

SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "mock_databricks_token"
DATABRICKS_SQL_CONNECT = 'databricks.sql.connect'
DATABRICKS_SQL_CONNECT_CURSOR = 'databricks.sql.connect.cursor'
MOCKED_QUERY= 'WITH raw_events AS (SELECT DISTINCT EventDate, TagName, from_utc_timestamp(to_timestamp(date_format(EventTime, \'yyyy-MM-dd HH:mm:ss.SSS\')), "+0000") as EventTime, Status, Value FROM `mocked-buiness-unit`.`sensors`.`mocked-asset_mocked-data-security-level_events_mocked-data-type` WHERE EventDate BETWEEN date_sub(to_date(to_timestamp("2011-01-01T00:00:00+00:00")), 1) AND date_add(to_date(to_timestamp("2011-01-02T23:59:59+00:00")), 1) AND TagName in (\'MOCKED-TAGNAME\')  ) ,date_array AS (SELECT explode(sequence(from_utc_timestamp(to_timestamp("2011-01-01T00:00:00+00:00"), "+0000"), from_utc_timestamp(to_timestamp("2011-01-02T23:59:59+00:00"), "+0000"), INTERVAL \'15 minute\')) AS EventTime, explode(array(\'MOCKED-TAGNAME\')) AS TagName) ,window_events AS (SELECT coalesce(a.TagName, b.TagName) AS TagName, coalesce(a.EventTime, b.EventTime) as EventTime, window(coalesce(a.EventTime, b.EventTime), \'15 minute\').start WindowEventTime, b.Status, b.Value FROM date_array a FULL OUTER JOIN raw_events b ON CAST(a.EventTime AS long) = CAST(b.EventTime AS long) AND a.TagName = b.TagName) ,fill_status AS (SELECT *, last_value(Status, true) OVER (PARTITION BY TagName ORDER BY EventTime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as Fill_Status, CASE WHEN Fill_Status = "Good" THEN Value ELSE null END AS Good_Value FROM window_events) ,fill_value AS (SELECT *, last_value(Good_Value, true) OVER (PARTITION BY TagName ORDER BY EventTime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Fill_Value FROM fill_status) ,twa_calculations AS (SELECT TagName, EventTime, WindowEventTime, false AS Step, Status, Value, Fill_Status, Fill_Value, lead(EventTime) OVER (PARTITION BY TagName ORDER BY EventTime) AS Next_EventTime, lead(Fill_Status) OVER (PARTITION BY TagName ORDER BY EventTime) AS Next_Status ,CASE WHEN Next_Status = "Good" OR (Fill_Status = "Good" AND Next_Status = "Bad") THEN lead(Fill_Value) OVER (PARTITION BY TagName ORDER BY EventTime) ELSE Value END AS Next_Value_For_Status ,CASE WHEN Fill_Status = "Good" THEN Next_Value_For_Status ELSE 0 END AS Next_Value ,CASE WHEN Fill_Status = "Good" and Next_Status = "Good" THEN ((cast(Next_EventTime as double) - cast(EventTime as double)) / 60) WHEN Fill_Status = "Good" and Next_Status != "Good" THEN ((cast(Next_EventTime as integer) - cast(EventTime as double)) / 60) ELSE 0 END AS good_minutes ,CASE WHEN Step == false THEN ((Fill_Value + Next_Value) * 0.5) * good_minutes ELSE (Fill_Value * good_minutes) END AS twa_value FROM fill_value) ,project_result AS (SELECT TagName, WindowEventTime AS EventTime, sum(twa_value) / sum(good_minutes) AS Value from twa_calculations GROUP BY TagName, WindowEventTime) SELECT * FROM project_result WHERE EventTime BETWEEN to_timestamp("2011-01-01T00:00:00") AND to_timestamp("2011-01-02T23:59:59") ORDER BY TagName, EventTime '
MOCKED_PARAMETER_DICT = {
        "business_unit": "mocked-buiness-unit",
        "region": "mocked-region",
        "asset": "mocked-asset",
        "data_security_level": "mocked-data-security-level",
        "data_type": "mocked-data-type",
        "tag_names": ["MOCKED-TAGNAME"],
        "start_date": "2011-01-01",
        "end_date": "2011-01-02",
        "time_interval_rate": "15",
        "time_interval_unit": "minute",
        "window_length": 1,
        "step": "false",
        "include_bad_data": True
        }

def test_time_weighted_average(mocker: MockerFixture):
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(MockedCursor, "fetchall_arrow", return_value =  pa.Table.from_pandas(pd.DataFrame(data={'a': [1], 'b': [2], 'c': [3], 'd': [4]})))
    mocked_close = mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN)

    actual = time_weighted_average_get(mocked_connection, MOCKED_PARAMETER_DICT)

    mocked_cursor.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_QUERY)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)

def test_time_weighted_average_with_window_size_mins(mocker: MockerFixture):
    MOCKED_PARAMETER_DICT["window_size_mins"] = 15
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(MockedCursor, "fetchall_arrow", return_value =  pa.Table.from_pandas(pd.DataFrame(data={'a': [1], 'b': [2], 'c': [3], 'd': [4]})))
    mocked_close = mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN)

    actual = time_weighted_average_get(mocked_connection, MOCKED_PARAMETER_DICT)

    mocked_cursor.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_QUERY)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)

def test_time_weighted_average_fails(mocker: MockerFixture):
    mocker.spy(MockedDBConnection, "cursor")
    mocker.spy(MockedCursor, "execute")
    mocker.patch.object(MockedCursor, "fetchall_arrow", side_effect=Exception)
    mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN)

    with pytest.raises(Exception):
        time_weighted_average_get(mocked_connection, MOCKED_PARAMETER_DICT)

def test_time_weighted_average_tag_name_not_list_fails(mocker: MockerFixture):
    MOCKED_PARAMETER_DICT["tag_names"] = "abc"
    mocker.spy(MockedDBConnection, "cursor")
    mocker.spy(MockedCursor, "execute")
    mocker.patch.object(MockedCursor, "fetchall_arrow", side_effect=Exception)
    mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN)

    with pytest.raises(Exception):
        time_weighted_average_get(mocked_connection, MOCKED_PARAMETER_DICT)