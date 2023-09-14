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
from src.sdk.python.rtdip_sdk.queries.time_series.circular_average import (
    get as circular_average_get,
)

SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "mock_databricks_token"
DATABRICKS_SQL_CONNECT = "databricks.sql.connect"
DATABRICKS_SQL_CONNECT_CURSOR = "databricks.sql.connect.cursor"
MOCKED_QUERY = 'WITH raw_events AS (SELECT `EventTime`, `TagName`,  `Status`,  `Value` FROM `mocked-business-unit`.`sensors`.`mocked-asset_mocked-data-security-level_events_mocked-data-type` WHERE `EventTime` BETWEEN TO_TIMESTAMP("2011-01-01T00:00:00+00:00") AND TO_TIMESTAMP("2011-01-02T23:59:59+00:00") AND `TagName` IN (\'MOCKED-TAGNAME\') ) ,date_array AS (SELECT EXPLODE(SEQUENCE(FROM_UTC_TIMESTAMP(TO_TIMESTAMP("2011-01-01T00:00:00+00:00"), "+0000"), FROM_UTC_TIMESTAMP(TO_TIMESTAMP("2011-01-02T23:59:59+00:00"), "+0000"), INTERVAL \'15 minute\')) AS `EventTime`, EXPLODE(ARRAY(\'MOCKED-TAGNAME\')) AS `TagName`)  ,window_events AS (SELECT COALESCE(a.`TagName`, b.`TagName`) AS `TagName`, COALESCE(a.`EventTime`, b.`EventTime`) AS `EventTime`, WINDOW(COALESCE(a.`EventTime`, b.`EventTime`), \'15 minute\').START `WindowEventTime`, b.`Status`, b.`Value` FROM date_array a FULL OUTER JOIN raw_events b ON CAST(a.`EventTime` AS LONG) = CAST(b.`EventTime` AS LONG) AND a.`TagName` = b.`TagName`) ,calculation_set_up AS (SELECT `EventTime`, `WindowEventTime`, `TagName`, `Value`, MOD(`Value` - 0, (360 - 0))*(2*pi()/(360 - 0)) AS `Value_in_Radians`, LAG(`EventTime`) OVER (PARTITION BY `TagName` ORDER BY `EventTime`) AS `Previous_EventTime`, (unix_millis(`EventTime`) - unix_millis(`Previous_EventTime`)) / 86400000 AS Time_Difference, COS(`Value_in_Radians`) AS Cos_Value, SIN(`Value_in_Radians`) AS Sin_Value FROM window_events) ,circular_average_calculations AS (SELECT `WindowEventTime`, `TagName`, Time_Difference, AVG(Cos_Value) OVER (PARTITION BY `TagName` ORDER BY `EventTime` ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS Average_Cos, AVG(Sin_Value) OVER (PARTITION BY `TagName` ORDER BY `EventTime` ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS Average_Sin, SQRT(POW(Average_Cos, 2) + POW(Average_Sin, 2)) AS Vector_Length, Average_Cos/Vector_Length AS Rescaled_Average_Cos, Average_Sin/Vector_Length AS Rescaled_Average_Sin, Time_Difference * Rescaled_Average_Cos AS Diff_Average_Cos, Time_Difference * Rescaled_Average_Sin AS Diff_Average_Sin FROM calculation_set_up)  ,circular_average_results AS (SELECT `WindowEventTime` AS `EventTime`, `TagName`, sum(Diff_Average_Cos)/sum(Time_Difference) AS Cos_Time_Averages, sum(Diff_Average_Sin)/sum(Time_Difference) AS Sin_Time_Averages, array_min(array(1, sqrt(pow(Cos_Time_Averages, 2) + pow(Sin_Time_Averages, 2)))) AS R, mod(2*pi() + atan2(Sin_Time_Averages, Cos_Time_Averages), 2*pi()) AS Circular_Average_Value_in_Radians, (Circular_Average_Value_in_Radians * (360 - 0)) / (2*pi())+ 0 AS Circular_Average_Value_in_Degrees FROM circular_average_calculations GROUP BY `TagName`, `WindowEventTime`) ,project AS (SELECT `EventTime`, `TagName`, Circular_Average_Value_in_Degrees AS `Value` FROM circular_average_results) SELECT * FROM project ORDER BY `TagName`, `EventTime` '
MOCKED_QUERY_OFFSET_LIMIT = "LIMIT 10 OFFSET 10 "
MOCKED_QUERY_PIVOT = 'WITH raw_events AS (SELECT `EventTime`, `TagName`,  `Status`,  `Value` FROM `mocked-business-unit`.`sensors`.`mocked-asset_mocked-data-security-level_events_mocked-data-type` WHERE `EventTime` BETWEEN TO_TIMESTAMP("2011-01-01T00:00:00+00:00") AND TO_TIMESTAMP("2011-01-02T23:59:59+00:00") AND `TagName` IN (\'MOCKED-TAGNAME\') ) ,date_array AS (SELECT EXPLODE(SEQUENCE(FROM_UTC_TIMESTAMP(TO_TIMESTAMP("2011-01-01T00:00:00+00:00"), "+0000"), FROM_UTC_TIMESTAMP(TO_TIMESTAMP("2011-01-02T23:59:59+00:00"), "+0000"), INTERVAL \'15 minute\')) AS `EventTime`, EXPLODE(ARRAY(\'MOCKED-TAGNAME\')) AS `TagName`)  ,window_events AS (SELECT COALESCE(a.`TagName`, b.`TagName`) AS `TagName`, COALESCE(a.`EventTime`, b.`EventTime`) AS `EventTime`, WINDOW(COALESCE(a.`EventTime`, b.`EventTime`), \'15 minute\').START `WindowEventTime`, b.`Status`, b.`Value` FROM date_array a FULL OUTER JOIN raw_events b ON CAST(a.`EventTime` AS LONG) = CAST(b.`EventTime` AS LONG) AND a.`TagName` = b.`TagName`) ,calculation_set_up AS (SELECT `EventTime`, `WindowEventTime`, `TagName`, `Value`, MOD(`Value` - 0, (360 - 0))*(2*pi()/(360 - 0)) AS `Value_in_Radians`, LAG(`EventTime`) OVER (PARTITION BY `TagName` ORDER BY `EventTime`) AS `Previous_EventTime`, (unix_millis(`EventTime`) - unix_millis(`Previous_EventTime`)) / 86400000 AS Time_Difference, COS(`Value_in_Radians`) AS Cos_Value, SIN(`Value_in_Radians`) AS Sin_Value FROM window_events) ,circular_average_calculations AS (SELECT `WindowEventTime`, `TagName`, Time_Difference, AVG(Cos_Value) OVER (PARTITION BY `TagName` ORDER BY `EventTime` ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS Average_Cos, AVG(Sin_Value) OVER (PARTITION BY `TagName` ORDER BY `EventTime` ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS Average_Sin, SQRT(POW(Average_Cos, 2) + POW(Average_Sin, 2)) AS Vector_Length, Average_Cos/Vector_Length AS Rescaled_Average_Cos, Average_Sin/Vector_Length AS Rescaled_Average_Sin, Time_Difference * Rescaled_Average_Cos AS Diff_Average_Cos, Time_Difference * Rescaled_Average_Sin AS Diff_Average_Sin FROM calculation_set_up)  ,circular_average_results AS (SELECT `WindowEventTime` AS `EventTime`, `TagName`, sum(Diff_Average_Cos)/sum(Time_Difference) AS Cos_Time_Averages, sum(Diff_Average_Sin)/sum(Time_Difference) AS Sin_Time_Averages, array_min(array(1, sqrt(pow(Cos_Time_Averages, 2) + pow(Sin_Time_Averages, 2)))) AS R, mod(2*pi() + atan2(Sin_Time_Averages, Cos_Time_Averages), 2*pi()) AS Circular_Average_Value_in_Radians, (Circular_Average_Value_in_Radians * (360 - 0)) / (2*pi())+ 0 AS Circular_Average_Value_in_Degrees FROM circular_average_calculations GROUP BY `TagName`, `WindowEventTime`) ,project AS (SELECT `EventTime`, `TagName`, Circular_Average_Value_in_Degrees AS `Value` FROM circular_average_results) ,pivot AS (SELECT * FROM project PIVOT (FIRST(`Value`) FOR `TagName` IN (\'MOCKED-TAGNAME\'))) SELECT * FROM pivot ORDER BY `EventTime` '
MOCKED_PARAMETER_DICT = {
    "business_unit": "mocked-business-unit",
    "region": "mocked-region",
    "asset": "mocked-asset",
    "data_security_level": "mocked-data-security-level",
    "data_type": "mocked-data-type",
    "tag_names": ["MOCKED-TAGNAME"],
    "start_date": "2011-01-01",
    "end_date": "2011-01-02",
    "time_interval_rate": "15",
    "time_interval_unit": "minute",
    "lower_bound": 0,
    "upper_bound": 360,
    "include_bad_data": True,
    "pivot": False,
}


def test_circular_average(mocker: MockerFixture):
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_connection_close = mocker.spy(MockedDBConnection, "close")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(
        MockedCursor,
        "fetchall_arrow",
        return_value=pa.Table.from_pandas(
            pd.DataFrame(data={"a": [1], "b": [2], "c": [3], "d": [4]})
        ),
    )
    mocked_close = mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    actual = circular_average_get(mocked_connection, MOCKED_PARAMETER_DICT)

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_QUERY)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_circular_average_offset_limit(mocker: MockerFixture):
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_connection_close = mocker.spy(MockedDBConnection, "close")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(
        MockedCursor,
        "fetchall_arrow",
        return_value=pa.Table.from_pandas(
            pd.DataFrame(data={"a": [1], "b": [2], "c": [3], "d": [4]})
        ),
    )
    mocked_close = mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    MOCKED_OFFSET_LIMIT = MOCKED_PARAMETER_DICT.copy()
    MOCKED_OFFSET_LIMIT["limit"] = 10
    MOCKED_OFFSET_LIMIT["offset"] = 10
    actual = circular_average_get(mocked_connection, MOCKED_OFFSET_LIMIT)

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(
        mocker.ANY, query=MOCKED_QUERY + MOCKED_QUERY_OFFSET_LIMIT
    )
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_circular_average_pivot(mocker: MockerFixture):
    mocked_cursor = mocker.spy(MockedDBConnection, "cursor")
    mocked_connection_close = mocker.spy(MockedDBConnection, "close")
    mocked_execute = mocker.spy(MockedCursor, "execute")
    mocked_fetch_all = mocker.patch.object(
        MockedCursor,
        "fetchall_arrow",
        return_value=pa.Table.from_pandas(
            pd.DataFrame(data={"a": [1], "b": [2], "c": [3], "d": [4]})
        ),
    )
    mocked_close = mocker.spy(MockedCursor, "close")
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value=MockedDBConnection())

    mocked_connection = DatabricksSQLConnection(
        SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN
    )

    MOCKED_PARAMETER_DICT["pivot"] = True
    actual = circular_average_get(mocked_connection, MOCKED_PARAMETER_DICT)

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_QUERY_PIVOT)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_circular_average_fails(mocker: MockerFixture):
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
        circular_average_get(mocked_connection, MOCKED_PARAMETER_DICT)


def test_circular_average_tag_name_not_list_fails(mocker: MockerFixture):
    MOCKED_PARAMETER_DICT["tag_names"] = "abc"
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
        circular_average_get(mocked_connection, MOCKED_PARAMETER_DICT)
