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
from src.sdk.python.rtdip_sdk.queries.time_series.time_weighted_average import (
    get as time_weighted_average_get,
)

SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "mock_databricks_token"
DATABRICKS_SQL_CONNECT = "databricks.sql.connect"
DATABRICKS_SQL_CONNECT_CURSOR = "databricks.sql.connect.cursor"
MOCKED_QUERY = 'WITH raw_events AS (SELECT DISTINCT `TagName`, from_utc_timestamp(to_timestamp(date_format(`EventTime`, \'yyyy-MM-dd HH:mm:ss.SSS\')), "+0000") AS `EventTime`,  `Status`,  `Value` FROM `mocked-business-unit`.`sensors`.`mocked-asset_mocked-data-security-level_events_mocked-data-type` WHERE to_date(`EventTime`) BETWEEN date_sub(to_date(to_timestamp("2011-01-01T00:00:00+00:00")), 1) AND date_add(to_date(to_timestamp("2011-01-02T23:59:59+00:00")), 1) AND `TagName` IN (\'MOCKED-TAGNAME\')  ) ,date_array AS (SELECT explode(sequence(from_utc_timestamp(to_timestamp("2011-01-01T00:00:00+00:00"), "+0000"), from_utc_timestamp(to_timestamp("2011-01-02T23:59:59+00:00"), "+0000"), INTERVAL \'15 minute\')) AS `EventTime`, explode(array(\'MOCKED-TAGNAME\')) AS `TagName`) ,boundary_events AS (SELECT coalesce(a.`TagName`, b.`TagName`) AS `TagName`, coalesce(a.`EventTime`, b.`EventTime`) AS `EventTime`, b.`Status`, b.`Value` FROM date_array a FULL OUTER JOIN raw_events b ON a.`EventTime` = b.`EventTime` AND a.`TagName` = b.`TagName`) ,window_buckets AS (SELECT `EventTime` AS window_start, LEAD(`EventTime`) OVER (ORDER BY `EventTime`) AS window_end FROM (SELECT distinct `EventTime` FROM date_array) ) ,window_events AS (SELECT /*+ RANGE_JOIN(b, 900 ) */ b.`TagName`, b.`EventTime`, a.window_start AS `WindowEventTime`, b.`Status`, b.`Value` FROM boundary_events b LEFT OUTER JOIN window_buckets a ON a.window_start <= b.`EventTime` AND a.window_end > b.`EventTime`) ,fill_status AS (SELECT *, last_value(`Status`, true) OVER (PARTITION BY `TagName` ORDER BY `EventTime` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `Fill_Status`, CASE WHEN `Fill_Status` = "Good" THEN `Value` ELSE null END AS `Good_Value` FROM window_events) ,fill_value AS (SELECT *, last_value(`Good_Value`, true) OVER (PARTITION BY `TagName` ORDER BY `EventTime` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `Fill_Value` FROM fill_status) ,fill_step AS (SELECT *, false AS Step FROM fill_value) ,interpolate AS (SELECT *, CASE WHEN `Step` = false AND `Status` IS NULL AND `Value` IS NULL THEN lag(`EventTime`) OVER ( PARTITION BY `TagName` ORDER BY `EventTime` ) ELSE NULL END AS `Previous_EventTime`, CASE WHEN `Step` = false AND `Status` IS NULL AND `Value` IS NULL THEN lag(`Fill_Value`) OVER ( PARTITION BY `TagName` ORDER BY `EventTime` ) ELSE NULL END AS `Previous_Fill_Value`, lead(`EventTime`) OVER ( PARTITION BY `TagName` ORDER BY `EventTime` ) AS `Next_EventTime`, CASE WHEN `Step` = false AND `Status` IS NULL AND `Value` IS NULL THEN lead(`Fill_Value`) OVER ( PARTITION BY `TagName` ORDER BY `EventTime` ) ELSE NULL END AS `Next_Fill_Value`, CASE WHEN `Step` = false AND `Status` IS NULL AND `Value` IS NULL THEN `Previous_Fill_Value` + ( (`Next_Fill_Value` - `Previous_Fill_Value`) * ( ( unix_timestamp(`EventTime`) - unix_timestamp(`Previous_EventTime`) ) / ( unix_timestamp(`Next_EventTime`) - unix_timestamp(`Previous_EventTime`) ) ) ) ELSE NULL END AS `Interpolated_Value`, coalesce(`Interpolated_Value`, `Fill_Value`) as `Event_Value` FROM fill_step ),twa_calculations AS (SELECT `TagName`, `EventTime`, `WindowEventTime`, `Step`, `Status`, `Value`, `Previous_EventTime`, `Previous_Fill_Value`, `Next_EventTime`, `Next_Fill_Value`, `Interpolated_Value`, `Fill_Status`, `Fill_Value`, `Event_Value`, lead(`Fill_Status`) OVER (PARTITION BY `TagName` ORDER BY `EventTime`) AS `Next_Status` , CASE WHEN `Next_Status` = "Good" OR (`Fill_Status` = "Good" AND `Next_Status` != "Good") THEN lead(`Event_Value`) OVER (PARTITION BY `TagName` ORDER BY `EventTime`) ELSE `Value` END AS `Next_Value_For_Status` , CASE WHEN `Fill_Status` = "Good" THEN `Next_Value_For_Status` ELSE 0 END AS `Next_Value` , CASE WHEN `Fill_Status` = "Good" AND `Next_Status` = "Good" THEN ((cast(`Next_EventTime` AS double) - cast(`EventTime` AS double)) / 60) WHEN `Fill_Status` = "Good" AND `Next_Status` != "Good" THEN ((cast(`Next_EventTime` AS integer) - cast(`EventTime` AS double)) / 60) ELSE 0 END AS good_minutes , CASE WHEN Step == false THEN ((`Event_Value` + `Next_Value`) * 0.5) * good_minutes ELSE (`Event_Value` * good_minutes) END AS twa_value FROM interpolate) ,twa AS (SELECT `TagName`, `WindowEventTime` AS `EventTime`, sum(twa_value) / sum(good_minutes) AS `Value` from twa_calculations GROUP BY `TagName`, `WindowEventTime`) ,project AS (SELECT * FROM twa WHERE `EventTime` BETWEEN to_timestamp("2011-01-01T00:00:00") AND to_timestamp("2011-01-02T23:59:59")) SELECT * FROM project ORDER BY `TagName`, `EventTime` '
MOCKED_QUERY_OFFSET_LIMIT = "LIMIT 10 OFFSET 10 "
MOCKED_QUERY_PIVOT = 'WITH raw_events AS (SELECT DISTINCT `TagName`, from_utc_timestamp(to_timestamp(date_format(`EventTime`, \'yyyy-MM-dd HH:mm:ss.SSS\')), "+0000") AS `EventTime`,  `Status`,  `Value` FROM `mocked-business-unit`.`sensors`.`mocked-asset_mocked-data-security-level_events_mocked-data-type` WHERE to_date(`EventTime`) BETWEEN date_sub(to_date(to_timestamp("2011-01-01T00:00:00+00:00")), 1) AND date_add(to_date(to_timestamp("2011-01-02T23:59:59+00:00")), 1) AND `TagName` IN (\'MOCKED-TAGNAME\')  ) ,date_array AS (SELECT explode(sequence(from_utc_timestamp(to_timestamp("2011-01-01T00:00:00+00:00"), "+0000"), from_utc_timestamp(to_timestamp("2011-01-02T23:59:59+00:00"), "+0000"), INTERVAL \'15 minute\')) AS `EventTime`, explode(array(\'MOCKED-TAGNAME\')) AS `TagName`) ,boundary_events AS (SELECT coalesce(a.`TagName`, b.`TagName`) AS `TagName`, coalesce(a.`EventTime`, b.`EventTime`) AS `EventTime`, b.`Status`, b.`Value` FROM date_array a FULL OUTER JOIN raw_events b ON a.`EventTime` = b.`EventTime` AND a.`TagName` = b.`TagName`) ,window_buckets AS (SELECT `EventTime` AS window_start, LEAD(`EventTime`) OVER (ORDER BY `EventTime`) AS window_end FROM (SELECT distinct `EventTime` FROM date_array) ) ,window_events AS (SELECT /*+ RANGE_JOIN(b, 900 ) */ b.`TagName`, b.`EventTime`, a.window_start AS `WindowEventTime`, b.`Status`, b.`Value` FROM boundary_events b LEFT OUTER JOIN window_buckets a ON a.window_start <= b.`EventTime` AND a.window_end > b.`EventTime`) ,fill_status AS (SELECT *, last_value(`Status`, true) OVER (PARTITION BY `TagName` ORDER BY `EventTime` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `Fill_Status`, CASE WHEN `Fill_Status` = "Good" THEN `Value` ELSE null END AS `Good_Value` FROM window_events) ,fill_value AS (SELECT *, last_value(`Good_Value`, true) OVER (PARTITION BY `TagName` ORDER BY `EventTime` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `Fill_Value` FROM fill_status)  ,fill_step AS (SELECT *, IFNULL(Step, false) AS Step FROM fill_value f LEFT JOIN `mocked-business-unit`.`sensors`.`mocked-asset_mocked-data-security-level_metadata` m ON f.`TagName` = m.`TagName`) ,interpolate AS (SELECT *, CASE WHEN `Step` = false AND `Status` IS NULL AND `Value` IS NULL THEN lag(`EventTime`) OVER ( PARTITION BY `TagName` ORDER BY `EventTime` ) ELSE NULL END AS `Previous_EventTime`, CASE WHEN `Step` = false AND `Status` IS NULL AND `Value` IS NULL THEN lag(`Fill_Value`) OVER ( PARTITION BY `TagName` ORDER BY `EventTime` ) ELSE NULL END AS `Previous_Fill_Value`, lead(`EventTime`) OVER ( PARTITION BY `TagName` ORDER BY `EventTime` ) AS `Next_EventTime`, CASE WHEN `Step` = false AND `Status` IS NULL AND `Value` IS NULL THEN lead(`Fill_Value`) OVER ( PARTITION BY `TagName` ORDER BY `EventTime` ) ELSE NULL END AS `Next_Fill_Value`, CASE WHEN `Step` = false AND `Status` IS NULL AND `Value` IS NULL THEN `Previous_Fill_Value` + ( (`Next_Fill_Value` - `Previous_Fill_Value`) * ( ( unix_timestamp(`EventTime`) - unix_timestamp(`Previous_EventTime`) ) / ( unix_timestamp(`Next_EventTime`) - unix_timestamp(`Previous_EventTime`) ) ) ) ELSE NULL END AS `Interpolated_Value`, coalesce(`Interpolated_Value`, `Fill_Value`) as `Event_Value` FROM fill_step ),twa_calculations AS (SELECT `TagName`, `EventTime`, `WindowEventTime`, `Step`, `Status`, `Value`, `Previous_EventTime`, `Previous_Fill_Value`, `Next_EventTime`, `Next_Fill_Value`, `Interpolated_Value`, `Fill_Status`, `Fill_Value`, `Event_Value`, lead(`Fill_Status`) OVER (PARTITION BY `TagName` ORDER BY `EventTime`) AS `Next_Status` , CASE WHEN `Next_Status` = "Good" OR (`Fill_Status` = "Good" AND `Next_Status` != "Good") THEN lead(`Event_Value`) OVER (PARTITION BY `TagName` ORDER BY `EventTime`) ELSE `Value` END AS `Next_Value_For_Status` , CASE WHEN `Fill_Status` = "Good" THEN `Next_Value_For_Status` ELSE 0 END AS `Next_Value` , CASE WHEN `Fill_Status` = "Good" AND `Next_Status` = "Good" THEN ((cast(`Next_EventTime` AS double) - cast(`EventTime` AS double)) / 60) WHEN `Fill_Status` = "Good" AND `Next_Status` != "Good" THEN ((cast(`Next_EventTime` AS integer) - cast(`EventTime` AS double)) / 60) ELSE 0 END AS good_minutes , CASE WHEN Step == false THEN ((`Event_Value` + `Next_Value`) * 0.5) * good_minutes ELSE (`Event_Value` * good_minutes) END AS twa_value FROM interpolate) ,twa AS (SELECT `TagName`, `WindowEventTime` AS `EventTime`, sum(twa_value) / sum(good_minutes) AS `Value` from twa_calculations GROUP BY `TagName`, `WindowEventTime`) ,project AS (SELECT * FROM twa WHERE `EventTime` BETWEEN to_timestamp("2011-01-01T00:00:00") AND to_timestamp("2011-01-02T23:59:59")) ,pivot AS (SELECT * FROM project PIVOT (FIRST(`Value`) FOR `TagName` IN (\'MOCKED-TAGNAME\'))) SELECT * FROM pivot ORDER BY `EventTime` '
METADATA_MOCKED_QUERY = 'WITH raw_events AS (SELECT DISTINCT `TagName`, from_utc_timestamp(to_timestamp(date_format(`EventTime`, \'yyyy-MM-dd HH:mm:ss.SSS\')), "+0000") AS `EventTime`,  `Status`,  `Value` FROM `mocked-business-unit`.`sensors`.`mocked-asset_mocked-data-security-level_events_mocked-data-type` WHERE to_date(`EventTime`) BETWEEN date_sub(to_date(to_timestamp("2011-01-01T00:00:00+00:00")), 1) AND date_add(to_date(to_timestamp("2011-01-02T23:59:59+00:00")), 1) AND `TagName` IN (\'MOCKED-TAGNAME\')  ) ,date_array AS (SELECT explode(sequence(from_utc_timestamp(to_timestamp("2011-01-01T00:00:00+00:00"), "+0000"), from_utc_timestamp(to_timestamp("2011-01-02T23:59:59+00:00"), "+0000"), INTERVAL \'15 minute\')) AS `EventTime`, explode(array(\'MOCKED-TAGNAME\')) AS `TagName`) ,boundary_events AS (SELECT coalesce(a.`TagName`, b.`TagName`) AS `TagName`, coalesce(a.`EventTime`, b.`EventTime`) AS `EventTime`, b.`Status`, b.`Value` FROM date_array a FULL OUTER JOIN raw_events b ON a.`EventTime` = b.`EventTime` AND a.`TagName` = b.`TagName`) ,window_buckets AS (SELECT `EventTime` AS window_start, LEAD(`EventTime`) OVER (ORDER BY `EventTime`) AS window_end FROM (SELECT distinct `EventTime` FROM date_array) ) ,window_events AS (SELECT /*+ RANGE_JOIN(b, 900 ) */ b.`TagName`, b.`EventTime`, a.window_start AS `WindowEventTime`, b.`Status`, b.`Value` FROM boundary_events b LEFT OUTER JOIN window_buckets a ON a.window_start <= b.`EventTime` AND a.window_end > b.`EventTime`) ,fill_status AS (SELECT *, last_value(`Status`, true) OVER (PARTITION BY `TagName` ORDER BY `EventTime` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `Fill_Status`, CASE WHEN `Fill_Status` = "Good" THEN `Value` ELSE null END AS `Good_Value` FROM window_events) ,fill_value AS (SELECT *, last_value(`Good_Value`, true) OVER (PARTITION BY `TagName` ORDER BY `EventTime` ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS `Fill_Value` FROM fill_status)  ,fill_step AS (SELECT *, IFNULL(Step, false) AS Step FROM fill_value f LEFT JOIN `mocked-business-unit`.`sensors`.`mocked-asset_mocked-data-security-level_metadata` m ON f.`TagName` = m.`TagName`) ,interpolate AS (SELECT *, CASE WHEN `Step` = false AND `Status` IS NULL AND `Value` IS NULL THEN lag(`EventTime`) OVER ( PARTITION BY `TagName` ORDER BY `EventTime` ) ELSE NULL END AS `Previous_EventTime`, CASE WHEN `Step` = false AND `Status` IS NULL AND `Value` IS NULL THEN lag(`Fill_Value`) OVER ( PARTITION BY `TagName` ORDER BY `EventTime` ) ELSE NULL END AS `Previous_Fill_Value`, lead(`EventTime`) OVER ( PARTITION BY `TagName` ORDER BY `EventTime` ) AS `Next_EventTime`, CASE WHEN `Step` = false AND `Status` IS NULL AND `Value` IS NULL THEN lead(`Fill_Value`) OVER ( PARTITION BY `TagName` ORDER BY `EventTime` ) ELSE NULL END AS `Next_Fill_Value`, CASE WHEN `Step` = false AND `Status` IS NULL AND `Value` IS NULL THEN `Previous_Fill_Value` + ( (`Next_Fill_Value` - `Previous_Fill_Value`) * ( ( unix_timestamp(`EventTime`) - unix_timestamp(`Previous_EventTime`) ) / ( unix_timestamp(`Next_EventTime`) - unix_timestamp(`Previous_EventTime`) ) ) ) ELSE NULL END AS `Interpolated_Value`, coalesce(`Interpolated_Value`, `Fill_Value`) as `Event_Value` FROM fill_step ),twa_calculations AS (SELECT `TagName`, `EventTime`, `WindowEventTime`, `Step`, `Status`, `Value`, `Previous_EventTime`, `Previous_Fill_Value`, `Next_EventTime`, `Next_Fill_Value`, `Interpolated_Value`, `Fill_Status`, `Fill_Value`, `Event_Value`, lead(`Fill_Status`) OVER (PARTITION BY `TagName` ORDER BY `EventTime`) AS `Next_Status` , CASE WHEN `Next_Status` = "Good" OR (`Fill_Status` = "Good" AND `Next_Status` != "Good") THEN lead(`Event_Value`) OVER (PARTITION BY `TagName` ORDER BY `EventTime`) ELSE `Value` END AS `Next_Value_For_Status` , CASE WHEN `Fill_Status` = "Good" THEN `Next_Value_For_Status` ELSE 0 END AS `Next_Value` , CASE WHEN `Fill_Status` = "Good" AND `Next_Status` = "Good" THEN ((cast(`Next_EventTime` AS double) - cast(`EventTime` AS double)) / 60) WHEN `Fill_Status` = "Good" AND `Next_Status` != "Good" THEN ((cast(`Next_EventTime` AS integer) - cast(`EventTime` AS double)) / 60) ELSE 0 END AS good_minutes , CASE WHEN Step == false THEN ((`Event_Value` + `Next_Value`) * 0.5) * good_minutes ELSE (`Event_Value` * good_minutes) END AS twa_value FROM interpolate) ,twa AS (SELECT `TagName`, `WindowEventTime` AS `EventTime`, sum(twa_value) / sum(good_minutes) AS `Value` from twa_calculations GROUP BY `TagName`, `WindowEventTime`) ,project AS (SELECT * FROM twa WHERE `EventTime` BETWEEN to_timestamp("2011-01-01T00:00:00") AND to_timestamp("2011-01-02T23:59:59")) SELECT * FROM project ORDER BY `TagName`, `EventTime` '
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
    "window_length": 1,
    "step": "false",
    "include_bad_data": True,
    "pivot": False,
}


def test_time_weighted_average(mocker: MockerFixture):
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

    actual = time_weighted_average_get(mocked_connection, MOCKED_PARAMETER_DICT)

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_QUERY)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_time_weighted_average_offset_limit(mocker: MockerFixture):
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

    MOCKED_PARAMETER_DICT_OFFSET_LIMIT = MOCKED_PARAMETER_DICT.copy()
    MOCKED_PARAMETER_DICT_OFFSET_LIMIT["offset"] = 10
    MOCKED_PARAMETER_DICT_OFFSET_LIMIT["limit"] = 10
    actual = time_weighted_average_get(
        mocked_connection, MOCKED_PARAMETER_DICT_OFFSET_LIMIT
    )

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(
        mocker.ANY, query=MOCKED_QUERY + MOCKED_QUERY_OFFSET_LIMIT
    )
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_time_weighted_average_with_window_size_mins(mocker: MockerFixture):
    MOCKED_PARAMETER_DICT["window_size_mins"] = 15
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

    actual = time_weighted_average_get(mocked_connection, MOCKED_PARAMETER_DICT)

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_QUERY)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_time_weighted_average_metadata_step(mocker: MockerFixture):
    MOCKED_PARAMETER_DICT["step"] = "metadata"
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

    actual = time_weighted_average_get(mocked_connection, MOCKED_PARAMETER_DICT)

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=METADATA_MOCKED_QUERY)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_time_weighted_average_pivot(mocker: MockerFixture):
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
    MOCKED_PARAMETER_DICT["step"] = "metadata"
    actual = time_weighted_average_get(mocked_connection, MOCKED_PARAMETER_DICT)

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_QUERY_PIVOT)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_time_weighted_average_fails(mocker: MockerFixture):
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
        time_weighted_average_get(mocked_connection, MOCKED_PARAMETER_DICT)


def test_time_weighted_average_tag_name_not_list_fails(mocker: MockerFixture):
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
        time_weighted_average_get(mocked_connection, MOCKED_PARAMETER_DICT)
