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
from src.sdk.python.rtdip_sdk.queries.time_series.resample import get as resample_get

SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "mock_databricks_token"
DATABRICKS_SQL_CONNECT = "databricks.sql.connect"
DATABRICKS_SQL_CONNECT_CURSOR = "databricks.sql.connect.cursor"
MOCKED_QUERY = 'WITH raw_events AS (SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(`EventTime`, \'yyyy-MM-dd HH:mm:ss.SSS\')), "+0000") AS `EventTime`, `TagName`,  `Status`,  `Value` FROM `mocked-business-unit`.`sensors`.`mocked-asset_mocked-data-security-level_events_mocked-data-type` WHERE `EventTime` BETWEEN to_timestamp("2011-01-01T00:00:00+00:00") AND to_timestamp("2011-01-02T23:59:59+00:00") AND `TagName` IN (\'MOCKED-TAGNAME\')  AND `Status` = \'Good\' ) ,date_array AS (SELECT explode(sequence(from_utc_timestamp(to_timestamp("2011-01-01T00:00:00+00:00"), "+0000"), from_utc_timestamp(to_timestamp("2011-01-02T23:59:59+00:00"), "+0000"), INTERVAL \'15 minute\')) AS timestamp_array) ,window_buckets AS (SELECT timestamp_array AS window_start, LEAD(timestamp_array) OVER (ORDER BY timestamp_array) AS window_end FROM date_array) ,resample AS (SELECT /*+ RANGE_JOIN(d, 900 ) */ d.window_start, d.window_end, e.`TagName`, avg(e.`Value`) OVER (PARTITION BY e.`TagName`, d.window_start ORDER BY e.`EventTime` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `Value` FROM window_buckets d INNER JOIN raw_events e ON d.window_start <= e.`EventTime` AND d.window_end > e.`EventTime`) ,project AS (SELECT window_start AS `EventTime`, `TagName`, `Value` FROM resample GROUP BY window_start, `TagName`, `Value` ORDER BY `TagName`, `EventTime` ) SELECT * FROM project '
MOCKED_QUERY_OFFSET_LIMIT = "LIMIT 10 OFFSET 10 "
MOCKED_QUERY_PIVOT = 'WITH raw_events AS (SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(`EventTime`, \'yyyy-MM-dd HH:mm:ss.SSS\')), "+0000") AS `EventTime`, `TagName`,  `Status`,  `Value` FROM `mocked-business-unit`.`sensors`.`mocked-asset_mocked-data-security-level_events_mocked-data-type` WHERE `EventTime` BETWEEN to_timestamp("2011-01-01T00:00:00+00:00") AND to_timestamp("2011-01-02T23:59:59+00:00") AND `TagName` IN (\'MOCKED-TAGNAME\')  AND `Status` = \'Good\' ) ,date_array AS (SELECT explode(sequence(from_utc_timestamp(to_timestamp("2011-01-01T00:00:00+00:00"), "+0000"), from_utc_timestamp(to_timestamp("2011-01-02T23:59:59+00:00"), "+0000"), INTERVAL \'15 minute\')) AS timestamp_array) ,window_buckets AS (SELECT timestamp_array AS window_start, LEAD(timestamp_array) OVER (ORDER BY timestamp_array) AS window_end FROM date_array) ,resample AS (SELECT /*+ RANGE_JOIN(d, 900 ) */ d.window_start, d.window_end, e.`TagName`, avg(e.`Value`) OVER (PARTITION BY e.`TagName`, d.window_start ORDER BY e.`EventTime` ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS `Value` FROM window_buckets d INNER JOIN raw_events e ON d.window_start <= e.`EventTime` AND d.window_end > e.`EventTime`) ,project AS (SELECT window_start AS `EventTime`, `TagName`, `Value` FROM resample GROUP BY window_start, `TagName`, `Value` ORDER BY `TagName`, `EventTime` ) ,pivot AS (SELECT * FROM project PIVOT (FIRST(`Value`) FOR `TagName` IN (\'MOCKED-TAGNAME\'))) SELECT * FROM pivot ORDER BY `EventTime` '
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
    "agg_method": "avg",
    "include_bad_data": False,
    "pivot": False,
}


def test_resample(mocker: MockerFixture):
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

    actual = resample_get(mocked_connection, MOCKED_PARAMETER_DICT)

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_QUERY)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_resample_offset_limit(mocker: MockerFixture):
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

    MOCKED_PARAMETER_OFFSET_LIMIT = MOCKED_PARAMETER_DICT.copy()
    MOCKED_PARAMETER_OFFSET_LIMIT["offset"] = 10
    MOCKED_PARAMETER_OFFSET_LIMIT["limit"] = 10
    actual = resample_get(mocked_connection, MOCKED_PARAMETER_OFFSET_LIMIT)

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(
        mocker.ANY, query=MOCKED_QUERY + MOCKED_QUERY_OFFSET_LIMIT
    )
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_resample_sample_rate_unit(mocker: MockerFixture):
    MOCKED_PARAMETER_DICT["sample_rate"] = "15"
    MOCKED_PARAMETER_DICT["sample_unit"] = "minute"
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

    actual = resample_get(mocked_connection, MOCKED_PARAMETER_DICT)

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_QUERY)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_resample_pivot(mocker: MockerFixture):
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
    actual = resample_get(mocked_connection, MOCKED_PARAMETER_DICT)

    mocked_cursor.assert_called_once()
    mocked_connection_close.assert_called_once()
    mocked_execute.assert_called_once_with(mocker.ANY, query=MOCKED_QUERY_PIVOT)
    mocked_fetch_all.assert_called_once()
    mocked_close.assert_called_once()
    assert isinstance(actual, pd.DataFrame)


def test_resample_fails(mocker: MockerFixture):
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
        resample_get(mocked_connection, MOCKED_PARAMETER_DICT)


def test_resample_tag_name_not_list_fails(mocker: MockerFixture):
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
        resample_get(mocked_connection, MOCKED_PARAMETER_DICT)
