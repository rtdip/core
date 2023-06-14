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
from src.sdk.python.rtdip_sdk.queries.time_series.time_weighted_average import get as time_weighted_get
import pandas as pd
import pyarrow as pa
import pytest
import pytz
from pytest_mock import MockerFixture
from tests.sdk.python.rtdip_sdk.connectors.odbc.test_db_sql_connector import MockedDBConnection, MockedCursor 
from src.sdk.python.rtdip_sdk.connectors import DatabricksSQLConnection

class MockedMetadataFunction: 
    def metadata_get(self) -> pd.DataFrame:
        return pd.DataFrame

    
SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "mock_databricks_token"
DATABRICKS_SQL_CONNECT = 'databricks.sql.connect'
MOCKED_PARAMETER_DICT = {
        "business_unit": "mocked-buiness-unit",
        "region": "mocked-region",
        "asset": "mocked-asset",
        "data_security_level": "mocked-data-security-level",
        "data_type": "mocked-data-type",
        "tag_names": ["MOCKED-TAGNAME-1"],
        "start_date": "2022-03-01T00:00:00+00:00",
        "end_date": "2022-03-02T23:59:59+00:00",
        "window_size_mins": 10,
        "include_bad_data": False,
        "step": "true"
        }

df =  {"EventTime": [pd.to_datetime("2022-01-01 00:10:00+00:00").replace(tzinfo=pytz.timezone("Etc/UTC")), pd.to_datetime("2022-01-01 14:10:00+00:00").replace(tzinfo=pytz.timezone("Etc/UTC"))], "TagName": ["MOCKED-TAGNAME", "MOCKED-TAGNAME"], "Status": ["Good", "Good"], "Value":[177.09220, 160.01111]}

def test_time_weighted_average_with_date_only(mocker: MockerFixture):
    MOCKED_PARAMETER_DICT["start_date"]="2022-03-01"
    MOCKED_PARAMETER_DICT["end_date"]="2022-03-02"
    mocker.patch.object(MockedCursor, "fetchall_arrow", return_value = pa.Table.from_pandas(pd.DataFrame(data=df)))
    
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection())
   
    mocked_connection = DatabricksSQLConnection(SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN)

    actual = time_weighted_get(mocked_connection, MOCKED_PARAMETER_DICT)
    
    assert isinstance(actual, pd.DataFrame)

def test_time_weighted_average_with_datetime_only(mocker: MockerFixture):
    MOCKED_PARAMETER_DICT["start_date"]="2022-03-01T00:00:00"
    MOCKED_PARAMETER_DICT["end_date"]="2022-03-02T23:59:59"
    mocker.patch.object(MockedCursor, "fetchall_arrow", return_value = pa.Table.from_pandas(pd.DataFrame(data=df)))
    
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection())
   
    mocked_connection = DatabricksSQLConnection(SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN)

    actual = time_weighted_get(mocked_connection, MOCKED_PARAMETER_DICT)
    
    assert isinstance(actual, pd.DataFrame)

def test_time_weighted_average_with_datetimezone(mocker: MockerFixture):
    MOCKED_PARAMETER_DICT["start_date"]="2022-03-01T00:00:00+00:00"
    MOCKED_PARAMETER_DICT["end_date"]="2022-03-02T23:59:59+00:00"
    mocker.patch.object(MockedCursor, "fetchall_arrow", return_value = pa.Table.from_pandas(pd.DataFrame(data=df)))
    
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection())
   
    mocked_connection = DatabricksSQLConnection(SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN)

    actual = time_weighted_get(mocked_connection, MOCKED_PARAMETER_DICT)
    
    assert isinstance(actual, pd.DataFrame)

def test_time_weighted_average_step_enabled(mocker: MockerFixture):
    MOCKED_PARAMETER_DICT["step"]="true"
    mocker.patch.object(MockedCursor, "fetchall_arrow", return_value = pa.Table.from_pandas(pd.DataFrame(data=df)))
    
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection())
   
    mocked_connection = DatabricksSQLConnection(SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN)

    actual = time_weighted_get(mocked_connection, MOCKED_PARAMETER_DICT)
    
    assert isinstance(actual, pd.DataFrame)

def test_time_weighted_average_step_disabled(mocker: MockerFixture):
    MOCKED_PARAMETER_DICT["step"]="false"
    mocker.patch.object(MockedCursor, "fetchall_arrow", return_value = pa.Table.from_pandas(pd.DataFrame(data=df)))
    
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection())
   
    mocked_connection = DatabricksSQLConnection(SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN)

    actual = time_weighted_get(mocked_connection, MOCKED_PARAMETER_DICT)
    
    assert isinstance(actual, pd.DataFrame)

def test_time_weighted_average_with_window_length(mocker: MockerFixture):
    MOCKED_PARAMETER_DICT["window_length"]=10
    mocker.patch.object(MockedCursor, "fetchall_arrow", return_value = pa.Table.from_pandas(pd.DataFrame(data=df)))
    
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection())
   
    mocked_connection = DatabricksSQLConnection(SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN)

    actual = time_weighted_get(mocked_connection, MOCKED_PARAMETER_DICT)
    
    assert isinstance(actual, pd.DataFrame)

def test_time_weighted_average_fails(mocker: MockerFixture):
    mocker.patch(DATABRICKS_SQL_CONNECT, return_value = MockedDBConnection())
    mocker.patch('src.sdk.python.rtdip_sdk.queries.time_series.time_weighted_average', return_value = Exception)

    mocked_connection = DatabricksSQLConnection(SERVER_HOSTNAME, HTTP_PATH, ACCESS_TOKEN)
    
    with pytest.raises(Exception):
        time_weighted_get(mocked_connection, MOCKED_PARAMETER_DICT)