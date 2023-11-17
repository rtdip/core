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

import logging
import pandas as pd
from _weather_query_builder import _query_builder

SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "mock_databricks_token"
DATABRICKS_SQL_CONNECT = "databricks.sql.connect"
DATABRICKS_SQL_CONNECT_CURSOR = "databricks.sql.connect.cursor"
INTERPOLATION_METHOD = "test/test/test"
MOCKED_QUERY_OFFSET_LIMIT = "LIMIT 10 OFFSET 10 "

MOCKED_PARAMETER_DICT_GRID = {
    "forecast": "mocked-forecast",
    "region": "mocked-region",
    "data_security_level": "mocked-data-security-level",
    "data_type": "mocked-data-type",
    "min_lat": 1.1,
    "max_lat": 1.1,
    "min_lon": 1.1,
    "max_lon": 1.1,
    "start_date": "2020-01-01",
    "end_date": "2020-01-02",
    "timestamp_column": "EventTime",
    }

MOCKED_PARAMETER_DICT_POINT = {
    "forecast": "mocked-forecast",
    "region": "mocked-region",
    "data_security_level": "mocked-data-security-level",
    "data_type": "mocked-data-type",
    "lat": 1.1,
    "lon": 1.1,
    "start_date": "2020-01-01",
    "end_date": "2020-01-02",
    "timestamp_column": "EventTime",

}
    
query = _query_builder(MOCKED_PARAMETER_DICT_POINT, "raw_point")

print(query)



