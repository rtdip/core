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

SERVER_HOSTNAME = "mock.cloud.databricks.com"
HTTP_PATH = "sql/mock/mock-test"
ACCESS_TOKEN = "mock_databricks_token"
DATABRICKS_SQL_CONNECT = "databricks.sql.connect"
DATABRICKS_SQL_CONNECT_CURSOR = "databricks.sql.connect.cursor"
MOCKED_QUERY_OFFSET_LIMIT = "LIMIT 10 OFFSET 10 "
MOCKED_BASE_PARAMETER_DICT = {
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

RAW_MOCKED_QUERY = 'SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(`EventTime`, \'yyyy-MM-dd HH:mm:ss.SSS\')), "+0000") AS `EventTime`, `TagName`,  `Status`,  `Value` FROM `mocked-buiness-unit`.`sensors`.`mocked-asset_mocked-data-security-level_events_mocked-data-type` WHERE `EventTime` BETWEEN to_timestamp("2011-01-01T00:00:00+00:00") AND to_timestamp("2011-01-02T23:59:59+00:00") AND `TagName` IN (\'MOCKED-TAGNAME\') ORDER BY `TagName`, `EventTime` '
RESAMPLE_MOCKED_QUERY = 'SELECT DISTINCT from_utc_timestamp(to_timestamp(date_format(`EventTime`, \'yyyy-MM-dd HH:mm:ss.SSS\')), "+0000") AS `EventTime`, `TagName`,  `Status`,  `Value` FROM `mocked-buiness-unit`.`sensors`.`mocked-asset_mocked-data-security-level_events_mocked-data-type` WHERE `EventTime` BETWEEN to_timestamp("2011-01-01T00:00:00+00:00") AND to_timestamp("2011-01-02T23:59:59+00:00") AND `TagName` IN (\'MOCKED-TAGNAME\') ORDER BY `TagName`, `EventTime` '
