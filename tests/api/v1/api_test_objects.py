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

from pytest_mock import MockerFixture
from tests.sdk.python.rtdip_sdk.connectors.odbc.test_db_sql_connector import (
    MockedDBConnection,
)
from tests.sdk.python.rtdip_sdk.queries.time_series._test_base import (
    DATABRICKS_SQL_CONNECT,
)
import os

START_DATE = "2011-01-01T00:00:00+00:00"
END_DATE = "2011-01-02T00:00:00+00:00"
BASE_URL = "https://test"

BASE_MOCKED_PARAMETER_DICT = {
    "business_unit": "mocked-buiness-unit",
    "region": "mocked-region",
    "asset": "mocked-asset",
    "data_security_level": "mocked-data-security-level",
}

METADATA_MOCKED_PARAMETER_DICT = BASE_MOCKED_PARAMETER_DICT.copy()
METADATA_MOCKED_PARAMETER_DICT["tag_name"] = "MOCKED-TAGNAME1"

METADATA_MOCKED_PARAMETER_ERROR_DICT = METADATA_MOCKED_PARAMETER_DICT.copy()
METADATA_MOCKED_PARAMETER_ERROR_DICT.pop("business_unit")

METADATA_POST_MOCKED_PARAMETER_DICT = METADATA_MOCKED_PARAMETER_DICT.copy()
METADATA_POST_MOCKED_PARAMETER_DICT.pop("tag_name")

METADATA_POST_BODY_MOCKED_PARAMETER_DICT = {}
METADATA_POST_BODY_MOCKED_PARAMETER_DICT["tag_name"] = [
    "MOCKED-TAGNAME1",
    "MOCKED-TAGNAME2",
]

RAW_MOCKED_PARAMETER_DICT = BASE_MOCKED_PARAMETER_DICT.copy()
RAW_MOCKED_PARAMETER_DICT["data_type"] = "mocked-data-type"
RAW_MOCKED_PARAMETER_DICT["tag_name"] = "MOCKED-TAGNAME1"
RAW_MOCKED_PARAMETER_DICT["include_bad_data"] = True
RAW_MOCKED_PARAMETER_DICT["start_date"] = START_DATE
RAW_MOCKED_PARAMETER_DICT["end_date"] = END_DATE

RAW_POST_MOCKED_PARAMETER_DICT = RAW_MOCKED_PARAMETER_DICT.copy()
RAW_POST_MOCKED_PARAMETER_DICT.pop("tag_name")

RAW_POST_BODY_MOCKED_PARAMETER_DICT = {}
RAW_POST_BODY_MOCKED_PARAMETER_DICT["tag_name"] = ["MOCKED-TAGNAME1", "MOCKED-TAGNAME2"]

RAW_MOCKED_PARAMETER_ERROR_DICT = RAW_MOCKED_PARAMETER_DICT.copy()
RAW_MOCKED_PARAMETER_ERROR_DICT.pop("start_date")

SQL_POST_MOCKED_PARAMETER_DICT = {}
SQL_POST_MOCKED_PARAMETER_DICT["limit"] = 100
SQL_POST_MOCKED_PARAMETER_DICT["offset"] = 100


SQL_POST_BODY_MOCKED_PARAMETER_DICT = {}
SQL_POST_BODY_MOCKED_PARAMETER_DICT["sql_statement"] = "SELECT * FROM 1"

RESAMPLE_MOCKED_PARAMETER_DICT = RAW_MOCKED_PARAMETER_DICT.copy()
RESAMPLE_MOCKED_PARAMETER_ERROR_DICT = RAW_MOCKED_PARAMETER_ERROR_DICT.copy()

RESAMPLE_MOCKED_PARAMETER_DICT["sample_rate"] = "15"
RESAMPLE_MOCKED_PARAMETER_DICT["sample_unit"] = "minute"
RESAMPLE_MOCKED_PARAMETER_DICT["time_interval_rate"] = "15"
RESAMPLE_MOCKED_PARAMETER_DICT["time_interval_unit"] = "minute"
RESAMPLE_MOCKED_PARAMETER_DICT["agg_method"] = "avg"
RESAMPLE_MOCKED_PARAMETER_ERROR_DICT["sample_rate"] = "15"
RESAMPLE_MOCKED_PARAMETER_ERROR_DICT["sample_unit"] = "minute"
RESAMPLE_MOCKED_PARAMETER_ERROR_DICT["time_interval_rate"] = "1"
RESAMPLE_MOCKED_PARAMETER_ERROR_DICT["time_interval_unit"] = "minute"
RESAMPLE_MOCKED_PARAMETER_ERROR_DICT["agg_method"] = "avg"

RESAMPLE_POST_MOCKED_PARAMETER_DICT = RESAMPLE_MOCKED_PARAMETER_DICT.copy()
RESAMPLE_POST_MOCKED_PARAMETER_DICT.pop("tag_name")

RESAMPLE_POST_BODY_MOCKED_PARAMETER_DICT = {}
RESAMPLE_POST_BODY_MOCKED_PARAMETER_DICT["tag_name"] = [
    "MOCKED-TAGNAME1",
    "MOCKED-TAGNAME2",
]

PLOT_MOCKED_PARAMETER_DICT = RAW_MOCKED_PARAMETER_DICT.copy()
PLOT_MOCKED_PARAMETER_ERROR_DICT = RAW_MOCKED_PARAMETER_ERROR_DICT.copy()

PLOT_MOCKED_PARAMETER_DICT["sample_rate"] = "15"
PLOT_MOCKED_PARAMETER_DICT["sample_unit"] = "minute"
PLOT_MOCKED_PARAMETER_DICT["time_interval_rate"] = "15"
PLOT_MOCKED_PARAMETER_DICT["time_interval_unit"] = "minute"
PLOT_MOCKED_PARAMETER_ERROR_DICT["sample_rate"] = "15"
PLOT_MOCKED_PARAMETER_ERROR_DICT["sample_unit"] = "minute"
PLOT_MOCKED_PARAMETER_ERROR_DICT["time_interval_rate"] = "1"
PLOT_MOCKED_PARAMETER_ERROR_DICT["time_interval_unit"] = "minute"

PLOT_POST_MOCKED_PARAMETER_DICT = RESAMPLE_MOCKED_PARAMETER_DICT.copy()
PLOT_POST_MOCKED_PARAMETER_DICT.pop("tag_name")

PLOT_POST_BODY_MOCKED_PARAMETER_DICT = {}
PLOT_POST_BODY_MOCKED_PARAMETER_DICT["tag_name"] = [
    "MOCKED-TAGNAME1",
    "MOCKED-TAGNAME2",
]

INTERPOLATE_MOCKED_PARAMETER_DICT = RESAMPLE_MOCKED_PARAMETER_DICT.copy()
INTERPOLATE_MOCKED_PARAMETER_ERROR_DICT = RESAMPLE_MOCKED_PARAMETER_ERROR_DICT.copy()

INTERPOLATE_MOCKED_PARAMETER_DICT["interpolation_method"] = "forward_fill"
INTERPOLATE_MOCKED_PARAMETER_ERROR_DICT["interpolation_method"] = "forward_fill"

INTERPOLATE_POST_MOCKED_PARAMETER_DICT = INTERPOLATE_MOCKED_PARAMETER_DICT.copy()
INTERPOLATE_POST_MOCKED_PARAMETER_DICT.pop("tag_name")

INTERPOLATE_POST_BODY_MOCKED_PARAMETER_DICT = {}
INTERPOLATE_POST_BODY_MOCKED_PARAMETER_DICT["tag_name"] = [
    "MOCKED-TAGNAME1",
    "MOCKED-TAGNAME2",
]

INTERPOLATION_AT_TIME_MOCKED_PARAMETER_DICT = BASE_MOCKED_PARAMETER_DICT.copy()
INTERPOLATION_AT_TIME_MOCKED_PARAMETER_ERROR_DICT = BASE_MOCKED_PARAMETER_DICT.copy()

INTERPOLATION_AT_TIME_MOCKED_PARAMETER_DICT["tag_name"] = ["MOCKED-TAGNAME1"]
INTERPOLATION_AT_TIME_MOCKED_PARAMETER_DICT["timestamps"] = [START_DATE, END_DATE]
INTERPOLATION_AT_TIME_MOCKED_PARAMETER_DICT["window_length"] = 10
INTERPOLATION_AT_TIME_MOCKED_PARAMETER_DICT["data_type"] = "mocked-data-type"
INTERPOLATION_AT_TIME_MOCKED_PARAMETER_DICT["include_bad_data"] = True
INTERPOLATION_AT_TIME_MOCKED_PARAMETER_ERROR_DICT["data_type"] = "mocked-data-type"
INTERPOLATION_AT_TIME_MOCKED_PARAMETER_ERROR_DICT["window_length"] = 10
INTERPOLATION_AT_TIME_MOCKED_PARAMETER_ERROR_DICT["include_bad_data"] = True

INTERPOLATION_AT_TIME_POST_MOCKED_PARAMETER_DICT = (
    INTERPOLATION_AT_TIME_MOCKED_PARAMETER_DICT.copy()
)
INTERPOLATION_AT_TIME_POST_MOCKED_PARAMETER_DICT.pop("tag_name")

INTERPOLATION_AT_TIME_POST_BODY_MOCKED_PARAMETER_DICT = {}
INTERPOLATION_AT_TIME_POST_BODY_MOCKED_PARAMETER_DICT["tag_name"] = ["MOCKED-TAGNAME1"]

TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT = RAW_MOCKED_PARAMETER_DICT.copy()
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_ERROR_DICT = (
    RAW_MOCKED_PARAMETER_ERROR_DICT.copy()
)

TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT["window_size_mins"] = "15"
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT["time_interval_rate"] = "15"
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT["time_interval_unit"] = "minute"
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT["window_length"] = 10
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT["step"] = "metadata"
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_ERROR_DICT["window_size_mins"] = "15"
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_ERROR_DICT["time_interval_rate"] = "15"
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_ERROR_DICT["time_interval_unit"] = "minute"
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_ERROR_DICT["window_length"] = 10
TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_ERROR_DICT["step"] = "metadata"

TIME_WEIGHTED_AVERAGE_POST_MOCKED_PARAMETER_DICT = (
    TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT.copy()
)
TIME_WEIGHTED_AVERAGE_POST_MOCKED_PARAMETER_DICT.pop("tag_name")

TIME_WEIGHTED_AVERAGE_POST_BODY_MOCKED_PARAMETER_DICT = {}
TIME_WEIGHTED_AVERAGE_POST_BODY_MOCKED_PARAMETER_DICT["tag_name"] = [
    "MOCKED-TAGNAME1",
    "MOCKED-TAGNAME2",
]
CIRCULAR_AVERAGE_MOCKED_PARAMETER_DICT = RAW_MOCKED_PARAMETER_DICT.copy()
CIRCULAR_AVERAGE_MOCKED_PARAMETER_ERROR_DICT = RAW_MOCKED_PARAMETER_ERROR_DICT.copy()

CIRCULAR_AVERAGE_MOCKED_PARAMETER_DICT["time_interval_rate"] = "15"
CIRCULAR_AVERAGE_MOCKED_PARAMETER_DICT["time_interval_unit"] = "minute"
CIRCULAR_AVERAGE_MOCKED_PARAMETER_DICT["lower_bound"] = 5
CIRCULAR_AVERAGE_MOCKED_PARAMETER_DICT["upper_bound"] = 20
CIRCULAR_AVERAGE_MOCKED_PARAMETER_ERROR_DICT["time_interval_rate"] = "15"
CIRCULAR_AVERAGE_MOCKED_PARAMETER_ERROR_DICT["time_interval_unit"] = "minute"
CIRCULAR_AVERAGE_MOCKED_PARAMETER_ERROR_DICT["lower_bound"] = 5
CIRCULAR_AVERAGE_MOCKED_PARAMETER_ERROR_DICT["upper_bound"] = 20

CIRCULAR_AVERAGE_POST_MOCKED_PARAMETER_DICT = (
    CIRCULAR_AVERAGE_MOCKED_PARAMETER_DICT.copy()
)
CIRCULAR_AVERAGE_POST_MOCKED_PARAMETER_DICT.pop("tag_name")

CIRCULAR_AVERAGE_POST_BODY_MOCKED_PARAMETER_DICT = {}
CIRCULAR_AVERAGE_POST_BODY_MOCKED_PARAMETER_DICT["tag_name"] = [
    "MOCKED-TAGNAME1",
    "MOCKED-TAGNAME2",
]

SUMMARY_MOCKED_PARAMETER_DICT = BASE_MOCKED_PARAMETER_DICT.copy()
SUMMARY_MOCKED_PARAMETER_DICT["data_type"] = "mocked-data-type"
SUMMARY_MOCKED_PARAMETER_DICT["tag_name"] = "MOCKED-TAGNAME1"
SUMMARY_MOCKED_PARAMETER_DICT["include_bad_data"] = True
SUMMARY_MOCKED_PARAMETER_DICT["start_date"] = START_DATE
SUMMARY_MOCKED_PARAMETER_DICT["end_date"] = END_DATE

SUMMARY_POST_MOCKED_PARAMETER_DICT = SUMMARY_MOCKED_PARAMETER_DICT.copy()
SUMMARY_POST_MOCKED_PARAMETER_DICT.pop("tag_name")

SUMMARY_POST_BODY_MOCKED_PARAMETER_DICT = {}
SUMMARY_POST_BODY_MOCKED_PARAMETER_DICT["tag_name"] = [
    "MOCKED-TAGNAME1",
    "MOCKED-TAGNAME2",
]

SUMMARY_MOCKED_PARAMETER_ERROR_DICT = SUMMARY_MOCKED_PARAMETER_DICT.copy()
SUMMARY_MOCKED_PARAMETER_ERROR_DICT.pop("start_date")

TEST_HEADERS = {
    "Authorization": "Bearer Test Token",
    "x-databricks-server-hostname": "test_server",
    "x-databricks-http-path": "test_path",
}
TEST_HEADERS_POST = {
    "Authorization": "Bearer Test Token",
    "Content-Type": "application/json",
    "x-databricks-server-hostname": "test_server",
    "x-databricks-http-path": "test_path",
}


# Batch api test parameters
BATCH_MOCKED_PARAMETER_DICT = {
    "region": "mocked-region",
}

BATCH_POST_PAYLOAD_SINGLE_WITH_GET = {
    "requests": [
        {
            "url": "/events/summary",
            "method": "GET",
            "headers": TEST_HEADERS,
            "params": SUMMARY_MOCKED_PARAMETER_DICT.copy(),
        }
    ]
}

BATCH_POST_PAYLOAD_SINGLE_WITH_MISSING_BUSINESS_UNIT = {
    "requests": [
        {
            "url": "/events/summary",
            "method": "GET",
            "headers": TEST_HEADERS,
            "params": SUMMARY_MOCKED_PARAMETER_DICT.copy(),
        }
    ]
}
BATCH_POST_PAYLOAD_SINGLE_WITH_MISSING_BUSINESS_UNIT["requests"][0]["params"].pop(
    "business_unit"
)


BATCH_POST_PAYLOAD_SINGLE_WITH_MISSING_BUSINESS_UNIT = {
    "requests": [
        {
            "url": "/events/summary",
            "method": "GET",
            "headers": TEST_HEADERS,
            "params": SUMMARY_MOCKED_PARAMETER_DICT.copy(),
        }
    ]
}
BATCH_POST_PAYLOAD_SINGLE_WITH_MISSING_BUSINESS_UNIT["requests"][0]["params"].pop(
    "business_unit"
)


BATCH_POST_PAYLOAD_SINGLE_WITH_POST = {
    "requests": [
        {
            "url": "/events/timeweightedaverage",
            "method": "POST",
            "headers": TEST_HEADERS,
            "params": TIME_WEIGHTED_AVERAGE_MOCKED_PARAMETER_DICT,
            "body": TIME_WEIGHTED_AVERAGE_POST_BODY_MOCKED_PARAMETER_DICT,
        }
    ]
}

BATCH_POST_PAYLOAD_SINGLE_WITH_GET_ERROR_DICT = {
    "requests": [
        {
            "url": "/api/v1/events/raw",  # Invalid URL since it should be /events/raw
            "method": "GET",
            "headers": TEST_HEADERS,
            "params": SUMMARY_MOCKED_PARAMETER_DICT,
        }
    ]
}

BATCH_POST_PAYLOAD_SINGLE_WITH_POST_ERROR_DICT = {
    "requests": [
        {
            "url": "/events/raw",
            "method": "POST",
            "headers": TEST_HEADERS,
            "params": RAW_MOCKED_PARAMETER_DICT,
            # No body supplied
        }
    ]
}

BATCH_POST_PAYLOAD_MULTIPLE = {
    "requests": [
        {
            "url": "/events/interpolationattime",
            "method": "GET",
            "headers": TEST_HEADERS,
            "params": INTERPOLATION_AT_TIME_MOCKED_PARAMETER_DICT,
        },
        {
            "url": "/events/circularaverage",
            "method": "POST",
            "headers": TEST_HEADERS,
            "params": CIRCULAR_AVERAGE_MOCKED_PARAMETER_DICT,
            "body": CIRCULAR_AVERAGE_POST_BODY_MOCKED_PARAMETER_DICT,
        },
    ]
}

BATCH_POST_PAYLOAD_ONE_SUCCESS_ONE_FAIL = {
    "requests": [
        {
            "url": "/sql/execute",
            "method": "POST",
            "headers": TEST_HEADERS,
            "params": {},
            "body": {
                "sql_statement": "SELECT * FROM 1",
            },
        },
        {
            "url": "/events/raw",
            "method": "GET",
            "headers": TEST_HEADERS,
            "params": {},
        },
    ]
}

BATCH_POST_PAYLOAD_ONE_SUCCESS_ONE_FAIL = {
    "requests": [
        {
            "url": "/sql/execute",
            "method": "POST",
            "headers": TEST_HEADERS,
            "params": {},
            "body": {
                "sql_statement": "SELECT * FROM 1",
            },
        },
        {
            "url": "/events/raw",
            "method": "GET",
            "headers": TEST_HEADERS,
            "params": {},
        },
    ]
}

# Tag mapping test parameters

MOCK_TAG_MAPPING_SINGLE = {
    "outputs": [
        {
            "TagName": "Tagname1",
            "CatalogName": "rtdip",
            "SchemaName": "sensors",
            "DataTable": "asset1_restricted_events_float",
        }
    ]
}

MOCK_TAG_MAPPING_MULTIPLE = {
    "outputs": [
        {
            "TagName": "Tagname1",
            "CatalogName": "rtdip",
            "SchemaName": "sensors",
            "DataTable": "asset1_restricted_events_float",
        },
        {
            "TagName": "Tagname2",
            "CatalogName": "rtdip",
            "SchemaName": "sensors",
            "DataTable": "asset1_restricted_events_float",
        },
        {
            "TagName": "Tagname3",
            "CatalogName": "rtdip",
            "SchemaName": "sensors",
            "DataTable": "asset2_restricted_events_integer",
        },
    ]
}

MOCK_TAG_MAPPING_EMPTY = {
    "outputs": [
        {
            "TagName": "Tagname1",
            "CatalogName": None,
            "SchemaName": None,
            "DataTable": None,
        }
    ]
}

MOCK_TAG_MAPPING_BODY = {"dataframe_records": [{"TagName": "MOCKED-TAGNAME1"}]}

MOCK_MAPPING_ENDPOINT_URL = "https://mockdatabricksmappingurl.com/serving-endpoints/metadata-mapping/invocations"


# Mocker set-up utility


def mocker_setup(
    mocker: MockerFixture,
    patch_method,
    test_data,
    side_effect=None,
    patch_side_effect=None,
    tag_mapping_data=None,
):
    mocker.patch(
        DATABRICKS_SQL_CONNECT,
        return_value=MockedDBConnection(),
        side_effect=side_effect,
    )

    if patch_side_effect is not None:
        mocker.patch(patch_method, side_effect=patch_side_effect)
    else:
        mocker.patch(patch_method, return_value=test_data)

    mocker.patch("src.api.auth.azuread.get_azure_ad_token", return_value="token")

    # Create a mock response object for tag mapping endpoint with a .json() method that returns the mock data
    if tag_mapping_data is not None:
        mock_response = mocker.MagicMock()
        mock_response.json.return_value = tag_mapping_data
        mock_response.status_code = 200

        # Patch 'requests.post' to return the mock response
        mocker.patch("requests.post", return_value=mock_response)

    return mocker
