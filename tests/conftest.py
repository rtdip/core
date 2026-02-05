# Copyright 2022 RTDIP
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
from datetime import datetime, timezone
import pytest
import os
import shutil

from src.sdk.python.rtdip_sdk.connectors.grpc.spark_connector import SparkConnection
from src.sdk.python.rtdip_sdk.pipelines.destinations import *  # NOSONAR
from src.sdk.python.rtdip_sdk.pipelines.sources import *  # NOSONAR
from src.sdk.python.rtdip_sdk.pipelines.utilities.spark.session import (
    SparkSessionUtility,
)

SPARK_TESTING_CONFIGURATION = {
    "spark.executor.cores": "4",
    "spark.executor.instances": "4",
    "spark.sql.shuffle.partitions": "4",
    "spark.app.name": "test_app",
    "spark.master": "local[*]",
}

datetime_format = "%Y-%m-%dT%H:%M:%S.%f000Z"


@pytest.fixture(scope="session")
def spark_session():
    spark = SparkSessionUtility(SPARK_TESTING_CONFIGURATION.copy()).execute()
    path = spark.conf.get("spark.sql.warehouse.dir")
    prefix = "file:"
    if path.startswith(prefix):
        path = path[len(prefix) :]
    if os.path.isdir(path):
        shutil.rmtree(path)
    yield spark
    spark.stop()
    if os.path.isdir(path):
        shutil.rmtree(path)


@pytest.fixture(scope="session")
def spark_connection(spark_session: SparkSession):
    table_name = "test_table"
    data = [
        {
            "EventTime": datetime(2022, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
            "TagName": "TestTag",
            "Status": "Good",
            "Value": 1.5,
        },
        {
            "EventTime": datetime(2022, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            "TagName": "TestTag",
            "Status": "Good",
            "Value": 2.0,
        },
        {
            "EventTime": datetime(2022, 1, 2, 0, 0, 0, tzinfo=timezone.utc),
            "TagName": "TestTag",
            "Status": "Good",
            "Value": 1.0,
        },
    ]
    df = spark_session.createDataFrame(data)
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)
    return SparkConnection(spark=spark_session)


def expected_result(data, limit="null", offset="null", next="null"):
    expected_df = pd.json_normalize(data)
    expected = expected_df.to_json(orient="table", index=False, date_unit="ns")
    expected = (
        expected.replace(',"tz":"UTC"', "").rstrip("}")
        + ',"pagination":{'
        + '"limit":{},"offset":{},"next":{}'.format(limit, offset, next)
        + "}}"
    )
    return expected


@pytest.fixture(scope="session")
def api_test_data():
    # Mock Raw Data
    test_raw_data = {
        "EventTime": datetime.now(timezone.utc),
        "TagName": "TestTag",
        "Status": "Good",
        "Value": 1.5,
    }
    mock_raw_data = test_raw_data.copy()
    mock_raw_data["EventTime"] = mock_raw_data["EventTime"].strftime(datetime_format)
    mock_raw_df = {
        "data": json.dumps(mock_raw_data, separators=(",", ":")),
        "count": 1,
        "sample_row": json.dumps(mock_raw_data, separators=(",", ":")),
    }
    expected_raw = expected_result(test_raw_data)

    # Mock Aggregated Data
    test_agg_data = {
        "EventTime": datetime.now(timezone.utc),
        "TagName": "TestTag",
        "Value": 1.5,
    }
    mock_agg_data = test_agg_data.copy()
    mock_agg_data["EventTime"] = mock_agg_data["EventTime"].strftime(datetime_format)
    mock_agg_df = {
        "data": json.dumps(mock_agg_data, separators=(",", ":")),
        "count": 1,
        "sample_row": json.dumps(mock_agg_data, separators=(",", ":")),
    }
    expected_agg = expected_result(test_agg_data)

    # Summary Data
    test_plot_data = {
        "EventTime": datetime.now(timezone.utc),
        "TagName": "TestTag",
        "Average": 1.01,
        "Min": 1.01,
        "Max": 1.01,
        "First": 1.01,
        "Last": 1.01,
        "StdDev": 1.01,
    }

    mock_plot_data = test_plot_data.copy()
    mock_plot_data["EventTime"] = mock_plot_data["EventTime"].strftime(datetime_format)
    mock_plot_df = {
        "data": json.dumps(mock_plot_data, separators=(",", ":")),
        "count": 1,
        "sample_row": json.dumps(mock_plot_data, separators=(",", ":")),
    }
    expected_plot = expected_result(test_plot_data)

    test_summary_data = {
        "TagName": "TestTag",
        "Count": 10.0,
        "Avg": 5.05,
        "Min": 1.0,
        "Max": 10.0,
        "StDev": 3.02,
        "Sum": 25.0,
        "Var": 0.0,
    }

    mock_summary_df = {
        "data": json.dumps(test_summary_data, separators=(",", ":")),
        "count": 1,
        "sample_row": json.dumps(test_summary_data, separators=(",", ":")),
    }
    expected_summary = expected_result(test_summary_data)

    test_metadata = {
        "TagName": "TestTag",
        "UoM": "UoM1",
        "Description": "Test Description",
    }

    mock_metadata_df = {
        "data": json.dumps(test_metadata, separators=(",", ":")),
        "count": 1,
        "sample_row": json.dumps(test_metadata, separators=(",", ":")),
    }
    expected_metadata = expected_result(test_metadata)

    test_latest_data = {
        "TagName": "TestTag",
        "EventTime": datetime.now(timezone.utc),
        "Status": "Good",
        "Value": "1.01",
        "ValueType": "string",
        "GoodEventTime": datetime.now(timezone.utc),
        "GoodValue": "1.01",
        "GoodValueType": "string",
    }

    mock_latest_data = test_latest_data.copy()
    mock_latest_data["EventTime"] = mock_latest_data["EventTime"].strftime(
        datetime_format
    )
    mock_latest_data["GoodEventTime"] = mock_latest_data["GoodEventTime"].strftime(
        datetime_format
    )
    mock_latest_df = {
        "data": json.dumps(mock_latest_data, separators=(",", ":")),
        "count": 1,
        "sample_row": json.dumps(mock_latest_data, separators=(",", ":")),
    }
    expected_latest = expected_result(test_latest_data)

    expected_sql = expected_result(test_raw_data, "100", "100")

    return {
        "mock_data_raw": mock_raw_df,
        "expected_raw": expected_raw,
        "mock_data_agg": mock_agg_df,
        "expected_agg": expected_agg,
        "mock_data_plot": mock_plot_df,
        "expected_plot": expected_plot,
        "mock_data_summary": mock_summary_df,
        "expected_summary": expected_summary,
        "mock_data_metadata": mock_metadata_df,
        "expected_metadata": expected_metadata,
        "mock_data_latest": mock_latest_df,
        "expected_latest": expected_latest,
        "expected_sql": expected_sql,
    }
