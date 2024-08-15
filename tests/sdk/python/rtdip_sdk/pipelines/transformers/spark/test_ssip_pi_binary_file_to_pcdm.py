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
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.ssip_pi_binary_file_to_pcdm import (
    SSIPPIBinaryFileToPCDMTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    PyPiLibrary,
)
from src.sdk.python.rtdip_sdk._sdk_utils.pandas import (
    _prepare_pandas_to_convert_to_spark,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DateType,
)
from datetime import datetime
import pandas as pd
from pandas.testing import assert_frame_equal


def test_ssip_binary_file_to_pcdm_setup():
    ssip_pi_binary_file_to_pcdm = SSIPPIBinaryFileToPCDMTransformer(None)
    assert ssip_pi_binary_file_to_pcdm.system_type().value == 2
    assert ssip_pi_binary_file_to_pcdm.libraries() == Libraries(
        maven_libraries=[],
        pypi_libraries=[
            PyPiLibrary(name="pyarrow", version="14.0.2", repo=None),
            PyPiLibrary(name="pandas", version="2.0.1", repo=None),
        ],
        pythonwheel_libraries=[],
    )
    assert isinstance(ssip_pi_binary_file_to_pcdm.settings(), dict)
    assert ssip_pi_binary_file_to_pcdm.pre_transform_validation()
    assert ssip_pi_binary_file_to_pcdm.post_transform_validation()


def _test_ssip_binary_file_to_pcdm_udf(
    spark_session: SparkSession, ssip_pi_binary_data: [dict], expected_data: [dict]
):
    parquet_df = pd.DataFrame(ssip_pi_binary_data)
    binary_data = parquet_df.to_parquet()
    ssip_pi_binary_df = pd.DataFrame(
        [
            {
                "path": "test",
                "modificationTime": "1",
                "length": 172,
                "content": binary_data,
            }
        ]
    )

    ssip_pi_binary_df = _prepare_pandas_to_convert_to_spark(ssip_pi_binary_df)
    binary_file_df: DataFrame = spark_session.createDataFrame(ssip_pi_binary_df)

    expected_schema = StructType(
        [
            StructField("EventDate", DateType(), True),
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", StringType(), True),
            StructField("ValueType", StringType(), True),
            StructField("ChangeType", StringType(), True),
        ]
    )

    udf_expected_df = pd.DataFrame(expected_data)

    udf_actual_df = SSIPPIBinaryFileToPCDMTransformer(None)._convert_binary_to_pandas(
        ssip_pi_binary_df
    )

    ssip_binary_file_to_pcdm_transformer = SSIPPIBinaryFileToPCDMTransformer(
        binary_file_df
    )
    actual_df = ssip_binary_file_to_pcdm_transformer.transform()
    assert expected_schema == actual_df.schema
    assert_frame_equal(udf_expected_df, udf_actual_df)


def test_ssip_binary_file_to_pcdm_files(spark_session: SparkSession):
    ssip_pi_binary_data = [
        {
            "TagName": "Test1",
            "EventTime": datetime.fromisoformat("2023-04-19T16:41:55.002+00:00"),
            "Status": "Good",
            "Value": "1.0",
        },
        {
            "TagName": "Test2",
            "EventTime": datetime.fromisoformat("2023-04-19T16:41:55.056+00:00"),
            "Status": "Bad",
            "Value": "test",
        },
    ]

    expected_data = [
        {
            "EventDate": datetime(2023, 4, 19).date(),
            "TagName": "Test1",
            "EventTime": datetime.fromisoformat("2023-04-19T16:41:55.002+00:00"),
            "Status": "Good",
            "Value": "1.0",
            "ValueType": "string",
            "ChangeType": "insert",
        },
        {
            "EventDate": datetime(2023, 4, 19).date(),
            "TagName": "Test2",
            "EventTime": datetime.fromisoformat("2023-04-19T16:41:55.056+00:00"),
            "Status": "Bad",
            "Value": "test",
            "ValueType": "string",
            "ChangeType": "insert",
        },
    ]

    _test_ssip_binary_file_to_pcdm_udf(
        spark_session, ssip_pi_binary_data, expected_data
    )


def test_ssip_binary_file_to_pcdm_eventhub(spark_session: SparkSession):
    ssip_pi_binary_data = [
        {
            "TagName": "Test1",
            "EventTime": datetime.fromisoformat("2023-04-19T16:41:55.002+00:00"),
            "Status": "Good",
            "Value": "1.0",
            "ValueType": "float",
            "ChangeType": "merge",
        },
        {
            "TagName": "Test2",
            "EventTime": datetime.fromisoformat("2023-04-19T16:41:58.002+00:00"),
            "Status": "Good",
            "Value": "1.0",
            "ValueType": "string",
            "ChangeType": "insert",
        },
    ]

    expected_data = [
        {
            "EventDate": datetime(2023, 4, 19).date(),
            "TagName": "Test1",
            "EventTime": datetime.fromisoformat("2023-04-19T16:41:55.002+00:00"),
            "Status": "Good",
            "Value": "1.0",
            "ValueType": "float",
            "ChangeType": "merge",
        },
        {
            "EventDate": datetime(2023, 4, 19).date(),
            "TagName": "Test2",
            "EventTime": datetime.fromisoformat("2023-04-19T16:41:58.002+00:00"),
            "Status": "Good",
            "Value": "1.0",
            "ValueType": "string",
            "ChangeType": "insert",
        },
    ]

    _test_ssip_binary_file_to_pcdm_udf(
        spark_session, ssip_pi_binary_data, expected_data
    )
