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
import json
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.ssip_pi_binary_json_to_pcdm import (
    SSIPPIJsonStreamToPCDMTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DateType,
)
from datetime import datetime


def test_ssip_binary_json_to_pcdm_setup():
    ssip_pi_binary_json_to_pcdm = SSIPPIJsonStreamToPCDMTransformer(
        None, None, "col1", "col2"
    )
    assert ssip_pi_binary_json_to_pcdm.system_type().value == 2
    assert ssip_pi_binary_json_to_pcdm.libraries() == Libraries(
        maven_libraries=[], pypi_libraries=[], pythonwheel_libraries=[]
    )
    assert isinstance(ssip_pi_binary_json_to_pcdm.settings(), dict)
    assert ssip_pi_binary_json_to_pcdm.pre_transform_validation()
    assert ssip_pi_binary_json_to_pcdm.post_transform_validation()


def test_ssip_binary_json_to_pcdm(spark_session: SparkSession):
    ssip_pi_binary_data = [
        {
            "TagName": "Test1",
            "EventTime": "2023-04-19T16:41:55.002+00:00",
            "Quality": "Good",
            "Value": "1.0",
        },
        {
            "TagName": "Test2",
            "EventTime": "2023-04-19T16:41:55.056+00:00",
            "Quality": "Bad",
            "Value": "test",
        },
    ]

    binary_json_df: DataFrame = spark_session.createDataFrame(
        [
            {
                "body": json.dumps(ssip_pi_binary_data[0]),
                "properties": {"PointType": "Float32", "Action": "Add"},
            },
            {
                "body": json.dumps(ssip_pi_binary_data[1]),
                "properties": {"PointType": "String", "Action": "Update"},
            },
        ]
    )

    expected_schema = StructType(
        [
            StructField("EventDate", DateType(), True),
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", StringType(), True),
            StructField("ValueType", StringType(), False),
            StructField("ChangeType", StringType(), True),
        ]
    )

    expected_data = [
        {
            "EventDate": datetime(2023, 4, 19).date(),
            "TagName": "Test1",
            "EventTime": datetime.fromisoformat("2023-04-19T16:41:55.002+00:00"),
            "Status": "Good",
            "Value": "1.0",
            "ValueType": "float",
            "ChangeType": "insert",
        },
        {
            "EventDate": datetime(2023, 4, 19).date(),
            "TagName": "Test2",
            "EventTime": datetime.fromisoformat("2023-04-19T16:41:55.056+00:00"),
            "Status": "Bad",
            "Value": "test",
            "ValueType": "string",
            "ChangeType": "update",
        },
    ]
    expected_df: DataFrame = spark_session.createDataFrame(
        schema=expected_schema, data=expected_data
    )
    ssip_binary_file_to_pcdm_transformer = SSIPPIJsonStreamToPCDMTransformer(
        spark_session, binary_json_df, "body", "properties"
    )
    actual_df = ssip_binary_file_to_pcdm_transformer.transform()

    assert expected_schema == actual_df.schema
    assert (
        expected_df.orderBy("TagName").collect()
        == actual_df.orderBy("TagName").collect()
    )
