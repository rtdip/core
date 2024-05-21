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
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.aio_json_to_pcdm import (
    AIOJsonToPCDMTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from dateutil import parser


def test_aio_json_to_pcdm(spark_session: SparkSession):
    aio_json_data = '{"SequenceNumber":12345,"Timestamp":"2024-05-13T13:05:10.975317Z","DataSetWriterName":"test","MessageType":"test","Payload":{"test_tag1":{"SourceTimestamp":"2024-05-13T13:05:19.7278555Z","Value":67},"test_tag2":{"SourceTimestamp":"2024-05-13T13:05:19.7288616Z","Value":165.5}}}'
    aio_df: DataFrame = spark_session.createDataFrame([aio_json_data], "string").toDF(
        "body"
    )

    expected_schema = StructType(
        [
            StructField("EventTime", TimestampType(), True),
            StructField("TagName", StringType(), False),
            StructField("Status", StringType(), False),
            StructField("Value", StringType(), True),
            StructField("ValueType", StringType(), False),
            StructField("ChangeType", StringType(), False),
        ]
    )

    expected_data = [
        {
            "TagName": "test_tag1",
            "Value": "67",
            "EventTime": parser.parse("2024-05-13T13:05:19.7278555Z"),
            "Status": "Good",
            "ValueType": "float",
            "ChangeType": "insert",
        },
        {
            "TagName": "test_tag2",
            "Value": "165.5",
            "EventTime": parser.parse("2024-05-13T13:05:19.7288616Z"),
            "Status": "Good",
            "ValueType": "float",
            "ChangeType": "insert",
        },
    ]

    expected_df: DataFrame = spark_session.createDataFrame(
        schema=expected_schema, data=expected_data
    )

    aio_json_to_pcdm_transformer = AIOJsonToPCDMTransformer(
        data=aio_df, source_column_name="body"
    )
    actual_df = aio_json_to_pcdm_transformer.transform()

    assert aio_json_to_pcdm_transformer.system_type() == SystemType.PYSPARK
    assert isinstance(aio_json_to_pcdm_transformer.libraries(), Libraries)
    assert expected_schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()
