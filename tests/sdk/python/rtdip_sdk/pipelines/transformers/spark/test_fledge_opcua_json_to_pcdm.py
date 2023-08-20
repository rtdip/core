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
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.fledge_opcua_json_to_pcdm import (
    FledgeOPCUAJsonToPCDMTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime


def test_fledge_json_to_pcdm(spark_session: SparkSession):
    fledge_json_data = '[{"asset":"testAsset1","readings":{"testTag1":-0.913545458},"timestamp":"2023-05-03 08:45:42.509118+00:00"}, {"asset":"testAsset2","readings":{"testTag2":-0.913545458},"timestamp":"2023-05-04 08:45:42.509118+00:00"}]'
    fledge_df: DataFrame = spark_session.createDataFrame([{"body": fledge_json_data}])

    expected_schema = StructType(
        [
            StructField("TagName", StringType(), False),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), False),
            StructField("Value", StringType(), True),
            StructField("ValueType", StringType(), True),
            StructField("ChangeType", StringType(), False),
        ]
    )

    expected_data = [
        {
            "TagName": "testTag1",
            "Value": "-0.913545458",
            "EventTime": datetime.fromisoformat("2023-05-03T08:45:42.509118+00:00"),
            "Status": "Good",
            "ValueType": "float",
            "ChangeType": "insert",
        },
        {
            "TagName": "testTag2",
            "Value": "-0.913545458",
            "EventTime": datetime.fromisoformat("2023-05-04T08:45:42.509118+00:00"),
            "Status": "Good",
            "ValueType": "float",
            "ChangeType": "insert",
        },
    ]

    expected_df: DataFrame = spark_session.createDataFrame(
        schema=expected_schema, data=expected_data
    )

    eventhub_json_to_fledge_transformer = FledgeOPCUAJsonToPCDMTransformer(
        data=fledge_df, source_column_name="body"
    )
    actual_df = eventhub_json_to_fledge_transformer.transform()

    assert eventhub_json_to_fledge_transformer.system_type() == SystemType.PYSPARK
    assert isinstance(eventhub_json_to_fledge_transformer.libraries(), Libraries)
    assert expected_schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()
