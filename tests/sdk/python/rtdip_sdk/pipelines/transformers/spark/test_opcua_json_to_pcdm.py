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
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.opcua_json_to_pcdm import (
    OPCUAJsonToPCDMTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from dateutil import parser


def test_aio_json_to_pcdm(spark_session: SparkSession):
    opcua_json_data = '{"MessageId":"12345","MessageType":"test","PublisherId":"opcua_pub","Messages":[{"DataSetWriterId":12345,"Timestamp":"2024-05-07T09:54:31.6769914Z","Payload":{"tag_1":{"Value":100.2}}},{"DataSetWriterId":56789,"Timestamp":"2024-05-07T09:54:31.6509972Z","Payload":{"tag_2":{"Value":79}}}]}'
    opcua_df: DataFrame = spark_session.createDataFrame(
        [opcua_json_data], "string"
    ).toDF("body")

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
            "TagName": "tag_1",
            "Value": "100.2",
            "EventTime": parser.parse("2024-05-07T09:54:31.6769914Z"),
            "Status": "Good",
            "ValueType": "float",
            "ChangeType": "insert",
        },
        {
            "TagName": "tag_2",
            "Value": "79",
            "EventTime": parser.parse("2024-05-07T09:54:31.6509972Z"),
            "Status": "Good",
            "ValueType": "float",
            "ChangeType": "insert",
        },
    ]

    expected_df: DataFrame = spark_session.createDataFrame(
        schema=expected_schema, data=expected_data
    )

    opcua_json_to_pcdm_transformer = OPCUAJsonToPCDMTransformer(
        data=opcua_df, source_column_name="body"
    )
    actual_df = opcua_json_to_pcdm_transformer.transform()

    assert opcua_json_to_pcdm_transformer.system_type() == SystemType.PYSPARK
    assert isinstance(opcua_json_to_pcdm_transformer.libraries(), Libraries)
    assert expected_schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()
