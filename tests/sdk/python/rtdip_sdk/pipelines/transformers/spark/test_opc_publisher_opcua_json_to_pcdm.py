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
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.opc_publisher_opcua_json_to_pcdm import (
    OPCPublisherOPCUAJsonToPCDMTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark import (
    OPC_PUBLISHER_SCHEMA,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime


def test_opc_publisher_json_to_pcdm(spark_session: SparkSession):
    opcua_json_data = '[{"NodeId":"ns=2;s=Test1","EndpointUrl":"opc.tcp://test.ot.test.com:4840/","DisplayName":"Test1","Value":{"Value":1.0,"SourceTimestamp":"2023-04-19T16:41:55.002Z"}},{"NodeId":"ns=2;s=Test2","EndpointUrl":"opc.tcp://test.ot.test.com:4840/","DisplayName":"Test2","Value":{"Value":2.0,"StatusCode":{"Symbol":"BadCommunicationError","Code":3},"SourceTimestamp":"2023-04-19T16:41:55.056Z"}}]'
    opcua_df: DataFrame = spark_session.createDataFrame([{"body": opcua_json_data}])

    expected_schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), True),
            StructField("Value", StringType(), True),
            StructField("ValueType", StringType(), False),
            StructField("ChangeType", StringType(), False),
        ]
    )

    expected_data = [
        {
            "TagName": "Test1",
            "EventTime": datetime.fromisoformat("2023-04-19T16:41:55.002+00:00"),
            "Status": "Good",
            "Value": "1.0",
            "ValueType": "float",
            "ChangeType": "insert",
        },
        {
            "TagName": "Test2",
            "EventTime": datetime.fromisoformat("2023-04-19T16:41:55.056+00:00"),
            "Status": "BadCommunicationError",
            "Value": "2.0",
            "ValueType": "float",
            "ChangeType": "insert",
        },
    ]

    expected_df: DataFrame = spark_session.createDataFrame(
        schema=expected_schema, data=expected_data
    )
    eventhub_json_to_opcua_transformer = OPCPublisherOPCUAJsonToPCDMTransformer(
        opcua_df, source_column_name="body", status_null_value="Good"
    )
    actual_df = eventhub_json_to_opcua_transformer.transform()

    assert eventhub_json_to_opcua_transformer.system_type() == SystemType.PYSPARK
    assert isinstance(eventhub_json_to_opcua_transformer.libraries(), Libraries)
    assert expected_schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()
