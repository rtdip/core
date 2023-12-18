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
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.opc_publisher_opcae_json_to_pcdm import (
    OPCPublisherOPCAEJsonToPCDMTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark import (
    OPC_PUBLISHER_AE_SCHEMA,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    DoubleType,
    BooleanType,
)
from datetime import datetime


def test_opc_publisher_json_to_pcdm(spark_session: SparkSession):
    opcua_json_data = '[{"NodeId":"ns=6;s=MyLevel.Alarm","EndpointUrl":"opc.tcp://xxxxxxxxx/OPCUA/SimulationServer","DisplayName":"MyLevelAlarm","Value":{"ConditionId":{"Value":"https://www.prosysopc.com/OPCUA/SampleAddressSpace#s=MyLevel.Alarm","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"AckedState":{"Value":"Unacknowledged","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"AckedState/FalseState":{"Value":"Unacknowledged","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"AckedState/Id":{"Value":false,"SourceTimestamp":"2023-10-19T13:08:08.503Z"},"AckedState/TrueState":{"Value":"Acknowledged","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"ActiveState":{"Value":"Inactive","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"ActiveState/FalseState":{"Value":"Inactive","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"ActiveState/Id":{"Value":false,"SourceTimestamp":"2023-10-19T13:08:08.503Z"},"ActiveState/TrueState":{"Value":"Active","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"EnabledState":{"Value":"Enabled","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"EnabledState/FalseState":{"Value":"Disabled","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"EnabledState/Id":{"Value":true,"SourceTimestamp":"2023-10-19T13:08:08.503Z"},"EnabledState/TrueState":{"Value":"Enabled","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"EventId":{"Value":"AAAAAAAAGycAAAAAAAAbJg==","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"EventType":{"Value":"i=9482","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"HighHighLimit":{"Value":90,"SourceTimestamp":"2023-10-19T13:08:08.503Z"},"HighLimit":{"Value":70,"SourceTimestamp":"2023-10-19T13:08:08.503Z"},"InputNode":{"Value":null,"SourceTimestamp":"2023-10-19T13:08:08.503Z"},"LowLimit":{"Value":30,"SourceTimestamp":"2023-10-19T13:08:08.503Z"},"LowLowLimit":{"Value":10,"SourceTimestamp":"2023-10-19T13:08:08.503Z"},"Message":{"Value":"Level exceeded","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"Quality":{"Value":null,"SourceTimestamp":"2023-10-19T13:08:08.503Z"},"ReceiveTime":{"Value":"2023-10-19T13:08:08.503Z","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"Retain":{"Value":true,"SourceTimestamp":"2023-10-19T13:08:08.503Z"},"Severity":{"Value":500,"SourceTimestamp":"2023-10-19T13:08:08.503Z"},"SourceName":{"Value":"MyLevel","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"SourceNode":{"Value":"https://www.prosysopc.com/OPCUA/SampleAddressSpace#s=MyLevel","SourceTimestamp":"2023-10-19T13:08:08.503Z"},"Time":{"Value":"2023-10-19T13:08:08.503Z","SourceTimestamp":"2023-10-19T13:08:08.503Z"}}}]'
    opcua_df: DataFrame = spark_session.createDataFrame([{"body": opcua_json_data}])

    expected_schema = StructType(
        [
            StructField("NodeId", StringType(), True),
            StructField("DisplayName", StringType(), True),
            StructField("ConditionId", StringType(), True),
            StructField("AckedState", StringType(), True),
            StructField("AckedState/FalseState", StringType(), True),
            StructField("AckedState/Id", BooleanType(), True),
            StructField("AckedState/TrueState", StringType(), True),
            StructField("ActiveState", StringType(), True),
            StructField("ActiveState/FalseState", StringType(), True),
            StructField("ActiveState/Id", BooleanType(), True),
            StructField("ActiveState/TrueState", StringType(), True),
            StructField("EnabledState", StringType(), True),
            StructField("EnabledState/FalseState", StringType(), True),
            StructField("EnabledState/Id", BooleanType(), True),
            StructField("EnabledState/TrueState", StringType(), True),
            StructField("EventId", StringType(), True),
            StructField("EventType", StringType(), True),
            StructField("HighHighLimit", DoubleType(), True),
            StructField("HighLimit", DoubleType(), True),
            StructField("InputNode", StringType(), True),
            StructField("LowLimit", DoubleType(), True),
            StructField("LowLowLimit", DoubleType(), True),
            StructField("Message", StringType(), True),
            StructField("Quality", StringType(), True),
            StructField("ReceiveTime", TimestampType(), True),
            StructField("Retain", BooleanType(), True),
            StructField("Severity", DoubleType(), True),
            StructField("SourceName", StringType(), True),
            StructField("SourceNode", StringType(), True),
            StructField("EventTime", TimestampType(), True),
        ]
    )

    expected_data = [
        {
            "NodeId": "ns=6;s=MyLevel.Alarm",
            "DisplayName": "MyLevelAlarm",
            "ConditionId": "https://www.prosysopc.com/OPCUA/SampleAddressSpace#s=MyLevel.Alarm",
            "AckedState": "Unacknowledged",
            "AckedState/FalseState": "Unacknowledged",
            "AckedState/Id": False,
            "AckedState/TrueState": "Acknowledged",
            "ActiveState": "Inactive",
            "ActiveState/FalseState": "Inactive",
            "ActiveState/Id": False,
            "ActiveState/TrueState": "Active",
            "EnabledState": "Enabled",
            "EnabledState/FalseState": "Disabled",
            "EnabledState/Id": True,
            "EnabledState/TrueState": "Enabled",
            "EventId": "AAAAAAAAGycAAAAAAAAbJg==",
            "EventType": "i=9482",
            "HighHighLimit": 90.0,
            "HighLimit": 70.0,
            "InputNode": None,
            "LowLimit": 30.0,
            "LowLowLimit": 10.0,
            "Message": "Level exceeded",
            "Quality": None,
            "ReceiveTime": datetime.fromisoformat("2023-10-19T13:08:08.503+00:00"),
            "Retain": True,
            "Severity": 500.0,
            "SourceName": "MyLevel",
            "SourceNode": "https://www.prosysopc.com/OPCUA/SampleAddressSpace#s=MyLevel",
            "EventTime": datetime.fromisoformat("2023-10-19T13:08:08.503+00:00"),
        }
    ]

    expected_df: DataFrame = spark_session.createDataFrame(
        schema=expected_schema, data=expected_data
    )
    eventhub_json_to_opcae_transformer = OPCPublisherOPCAEJsonToPCDMTransformer(
        opcua_df, source_column_name="body"
    )
    actual_df = eventhub_json_to_opcae_transformer.transform()

    assert eventhub_json_to_opcae_transformer.system_type() == SystemType.PYSPARK
    assert isinstance(eventhub_json_to_opcae_transformer.libraries(), Libraries)
    assert expected_schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()
