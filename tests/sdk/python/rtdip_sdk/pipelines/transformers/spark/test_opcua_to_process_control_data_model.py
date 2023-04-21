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
sys.path.insert(0, '.')
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.opcua_to_process_control_data_model import OPCUAToProcessControlDataModel
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries, SystemType
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark import OPCUA_SCHEMA
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType, FloatType
from datetime import date, datetime
import json

def test_opcua_to_process_control_data_model(spark_session: SparkSession):
    opcua_to_process_control_data_model_transformer = OPCUAToProcessControlDataModel(source_column_name="OPCUA", status_null_value="Good")

    opcua_schema = StructType([
        StructField("body", StringType(), True),
        StructField("OPCUA", OPCUA_SCHEMA, True)
    ])

    opcua_data = [
        {"body": '{"NodeId":"ns=2;s=Test1","EndpointUrl":"opc.tcp://test.ot.test.com:4840/","DisplayName":"Test1","Value":{"Value":1.0,"SourceTimestamp":"2023-04-19T16:41:55.002Z"}}',
        "OPCUA": {"ApplicationUri": None, "DisplayName":"Test1", "NodeId":"ns=2;s=Test1", "Value":{"SourceTimestamp":"2023-04-19T16:41:55.002Z", "Value":1.0,"StatusCode":None}}},
        {"body": '{"NodeId":"ns=2;s=Test2","EndpointUrl":"opc.tcp://test.ot.test.com:4840/","DisplayName":"Test2","Value":{"Value":2.0,"StatusCode":{"Symbol":"BadCommunicationError","Code":3},"SourceTimestamp":"2023-04-19T16:41:55Z"}}',
        "OPCUA": {"ApplicationUri": None, "NodeId":"ns=2;s=Test2", "DisplayName":"Test2","Value":{"Value":2.0,"StatusCode":{"Symbol":"BadCommunicationError","Code":3},"SourceTimestamp":"2023-04-19T16:41:55Z"}}}
    ]
    
    opcua_df: DataFrame = spark_session.createDataFrame(
        schema=opcua_schema,
        data=opcua_data
    )

    expected_schema = StructType([
        StructField("EventDate", DateType(), True),
        StructField("TagName", StringType(), True),
        StructField("EventTime", TimestampType(), True),
        StructField("Status", StringType(), True),
        StructField("Value", StringType(), True),
    ])

    expected_data = [
        {"EventDate": date.fromisoformat("2023-04-19"), "TagName": "Test1", "EventTime": datetime.fromisoformat("2023-04-19T16:41:55.002+00:00"), "Status": "Good", "Value": 1.0},
        {"EventDate": date.fromisoformat("2023-04-19"), "TagName": "Test2", "EventTime": datetime.fromisoformat("2023-04-19T16:41:55+00:00"), "Status": "BadCommunicationError", "Value": 2.0},
    ]

    expected_df: DataFrame = spark_session.createDataFrame(
        schema=expected_schema,
        data=expected_data
    )

    actual_df = opcua_to_process_control_data_model_transformer.transform(opcua_df)

    assert opcua_to_process_control_data_model_transformer.system_type() == SystemType.PYSPARK
    assert isinstance(opcua_to_process_control_data_model_transformer.libraries(), Libraries)
    assert expected_schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()