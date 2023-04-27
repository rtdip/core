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
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.json_to_opcua import JsonToOPCUATransformer
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries, SystemType
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark import OPCUA_SCHEMA
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, ArrayType, StringType
import json

def test_spark_json_to_opcua(spark_session: SparkSession):
    eventhub_json_to_opcua_transformer = JsonToOPCUATransformer(source_column_name="body")
    opcua_json_data = '[{"NodeId":"ns=2;s=Test1","EndpointUrl":"opc.tcp://test.ot.test.com:4840/","DisplayName":"Test1","Value":{"Value":1.0,"SourceTimestamp":"2023-04-19T16:41:55.002Z"}},{"NodeId":"ns=2;s=Test2","EndpointUrl":"opc.tcp://test.ot.test.com:4840/","DisplayName":"Test2","Value":{"Value":2.0,"StatusCode":{"Symbol":"BadCommunicationError","Code":3},"SourceTimestamp":"2023-04-19T16:41:55.056Z"}}]'
    opcua_df: DataFrame = spark_session.createDataFrame([{"body": opcua_json_data}])

    expected_schema = StructType([
        StructField("body", StringType(), True),
        StructField("OPCUA", OPCUA_SCHEMA, True)
    ])

    expected_data = [
        {"body": '{"NodeId":"ns=2;s=Test1","EndpointUrl":"opc.tcp://test.ot.test.com:4840/","DisplayName":"Test1","Value":{"Value":1.0,"SourceTimestamp":"2023-04-19T16:41:55.002Z"}}',
        "OPCUA": {"ApplicationUri": None, "DisplayName":"Test1", "NodeId":"ns=2;s=Test1", "Value":{"SourceTimestamp":"2023-04-19T16:41:55.002Z", "Value":1.0,"StatusCode":None}}},
        {"body": '{"NodeId":"ns=2;s=Test2","EndpointUrl":"opc.tcp://test.ot.test.com:4840/","DisplayName":"Test2","Value":{"Value":2.0,"StatusCode":{"Symbol":"BadCommunicationError","Code":3},"SourceTimestamp":"2023-04-19T16:41:55.056Z"}}',
        "OPCUA": {"ApplicationUri": None, "NodeId":"ns=2;s=Test2", "DisplayName":"Test2","Value":{"Value":2.0,"StatusCode":{"Symbol":"BadCommunicationError","Code":3},"SourceTimestamp":"2023-04-19T16:41:55.056Z"}}}
    ]
    
    expected_df: DataFrame = spark_session.createDataFrame(
        schema=expected_schema,
        data=expected_data
    )

    actual_df = eventhub_json_to_opcua_transformer.transform(opcua_df)

    assert eventhub_json_to_opcua_transformer.system_type() == SystemType.PYSPARK
    assert isinstance(eventhub_json_to_opcua_transformer.libraries(), Libraries)
    assert expected_schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()