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
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.edgex_opcua_json_to_pcdm import (
    EdgeXOPCUAJsonToPCDMTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime, timezone


def test_edgex_json_to_pcdm(spark_session: SparkSession):
    edgex_json_data = '{"apiVersion":"v2","id":"test","deviceName":"testDevice","profileName":"test","sourceName":"Bool","origin":1683866798739958852,"readings":[{"id":"test","origin":1683866798739958852,"deviceName":"test","resourceName":"BoolTag","profileName":"Random","valueType":"Bool","value":"true"}]}'
    edgex_df: DataFrame = spark_session.createDataFrame([{"body": edgex_json_data}])

    expected_schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), False),
            StructField("Value", StringType(), True),
            StructField("ValueType", StringType(), False),
            StructField("ChangeType", StringType(), False),
        ]
    )

    expected_data = [
        {
            "TagName": "BoolTag",
            "EventTime": datetime.fromisoformat("2023-05-12 04:46:38.739958"),
            "Status": "Good",
            "Value": "true",
            "ValueType": "bool",
            "ChangeType": "insert",
        }
    ]

    expected_df: DataFrame = spark_session.createDataFrame(
        schema=expected_schema, data=expected_data
    )

    eventhub_json_to_edgex_transformer = EdgeXOPCUAJsonToPCDMTransformer(
        data=edgex_df, source_column_name="body"
    )
    actual_df = eventhub_json_to_edgex_transformer.transform()

    assert eventhub_json_to_edgex_transformer.system_type() == SystemType.PYSPARK
    assert isinstance(eventhub_json_to_edgex_transformer.libraries(), Libraries)
    assert expected_schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()
