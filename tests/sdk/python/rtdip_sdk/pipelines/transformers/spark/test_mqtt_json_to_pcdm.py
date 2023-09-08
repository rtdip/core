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
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.mqtt_json_to_pcdm import (
    MQTTJsonToPCDMTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime, timezone


def test_mqtt_json_to_pcdm(spark_session: SparkSession):
    mqtt_json_data = (
        '{"d":["1685025760.46"],"dID":"502","m":"data","t":1694013193129,"v":"8"}'
    )
    mqtt_df: DataFrame = spark_session.createDataFrame([{"body": mqtt_json_data}])

    expected_schema = StructType(
        [
            StructField("EventTime", TimestampType(), True),
            StructField("TagName", StringType(), True),
            StructField("Status", StringType(), False),
            StructField("Value", StringType(), True),
            StructField("ValueType", StringType(), True),
            StructField("ChangeType", StringType(), False),
        ]
    )

    expected_data = [
        {
            "EventTime": datetime.fromisoformat("2023-09-06T15:13:13.000+00:00"),
            "TagName": "502_obc_timeStamp",
            "Status": "Good",
            "Value": "1685025760.46",
            "ValueType": "double",
            "ChangeType": "insert",
        }
    ]

    expected_df: DataFrame = spark_session.createDataFrame(
        schema=expected_schema, data=expected_data
    )

    mqtt_json_to_pcdm_transformer = MQTTJsonToPCDMTransformer(
        data=mqtt_df, source_column_name="body", version=10
    )
    actual_df = mqtt_json_to_pcdm_transformer.transform()

    assert mqtt_json_to_pcdm_transformer.system_type() == SystemType.PYSPARK
    assert isinstance(mqtt_json_to_pcdm_transformer.libraries(), Libraries)
    assert expected_schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()
