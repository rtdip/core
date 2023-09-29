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
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.honeywell_apm_to_pcdm import (
    HoneywellAPMJsonToPCDMTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime, timezone


def test_honeywell_apm_to_pcdm(spark_session: SparkSession):
    honeywell_json_data = '{"SystemTimeSeries": {"Id": "testId","TenantId": "testTenantId","IdType": "calculatedpoint","Samples": [{"ItemName": "test.item1", "Time": "2023-07-31T06:53:00+00:00","Value": "5.0","Unit": null,"NormalizedQuality": "good", "HighValue": null,"LowValue": null,"TargetValue": null},{"ItemName": "test_item2","Time": "2023-07-31T06:53:00+00:00","Value": 0.0,"Unit": null,"NormalizedQuality": "good","HighValue": null,"LowValue": null,"TargetValue": null},{"ItemName": "testItem3","Time": "2023-07-31T06:53:00.205+00:00","Value": "test_string","Unit": null,"NormalizedQuality": "good","HighValue": null,"LowValue": null,"TargetValue": null}]}}'
    honeywell_df: DataFrame = spark_session.createDataFrame(
        [{"body": honeywell_json_data}]
    )

    expected_schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), False),
            StructField("Value", StringType(), True),
            StructField("ValueType", StringType(), True),
            StructField("ChangeType", StringType(), False),
        ]
    )

    expected_data = [
        {
            "TagName": "test.item1",
            "EventTime": datetime.fromisoformat("2023-07-31T06:53:00+00:00"),
            "Status": "Good",
            "Value": 5.0,
            "ValueType": "float",
            "ChangeType": "insert",
        },
        {
            "TagName": "test_item2",
            "EventTime": datetime.fromisoformat("2023-07-31T06:53:00+00:00"),
            "Status": "Good",
            "Value": 0.0,
            "ValueType": "float",
            "ChangeType": "insert",
        },
        {
            "TagName": "testItem3",
            "EventTime": datetime.fromisoformat("2023-07-31T06:53:00.205+00:00"),
            "Status": "Good",
            "Value": "test_string",
            "ValueType": "string",
            "ChangeType": "insert",
        },
    ]

    expected_df: DataFrame = spark_session.createDataFrame(
        schema=expected_schema, data=expected_data
    )

    honeywell_eventhub_json_to_PCDM_transformer = HoneywellAPMJsonToPCDMTransformer(
        data=honeywell_df, source_column_name="body"
    )
    actual_df = honeywell_eventhub_json_to_PCDM_transformer.transform()

    assert (
        honeywell_eventhub_json_to_PCDM_transformer.system_type() == SystemType.PYSPARK
    )
    assert isinstance(
        honeywell_eventhub_json_to_PCDM_transformer.libraries(), Libraries
    )
    assert expected_schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()
