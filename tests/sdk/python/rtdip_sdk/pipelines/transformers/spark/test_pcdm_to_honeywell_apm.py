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
import os

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.pcdm_to_honeywell_apm import (
    PCDMToHoneywellAPMTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)

from pyspark.sql import SparkSession, DataFrame
from pytest_mock import MockerFixture
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import regexp_replace
from datetime import datetime
import json


def test_pcdm_to_honeywell_apm(spark_session: SparkSession, mocker: MockerFixture):
    pcdm_schema = StructType(
        [
            StructField("TagName", StringType(), True),
            StructField("EventTime", TimestampType(), True),
            StructField("Status", StringType(), False),
            StructField("Value", StringType(), True),
            StructField("ValueType", StringType(), False),
            StructField("ChangeType", StringType(), False),
        ]
    )

    pcdm_data = [
        {
            "TagName": "test.item1",
            "EventTime": datetime.fromisoformat("2023-07-31T06:53:00+00:00"),
            "Status": "Good",
            "Value": 5.0,
            "ValueType": "float",
            "ChangeType": "insert",
        },
        {
            "TagName": "Test_item2",
            "EventTime": datetime.fromisoformat("2023-07-31T06:54:00+00:00"),
            "Status": "Good",
            "Value": 1,
            "ValueType": "float",
            "ChangeType": "insert",
        },
    ]

    pcdm_df: DataFrame = spark_session.createDataFrame(
        schema=pcdm_schema, data=pcdm_data
    )
    honeywell_json_data = {
        "CloudPlatformEvent": {
            "CreatedTime": "2023-08-10T06:53:00+00:00",
            "Id": "2b2a64f6-bfee-49f5-9d1b-04df844e80be",
            "CreatorId": "065a7343-a3b5-4ecd-9bac-19cdff5cf048",
            "CreatorType": "CloudPlatformSystem",
            "GeneratorId": None,
            "GeneratorType": "CloudPlatformTenant",
            "TargetId": "065a7343-a3b5-4ecd-9bac-19cdff5cf048",
            "TargetType": "CloudPlatformTenant",
            "TargetContext": None,
            "Body": {
                "type": "TextualBody",
                "value": '{"SystemGuid":"065a7343-a3b5-4ecd-9bac-19cdff5cf048","HistorySamples":[{"ItemName":"test.item1","Quality":"Good","Time":"2023-07-31T06:53:00+00:00","Value":5},{"ItemName":"Test_item2","Quality":"Good","Time":"2023-07-31T06:54:00+00:00","Value":1}]}',
                "format": "application/json",
            },
            "BodyProperties": [
                {"Key": "SystemType", "Value": "apm-system"},
                {"Key": "SystemGuid", "Value": "065a7343-a3b5-4ecd-9bac-19cdff5cf048"},
            ],
            "EventType": "DataChange.Update",
        },
        "AnnotationStreamIds": ",",
    }
    expected_df = spark_session.createDataFrame([honeywell_json_data])
    PCDM_to_honeywell_eventhub_json_transformer = PCDMToHoneywellAPMTransformer(
        data=pcdm_df, history_samples_per_message=3
    )

    actual_df = PCDM_to_honeywell_eventhub_json_transformer.transform()
    dict = actual_df.collect()[0]["CloudPlatformEvent"]

    assert len(dict) == 1
    assert (
        PCDM_to_honeywell_eventhub_json_transformer.system_type() == SystemType.PYSPARK
    )
    assert isinstance(
        PCDM_to_honeywell_eventhub_json_transformer.libraries(), Libraries
    )
    assert len(dict) == 1
    assert len(dict["CloudPlatformEvent"]) == 12
    assert len(dict["CloudPlatformEvent"]["Body"]) == 3
    assert len(dict["CloudPlatformEvent"]["BodyProperties"]) == 2
    assert len(dict["CloudPlatformEvent"]["BodyProperties"][0]) == 2
    assert len(dict["CloudPlatformEvent"]["BodyProperties"][1]) == 2
