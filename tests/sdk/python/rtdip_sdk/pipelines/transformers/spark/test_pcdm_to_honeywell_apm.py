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
from datetime import datetime
import uuid


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
    PCDM_to_honeywell_eventhub_json_transformer = PCDMToHoneywellAPMTransformer(
        data=pcdm_df, history_samples_per_message=3
    )

    actual_df = PCDM_to_honeywell_eventhub_json_transformer.transform()
    df_row = actual_df.collect()[0]
    assert isinstance(uuid.UUID(df_row["CloudPlatformEvent"]["CreatorId"]), uuid.UUID)
    assert (
        PCDM_to_honeywell_eventhub_json_transformer.system_type() == SystemType.PYSPARK
    )
    assert isinstance(
        PCDM_to_honeywell_eventhub_json_transformer.libraries(), Libraries
    )
    assert len(df_row) == 2
    assert len(df_row["CloudPlatformEvent"]) == 12
    assert len(df_row["CloudPlatformEvent"]["Body"]) == 3
    assert len(df_row["CloudPlatformEvent"]["BodyProperties"]) == 2
    assert len(df_row["CloudPlatformEvent"]["BodyProperties"][0]) == 2
    assert len(df_row["CloudPlatformEvent"]["BodyProperties"][1]) == 2
