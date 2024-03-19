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
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.mirico_json_to_pcdm import (
    MiricoJsonToPCDMTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from datetime import datetime
import pytest
from src.sdk.python.rtdip_sdk._sdk_utils.compare_versions import (
    _package_version_meets_minimum,
)

EVENTTIME = datetime.fromisoformat("2023-11-03T16:21:16")


def test_mirico_json_to_pcdm(spark_session: SparkSession):
    mirico_json_data = '{"timeStamp": "2023-11-03T16:21:16", "siteName": "test_site_name", "gasTypeId": 3, "quality": 10}'
    mirico_df: DataFrame = spark_session.createDataFrame([{"body": mirico_json_data}])

    expected_schema = StructType(
        [
            StructField("EventTime", TimestampType(), True),
            StructField("TagName", StringType(), False),
            StructField("Status", StringType(), False),
            StructField("Value", StringType(), True),
            StructField("ValueType", StringType(), True),
            StructField("ChangeType", StringType(), False),
        ]
    )

    expected_data = [
        {
            "EventTime": EVENTTIME,
            "TagName": "TEST_SITE_NAME_GASTYPEID",
            "Status": "Good",
            "Value": 3,
            "ValueType": "float",
            "ChangeType": "insert",
        },
        {
            "EventTime": EVENTTIME,
            "TagName": "TEST_SITE_NAME_QUALITY",
            "Status": "Good",
            "Value": 10,
            "ValueType": "float",
            "ChangeType": "insert",
        },
    ]

    expected_df: DataFrame = spark_session.createDataFrame(
        schema=expected_schema, data=expected_data
    )

    try:
        if _package_version_meets_minimum("pyspark", "3.4.0"):
            mirico_json_to_pcdm_transformer = MiricoJsonToPCDMTransformer(
                data=mirico_df, source_column_name="body"
            )
            actual_df = mirico_json_to_pcdm_transformer.transform()

            assert mirico_json_to_pcdm_transformer.system_type() == SystemType.PYSPARK
            assert isinstance(mirico_json_to_pcdm_transformer.libraries(), Libraries)
            assert expected_schema == actual_df.schema
            assert expected_df.collect() == actual_df.collect()
    except:
        with pytest.raises(Exception):
            mirico_json_to_pcdm_transformer = MiricoJsonToPCDMTransformer(
                data=mirico_df, source_column_name="body"
            )
