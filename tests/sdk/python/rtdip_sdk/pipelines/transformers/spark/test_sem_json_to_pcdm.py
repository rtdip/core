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
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.sem_json_to_pcdm import (
    SEMJsonToPCDMTransformer,
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


def test_sem_json_to_pcdm(spark_session: SparkSession):
    sem_json_data = '{"readings":[{"resourceName":"d","value":"[1685025760.46]"},{"resourceName":"dID","value":"502"},{"resourceName":"t", "value":"1695047439192"}]}'
    sem_df: DataFrame = spark_session.createDataFrame([{"body": sem_json_data}])

    expected_schema = StructType(
        [
            StructField("EventTime", TimestampType(), True),
            StructField("TagName", StringType(), True),
            StructField("Status", StringType(), False),
            StructField("Value", StringType(), False),
            StructField("ValueType", StringType(), True),
            StructField("ChangeType", StringType(), False),
        ]
    )

    expected_data = [
        {
            "EventTime": datetime.fromisoformat("2023-09-18T14:30:39.192+00:00"),
            "TagName": "502:obc_timeStamp",
            "Status": "Good",
            "Value": "1685025760.46",
            "ValueType": "float",
            "ChangeType": "insert",
        }
    ]

    expected_df: DataFrame = spark_session.createDataFrame(
        schema=expected_schema, data=expected_data
    )

    try:
        if _package_version_meets_minimum("pyspark", "3.4.0"):
            sem_json_to_pcdm_transformer = SEMJsonToPCDMTransformer(
                data=sem_df, source_column_name="body", version=10
            )
            actual_df = sem_json_to_pcdm_transformer.transform()

            assert sem_json_to_pcdm_transformer.system_type() == SystemType.PYSPARK
            assert isinstance(sem_json_to_pcdm_transformer.libraries(), Libraries)
            assert expected_schema == actual_df.schema
            assert expected_df.collect() == actual_df.collect()
    except:
        with pytest.raises(Exception):
            sem_json_to_pcdm_transformer = SEMJsonToPCDMTransformer(
                data=sem_df, source_column_name="body", version=10
            )
