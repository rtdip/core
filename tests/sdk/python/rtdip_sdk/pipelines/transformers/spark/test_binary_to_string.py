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
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.binary_to_string import (
    BinaryToStringTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)

from pyspark.sql import SparkSession, DataFrame


def test_spark_eventhub_transform_body_to_string(spark_session: SparkSession):
    expected_df: DataFrame = spark_session.createDataFrame([{"body": "1"}])
    binary_df = expected_df.withColumn("body", expected_df["body"].cast("binary"))
    eventhub_binary_to_string_transformer = BinaryToStringTransformer(
        binary_df, source_column_name="body", target_column_name="body"
    )
    actual_df = eventhub_binary_to_string_transformer.transform()

    assert eventhub_binary_to_string_transformer.system_type() == SystemType.PYSPARK
    assert isinstance(eventhub_binary_to_string_transformer.libraries(), Libraries)
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()
