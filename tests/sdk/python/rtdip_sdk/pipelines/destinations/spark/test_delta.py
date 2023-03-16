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
from src.sdk.python.rtdip_sdk.pipelines.destinations.spark.delta import SparkDeltaDestination
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session
from pyspark.sql.functions import lit
from pyspark.sql import SparkSession

def test_spark_delta_write_batch(spark_session: SparkSession):
    expected_df = spark_session.createDataFrame([{"id": "1"}])
    delta_destination = SparkDeltaDestination("test_spark_delta_write_batch", {}, "overwrite")
    delta_destination.write_batch(expected_df)
    actual_df = spark_session.table("test_spark_delta_write_batch")
    assert expected_df.schema == actual_df.schema
    assert expected_df.collect() == actual_df.collect()