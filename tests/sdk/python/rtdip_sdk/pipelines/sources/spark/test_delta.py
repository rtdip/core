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
from src.sdk.python.rtdip_sdk.pipelines.sources.spark.delta import SparkDeltaSource
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType, StructField, StringType

def test_spark_delta_read_batch(spark_session: SparkSession):
    df = spark_session.createDataFrame([{"id": "1"}])
    delta_destination = SparkDeltaDestination("test_spark_delta_read_batch", {}, "overwrite")       
    delta_source = SparkDeltaSource(spark_session, {}, "test_spark_delta_read_batch") 
    delta_destination.write_batch(df)
    actual_df = delta_source.read_batch()
    assert isinstance(actual_df, DataFrame)
    assert actual_df.schema == StructType([StructField('id', StringType(), True)])

def test_spark_delta_read_stream(spark_session: SparkSession):
    df = spark_session.createDataFrame([{"id": "1"}])
    delta_destination = SparkDeltaDestination("test_spark_delta_read_stream", {}, "overwrite")       
    delta_source = SparkDeltaSource(spark_session, {}, "test_spark_delta_read_stream") 
    delta_destination.write_stream(df)
    actual_df = delta_source.read_stream()
    assert isinstance(actual_df, DataFrame)
    assert actual_df.schema == StructType([StructField('id', StringType(), True)])