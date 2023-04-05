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

from src.sdk.python.rtdip_sdk.pipelines.utilities.spark.delta_table_create import DeltaTableCreateUtility
from src.sdk.python.rtdip_sdk.pipelines.utilities.spark.delta_table_vacuum import DeltaTableVacuumUtility
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, TimestampType, StringType, FloatType, DateType

def test_spark_delta_table_optimize(spark_session: SparkSession):
    delta_table_create = DeltaTableCreateUtility(
        spark=spark_session,
        table_name="test_table_delta_vacuum",
        columns= [StructField("EventDate", DateType(), False, {"delta.generationExpression": "CAST(EventTime AS DATE)"}),
                StructField("TagName", StringType(), False),
                StructField("EventTime", TimestampType(), False),
                StructField("Status", StringType(), True),
                StructField("Value", FloatType(), True)],
        partitioned_by=["EventDate"],
        properties={"delta.logRetentionDuration": "7 days", "delta.enableChangeDataFeed": "true"},
        comment="Test Table for Delta Vacuum"
    )

    delta_table_create.execute()

    delta_table_vacuum = DeltaTableVacuumUtility(
        spark=spark_session,
        table_name="test_table_delta_vacuum",
        retention_hours=1
    )

    result = delta_table_vacuum.execute()
    assert result
    
