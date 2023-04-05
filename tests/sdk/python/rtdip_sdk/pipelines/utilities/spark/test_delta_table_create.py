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
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField, TimestampType, StringType, FloatType, DateType

def test_spark_delta_table_create(spark_session: SparkSession):
    table_create_utility = DeltaTableCreateUtility(
        spark=spark_session,
        table_name="test_table_delta_create",
        columns= [StructField("EventDate", DateType(), False, {"delta.generationExpression": "CAST(EventTime AS DATE)"}),
                StructField("TagName", StringType(), False),
                StructField("EventTime", TimestampType(), False),
                StructField("Status", StringType(), True),
                StructField("Value", FloatType(), True)],
        partitioned_by=["EventDate"],
        properties={"delta.logRetentionDuration": "7 days", "delta.enableChangeDataFeed": "true"},
        comment="Test Table for Delta Create"
    )

    result = table_create_utility.execute()
    assert result
    table_describe = spark_session.sql("DESCRIBE TABLE EXTENDED test_table_delta_create").toPandas().values.tolist()
    assert table_describe[0][0] == "EventDate"
    assert table_describe[0][1] == "date"
    assert table_describe[1][0] == "TagName"
    assert table_describe[1][1] == "string"
    assert table_describe[2][0] == "EventTime"
    assert table_describe[2][1] == "timestamp"
    assert table_describe[3][0] == "Status"
    assert table_describe[3][1] == "string"
    assert table_describe[4][0] == "Value"
    assert table_describe[4][1] == "float"
    assert table_describe[7][1] == "EventDate"
    assert table_describe[10][1] == "default.test_table_delta_create"
    assert table_describe[11][1] == "Test Table for Delta Create"
    assert "spark-warehouse/test_table_delta_create" in table_describe[12][1]
    assert "delta.enableChangeDataFeed=true" in table_describe[14][1]
    assert "delta.logRetentionDuration=7 days" in table_describe[14][1]
    
