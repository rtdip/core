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

from src.sdk.python.rtdip_sdk.pipelines.utilities.spark.delta_table_create import (
    DeltaTableCreateUtility,
    DeltaTableColumn,
)
from src.sdk.python.rtdip_sdk.pipelines.utilities.spark.delta_table_vacuum import (
    DeltaTableVacuumUtility,
)
from pyspark.sql import SparkSession


def test_spark_delta_table_optimize(spark_session: SparkSession):
    delta_table_create = DeltaTableCreateUtility(
        spark=spark_session,
        table_name="test_table_delta_vacuum",
        columns=[
            DeltaTableColumn(
                name="EventDate",
                type="date",
                nullable=False,
                metadata={"delta.generationExpression": "CAST(EventTime AS DATE)"},
            ),
            DeltaTableColumn(name="TagName", type="string", nullable=False),
            DeltaTableColumn(name="EventTime", type="timestamp", nullable=False),
            DeltaTableColumn(name="Status", type="string", nullable=True),
            DeltaTableColumn(name="Value", type="float", nullable=True),
        ],
        partitioned_by=["EventDate"],
        properties={
            "delta.logRetentionDuration": "7 days",
            "delta.enableChangeDataFeed": "true",
        },
        comment="Test Table for Delta Vacuum",
    )

    delta_table_create.execute()

    delta_table_vacuum = DeltaTableVacuumUtility(
        spark=spark_session, table_name="test_table_delta_vacuum", retention_hours=168
    )

    result = delta_table_vacuum.execute()
    assert result
