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

from typing import List
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, array, lit, struct, explode
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    DoubleType,
    StringType,
    LongType,
)

MISO_SCHEMA = StructType(
    [
        StructField("Datetime", TimestampType(), True),
        StructField("Lrz1", DoubleType(), True),
        StructField("Lrz2_7", DoubleType(), True),
        StructField("Lrz3_5", DoubleType(), True),
        StructField("Lrz4", DoubleType(), True),
        StructField("Lrz6", DoubleType(), True),
        StructField("Lrz8_9_10", DoubleType(), True),
        StructField("Miso", DoubleType(), True),
    ]
)

PJM_SCHEMA = StructType(
    [
        StructField("StartTime", TimestampType(), True),
        StructField("EndTime", TimestampType(), True),
        StructField("Zone", StringType(), True),
        StructField("Load", DoubleType(), True),
    ]
)

CAISO_SCHEMA = StructType(
    [
        StructField('StartTime', TimestampType(), True),
        StructField('EndTime', TimestampType(), True),
        StructField('LoadType', LongType(), True),
        StructField('OprDt', StringType(), True),
        StructField('OprHr', LongType(), True),
        StructField('OprInterval', LongType(), True),
        StructField('MarketRunId', StringType(), True),
        StructField('TacAreaName', StringType(), True),
        StructField('Label', StringType(), True),
        StructField('XmlDataItem', StringType(), True),
        StructField('Pos', DoubleType(), True),
        StructField('Load', DoubleType(), True),
        StructField('ExecutionType', StringType(), True),
        StructField('Group', LongType(), True)]
)


def melt(
    df: DataFrame,
    id_vars: List[str],
    value_vars: List[str],
    var_name: str = "variable",
    value_name: str = "value",
) -> DataFrame:
    """
    Unpivot the data. Convert column values into rows.

    Args:
        df: Data to be unpivot.
        id_vars: Columns to keep after unpivot.
        value_vars: Columns to be converted into rows.
        var_name: New column name to store previous column names.
        value_name: New column name to store values of unpivoted columns.

    Returns: Data after unpivot process.

    """

    _vars_and_vals = array(
        *(struct(lit(c).alias(var_name), col(c).alias(value_name)) for c in value_vars)
    )
    df = df.withColumn("_vars_and_vals", explode(_vars_and_vals))
    cols = list(map(col, id_vars)) + [
        col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]
    ]

    return df.select(*cols)
