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
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, array, lit, struct, explode
from pyspark.sql.types import StructType, StructField, TimestampType, DoubleType, StringType

MISO_SCHEMA = StructType([
    StructField("DATE_TIME", TimestampType(), True),
    StructField("LRZ1", DoubleType(), True),
    StructField("LRZ2_7", DoubleType(), True),
    StructField("LRZ3_5", DoubleType(), True),
    StructField("LRZ4", DoubleType(), True),
    StructField("LRZ6", DoubleType(), True),
    StructField("LRZ8_9_10", DoubleType(), True),
    StructField("MISO", DoubleType(), True),
])

SMDM_USAGE_SCHEMA = StructType([
    StructField("uid", StringType(), True),
    StructField("series_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("interval_timestamp", TimestampType(), True),
    StructField("value", DoubleType(), True)
])


def melt(
        df: DataFrame, id_vars: List[str], value_vars: List[str],
        var_name: str = "variable", value_name: str = "value") -> DataFrame:
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

    _vars_and_vals = array(*(struct(lit(c).alias(var_name), col(c).alias(value_name)) for c in value_vars))
    df = df.withColumn("_vars_and_vals", explode(_vars_and_vals))
    cols = list(map(col, id_vars)) + [col("_vars_and_vals")[x].alias(x) for x in [var_name, value_name]]

    return df.select(*cols)


def apply_schema(df: DataFrame, spark: SparkSession, schema: StructType) -> DataFrame:
    """
    Converts a Spark DataFrame structure into new structure based on the Schema.

    Args:
        df: Spark DataFrame to be converted.
        spark: Active Spark Session object.
        schema: New schema to apply.

    Returns: Converted Spark DataFrame with new schema.

    """

    df = df.select(schema.names)

    for field in schema.fields:
        df = df.withColumn(field.name, col(field.name).cast(field.dataType))

    return spark.createDataFrame(df.rdd, schema)
