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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, expr, sha2, concat, substring, lower, bround, to_utc_timestamp, date_format, \
    to_date, lpad, floor, hour

from ...interfaces import TransformerInterface
from ...._pipeline_utils.iso import melt, SMDM_USAGE_SCHEMA, apply_schema, MISO_SCHEMA
from ...._pipeline_utils.models import Libraries, SystemType


class MISORawToSMDMTransformer(TransformerInterface):
    """
    Converts raw MISO data into Smart Meter Data Model.

    Args:
        spark (SparkSession): Spark Session instance.
        data (DataFrame): Dataframe containing the raw MISO data.
    """

    spark: SparkSession
    data: DataFrame

    def __init__(self, spark: SparkSession, data: DataFrame):
        self.spark = spark
        self.data = data

    @staticmethod
    def system_type():
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def pre_transform_validation(self) -> bool:
        return True

    def post_transform_validation(self) -> bool:
        return True

    def transform(self) -> DataFrame:
        """
        Returns:
            DataFrame: A dataframe with the raw data converted into SMDM.
        """

        df = self.data

        assert str(df.schema) == str(MISO_SCHEMA)

        df = melt(df,
                  id_vars=["DATE_TIME"],
                  value_vars=["LRZ1", "LRZ2_7", "LRZ3_5", "LRZ4", "LRZ6", "LRZ8_9_10", "MISO"]
                  )

        df = (
            df
            .withColumn("uid", col("variable"))
            .withColumn("series_id", lit("series_std_001"))
            .withColumn("timestamp", to_utc_timestamp(col("DATE_TIME"), "US/Central"))
            .withColumn("interval_timestamp", col("timestamp") + expr("INTERVAL 1 HOURS"))
            .withColumn("value", bround(col("value"), 2))
        )

        df = apply_schema(df, self.spark, SMDM_USAGE_SCHEMA)

        return df
