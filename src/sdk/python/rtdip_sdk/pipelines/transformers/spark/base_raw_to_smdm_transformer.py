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
from abc import abstractmethod

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, expr
from pyspark.sql.types import StructType

from ..interfaces import TransformerInterface
from ..._pipeline_utils.smdm import SMDM_USAGE_SCHEMA, SMDM_META_SCHEMA, SMDMClassType
from ..._pipeline_utils.models import Libraries, SystemType


class BaseRawToSMDMTransformer(TransformerInterface):
    """
    Base class for all the Raw to Smart Meter Data Model Transformers.

    Args:
        spark (SparkSession): Spark Session instance.
        data (DataFrame): Dataframe containing the raw MISO data.
    """

    spark: SparkSession
    data: DataFrame
    input_schema: StructType
    target_schema: StructType
    class_type: SMDMClassType
    uid_col: str
    series_id_col: str
    timestamp_col: str
    interval_timestamp_col: str
    value_col: str
    series_parent_id_col: str
    name_col: str
    uom_col: str
    description_col: str
    timestamp_start_col: str
    timestamp_end_col: str
    time_zone_col: str
    version_col: str
    series_type_col: str
    model_type_col: str
    value_type_col: str
    properties_col: str

    def __init__(self,
                 spark: SparkSession,
                 data: DataFrame,
                 name: str = None,
                 description: str = None,
                 value_type: str = None,
                 version: str = None,
                 series_id: str = None,
                 series_parent_id: str = None,
                 ):
        self.spark = spark
        self.data = data
        self.name = name if name is not None else self.name_col
        self.description = description if description is not None else self.description_col
        self.value_type = value_type if value_type is not None else self.value_type_col
        self.version = version if version is not None else self.version_col
        self.series_id = series_id if series_id is not None else self.series_id_col
        self.series_parent_id = series_parent_id if series_parent_id is not None else self.series_parent_id_col

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
        assert str(self.data.schema) == str(self.input_schema)
        return True

    def post_transform_validation(self) -> bool:
        assert str(self.data.schema) == str(self.target_schema)
        return True

    def _get_transformed_df(self) -> DataFrame:
        if self.class_type == SMDMClassType.usage:
            self.target_schema = SMDM_USAGE_SCHEMA
            return self._get_usage_transformed_df()
        else:
            self.target_schema = SMDM_META_SCHEMA
            return self._get_meta_transformed_df()

    def _convert_into_target_schema(self) -> None:
        """
        Converts a Spark DataFrame structure into new structure based on the Target Schema.

        Returns: Nothing.

        """

        df = self.data
        df = df.select(self.target_schema.names)

        for field in self.target_schema.fields:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))

        self.data = self.spark.createDataFrame(df.rdd, self.target_schema)

    def transform(self) -> DataFrame:
        """
        Returns:
            DataFrame: A dataframe with the raw data converted into SMDM.
        """

        self.pre_transform_validation()
        self.data = self._get_transformed_df()
        self._convert_into_target_schema()
        self.post_transform_validation()

        return self.data

    def _add_uid_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("uid", expr(self.uid_col))

    def _add_series_id_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("series_id", expr(self.series_id))

    def _add_timestamp_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("timestamp", expr(self.timestamp_col))

    def _add_interval_timestamp_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("interval_timestamp", expr(self.interval_timestamp_col))

    def _add_value_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("value", expr(self.value_col))

    def _add_series_parent_id_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("series_parent_id", expr(self.series_parent_id))

    def _add_name_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("name", expr(self.name))

    def _add_uom_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("uom", expr(self.uom_col))

    def _add_description_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("description", expr(self.description))

    def _add_timestamp_start_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("timestamp_start", expr(self.timestamp_start_col))

    def _add_timestamp_end_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("timestamp_end", expr(self.timestamp_end_col))

    def _add_time_zone_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("time_zone", expr(self.time_zone_col))

    def _add_version_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("version", expr(self.version))

    def _add_series_type_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("series_type", expr(self.series_type_col))

    def _add_model_type_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("model_type", expr(self.model_type_col))

    def _add_value_type_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("value_type", expr(self.value_type))

    def _add_properties_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("properties", expr(self.properties_col))

    def _pre_process(self) -> DataFrame:
        return self.data

    @staticmethod
    def _post_process(df: DataFrame) -> DataFrame:
        return df

    def _get_usage_transformed_df(self) -> DataFrame:
        df = self._pre_process()

        df = self._add_uid_column(df)
        df = self._add_series_id_column(df)
        df = self._add_timestamp_column(df)
        df = self._add_interval_timestamp_column(df)
        df = self._add_value_column(df)

        df = self._post_process(df)

        return df

    def _get_meta_transformed_df(self) -> DataFrame:
        df = self._pre_process()

        df = self._add_uid_column(df)
        df = self._add_series_id_column(df)
        df = self._add_series_parent_id_column(df)
        df = self._add_name_column(df)
        df = self._add_uom_column(df)
        df = self._add_description_column(df)
        df = self._add_timestamp_start_column(df)
        df = self._add_timestamp_end_column(df)
        df = self._add_time_zone_column(df)
        df = self._add_version_column(df)
        df = self._add_series_type_column(df)
        df = self._add_model_type_column(df)
        df = self._add_value_type_column(df)
        df = self._add_properties_column(df)

        df = self._post_process(df)

        return df
