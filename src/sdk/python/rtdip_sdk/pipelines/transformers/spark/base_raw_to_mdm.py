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
from pyspark.sql.functions import col, expr, lit
from pyspark.sql.types import StructType

from ....data_models.timeseries import ValueType, SeriesType, ModelType
from ..interfaces import TransformerInterface
from ..._pipeline_utils.mdm import MDM_USAGE_SCHEMA, MDM_META_SCHEMA
from ..._pipeline_utils.models import Libraries, SystemType


class BaseRawToMDMTransformer(TransformerInterface):
    """
    Base class for all the Raw to Meters Data Model Transformers.

    Meters Data Model requires two outputs:
        - `UsageData` : To store measurement(value) as timeseries data.
        - `MetaData` : To store meters related meta information.

    It supports the generation of both the outputs as they share some common properties.

    Parameters:
        spark (SparkSession): Spark Session instance.
        data (DataFrame): Dataframe containing the raw MISO data.
        output_type (str): Must be one of `usage` or `meta`.
        name (str): Set this to override default `name` column.
        description (str): Set this to override default `description` column.
        value_type (ValueType): Set this to override default `value_type` column.
        version (str): Set this to override default `version` column.
        series_id (str): Set this to override default `series_id` column.
        series_parent_id (str): Set this to override default `series_parent_id` column.
    """

    spark: SparkSession
    data: DataFrame
    output_type: str
    input_schema: StructType
    target_schema: StructType
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
    series_type: SeriesType
    model_type: ModelType
    value_type: ValueType
    properties_col: str

    def __init__(
        self,
        spark: SparkSession,
        data: DataFrame,
        output_type: str,
        name: str = None,
        description: str = None,
        value_type: ValueType = None,
        version: str = None,
        series_id: str = None,
        series_parent_id: str = None,
    ):
        self.spark = spark
        self.data = data
        self.output_type = output_type
        self.name = name if name is not None else self.name_col
        self.description = (
            description if description is not None else self.description_col
        )
        self.value_type = value_type if value_type is not None else self.value_type
        self.version = version if version is not None else self.version_col
        self.series_id = series_id if series_id is not None else self.series_id_col
        self.series_parent_id = (
            series_parent_id
            if series_parent_id is not None
            else self.series_parent_id_col
        )

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
        valid_output_types = ["usage", "meta"]
        if self.output_type not in valid_output_types:
            raise ValueError(
                f"Invalid output_type `{self.output_type}` given. Must be one of {valid_output_types}"
            )

        assert str(self.data.schema) == str(self.input_schema)
        assert type(self.series_type).__name__ == SeriesType.__name__
        assert type(self.model_type).__name__ == ModelType.__name__
        assert type(self.value_type).__name__ == ValueType.__name__
        return True

    def post_transform_validation(self) -> bool:
        assert str(self.data.schema) == str(self.target_schema)
        return True

    def _get_transformed_df(self) -> DataFrame:
        if self.output_type == "usage":
            self.target_schema = MDM_USAGE_SCHEMA
            return self._get_usage_transformed_df()
        else:
            self.target_schema = MDM_META_SCHEMA
            return self._get_meta_transformed_df()

    def _convert_into_target_schema(self) -> None:
        """
        Converts a Spark DataFrame structure into new structure based on the Target Schema.

        Returns: Nothing.

        """

        df: DataFrame = self.data
        df = df.select(self.target_schema.names)

        for field in self.target_schema.fields:
            df = df.withColumn(field.name, col(field.name).cast(field.dataType))

        self.data = self.spark.createDataFrame(df.rdd, self.target_schema)

    def transform(self) -> DataFrame:
        """
        Returns:
            DataFrame: A dataframe with the raw data converted into MDM.
        """

        self.pre_transform_validation()
        self.data = self._get_transformed_df()
        self._convert_into_target_schema()
        self.post_transform_validation()

        return self.data

    def _add_uid_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("Uid", expr(self.uid_col))

    def _add_series_id_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("SeriesId", expr(self.series_id))

    def _add_timestamp_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("Timestamp", expr(self.timestamp_col))

    def _add_interval_timestamp_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("IntervalTimestamp", expr(self.interval_timestamp_col))

    def _add_value_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("Value", expr(self.value_col))

    def _add_series_parent_id_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("SeriesParentId", expr(self.series_parent_id))

    def _add_name_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("Name", expr(self.name))

    def _add_uom_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("Uom", expr(self.uom_col))

    def _add_description_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("Description", expr(self.description))

    def _add_timestamp_start_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("TimestampStart", expr(self.timestamp_start_col))

    def _add_timestamp_end_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("TimestampEnd", expr(self.timestamp_end_col))

    def _add_time_zone_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("Timezone", expr(self.time_zone_col))

    def _add_version_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("Version", expr(self.version))

    def _add_series_type_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("SeriesType", lit(self.series_type.value))

    def _add_model_type_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("ModelType", lit(self.model_type.value))

    def _add_value_type_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("ValueType", lit(self.value_type.value))

    def _add_properties_column(self, df: DataFrame) -> DataFrame:
        return df.withColumn("Properties", expr(self.properties_col))

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
