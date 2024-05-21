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
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import when, substring, lit, col, concat
from pyspark.sql.types import IntegerType

from ...interfaces import TransformerInterface
from ...._pipeline_utils.models import Libraries, SystemType
from ...._pipeline_utils.weather import WEATHER_DATA_MODEL


class RawForecastToWeatherDataModel(TransformerInterface):
    """
    Converts a raw forecast into weather data model.

    Parameters:
        spark (SparkSession): Spark Session instance.
        data (DataFrame): Dataframe to be transformed
    """

    spark: SparkSession
    data: DataFrame

    def __init__(
        self,
        spark: SparkSession,
        data: DataFrame,
    ) -> None:
        self.spark = spark
        self.data = data
        self.target_schema = WEATHER_DATA_MODEL

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK
        """
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def pre_transform_validation(self):
        return True

    def post_transform_validation(self) -> bool:
        assert str(self.data.schema) == str(self.target_schema)
        return True

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
            DataFrame: A Forecast dataframe converted into Weather Data Model
        """

        self.pre_transform_validation()

        processed_date = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

        df = (
            self.data.withColumn("WeatherDay", substring("FcstValidLocal", 0, 10))
            .withColumn(
                "WeatherHour",
                (substring("FcstValidLocal", 12, 2).cast(IntegerType()) + 1),
            )
            .withColumn("WeatherTimezoneOffset", substring("FcstValidLocal", 20, 5))
            .withColumn("WeatherType", lit("F"))
            .withColumn("ProcessedDate", lit(processed_date))
            .withColumnRenamed("Temp", "Temperature")
            .withColumnRenamed("Dewpt", "DewPoint")
            .withColumnRenamed("Rh", "Humidity")
            .withColumnRenamed("Hi", "HeatIndex")
            .withColumnRenamed("Wc", "WindChill")
            .withColumnRenamed("Wdir", "WindDirection")
            .withColumnRenamed("Wspd", "WindSpeed")
            .withColumnRenamed("Clds", "CloudCover")
            .withColumn("WetBulbTemp", lit(""))
            .withColumn("SolarIrradiance", lit(""))
            .withColumnRenamed("Qpf", "Precipitation")
            .withColumnRenamed("DayInd", "DayOrNight")
            .withColumnRenamed("Dow", "DayOfWeek")
            .withColumnRenamed("Gust", "WindGust")
            .withColumnRenamed("Mslp", "MslPressure")
            .withColumnRenamed("Num", "ForecastDayNum")
            .withColumnRenamed("Pop", "PropOfPrecip")
            .withColumnRenamed("PrecipType", "PrecipType")
            .withColumnRenamed("SnowQpf", "SnowAccumulation")
            .withColumnRenamed("UvIndex", "UvIndex")
            .withColumnRenamed("Vis", "Visibility")
        )

        columns = df.columns
        for column in columns:
            df = df.withColumn(
                column, when(col(column) == "", lit(None)).otherwise(col(column))
            )

        self.data = df
        self._convert_into_target_schema()
        self.post_transform_validation()

        return self.data
