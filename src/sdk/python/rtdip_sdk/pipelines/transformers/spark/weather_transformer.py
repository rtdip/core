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
from datetime import datetime

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import when, substring, lit, col, concat
from pyspark.sql.types import IntegerType

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.weather import COMM_FORECAST_SCHEMA


class WeatherTransformer(TransformerInterface):
    '''
    Converts a dataframe body column from a binary to a string.

    Args:
        spark (SparkSession): Spark Session instance.
        data (DataFrame): Dataframe to be transformed
    '''
    spark: SparkSession
    data: DataFrame

    def __init__(self,spark: SparkSession, data: DataFrame, ) -> None:
        self.spark = spark
        self.data = data
        self.target_schema = COMM_FORECAST_SCHEMA

    @staticmethod
    def system_type():
        '''
        Attributes:
            SystemType (Environment): Requires PYSPARK
        '''
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
        '''
        Returns:
            DataFrame: A dataframe converted to Common Forecast Weather Data Model
        '''

        processed_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(self.data)

        df = (
            self.data
                .withColumn("weather_id", concat(col("LATITUDE"), lit(","),col("LONGITUDE")))
                .withColumn("weather_day", substring("fcst_valid_local", 0, 10))
                .withColumn("weather_hour", (substring("fcst_valid_local", 12, 2).cast(IntegerType()) + 1))
                .withColumn("weather_timezone_offset", substring("fcst_valid_local", 20, 5))
                .withColumn("weather_type", lit("F"))
                .withColumn("processed_date", lit(processed_date))
                .withColumnRenamed("temp", "temperature")
                .withColumnRenamed("dewpt","dew_point")
                .withColumnRenamed("rh", "humidity")
                .withColumnRenamed("hi","heat_index")
                .withColumnRenamed("wc", "wind_chill")
                .withColumnRenamed("wdir", "wind_direction" )
                .withColumnRenamed("wspd", "wind_speed" )
                .withColumnRenamed("CLDS", "cloud_cover")
                .withColumn("wet_bulb_temp", lit(""))
                .withColumn("solar_irradiance", lit(""))
                .withColumnRenamed("qpf", "precipitation")
                .withColumnRenamed( "day_ind", "day_or_night")
                .withColumnRenamed("dow", "day_of_week")
                .withColumnRenamed("gust", "wind_gust")
                .withColumnRenamed("mslp", "msl_pressure")
                .withColumnRenamed("num", "forecast_day_num")
                .withColumnRenamed("pop", "prop_of_precip")
                .withColumnRenamed("precip_type", "precip_type")
                .withColumnRenamed("snow_qpf", "snow_accumulation")
                .withColumnRenamed("uv_index", "uv_index")
                .withColumnRenamed("vis","visibility")


                .withColumn("temperature", when(col("temperature") == "", lit(None)).otherwise(col("temperature")))
                .withColumn("dew_point", when(col("dew_point") == "", lit(None)).otherwise(col("dew_point")))
                .withColumn("humidity", when(col("humidity") == "", lit(None)).otherwise(col("humidity")))
                .withColumn("heat_index", when(col("heat_index") == "", lit(None)).otherwise(col("heat_index")))
                .withColumn("wind_chill", when(col("wind_chill") == "", lit(None)).otherwise(col("wind_chill")))
                .withColumn("wind_direction", when(col("wind_direction") == "", lit(None)).otherwise(col("wind_direction")))
                .withColumn("wind_speed", when(col("wind_speed") == "", lit(None)).otherwise(col("wind_speed")))
                .withColumn("cloud_cover", when(col("cloud_cover") == "", lit(None)).otherwise(col("cloud_cover")))
                .withColumn("precipitation", when(col("precipitation") == "", lit(None)).otherwise(col("precipitation")))
                .withColumn("wind_gust", when(col("wind_gust") == "", lit(None)).otherwise(col("wind_gust")))
                .withColumn("msl_pressure", when(col("msl_pressure") == "", lit(None)).otherwise(col("msl_pressure")))
                .withColumn("forecast_day_num", when(col("forecast_day_num") == "", lit(None)).otherwise(col("forecast_day_num")))
                .withColumn("prop_of_precip", when(col("prop_of_precip") == "", lit(None)).otherwise(col("prop_of_precip")))
                .withColumn("snow_accumulation", when(col("snow_accumulation") == "", lit(None)).otherwise(col("snow_accumulation")))
                .withColumn("uv_index", when(col("uv_index") == "", lit(None)).otherwise(col("uv_index")))
                .withColumn("visibility", when(col("visibility") == "", lit(None)).otherwise(col("visibility")))
        )

        self.data = df
        self._convert_into_target_schema()
        self.post_transform_validation()

        self.data.show()

        return self.data
