import os
import pytest

from pyspark.sql.functions import lit
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.raw_forecast_data_to_weather_data_model import  RawForecastToCommonDataModel
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.weather import WEATHER_DATA_MODEL, WEATHER_FORECAST_SCHEMA
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries, SystemType
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session
from pyspark.sql import SparkSession, DataFrame

parent_base_path: str = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_data")


def test_weather_transformer_single_station(spark_session: SparkSession):
    expected_df: DataFrame = spark_session.read.csv(f"{parent_base_path}/output.csv", header=True, schema=WEATHER_DATA_MODEL)
    input_df: DataFrame = spark_session.read.csv(f"{parent_base_path}/input.csv", header=True, schema=WEATHER_FORECAST_SCHEMA)

    expected_df = spark_session.createDataFrame(expected_df.rdd, schema=WEATHER_DATA_MODEL)

    transformer = RawForecastToCommonDataModel(spark_session, input_df)

    actual_df = transformer.transform()
    actual_df = actual_df.withColumn("processed_date", lit("2023-06-30 14:57:50"))

    cols = expected_df.columns
    actual_df = actual_df.orderBy(cols)
    expected_df = expected_df.orderBy(cols)

    assert transformer.system_type() == SystemType.PYSPARK
    assert isinstance(transformer.libraries(), Libraries)
    assert transformer.settings() == dict()
    assert str(actual_df.schema) == str(expected_df.schema)
    assert str(actual_df.collect()) == str(expected_df.collect())
