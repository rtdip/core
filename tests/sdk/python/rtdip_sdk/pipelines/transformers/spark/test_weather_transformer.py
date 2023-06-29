import os
import pytest
from pyspark.sql.functions import expr, lit

from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.weather_transformer import WeatherTransformer
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.weather import COMM_FORECAST_SCHEMA, WEATHER_FORECAST_SCHEMA
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries, SystemType
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session
from pyspark.sql import SparkSession, DataFrame

parent_base_path: str = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_data")

#
# def test_simple(spark_session: SparkSession):
#     assert 3==3

def test_weather_transformer_single_station(spark_session: SparkSession):
    # base_path: str = os.path.join(parent_base_path, "forecast")

    expected_df: DataFrame = spark_session.read.csv(f"{parent_base_path}/output_single_station.csv", header=True, schema=COMM_FORECAST_SCHEMA)
    input_df: DataFrame = spark_session.read.csv(f"{parent_base_path}/input.csv", header=True, schema=WEATHER_FORECAST_SCHEMA)


    expected_df = spark_session.createDataFrame(expected_df.rdd, schema=COMM_FORECAST_SCHEMA)
    print(COMM_FORECAST_SCHEMA)
    expected_df.printSchema()
    # input_df.printSchema()
    expected_df.show()

    transformer = WeatherTransformer(spark_session, input_df)
    actual_df = transformer.transform()

    # actual_df = actual_df.orderBy("uid", "timestamp")
    # expected_df = expected_df.orderBy("uid", "timestamp")

    assert transformer.system_type() == SystemType.PYSPARK
    assert isinstance(transformer.libraries(), Libraries)
    assert transformer.settings() == dict()
    assert str(actual_df.schema) == str(expected_df.schema)
    assert str(actual_df.collect()) == str(expected_df.collect())
