import os

from pyspark.sql.functions import lit, to_timestamp
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.the_weather_company.raw_forecast_to_weather_data_model import (
    RawForecastToWeatherDataModel,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.weather import (
    WEATHER_DATA_MODEL,
    WEATHER_FORECAST_SCHEMA,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from pyspark.sql import SparkSession, DataFrame

parent_base_path: str = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "raw_forecast_to_weather_data_model"
)


def test_raw_forecast_to_weather_data_model(spark_session: SparkSession):
    expected_df: DataFrame = spark_session.read.csv(
        f"{parent_base_path}/output.csv", header=True, schema=WEATHER_DATA_MODEL
    )
    input_df: DataFrame = spark_session.read.csv(
        f"{parent_base_path}/input.csv", header=True, schema=WEATHER_FORECAST_SCHEMA
    )

    expected_df = spark_session.createDataFrame(
        expected_df.rdd, schema=WEATHER_DATA_MODEL
    )

    transformer = RawForecastToWeatherDataModel(spark_session, input_df)

    actual_df = transformer.transform()
    actual_df = actual_df.withColumn(
        "ProcessedDate", to_timestamp(lit("2023-06-30 14:57:50"))
    )
    actual_df = spark_session.createDataFrame(actual_df.rdd, schema=WEATHER_DATA_MODEL)

    cols = expected_df.columns
    actual_df = actual_df.orderBy(cols)
    expected_df = expected_df.orderBy(cols)

    assert transformer.system_type() == SystemType.PYSPARK
    assert isinstance(transformer.libraries(), Libraries)
    assert transformer.settings() == dict()
    assert str(actual_df.schema) == str(expected_df.schema)
    assert str(actual_df.collect()) == str(expected_df.collect())
