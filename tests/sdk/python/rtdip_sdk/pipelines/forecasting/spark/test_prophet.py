import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, TimestampType, FloatType

from sktime.forecasting.model_selection import temporal_train_test_split

from src.sdk.python.rtdip_sdk.pipelines.forecasting.spark.prophet import (
    ProphetForecaster,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import SystemType


@pytest.fixture(scope="session")
def spark():
    """
    Create a SparkSession for all tests.
    """
    return (
        SparkSession.builder
            .master("local[*]")
            .appName("SCADA-Forecasting")
            .config("spark.driver.memory", "8g")
            .config("spark.executor.memory", "8g")
            .config("spark.driver.maxResultSize", "2g")
            .config("spark.sql.shuffle.partitions", "50")
            .config("spark.sql.execution.arrow.pyspark.enabled", "true")
            .getOrCreate()
    )


@pytest.fixture(scope="function")
def simple_prophet_pandas_data():
    """
    Creates simple univariate time series data (Pandas) for Prophet testing.
    """
    base_date = datetime(2024, 1, 1)
    data = []

    for i in range(30):
        ts = base_date + timedelta(days=i)
        value = 100.0 + i * 1.5  # simple upward trend
        data.append((ts, value))

    pdf = pd.DataFrame(data, columns=["ds", "y"])
    return pdf


@pytest.fixture(scope="function")
def spark_data_with_custom_columns(spark):
    """
    Creates Spark DataFrame with custom timestamp/target column names.
    """
    base_date = datetime(2024, 1, 1)
    data = []

    for i in range(10):
        ts = base_date + timedelta(days=i)
        value = 50.0 + i
        other = float(i * 2)
        data.append((ts, value, other))

    schema = StructType(
        [
            StructField("timestamp", TimestampType(), True),
            StructField("target", FloatType(), True),
            StructField("other_feature", FloatType(), True),
        ]
    )

    return spark.createDataFrame(data, schema=schema)


@pytest.fixture(scope="function")
def spark_data_missing_columns(spark):
    """
    Creates Spark DataFrame that is missing required columns for conversion.
    """
    base_date = datetime(2024, 1, 1)
    data = []

    for i in range(5):
        ts = base_date + timedelta(days=i)
        value = 10.0 + i
        data.append((ts, value))

    schema = StructType(
        [
            StructField("wrong_timestamp", TimestampType(), True),
            StructField("value", FloatType(), True),
        ]
    )

    return spark.createDataFrame(data, schema=schema)


def test_prophet_initialization_defaults():
    """
    Test that ProphetForecaster can be initialized with default parameters.
    """
    pf = ProphetForecaster()

    assert pf.use_only_timestamp_and_target is True
    assert pf.target_col == "y"
    assert pf.timestamp_col == "ds"
    assert pf.is_trained is False
    assert pf.prophet is not None


def test_prophet_custom_initialization():
    """
    Test that ProphetForecaster can be initialized with custom parameters.
    """
    pf = ProphetForecaster(
        use_only_timestamp_and_target=False,
        target_col="target",
        timestamp_col="timestamp",
        growth="logistic",
        n_changepoints=10,
        changepoint_range=0.9,
        yearly_seasonality="False",
        weekly_seasonality="auto",
        daily_seasonality="auto",
        seasonality_mode="multiplicative",
        seasonality_prior_scale=5.0,
        scaling="minmax",
    )

    assert pf.use_only_timestamp_and_target is False
    assert pf.target_col == "target"
    assert pf.timestamp_col == "timestamp"
    assert pf.prophet is not None


def test_system_type():
    """
    Test that system_type returns PYTHON.
    """
    system_type = ProphetForecaster.system_type()
    assert system_type == SystemType.PYTHON


def test_settings():
    """
    Test that settings method returns a dictionary.
    """
    settings = ProphetForecaster.settings()
    assert settings is not None
    assert isinstance(settings, dict)


def test_convert_spark_to_pandas_with_custom_columns(spark, spark_data_with_custom_columns):
    """
    Test that convert_spark_to_pandas selects and renames timestamp/target columns correctly.
    """
    pf = ProphetForecaster(
        use_only_timestamp_and_target=True,
        target_col="target",
        timestamp_col="timestamp",
    )

    pdf = pf.convert_spark_to_pandas(spark_data_with_custom_columns)

    # After conversion, columns should be renamed to ds and y
    assert list(pdf.columns) == ["ds", "y"]
    assert pd.api.types.is_datetime64_any_dtype(pdf["ds"])
    assert len(pdf) == spark_data_with_custom_columns.count()


def test_convert_spark_to_pandas_missing_columns_raises(spark, spark_data_missing_columns):
    """
    Test that convert_spark_to_pandas raises ValueError when required columns are missing.
    """
    pf = ProphetForecaster(
        use_only_timestamp_and_target=True,
        target_col="target",
        timestamp_col="timestamp",
    )

    with pytest.raises(ValueError, match="Required columns"):
        pf.convert_spark_to_pandas(spark_data_missing_columns)


def test_train_with_valid_data(spark, simple_prophet_pandas_data):
    """
    Test that train() fits the model and sets is_trained flag with valid data.
    """
    pf = ProphetForecaster()

    # Split using temporal_train_test_split as you described
    train_pdf, _ = temporal_train_test_split(simple_prophet_pandas_data, test_size=0.2)
    train_df = spark.createDataFrame(train_pdf)

    pf.train(train_df)

    assert pf.is_trained is True


def test_train_with_nan_raises_value_error(spark, simple_prophet_pandas_data):
    """
    Test that train() raises a ValueError when NaN values are present.
    """
    pdf_with_nan = simple_prophet_pandas_data.copy()
    pdf_with_nan.loc[5, "y"] = np.nan

    train_df = spark.createDataFrame(pdf_with_nan)
    pf = ProphetForecaster()

    with pytest.raises(ValueError, match="The dataframe contains NaN values"):
        pf.train(train_df)


def test_predict_without_training_raises(spark, simple_prophet_pandas_data):
    """
    Test that predict() without training raises a ValueError.
    """
    pf = ProphetForecaster()
    df = spark.createDataFrame(simple_prophet_pandas_data)

    with pytest.raises(ValueError, match="The model is not trained yet"):
        pf.predict(df, periods=5, freq="D")


def test_evaluate_without_training_raises(spark, simple_prophet_pandas_data):
    """
    Test that evaluate() without training raises a ValueError.
    """
    pf = ProphetForecaster()
    df = spark.createDataFrame(simple_prophet_pandas_data)

    with pytest.raises(ValueError, match="The model is not trained yet"):
        pf.evaluate(df, freq="D")


def test_predict_returns_spark_dataframe(spark, simple_prophet_pandas_data):
    """
    Test that predict() returns a Spark DataFrame with predictions.
    """
    pf = ProphetForecaster()

    train_pdf, _ = temporal_train_test_split(simple_prophet_pandas_data, test_size=0.2)
    train_df = spark.createDataFrame(train_pdf)

    pf.train(train_df)

    # Use the full DataFrame as base for future periods
    predict_df = spark.createDataFrame(simple_prophet_pandas_data)

    predictions_df = pf.predict(predict_df, periods=5, freq="D")

    assert predictions_df is not None
    assert predictions_df.count() > 0
    assert "yhat" in predictions_df.columns


def test_evaluate_returns_metrics_dict(spark, simple_prophet_pandas_data):
    """
    Test that evaluate() returns a metrics dictionary with expected keys and negative values.
    """
    pf = ProphetForecaster()

    train_pdf, test_pdf = temporal_train_test_split(simple_prophet_pandas_data, test_size=0.2)
    train_df = spark.createDataFrame(train_pdf)
    test_df = spark.createDataFrame(test_pdf)

    pf.train(train_df)

    metrics = pf.evaluate(test_df, freq="D")

    # Check that metrics is a dict and contains expected keys
    assert isinstance(metrics, dict)
    expected_keys = {"MAE", "RMSE", "MAPE", "MASE", "SMAPE"}
    assert expected_keys.issubset(metrics.keys())

    # AutoGluon style: metrics are negative
    for key in expected_keys:
        assert metrics[key] <= 0 or np.isnan(metrics[key])


def test_full_workflow_prophet(spark, simple_prophet_pandas_data):
    """
    Test a full workflow: train, predict, evaluate with ProphetForecaster.
    """
    pf = ProphetForecaster()

    train_pdf, test_pdf = temporal_train_test_split(simple_prophet_pandas_data, test_size=0.2)
    train_df = spark.createDataFrame(train_pdf)
    test_df = spark.createDataFrame(test_pdf)

    # Train
    pf.train(train_df)
    assert pf.is_trained is True

    # Evaluate
    metrics = pf.evaluate(test_df, freq="D")
    assert isinstance(metrics, dict)
    assert "MAE" in metrics

    # Predict separately
    predictions_df = pf.predict(test_df, periods=len(test_pdf), freq="D")
    assert predictions_df is not None
    assert predictions_df.count() > 0
    assert "yhat" in predictions_df.columns
