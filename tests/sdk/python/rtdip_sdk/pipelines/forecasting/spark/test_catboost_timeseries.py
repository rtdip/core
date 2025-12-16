
import pytest
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    TimestampType,
    FloatType,
)

from sktime.forecasting.base import ForecastingHorizon
from sktime.forecasting.model_selection import temporal_train_test_split

from src.sdk.python.rtdip_sdk.pipelines.forecasting.spark.catboost_timeseries import (
    CatboostTimeSeries,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[*]")
        .appName("CatBoost TimeSeries Unit Test")
        .getOrCreate()
    )


@pytest.fixture(scope="function")
def longer_timeseries_data(spark):
    """
    Creates longer time series data to ensure window_length requirements are met.
    """
    base_date = datetime(2024, 1, 1)
    data = []

    for i in range(80):
        ts = base_date + timedelta(hours=i)
        target = float(100 + i * 0.5 + (i % 7) * 1.0)
        feat1 = float(i % 10)
        data.append((ts, target, feat1))

    schema = StructType(
        [
            StructField("timestamp", TimestampType(), True),
            StructField("target", FloatType(), True),
            StructField("feat1", FloatType(), True),
        ]
    )

    return spark.createDataFrame(data, schema=schema)


@pytest.fixture(scope="function")
def missing_timestamp_col_data(spark):
    """
    Creates data missing the timestamp column to validate input checks.
    """
    data = [
        (100.0, 1.0),
        (102.0, 1.1),
    ]

    schema = StructType(
        [
            StructField("target", FloatType(), True),
            StructField("feat1", FloatType(), True),
        ]
    )

    return spark.createDataFrame(data, schema=schema)


@pytest.fixture(scope="function")
def missing_target_col_data(spark):
    """
    Creates data missing the target column to validate input checks.
    """
    data = [
        (datetime(2024, 1, 1), 1.0),
        (datetime(2024, 1, 2), 1.1),
    ]

    schema = StructType(
        [
            StructField("timestamp", TimestampType(), True),
            StructField("feat1", FloatType(), True),
        ]
    )

    return spark.createDataFrame(data, schema=schema)


@pytest.fixture(scope="function")
def nan_target_data(spark):
    """
    Creates data with NaN/None in target to validate training checks.
    """
    data = [
        (datetime(2024, 1, 1), 100.0, 1.0),
        (datetime(2024, 1, 2), None, 1.1),
        (datetime(2024, 1, 3), 105.0, 1.2),
    ]

    schema = StructType(
        [
            StructField("timestamp", TimestampType(), True),
            StructField("target", FloatType(), True),
            StructField("feat1", FloatType(), True),
        ]
    )

    return spark.createDataFrame(data, schema=schema)


def test_catboost_initialization():
    """
    Test that CatboostTimeSeries can be initialized with default parameters.
    """
    cb = CatboostTimeSeries()
    assert cb.target_col == "target"
    assert cb.timestamp_col == "timestamp"
    assert cb.is_trained is False
    assert cb.model is not None


def test_catboost_custom_initialization():
    """
    Test that CatboostTimeSeries can be initialized with custom parameters.
    """
    cb = CatboostTimeSeries(
        target_col="value",
        timestamp_col="time",
        window_length=12,
        strategy="direct",
        random_state=7,
        iterations=50,
        learning_rate=0.1,
        depth=4,
        verbose=False,
    )
    assert cb.target_col == "value"
    assert cb.timestamp_col == "time"
    assert cb.is_trained is False
    assert cb.model is not None


def test_convert_spark_to_pandas_missing_timestamp(missing_timestamp_col_data):
    """
    Test that missing timestamp column raises an error during conversion.
    """
    cb = CatboostTimeSeries()

    with pytest.raises(ValueError, match="Required column timestamp is missing"):
        cb.convert_spark_to_pandas(missing_timestamp_col_data)


def test_train_missing_target_column(missing_target_col_data):
    """
    Test that training fails if target column is missing.
    """
    cb = CatboostTimeSeries()

    with pytest.raises(ValueError, match="Required column target is missing"):
        cb.train(missing_target_col_data)


def test_train_nan_target_raises(nan_target_data):
    """
    Test that training fails if target contains NaN/None values.
    """
    cb = CatboostTimeSeries()

    with pytest.raises(ValueError, match="contains NaN/None values"):
        cb.train(nan_target_data)


def test_train_and_predict(longer_timeseries_data):
    """
    Test basic training and prediction workflow (out-of-sample horizon).
    """
    cb = CatboostTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        window_length=12,
        strategy="recursive",
        iterations=30,
        depth=4,
        learning_rate=0.1,
        verbose=False,
    )

    # Use temporal split (deterministic and order-preserving).
    full_pdf = cb.convert_spark_to_pandas(longer_timeseries_data).reset_index()
    train_pdf, test_pdf = temporal_train_test_split(full_pdf, test_size=0.25)

    spark = longer_timeseries_data.sql_ctx.sparkSession
    train_df = spark.createDataFrame(train_pdf)
    test_df = spark.createDataFrame(test_pdf)

    cb.train(train_df)
    assert cb.is_trained is True

    # Build OOS horizon using the test timestamps.
    test_pdf_idx = cb.convert_spark_to_pandas(test_df)
    fh = ForecastingHorizon(test_pdf_idx.index, is_relative=False)

    preds = cb.predict(
        predict_df=test_df.drop("target"),
        forecasting_horizon=fh,
    )

    assert preds is not None
    assert preds.count() == test_df.count()
    assert (
        "target" in preds.columns
    ), "Predictions should be returned in the target column name"


def test_predict_without_training(longer_timeseries_data):
    """
    Test that predicting without training raises an error.
    """
    cb = CatboostTimeSeries(window_length=12)

    # Create a proper out-of-sample test set and horizon.
    full_pdf = cb.convert_spark_to_pandas(longer_timeseries_data).reset_index()
    _, test_pdf = temporal_train_test_split(full_pdf, test_size=0.25)

    spark = longer_timeseries_data.sql_ctx.sparkSession
    test_df = spark.createDataFrame(test_pdf)

    test_pdf_idx = cb.convert_spark_to_pandas(test_df)
    fh = ForecastingHorizon(test_pdf_idx.index, is_relative=False)

    with pytest.raises(ValueError, match="The model is not trained yet"):
        cb.predict(
            predict_df=test_df.drop("target"),
            forecasting_horizon=fh,
        )


def test_predict_with_none_horizon(longer_timeseries_data):
    """
    Test that predict rejects a None forecasting horizon.
    """
    cb = CatboostTimeSeries(
        window_length=12, iterations=10, depth=3, learning_rate=0.1, verbose=False
    )

    full_pdf = cb.convert_spark_to_pandas(longer_timeseries_data).reset_index()
    train_pdf, test_pdf = temporal_train_test_split(full_pdf, test_size=0.25)

    spark = longer_timeseries_data.sql_ctx.sparkSession
    train_df = spark.createDataFrame(train_pdf)
    test_df = spark.createDataFrame(test_pdf)

    cb.train(train_df)

    with pytest.raises(ValueError, match="forecasting_horizon must not be None"):
        cb.predict(
            predict_df=test_df.drop("target"),
            forecasting_horizon=None,
        )


def test_predict_with_target_leakage_raises(longer_timeseries_data):
    """
    Test that predict rejects inputs that still contain the target column.
    """
    cb = CatboostTimeSeries(
        window_length=12, iterations=10, depth=3, learning_rate=0.1, verbose=False
    )

    full_pdf = cb.convert_spark_to_pandas(longer_timeseries_data).reset_index()
    train_pdf, test_pdf = temporal_train_test_split(full_pdf, test_size=0.25)

    spark = longer_timeseries_data.sql_ctx.sparkSession
    train_df = spark.createDataFrame(train_pdf)
    test_df = spark.createDataFrame(test_pdf)

    cb.train(train_df)

    test_pdf_idx = cb.convert_spark_to_pandas(test_df)
    fh = ForecastingHorizon(test_pdf_idx.index, is_relative=False)

    with pytest.raises(ValueError, match="must not contain the target column"):
        cb.predict(
            predict_df=test_df,
            forecasting_horizon=fh,
        )


def test_evaluate_without_training(longer_timeseries_data):
    """
    Test that evaluating without training raises an error.
    """
    cb = CatboostTimeSeries(window_length=12)

    with pytest.raises(ValueError, match="The model is not trained yet"):
        cb.evaluate(longer_timeseries_data)


def test_evaluate_full_workflow(longer_timeseries_data):
    """
    Test full workflow: train -> evaluate returns metric dict (out-of-sample only).
    """
    cb = CatboostTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        window_length=12,
        iterations=30,
        depth=4,
        learning_rate=0.1,
        verbose=False,
    )

    full_pdf = cb.convert_spark_to_pandas(longer_timeseries_data).reset_index()
    train_pdf, test_pdf = temporal_train_test_split(full_pdf, test_size=0.25)

    spark = longer_timeseries_data.sql_ctx.sparkSession
    train_df = spark.createDataFrame(train_pdf)
    test_df = spark.createDataFrame(test_pdf)

    cb.train(train_df)
    metrics = cb.evaluate(test_df)

    assert metrics is not None
    assert isinstance(metrics, dict)

    for key in ["MAE", "RMSE", "MAPE", "MASE", "SMAPE"]:
        assert key in metrics, f"Missing metric key: {key}"


def test_system_type():
    """
    Test that system_type returns PYTHON.
    """
    from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import SystemType

    system_type = CatboostTimeSeries.system_type()
    assert system_type == SystemType.PYTHON


def test_libraries():
    """
    Test that libraries method returns expected dependencies.
    """
    libraries = CatboostTimeSeries.libraries()
    assert libraries is not None
    assert len(libraries.pypi_libraries) > 0

    catboost_found = False
    sktime_found = False
    for lib in libraries.pypi_libraries:
        if lib.name == "catboost":
            catboost_found = True
        if lib.name == "sktime":
            sktime_found = True

    assert catboost_found, "catboost should be in the library dependencies"
    assert sktime_found, "sktime should be in the library dependencies"


def test_settings():
    """
    Test that settings method returns expected configuration.
    """
    settings = CatboostTimeSeries.settings()
    assert settings is not None
    assert isinstance(settings, dict)

