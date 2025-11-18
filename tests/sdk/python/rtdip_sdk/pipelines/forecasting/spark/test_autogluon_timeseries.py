import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
)
from datetime import datetime, timedelta
from src.sdk.python.rtdip_sdk.pipelines.forecasting.spark.autogluon_timeseries import (
    AutoGluonTimeSeries,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[*]")
        .appName("AutoGluon TimeSeries Unit Test")
        .getOrCreate()
    )


@pytest.fixture(scope="function")
def sample_timeseries_data(spark):
    """
    Creates sample time series data with multiple items for testing.
    """
    base_date = datetime(2024, 1, 1)
    data = []

    # Create data for two items with 50 time points each
    for item_id in ["sensor_A", "sensor_B"]:
        for i in range(50):
            timestamp = base_date + timedelta(hours=i)
            # Create a simple trend + noise pattern (use float)
            value = float(100 + i * 2 + (i % 10) * 5)
            data.append((item_id, timestamp, value))

    schema = StructType(
        [
            StructField("item_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("target", FloatType(), True),
        ]
    )

    return spark.createDataFrame(data, schema=schema)


@pytest.fixture(scope="function")
def simple_timeseries_data(spark):
    """
    Creates simple time series data for basic testing.
    """
    data = [
        ("A", datetime(2024, 1, 1), 100.0),
        ("A", datetime(2024, 1, 2), 102.0),
        ("A", datetime(2024, 1, 3), 105.0),
        ("A", datetime(2024, 1, 4), 103.0),
        ("A", datetime(2024, 1, 5), 107.0),
        ("A", datetime(2024, 1, 6), 110.0),
        ("A", datetime(2024, 1, 7), 112.0),
        ("A", datetime(2024, 1, 8), 115.0),
        ("A", datetime(2024, 1, 9), 118.0),
        ("A", datetime(2024, 1, 10), 120.0),
    ]

    schema = StructType(
        [
            StructField("item_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("target", FloatType(), True),
        ]
    )

    return spark.createDataFrame(data, schema=schema)


def test_autogluon_initialization():
    """
    Test that AutoGluonTimeSeries can be initialized with default parameters.
    """
    ag = AutoGluonTimeSeries()
    assert ag.target_col == "target"
    assert ag.timestamp_col == "timestamp"
    assert ag.item_id_col == "item_id"
    assert ag.prediction_length == 24
    assert ag.predictor is None


def test_autogluon_custom_initialization():
    """
    Test that AutoGluonTimeSeries can be initialized with custom parameters.
    """
    ag = AutoGluonTimeSeries(
        target_col="value",
        timestamp_col="time",
        item_id_col="sensor",
        prediction_length=12,
        eval_metric="RMSE",
    )
    assert ag.target_col == "value"
    assert ag.timestamp_col == "time"
    assert ag.item_id_col == "sensor"
    assert ag.prediction_length == 12
    assert ag.eval_metric == "RMSE"


def test_split_data(sample_timeseries_data):
    """
    Test that data splitting works correctly.
    """
    ag = AutoGluonTimeSeries()
    train_df, test_df = ag.split_data(sample_timeseries_data, train_ratio=0.8)

    total_count = sample_timeseries_data.count()
    train_count = train_df.count()
    test_count = test_df.count()

    assert train_count + test_count == total_count
    assert train_count > test_count
    assert abs(train_count / total_count - 0.8) < 0.1  


def test_train_and_predict(simple_timeseries_data):
    """
    Test basic training and prediction workflow.
    """
    ag = AutoGluonTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=2,
        time_limit=60,  
        preset="fast_training",
        verbosity=0,
    )

    train_df, test_df = ag.split_data(simple_timeseries_data, train_ratio=0.8)

    ag.train(train_df)

    assert ag.predictor is not None, "Predictor should be initialized after training"
    assert ag.model is not None, "Model should be set after training"


def test_predict_without_training(simple_timeseries_data):
    """
    Test that predicting without training raises an error.
    """
    ag = AutoGluonTimeSeries()

    with pytest.raises(ValueError, match="Model has not been trained yet"):
        ag.predict(simple_timeseries_data)


def test_evaluate_without_training(simple_timeseries_data):
    """
    Test that evaluating without training raises an error.
    """
    ag = AutoGluonTimeSeries()

    with pytest.raises(ValueError, match="Model has not been trained yet"):
        ag.evaluate(simple_timeseries_data)


def test_get_leaderboard_without_training():
    """
    Test that getting leaderboard without training raises an error.
    """
    ag = AutoGluonTimeSeries()

    with pytest.raises(ValueError, match="Model has not been trained yet"):
        ag.get_leaderboard()


def test_get_best_model_without_training():
    """
    Test that getting best model without training raises an error.
    """
    ag = AutoGluonTimeSeries()

    with pytest.raises(ValueError, match="Model has not been trained yet"):
        ag.get_best_model()


def test_full_workflow(sample_timeseries_data, tmp_path):
    """
    Test complete workflow: split, train, predict, evaluate, save, load.
    """
    ag = AutoGluonTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=5,
        time_limit=120,
        preset="fast_training",
        verbosity=0,
    )

    # Split data
    train_df, test_df = ag.split_data(sample_timeseries_data, train_ratio=0.8)

    # Train
    ag.train(train_df)
    assert ag.predictor is not None

    # Get leaderboard
    leaderboard = ag.get_leaderboard()
    assert leaderboard is not None
    assert len(leaderboard) > 0

    # Get best model
    best_model = ag.get_best_model()
    assert best_model is not None
    assert isinstance(best_model, str)

    # Predict
    predictions = ag.predict(train_df)
    assert predictions is not None
    assert predictions.count() > 0

    # Evaluate
    metrics = ag.evaluate(test_df)
    assert metrics is not None
    assert isinstance(metrics, dict)

    # Save model
    model_path = str(tmp_path / "autogluon_model")
    ag.save_model(model_path)

    # Load model
    ag2 = AutoGluonTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=5,
    )
    ag2.load_model(model_path)
    assert ag2.predictor is not None

    # Predict with loaded model
    predictions2 = ag2.predict(train_df)
    assert predictions2 is not None
    assert predictions2.count() > 0


def test_system_type():
    """
    Test that system_type returns PYTHON.
    """
    from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import SystemType

    system_type = AutoGluonTimeSeries.system_type()
    assert system_type == SystemType.PYTHON


def test_libraries():
    """
    Test that libraries method returns AutoGluon dependency.
    """
    libraries = AutoGluonTimeSeries.libraries()
    assert libraries is not None
    assert len(libraries.pypi_libraries) > 0

    autogluon_found = False
    for lib in libraries.pypi_libraries:
        if "autogluon" in lib.name:
            autogluon_found = True
            break

    assert autogluon_found, "AutoGluon should be in the library dependencies"


def test_settings():
    """
    Test that settings method returns expected configuration.
    """
    settings = AutoGluonTimeSeries.settings()
    assert settings is not None
    assert isinstance(settings, dict)
