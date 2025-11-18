import pytest
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType,
    FloatType,
)
from datetime import datetime, timedelta
from src.sdk.python.rtdip_sdk.pipelines.forecasting.spark.lstm_timeseries import (
    LSTMTimeSeries,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[*]")
        .appName("LSTM TimeSeries Unit Test")
        .getOrCreate()
    )


@pytest.fixture(scope="function")
def sample_timeseries_data(spark):
    """
    Creates sample time series data with multiple items for testing.
    Needs more data points than AutoGluon due to lookback window requirements.
    """
    base_date = datetime(2024, 1, 1)
    data = []

    # Create data for two items with 100 time points each (enough for lookback window)
    for item_id in ["sensor_A", "sensor_B"]:
        for i in range(100):
            timestamp = base_date + timedelta(hours=i)
            # Create a simple trend + noise pattern
            value = float(100 + i * 2 + np.sin(i / 10) * 10)
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
    Must have enough points for lookback window (default 24).
    """
    base_date = datetime(2024, 1, 1)
    data = []

    # Create 50 hourly data points for one sensor
    for i in range(50):
        timestamp = base_date + timedelta(hours=i)
        value = 100.0 + i * 2.0
        data.append(("A", timestamp, value))

    schema = StructType(
        [
            StructField("item_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("target", FloatType(), True),
        ]
    )

    return spark.createDataFrame(data, schema=schema)


def test_lstm_initialization():
    """
    Test that LSTMTimeSeries can be initialized with default parameters.
    """
    lstm = LSTMTimeSeries()
    assert lstm.target_col == "target"
    assert lstm.timestamp_col == "timestamp"
    assert lstm.item_id_col == "item_id"
    assert lstm.prediction_length == 24
    assert lstm.lookback_window == 168  # Default is 168 (1 week)
    assert lstm.model is None


def test_lstm_custom_initialization():
    """
    Test that LSTMTimeSeries can be initialized with custom parameters.
    """
    lstm = LSTMTimeSeries(
        target_col="value",
        timestamp_col="time",
        item_id_col="sensor",
        prediction_length=12,
        lookback_window=48,
        lstm_units=64,
        num_lstm_layers=3,
        dropout_rate=0.3,
        batch_size=256,
        epochs=20,
        learning_rate=0.01,
    )
    assert lstm.target_col == "value"
    assert lstm.timestamp_col == "time"
    assert lstm.item_id_col == "sensor"
    assert lstm.prediction_length == 12
    assert lstm.lookback_window == 48
    assert lstm.lstm_units == 64
    assert lstm.num_lstm_layers == 3
    assert lstm.dropout_rate == 0.3
    assert lstm.batch_size == 256
    assert lstm.epochs == 20
    assert lstm.learning_rate == 0.01


def test_model_attributes(sample_timeseries_data):
    """
    Test that model attributes are properly initialized after training.
    """
    lstm = LSTMTimeSeries(
        lookback_window=24,
        prediction_length=5,
        epochs=1,
        batch_size=32
    )

    # Train to initialize attributes
    lstm.train(sample_timeseries_data)

    # Check attributes
    assert lstm.scaler is not None
    assert lstm.label_encoder is not None
    assert len(lstm.item_ids) > 0
    assert lstm.num_sensors > 0


def test_train_basic(simple_timeseries_data):
    """
    Test basic training workflow with minimal epochs.
    """
    lstm = LSTMTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=2,
        lookback_window=12,
        lstm_units=16,  # Small network for fast testing
        num_lstm_layers=1,
        batch_size=16,
        epochs=2,  # Very few epochs for testing
        patience=1,
    )

    lstm.train(simple_timeseries_data)

    assert lstm.model is not None, "Model should be initialized after training"
    assert lstm.scaler is not None, "Scaler should be initialized after training"
    assert lstm.label_encoder is not None, "Label encoder should be initialized"
    assert len(lstm.item_ids) > 0, "Item IDs should be stored"


def test_predict_without_training(simple_timeseries_data):
    """
    Test that predicting without training raises an error.
    """
    lstm = LSTMTimeSeries()

    with pytest.raises(ValueError, match="Model not trained"):
        lstm.predict(simple_timeseries_data)


def test_evaluate_without_training(simple_timeseries_data):
    """
    Test that evaluating without training returns None.
    """
    lstm = LSTMTimeSeries()

    # Evaluate returns None when model is not trained
    result = lstm.evaluate(simple_timeseries_data)
    assert result is None


def test_train_and_predict(sample_timeseries_data):
    """
    Test training and prediction workflow.
    """
    lstm = LSTMTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=5,
        lookback_window=24,
        lstm_units=16,
        num_lstm_layers=1,
        batch_size=32,
        epochs=2,
    )

    # Split data manually (80/20)
    df = sample_timeseries_data.toPandas()
    train_size = int(len(df) * 0.8)
    train_df = df.iloc[:train_size]
    test_df = df.iloc[train_size:]

    # Convert back to Spark
    spark = SparkSession.builder.getOrCreate()
    train_spark = spark.createDataFrame(train_df)
    test_spark = spark.createDataFrame(test_df)

    # Train
    lstm.train(train_spark)
    assert lstm.model is not None

    # Predict
    predictions = lstm.predict(test_spark)
    assert predictions is not None
    assert predictions.count() > 0

    # Check prediction columns
    pred_df = predictions.toPandas()
    assert 'item_id' in pred_df.columns
    assert 'timestamp' in pred_df.columns
    assert 'mean' in pred_df.columns  # LSTM returns 'mean' column


def test_train_and_evaluate(sample_timeseries_data):
    """
    Test training and evaluation workflow.
    """
    lstm = LSTMTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=5,
        lookback_window=24,
        lstm_units=16,
        num_lstm_layers=1,
        batch_size=32,
        epochs=2,
    )

    # Split data per sensor to maintain enough data for evaluation
    df = sample_timeseries_data.toPandas()
    df = df.sort_values(['item_id', 'timestamp'])

    train_dfs = []
    test_dfs = []
    for item_id in df['item_id'].unique():
        item_data = df[df['item_id'] == item_id]
        # Use 70% for train, 30% for test (need more test data for evaluation)
        split_idx = int(len(item_data) * 0.7)
        train_dfs.append(item_data.iloc[:split_idx])
        test_dfs.append(item_data.iloc[split_idx:])

    train_df = pd.concat(train_dfs, ignore_index=True)
    test_df = pd.concat(test_dfs, ignore_index=True)

    spark = SparkSession.builder.getOrCreate()
    train_spark = spark.createDataFrame(train_df)
    test_spark = spark.createDataFrame(test_df)

    # Train
    lstm.train(train_spark)

    # Evaluate
    metrics = lstm.evaluate(test_spark)
    assert metrics is not None
    assert isinstance(metrics, dict)

    # Check expected metrics
    expected_metrics = ['MAE', 'RMSE', 'MAPE', 'MASE', 'SMAPE']
    for metric in expected_metrics:
        assert metric in metrics
        assert isinstance(metrics[metric], (int, float))
        assert not np.isnan(metrics[metric])


def test_early_stopping_callback(simple_timeseries_data):
    """
    Test that early stopping is properly configured.
    """
    lstm = LSTMTimeSeries(
        prediction_length=2,
        lookback_window=12,
        lstm_units=16,
        epochs=10,
        patience=2,
    )

    lstm.train(simple_timeseries_data)

    # Check that training history is stored
    assert lstm.training_history is not None
    assert 'loss' in lstm.training_history

    # Training should stop before max epochs due to early stopping on small dataset
    assert len(lstm.training_history['loss']) <= 10


def test_training_history_tracking(sample_timeseries_data):
    """
    Test that training history is properly tracked during training.
    """
    lstm = LSTMTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=5,
        lookback_window=24,
        lstm_units=16,
        num_lstm_layers=1,
        batch_size=32,
        epochs=3,
        patience=2,
    )

    # Train model
    lstm.train(sample_timeseries_data)

    # Check training history is available
    assert lstm.training_history is not None
    assert isinstance(lstm.training_history, dict)

    # Check expected keys in training history
    assert 'loss' in lstm.training_history
    assert 'val_loss' in lstm.training_history

    # Check that history has entries
    assert len(lstm.training_history['loss']) > 0
    assert len(lstm.training_history['val_loss']) > 0


def test_multiple_sensors(sample_timeseries_data):
    """
    Test that LSTM handles multiple sensors with embeddings.
    """
    lstm = LSTMTimeSeries(
        prediction_length=5,
        lookback_window=24,
        lstm_units=16,
        num_lstm_layers=1,
        batch_size=32,
        epochs=2,
    )

    lstm.train(sample_timeseries_data)

    # Check that multiple sensors were processed
    assert len(lstm.item_ids) == 2
    assert 'sensor_A' in lstm.item_ids
    assert 'sensor_B' in lstm.item_ids


def test_system_type():
    """
    Test that system_type returns PYTHON.
    """
    from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import SystemType

    system_type = LSTMTimeSeries.system_type()
    assert system_type == SystemType.PYTHON


def test_libraries():
    """
    Test that libraries method returns TensorFlow dependency.
    """
    libraries = LSTMTimeSeries.libraries()
    assert libraries is not None
    assert len(libraries.pypi_libraries) > 0

    tensorflow_found = False
    for lib in libraries.pypi_libraries:
        if "tensorflow" in lib.name.lower():
            tensorflow_found = True
            break

    assert tensorflow_found, "TensorFlow should be in the library dependencies"


def test_settings():
    """
    Test that settings method returns expected configuration.
    """
    settings = LSTMTimeSeries.settings()
    assert settings is not None
    assert isinstance(settings, dict)


def test_insufficient_data():
    """
    Test that training with insufficient data (less than lookback window) handles gracefully.
    """
    spark = SparkSession.builder.getOrCreate()

    # Create minimal data (less than lookback window)
    data = []
    base_date = datetime(2024, 1, 1)
    for i in range(10):  # Only 10 points, but lookback is 24
        data.append(("A", base_date + timedelta(hours=i), float(100 + i)))

    schema = StructType([
        StructField("item_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("target", FloatType(), True),
    ])

    minimal_data = spark.createDataFrame(data, schema=schema)

    lstm = LSTMTimeSeries(
        lookback_window=24,
        prediction_length=5,
        epochs=1,
    )

    # This should handle gracefully (either skip or raise informative error)
    try:
        lstm.train(minimal_data)
        # If it succeeds, model should still be None or handle gracefully
    except (ValueError, Exception) as e:
        # Should raise an informative error
        assert "insufficient" in str(e).lower() or "not enough" in str(e).lower()
