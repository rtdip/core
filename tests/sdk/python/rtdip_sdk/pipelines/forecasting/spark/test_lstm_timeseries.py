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


# Note: Uses spark_session fixture from tests/conftest.py
# Do NOT define a local spark fixture - it causes session conflicts with other tests


@pytest.fixture(scope="function")
def sample_timeseries_data(spark_session):
    """
    Creates sample time series data with multiple items for testing.
    Needs more data points than AutoGluon due to lookback window requirements.
    """
    base_date = datetime(2024, 1, 1)
    data = []

    for item_id in ["sensor_A", "sensor_B"]:
        for i in range(100):
            timestamp = base_date + timedelta(hours=i)
            value = float(100 + i * 2 + np.sin(i / 10) * 10)
            data.append((item_id, timestamp, value))

    schema = StructType(
        [
            StructField("item_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("target", FloatType(), True),
        ]
    )

    return spark_session.createDataFrame(data, schema=schema)


@pytest.fixture(scope="function")
def simple_timeseries_data(spark_session):
    """
    Creates simple time series data for basic testing.
    Must have enough points for lookback window (default 24).
    """
    base_date = datetime(2024, 1, 1)
    data = []

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

    return spark_session.createDataFrame(data, schema=schema)


def test_lstm_initialization():
    """
    Test that LSTMTimeSeries can be initialized with default parameters.
    """
    lstm = LSTMTimeSeries()
    assert lstm.target_col == "target"
    assert lstm.timestamp_col == "timestamp"
    assert lstm.item_id_col == "item_id"
    assert lstm.prediction_length == 24
    assert lstm.lookback_window == 168
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
    assert np.isclose(lstm.dropout_rate, 0.3, rtol=1e-09, atol=1e-09)
    assert lstm.batch_size == 256
    assert lstm.epochs == 20
    assert np.isclose(lstm.learning_rate, 0.01, rtol=1e-09, atol=1e-09)


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
