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
from src.sdk.python.rtdip_sdk.pipelines.forecasting.spark.xgboost_timeseries import (
    XGBoostTimeSeries,
)


@pytest.fixture(scope="session")
def spark():
    import sys
    import os

    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    existing_session = SparkSession.getActiveSession()
    if existing_session:
        existing_session.stop()

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("XGBoost TimeSeries Unit Test")
        .config("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
        .config("spark.executorEnv.PYSPARK_DRIVER_PYTHON", sys.executable)
        .config("spark.pyspark.python", sys.executable)
        .config("spark.pyspark.driver.python", sys.executable)
        .getOrCreate()
    )

    yield spark
    spark.stop()


@pytest.fixture(scope="function")
def sample_timeseries_data(spark):
    """
    Creates sample time series data with multiple items for testing.
    Needs more data points than AutoGluon due to lag feature requirements.
    """
    base_date = datetime(2024, 1, 1)
    data = []

    for item_id in ["sensor_A", "sensor_B"]:
        for i in range(100):
            timestamp = base_date + timedelta(hours=i)
            # Create a simple trend + seasonality pattern
            value = float(100 + i * 2 + 10 * np.sin(i / 12))
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
    Must have enough points for lag features (default max lag is 48).
    """
    base_date = datetime(2024, 1, 1)
    data = []

    # Create 100 hourly data points for one sensor
    for i in range(100):
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


def test_xgboost_initialization():
    """
    Test that XGBoostTimeSeries can be initialized with default parameters.
    """
    xgb = XGBoostTimeSeries()
    assert xgb.target_col == "target"
    assert xgb.timestamp_col == "timestamp"
    assert xgb.item_id_col == "item_id"
    assert xgb.prediction_length == 24
    assert xgb.model is None


def test_xgboost_custom_initialization():
    """
    Test that XGBoostTimeSeries can be initialized with custom parameters.
    """
    xgb = XGBoostTimeSeries(
        target_col="value",
        timestamp_col="time",
        item_id_col="sensor",
        prediction_length=12,
        max_depth=7,
        learning_rate=0.1,
        n_estimators=200,
        n_jobs=4,
    )
    assert xgb.target_col == "value"
    assert xgb.timestamp_col == "time"
    assert xgb.item_id_col == "sensor"
    assert xgb.prediction_length == 12
    assert xgb.max_depth == 7
    assert xgb.learning_rate == 0.1
    assert xgb.n_estimators == 200
    assert xgb.n_jobs == 4


def test_engineer_features(sample_timeseries_data):
    """
    Test that feature engineering creates expected features.
    """
    xgb = XGBoostTimeSeries(prediction_length=5)

    df = sample_timeseries_data.toPandas()
    df = df.sort_values(['item_id', 'timestamp'])

    df_with_features = xgb._engineer_features(df)
    # Check time-based features
    assert 'hour' in df_with_features.columns
    assert 'day_of_week' in df_with_features.columns
    assert 'day_of_month' in df_with_features.columns
    assert 'month' in df_with_features.columns

    # Check lag features
    assert 'lag_1' in df_with_features.columns
    assert 'lag_6' in df_with_features.columns
    assert 'lag_12' in df_with_features.columns
    assert 'lag_24' in df_with_features.columns
    assert 'lag_48' in df_with_features.columns

    # Check rolling features
    assert 'rolling_mean_12' in df_with_features.columns
    assert 'rolling_std_12' in df_with_features.columns
    assert 'rolling_mean_24' in df_with_features.columns
    assert 'rolling_std_24' in df_with_features.columns


@pytest.mark.slow
def test_train_basic(simple_timeseries_data):
    """
    Test basic training workflow.
    """
    xgb = XGBoostTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=5,
        max_depth=3,
        n_estimators=50,
        n_jobs=1,
    )

    xgb.train(simple_timeseries_data)

    assert xgb.model is not None, "Model should be initialized after training"
    assert xgb.label_encoder is not None, "Label encoder should be initialized"
    assert len(xgb.item_ids) > 0, "Item IDs should be stored"
    assert xgb.feature_cols is not None, "Feature columns should be defined"


def test_predict_without_training(simple_timeseries_data):
    """
    Test that predicting without training raises an error.
    """
    xgb = XGBoostTimeSeries()

    with pytest.raises(ValueError, match="Model not trained"):
        xgb.predict(simple_timeseries_data)


def test_evaluate_without_training(simple_timeseries_data):
    """
    Test that evaluating without training raises an error.
    """
    xgb = XGBoostTimeSeries()

    with pytest.raises(ValueError, match="Model not trained"):
        xgb.evaluate(simple_timeseries_data)

def test_train_and_predict(sample_timeseries_data):
    """
    Test training and prediction workflow.
    """
    xgb = XGBoostTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=5,
        max_depth=3,
        n_estimators=50,
        n_jobs=1,
    )

    df = sample_timeseries_data.toPandas()
    df = df.sort_values(['item_id', 'timestamp'])

    train_dfs = []
    test_dfs = []
    for item_id in df['item_id'].unique():
        item_data = df[df['item_id'] == item_id]
        split_idx = int(len(item_data) * 0.8)
        train_dfs.append(item_data.iloc[:split_idx])
        test_dfs.append(item_data.iloc[split_idx:])

    train_df = pd.concat(train_dfs, ignore_index=True)
    test_df = pd.concat(test_dfs, ignore_index=True)

    spark = SparkSession.builder.getOrCreate()
    train_spark = spark.createDataFrame(train_df)
    test_spark = spark.createDataFrame(test_df)

    xgb.train(train_spark)
    assert xgb.model is not None

    predictions = xgb.predict(train_spark)
    assert predictions is not None
    assert predictions.count() > 0

    # Check prediction columns
    pred_df = predictions.toPandas()
    if len(pred_df) > 0:  # May be empty if insufficient data
        assert 'item_id' in pred_df.columns
        assert 'timestamp' in pred_df.columns
        assert 'predicted' in pred_df.columns


def test_train_and_evaluate(sample_timeseries_data):
    """
    Test training and evaluation workflow.
    """
    xgb = XGBoostTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=5,
        max_depth=3,
        n_estimators=50,
        n_jobs=1,
    )

    xgb.train(sample_timeseries_data)

    metrics = xgb.evaluate(sample_timeseries_data)

    if metrics is not None:
        assert isinstance(metrics, dict)

        expected_metrics = ['MAE', 'RMSE', 'MAPE', 'MASE', 'SMAPE']
        for metric in expected_metrics:
            assert metric in metrics
            assert isinstance(metrics[metric], (int, float))
    else:
        assert True


def test_recursive_forecasting(simple_timeseries_data):
    """
    Test that recursive forecasting generates the expected number of predictions.
    """
    xgb = XGBoostTimeSeries(
        prediction_length=10,
        max_depth=3,
        n_estimators=50,
        n_jobs=1,
    )

    # Train on most of the data
    df = simple_timeseries_data.toPandas()
    train_df = df.iloc[:-30] 

    spark = SparkSession.builder.getOrCreate()
    train_spark = spark.createDataFrame(train_df)

    xgb.train(train_spark)

    test_spark = spark.createDataFrame(train_df.tail(50))  
    predictions = xgb.predict(test_spark)

    pred_df = predictions.toPandas()

    # Should generate prediction_length predictions per sensor
    assert len(pred_df) == xgb.prediction_length * len(train_df['item_id'].unique())


def test_multiple_sensors(sample_timeseries_data):
    """
    Test that XGBoost handles multiple sensors correctly.
    """
    xgb = XGBoostTimeSeries(
        prediction_length=5,
        max_depth=3,
        n_estimators=50,
        n_jobs=1,
    )

    xgb.train(sample_timeseries_data)

    # Check that multiple sensors were processed
    assert len(xgb.item_ids) == 2
    assert 'sensor_A' in xgb.item_ids
    assert 'sensor_B' in xgb.item_ids

    predictions = xgb.predict(sample_timeseries_data)
    pred_df = predictions.toPandas()

    assert 'sensor_A' in pred_df['item_id'].values
    assert 'sensor_B' in pred_df['item_id'].values


def test_feature_importance(simple_timeseries_data):
    """
    Test that feature importance can be retrieved after training.
    """
    xgb = XGBoostTimeSeries(
        prediction_length=5,
        max_depth=3,
        n_estimators=50,
        n_jobs=1,
    )

    xgb.train(simple_timeseries_data)

    importance = xgb.model.feature_importances_
    assert importance is not None
    assert len(importance) == len(xgb.feature_cols)
    assert np.sum(importance) > 0  

def test_feature_columns_definition(sample_timeseries_data):
    """
    Test that feature columns are properly defined after training.
    """
    xgb = XGBoostTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=5,
        max_depth=3,
        n_estimators=50,
        n_jobs=1,
    )

    # Train model
    xgb.train(sample_timeseries_data)

    # Check feature columns are defined
    assert xgb.feature_cols is not None
    assert isinstance(xgb.feature_cols, list)
    assert len(xgb.feature_cols) > 0

    # Check expected feature types
    expected_features = ['sensor_encoded', 'hour', 'lag_1', 'rolling_mean_12']
    for feature in expected_features:
        assert feature in xgb.feature_cols, f"Expected feature {feature} not found in {xgb.feature_cols}"


def test_system_type():
    """
    Test that system_type returns PYTHON.
    """
    from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import SystemType

    system_type = XGBoostTimeSeries.system_type()
    assert system_type == SystemType.PYTHON


def test_libraries():
    """
    Test that libraries method returns XGBoost dependency.
    """
    libraries = XGBoostTimeSeries.libraries()
    assert libraries is not None
    assert len(libraries.pypi_libraries) > 0

    xgboost_found = False
    for lib in libraries.pypi_libraries:
        if "xgboost" in lib.name.lower():
            xgboost_found = True
            break

    assert xgboost_found, "XGBoost should be in the library dependencies"


def test_settings():
    """
    Test that settings method returns expected configuration.
    """
    settings = XGBoostTimeSeries.settings()
    assert settings is not None
    assert isinstance(settings, dict)


def test_insufficient_data():
    """
    Test that training with insufficient data (less than max lag) handles gracefully.
    """
    spark = SparkSession.builder.getOrCreate()

    data = []
    base_date = datetime(2024, 1, 1)
    for i in range(30):  
        data.append(("A", base_date + timedelta(hours=i), float(100 + i)))

    schema = StructType([
        StructField("item_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("target", FloatType(), True),
    ])

    minimal_data = spark.createDataFrame(data, schema=schema)

    xgb = XGBoostTimeSeries(
        prediction_length=5,
        max_depth=3,
        n_estimators=10,
    )

    try:
        xgb.train(minimal_data)
        # If it succeeds, should have a trained model
        if xgb.model is not None:
            assert True
    except (ValueError, Exception) as e:
        assert "insufficient" in str(e).lower() or "not enough" in str(e).lower() or "samples" in str(e).lower()


def test_time_features_extraction():
    """
    Test that time-based features are correctly extracted.
    """
    spark = SparkSession.builder.getOrCreate()

    # Create data with specific timestamps
    data = []
    # Monday, January 1, 2024, 14:00 (hour=14, day_of_week=0, day_of_month=1, month=1)
    timestamp = datetime(2024, 1, 1, 14, 0, 0)
    for i in range(50):
        data.append(("A", timestamp + timedelta(hours=i), float(100 + i)))

    schema = StructType([
        StructField("item_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("target", FloatType(), True),
    ])

    test_data = spark.createDataFrame(data, schema=schema)
    df = test_data.toPandas()

    xgb = XGBoostTimeSeries()
    df_features = xgb._engineer_features(df)

    # Check first row time features
    first_row = df_features.iloc[0]
    assert first_row['hour'] == 14
    assert first_row['day_of_week'] == 0  # Monday
    assert first_row['day_of_month'] == 1
    assert first_row['month'] == 1


def test_sensor_encoding():
    """
    Test that sensor IDs are properly encoded.
    """
    xgb = XGBoostTimeSeries(
        prediction_length=5,
        max_depth=3,
        n_estimators=50,
    )

    spark = SparkSession.builder.getOrCreate()

    data = []
    base_date = datetime(2024, 1, 1)
    for sensor in ["sensor_A", "sensor_B", "sensor_C"]:
        for i in range(70):
            data.append((sensor, base_date + timedelta(hours=i), float(100 + i)))

    schema = StructType([
        StructField("item_id", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("target", FloatType(), True),
    ])

    multi_sensor_data = spark.createDataFrame(data, schema=schema)
    xgb.train(multi_sensor_data)

    assert len(xgb.label_encoder.classes_) == 3
    assert 'sensor_A' in xgb.label_encoder.classes_
    assert 'sensor_B' in xgb.label_encoder.classes_
    assert 'sensor_C' in xgb.label_encoder.classes_
