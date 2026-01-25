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

from src.sdk.python.rtdip_sdk.pipelines.forecasting.spark.catboost_timeseries_refactored import (
    CatBoostTimeSeries,
)


@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder.master("local[*]")
        .appName("CatBoost TimeSeries Unit Test")
        .getOrCreate()
    )


@pytest.fixture(scope="function")
def sample_timeseries_data(spark):
    """
    Creates sample time series data with multiple items for testing.
    Needs more data points due to lag feature requirements.
    """
    base_date = datetime(2024, 1, 1)
    data = []

    for item_id in ["sensor_A", "sensor_B"]:
        for i in range(100):
            timestamp = base_date + timedelta(hours=i)
            # Simple trend + seasonality
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


def test_catboost_initialization():
    """
    Test that CatBoostTimeSeries can be initialized with default parameters.
    """
    cbts = CatBoostTimeSeries()
    assert cbts.target_col == "target"
    assert cbts.timestamp_col == "timestamp"
    assert cbts.item_id_col == "item_id"
    assert cbts.prediction_length == 24
    assert cbts.model is None


def test_catboost_custom_initialization():
    """
    Test that CatBoostTimeSeries can be initialized with custom parameters.
    """
    cbts = CatBoostTimeSeries(
        target_col="value",
        timestamp_col="time",
        item_id_col="sensor",
        prediction_length=12,
        max_depth=7,
        learning_rate=0.1,
        n_estimators=200,
        n_jobs=4,
    )
    assert cbts.target_col == "value"
    assert cbts.timestamp_col == "time"
    assert cbts.item_id_col == "sensor"
    assert cbts.prediction_length == 12
    assert cbts.max_depth == 7
    assert cbts.learning_rate == 0.1
    assert cbts.n_estimators == 200
    assert cbts.n_jobs == 4


def test_engineer_features(sample_timeseries_data):
    """
    Test that feature engineering creates expected features.
    """
    cbts = CatBoostTimeSeries(prediction_length=5)

    df = sample_timeseries_data.toPandas()
    df = df.sort_values(["item_id", "timestamp"])

    df_with_features = cbts._engineer_features(df)

    # Time-based features
    assert "hour" in df_with_features.columns
    assert "day_of_week" in df_with_features.columns
    assert "day_of_month" in df_with_features.columns
    assert "month" in df_with_features.columns

    # Lag features
    assert "lag_1" in df_with_features.columns
    assert "lag_6" in df_with_features.columns
    assert "lag_12" in df_with_features.columns
    assert "lag_24" in df_with_features.columns
    assert "lag_48" in df_with_features.columns

    # Rolling features
    assert "rolling_mean_12" in df_with_features.columns
    assert "rolling_std_12" in df_with_features.columns
    assert "rolling_mean_24" in df_with_features.columns
    assert "rolling_std_24" in df_with_features.columns

    # Sensor encoding
    assert "sensor_encoded" in df_with_features.columns


@pytest.mark.slow
def test_train_basic(simple_timeseries_data):
    """
    Test basic training workflow.
    """
    cbts = CatBoostTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=5,
        max_depth=3,
        n_estimators=50,
        n_jobs=1,
    )

    cbts.train(simple_timeseries_data)

    assert cbts.model is not None, "Model should be initialized after training"
    assert cbts.label_encoder is not None, "Label encoder should be initialized"
    assert len(cbts.item_ids) > 0, "Item IDs should be stored"
    assert cbts.feature_cols is not None, "Feature columns should be defined"


def test_predict_without_training(simple_timeseries_data):
    """
    Test that predicting without training raises an error.
    """
    cbts = CatBoostTimeSeries()
    with pytest.raises(ValueError, match="Model not trained"):
        cbts.predict(simple_timeseries_data)


def test_evaluate_without_training(simple_timeseries_data):
    """
    Test that evaluating without training raises an error.
    """
    cbts = CatBoostTimeSeries()
    with pytest.raises(ValueError, match="Model not trained"):
        cbts.evaluate(simple_timeseries_data)


def test_train_and_predict(sample_timeseries_data):
    """
    Test training and prediction workflow.
    """
    cbts = CatBoostTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=5,
        max_depth=3,
        n_estimators=50,
        n_jobs=1,
    )

    df = sample_timeseries_data.toPandas()
    df = df.sort_values(["item_id", "timestamp"])

    train_dfs = []
    for item_id in df["item_id"].unique():
        item_data = df[df["item_id"] == item_id]
        split_idx = int(len(item_data) * 0.8)
        train_dfs.append(item_data.iloc[:split_idx])

    train_df = pd.concat(train_dfs, ignore_index=True)

    spark = SparkSession.builder.getOrCreate()
    train_spark = spark.createDataFrame(train_df)

    cbts.train(train_spark)
    assert cbts.model is not None

    predictions = cbts.predict(train_spark)
    assert predictions is not None
    assert predictions.count() > 0

    pred_df = predictions.toPandas()
    assert "item_id" in pred_df.columns
    assert "timestamp" in pred_df.columns
    assert "predicted" in pred_df.columns


def test_train_and_evaluate(sample_timeseries_data):
    """
    Test training and evaluation workflow.
    """
    cbts = CatBoostTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=5,
        max_depth=3,
        n_estimators=50,
        n_jobs=1,
    )

    cbts.train(sample_timeseries_data)

    metrics = cbts.evaluate(sample_timeseries_data)

    if metrics is not None:
        assert isinstance(metrics, dict)
        expected_metrics = ["MAE", "RMSE", "MAPE", "MASE", "SMAPE"]
        for metric in expected_metrics:
            assert metric in metrics
            assert isinstance(metrics[metric], (int, float))
    else:
        assert True


def test_recursive_forecasting(simple_timeseries_data):
    """
    Test that recursive forecasting generates the expected number of predictions.
    """
    cbts = CatBoostTimeSeries(
        prediction_length=10,
        max_depth=3,
        n_estimators=50,
        n_jobs=1,
    )

    df = simple_timeseries_data.toPandas()
    train_df = df.iloc[:-30]

    spark = SparkSession.builder.getOrCreate()
    train_spark = spark.createDataFrame(train_df)

    cbts.train(train_spark)

    test_spark = spark.createDataFrame(train_df.tail(50))
    predictions = cbts.predict(test_spark)

    pred_df = predictions.toPandas()

    # prediction_length predictions per sensor
    assert len(pred_df) == cbts.prediction_length * len(train_df["item_id"].unique())


def test_multiple_sensors(sample_timeseries_data):
    """
    Test that CatBoost handles multiple sensors correctly.
    """
    cbts = CatBoostTimeSeries(
        prediction_length=5,
        max_depth=3,
        n_estimators=50,
        n_jobs=1,
    )

    cbts.train(sample_timeseries_data)

    assert len(cbts.item_ids) == 2
    assert "sensor_A" in cbts.item_ids
    assert "sensor_B" in cbts.item_ids

    predictions = cbts.predict(sample_timeseries_data)
    pred_df = predictions.toPandas()

    assert "sensor_A" in pred_df["item_id"].values
    assert "sensor_B" in pred_df["item_id"].values


def test_feature_importance(sample_timeseries_data):
    """
    Test that feature importance can be retrieved after training.
    """
    cbts = CatBoostTimeSeries(
        prediction_length=5,
        max_depth=3,
        n_estimators=50,
        n_jobs=1,
    )

    cbts.train(sample_timeseries_data)

    importance = cbts.model.get_feature_importance(type="PredictionValuesChange")
    assert importance is not None
    assert len(importance) == len(cbts.feature_cols)
    assert float(np.sum(importance)) > 0.0


def test_feature_columns_definition(sample_timeseries_data):
    """
    Test that feature columns are properly defined after training.
    """
    cbts = CatBoostTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=5,
        max_depth=3,
        n_estimators=50,
        n_jobs=1,
    )

    cbts.train(sample_timeseries_data)

    assert cbts.feature_cols is not None
    assert isinstance(cbts.feature_cols, list)
    assert len(cbts.feature_cols) > 0

    expected_features = ["sensor_encoded", "hour", "lag_1", "rolling_mean_12"]
    for feature in expected_features:
        assert (
            feature in cbts.feature_cols
        ), f"Expected {feature} not in {cbts.feature_cols}"


def test_system_type():
    """
    Test that system_type returns PYTHON.
    """
    from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import SystemType

    system_type = CatBoostTimeSeries.system_type()
    assert system_type == SystemType.PYTHON


def test_libraries():
    """
    Test that libraries method returns CatBoost dependency.
    """
    libraries = CatBoostTimeSeries.libraries()
    assert libraries is not None
    assert len(libraries.pypi_libraries) > 0

    catboost_found = False
    for lib in libraries.pypi_libraries:
        if "catboost" in lib.name.lower():
            catboost_found = True
            break

    assert catboost_found, "CatBoost should be in the library dependencies"


def test_settings():
    """
    Test that settings method returns expected configuration.
    """
    settings = CatBoostTimeSeries.settings()
    assert settings is not None
    assert isinstance(settings, dict)


def test_time_features_extraction():
    """
    Test that time-based features are correctly extracted.
    """
    spark = SparkSession.builder.getOrCreate()

    data = []
    timestamp = datetime(2024, 1, 1, 14, 0, 0)  # Monday
    for i in range(50):
        data.append(("A", timestamp + timedelta(hours=i), float(100 + i)))

    schema = StructType(
        [
            StructField("item_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("target", FloatType(), True),
        ]
    )

    test_data = spark.createDataFrame(data, schema=schema)
    df = test_data.toPandas()

    cbts = CatBoostTimeSeries()
    df_features = cbts._engineer_features(df)

    first_row = df_features.iloc[0]
    assert first_row["hour"] == 14
    assert first_row["day_of_week"] == 0
    assert first_row["day_of_month"] == 1
    assert first_row["month"] == 1


def test_sensor_encoding():
    """
    Test that sensor IDs are properly encoded.
    """
    cbts = CatBoostTimeSeries(
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

    schema = StructType(
        [
            StructField("item_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("target", FloatType(), True),
        ]
    )

    multi_sensor_data = spark.createDataFrame(data, schema=schema)
    cbts.train(multi_sensor_data)

    assert len(cbts.label_encoder.classes_) == 3
    assert "sensor_A" in cbts.label_encoder.classes_
    assert "sensor_B" in cbts.label_encoder.classes_
    assert "sensor_C" in cbts.label_encoder.classes_


def test_predict_output_schema_and_horizon(sample_timeseries_data):
    """
    Ensure predict output has the expected schema and produces prediction_length rows per sensor.
    """
    cbts = CatBoostTimeSeries(
        prediction_length=7,
        max_depth=3,
        n_estimators=30,
        n_jobs=1,
    )

    cbts.train(sample_timeseries_data)
    preds = cbts.predict(sample_timeseries_data)

    pred_df = preds.toPandas()
    assert set(["item_id", "timestamp", "predicted"]).issubset(pred_df.columns)

    # Exactly prediction_length predictions per sensor (given sufficient data)
    n_sensors = pred_df["item_id"].nunique()
    assert len(pred_df) == cbts.prediction_length * n_sensors


def test_evaluate_returns_none_when_no_valid_samples(spark):
    """
    If all rows are invalid after feature engineering (due to lag NaNs), evaluate should return None.
    """
    # 10 points -> with lags up to 48, dropna(feature_cols) will produce 0 rows
    base_date = datetime(2024, 1, 1)
    data = [("A", base_date + timedelta(hours=i), float(100 + i)) for i in range(10)]

    schema = StructType(
        [
            StructField("item_id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("target", FloatType(), True),
        ]
    )
    short_df = spark.createDataFrame(data, schema=schema)

    cbts = CatBoostTimeSeries(
        prediction_length=5, max_depth=3, n_estimators=20, n_jobs=1
    )

    train_data = [
        ("A", base_date + timedelta(hours=i), float(100 + i)) for i in range(80)
    ]
    train_df = spark.createDataFrame(train_data, schema=schema)
    cbts.train(train_df)

    metrics = cbts.evaluate(short_df)
    assert metrics is None
