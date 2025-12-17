"""
Unified Training and Comparison Script for AutoGluon, LSTM, and XGBoost

This script trains AutoGluon ensemble, LSTM, and XGBoost models on Shell sensor data,
evaluates them, and generates a comparison report.

REQUIREMENTS:
- pip install autogluon.timeseries
- pip install tensorflow
- pip install xgboost
- pip install pandas pyarrow scikit-learn
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import json

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

script_dir = os.path.dirname(os.path.abspath(__file__))
repo_root = os.path.abspath(os.path.join(script_dir, "..", "..", ".."))
sdk_path = os.path.join(repo_root, "src", "sdk", "python")
sys.path.insert(0, sdk_path)

from autogluon.timeseries import TimeSeriesDataFrame, TimeSeriesPredictor
from rtdip_sdk.pipelines.forecasting.spark.lstm_timeseries import LSTMTimeSeries
from rtdip_sdk.pipelines.forecasting.spark.xgboost_timeseries import XGBoostTimeSeries
from pyspark.sql import SparkSession

DATA_PATH = "ShellData_preprocessed_final.parquet"
OUTPUT_DIR = "comparison_results"
AUTOGLUON_DIR = os.path.join(OUTPUT_DIR, "autogluon")
LSTM_DIR = os.path.join(OUTPUT_DIR, "lstm")
XGBOOST_DIR = os.path.join(OUTPUT_DIR, "xgboost")

# Training Configuration
TOP_N_SENSORS = 10
PREDICTION_LENGTH = 24
TRAIN_RATIO = 0.7
VAL_RATIO = 0.15
TEST_RATIO = 0.15
TIME_LIMIT = 600
EVAL_METRIC = "MAE"
PRESET = "medium_quality"
FREQ = "h"


def load_shell_data(data_path, top_n_sensors=10, sample_ratio=None):
    """
    Load preprocessed Shell data and prepare for time series forecasting.
    """
    print("LOADING SHELL DATA")
    print(f"Loading data from: {data_path}")

    df = pd.read_parquet(data_path)
    print(f"Loaded {len(df):,} rows, {len(df.columns)} columns")

    if sample_ratio:
        original_len = len(df)
        df = df.sample(frac=sample_ratio, random_state=42)
        print(f"Sampled {sample_ratio*100}% of data: {len(df):,} rows")

    print(f"\nSelecting top {top_n_sensors} sensors by data volume")
    sensor_counts = df["TagName"].value_counts()
    top_sensors = sensor_counts.head(top_n_sensors).index.tolist()

    df_filtered = df[df["TagName"].isin(top_sensors)].copy()
    print(f"Selected {len(df_filtered):,} rows from {top_n_sensors} sensors")

    print("\nTop sensors:")
    for i, (sensor, count) in enumerate(sensor_counts.head(top_n_sensors).items(), 1):
        print(f"  {i}. {sensor}: {count:,} data points")

    ts_data = pd.DataFrame(
        {
            "item_id": df_filtered["TagName"],
            "timestamp": df_filtered["EventTime"],
            "target": df_filtered["Value"],
        }
    )

    # Remove any null values not cleared in preprocessing for some reason (shouldnt really happen)
    original_len = len(ts_data)
    ts_data = ts_data.dropna(subset=["target"])
    if len(ts_data) < original_len:
        print(f"  Removed {original_len - len(ts_data):,} rows with null target values")

    original_len = len(ts_data)
    ts_data = ts_data[ts_data["target"] != -1].copy()
    if len(ts_data) < original_len:
        print(
            f"  Removed {original_len - len(ts_data):,} rows with error markers (Value = -1)"
        )

    ts_data = ts_data.sort_values(["item_id", "timestamp"]).reset_index(drop=True)

    print(f"Final dataset: {len(ts_data):,} rows")
    print(f"Time range: {ts_data['timestamp'].min()} to {ts_data['timestamp'].max()}")
    print(
        f"Target range: [{ts_data['target'].min():.2f}, {ts_data['target'].max():.2f}]"
    )

    return ts_data


def split_timeseries_data(df, train_ratio=0.7, val_ratio=0.15, test_ratio=0.15):
    """
    Split time series data into train/val/test sets (time-aware, per sensor).
    Each sensor's timeline is split individually to ensure all sensors appear in all splits.
    """
    print("SPLITTING DATA")

    assert (
        abs(train_ratio + val_ratio + test_ratio - 1.0) < 0.001
    ), "Split ratios must sum to 1.0"

    train_dfs = []
    val_dfs = []
    test_dfs = []

    item_ids = df["item_id"].unique()
    print(f"Splitting {len(item_ids)} sensors individually...")

    for item_id in item_ids:
        item_data = df[df["item_id"] == item_id].copy()
        item_data = item_data.sort_values("timestamp")

        n = len(item_data)
        train_end = int(n * train_ratio)
        val_end = int(n * (train_ratio + val_ratio))

        train_dfs.append(item_data.iloc[:train_end])
        val_dfs.append(item_data.iloc[train_end:val_end])
        test_dfs.append(item_data.iloc[val_end:])

    # Combine splits again
    train_df = pd.concat(train_dfs, ignore_index=True)
    val_df = pd.concat(val_dfs, ignore_index=True)
    test_df = pd.concat(test_dfs, ignore_index=True)

    n = len(df)
    print(f"Split ratios: {train_ratio:.0%} / {val_ratio:.0%} / {test_ratio:.0%}")
    print(f"Train set: {len(train_df):,} rows ({len(train_df)/n:.1%})")
    print(
        f"  Time range: {train_df['timestamp'].min()} to {train_df['timestamp'].max()}"
    )
    print(f"  Unique sensors: {train_df['item_id'].nunique()}")
    print(f"Val set:   {len(val_df):,} rows ({len(val_df)/n:.1%})")
    print(f"  Time range: {val_df['timestamp'].min()} to {val_df['timestamp'].max()}")
    print(f"  Unique sensors: {val_df['item_id'].nunique()}")
    print(f"Test set:  {len(test_df):,} rows ({len(test_df)/n:.1%})")
    print(f"  Time range: {test_df['timestamp'].min()} to {test_df['timestamp'].max()}")
    print(f"  Unique sensors: {test_df['item_id'].nunique()}")

    return train_df, val_df, test_df


def create_timeseries_dataframe(df, freq="h"):
    """
    Convert pandas DataFrame to AutoGluon TimeSeriesDataFrame.
    """
    df["timestamp"] = pd.to_datetime(df["timestamp"])

    ts_df = TimeSeriesDataFrame.from_data_frame(
        df, id_column="item_id", timestamp_column="timestamp"
    )
    ts_df = ts_df.convert_frequency(freq=freq)

    return ts_df


def train_autogluon(
    train_df, prediction_length, eval_metric, time_limit, preset, freq="h", verbosity=2
):
    """Train AutoGluon time series predictor."""
    print("TRAINING AUTOGLUON MODELS")
    print(f"Prediction length: {prediction_length} time steps")
    print(f"Time frequency: {freq}")
    print(f"Evaluation metric: {eval_metric}")
    print(f"Time limit: {time_limit} seconds ({time_limit/60:.1f} minutes)")
    print(f"Quality preset: {preset}")

    train_data = create_timeseries_dataframe(train_df, freq=freq)
    print(f"\nCreated TimeSeriesDataFrame with {len(train_data)} rows")

    predictor = TimeSeriesPredictor(
        prediction_length=prediction_length,
        eval_metric=eval_metric,
        freq=freq,
        verbosity=verbosity,
    )

    # Train models
    print("\nStarting training")
    start_time = datetime.now()

    predictor.fit(train_data=train_data, time_limit=time_limit, presets=preset)

    end_time = datetime.now()
    training_duration = (end_time - start_time).total_seconds()

    print(
        f"Training completed in {training_duration:.1f} seconds ({training_duration/60:.1f} minutes)"
    )

    return predictor


def evaluate_autogluon(predictor, test_df, freq="h"):
    """Evaluate AutoGluon model and return metrics + future forecasts.

    Note: AutoGluon's predict() generates future forecasts, not predictions at test timestamps.
    The returned predictions_df contains forecasts beyond the test set. Not really suited for comparison with LSTM
    /XGBoost predictions
    (afaik, but thats what we have to work with for now).
    """
    print("EVALUATING AUTOGLUON")

    test_data = create_timeseries_dataframe(test_df, freq=freq)

    print("Computing metrics")
    metrics = predictor.evaluate(
        test_data, metrics=["MAE", "RMSE", "MAPE", "MASE", "SMAPE"]
    )

    print("\nAutoGluon Metrics:")
    for metric_name, metric_value in metrics.items():
        print(f"{metric_name:20s}: {metric_value:.4f}")

    leaderboard = predictor.leaderboard()
    best_model = leaderboard.iloc[0]["model"]
    print(f"\nBest Model: {best_model}")

    print("\nGenerating future forecasts")
    predictions = predictor.predict(test_data)
    predictions_df = predictions.reset_index()
    # predictions_df has columns: ['item_id', 'timestamp', 'mean', quantiles...]

    print(f"Generated {len(predictions_df)} future forecast rows")
    print(
        f"Note: These are forecasts beyond the test set, not aligned with test timestamps"
    )

    return metrics, leaderboard, predictions_df


def train_lstm(train_df, val_df, prediction_length=24, lookback_window=168):
    """Train LSTM model."""
    print("TRAINING LSTM MODEL")

    spark = (
        SparkSession.builder.appName("LSTM Training").master("local[*]").getOrCreate()
    )

    train_spark = spark.createDataFrame(train_df)

    # Initialize LSTM
    lstm_model = LSTMTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=prediction_length,
        lookback_window=72,  # Should probably be 168, but reduce to speed up training
        lstm_units=48,  # Should probably be 64, but reduce to speed up training
        num_lstm_layers=2,
        dropout_rate=0.2,
        batch_size=512,
        epochs=10,
        patience=5,
    )

    lstm_model.train(train_spark)

    return lstm_model


def evaluate_lstm(lstm_model, test_df):
    """Evaluate LSTM model and return metrics + predictions."""
    print("EVALUATING LSTM")

    spark = (
        SparkSession.builder.appName("LSTM Evaluation").master("local[*]").getOrCreate()
    )

    test_spark = spark.createDataFrame(test_df)

    print("Computing metrics")
    metrics = lstm_model.evaluate(test_spark)

    if metrics:
        print("\nLSTM Metrics:")
        for metric_name, metric_value in metrics.items():
            print(f"{metric_name:20s}: {metric_value:.4f}")

    print("\nGenerating future forecasts")
    predictions_spark = lstm_model.predict(test_spark)
    predictions_df = predictions_spark.toPandas()

    print(f"Generated {len(predictions_df)} prediction rows")

    return metrics, predictions_df


def train_xgboost(train_df, prediction_length=24):
    """Train XGBoost model."""
    print("TRAINING XGBOOST MODEL")

    spark = (
        SparkSession.builder.appName("XGBoost Training")
        .master("local[*]")
        .getOrCreate()
    )

    train_spark = spark.createDataFrame(train_df)

    # Initialize XGBoost
    xgboost_model = XGBoostTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=prediction_length,
        max_depth=5,
        learning_rate=0.05,
        n_estimators=150,
        n_jobs=-1,
    )

    xgboost_model.train(train_spark)

    return xgboost_model


def evaluate_xgboost(xgboost_model, test_df):
    """Evaluate XGBoost model and return metrics + predictions."""
    print("EVALUATING XGBOOST")

    spark = (
        SparkSession.builder.appName("XGBoost Evaluation")
        .master("local[*]")
        .getOrCreate()
    )

    test_spark = spark.createDataFrame(test_df)

    print("Computing metrics...")
    metrics = xgboost_model.evaluate(test_spark)

    if metrics:
        print("\nXGBoost Metrics:")
        print("-" * 80)
        for metric_name, metric_value in metrics.items():
            print(f"{metric_name:20s}: {abs(metric_value):.4f}")

    print("\nGenerating future forecasts")
    predictions_spark = xgboost_model.predict(test_spark)
    predictions_df = predictions_spark.toPandas()

    print(f"Generated {len(predictions_df)} prediction rows")

    return metrics, predictions_df


def compute_residuals(predictions_df, test_df, model_name):
    """Compute residuals (predicted - actual) for a model."""
    predictions_df = predictions_df.copy()
    test_df = test_df.copy()
    predictions_df["timestamp"] = pd.to_datetime(predictions_df["timestamp"])
    test_df["timestamp"] = pd.to_datetime(test_df["timestamp"])

    # Rename 'mean' to 'predicted' if needed (AutoGluon formatting, match others)
    if "mean" in predictions_df.columns:
        predictions_df = predictions_df.rename(columns={"mean": "predicted"})

    # Try exact timestamp match first
    merged = pd.merge(
        test_df[["item_id", "timestamp", "target"]],
        predictions_df[["item_id", "timestamp", "predicted"]],
        on=["item_id", "timestamp"],
        how="inner",
    )

    # If no exact matches found, use hourly normalization
    if len(merged) == 0:
        print(
            f"   No exact timestamp matches for {model_name}, using hourly normalization"
        )
        predictions_df["timestamp_normalized"] = predictions_df["timestamp"].dt.floor(
            "h"
        )
        test_df["timestamp_normalized"] = test_df["timestamp"].dt.floor("h")

        merged = pd.merge(
            test_df[["item_id", "timestamp", "timestamp_normalized", "target"]],
            predictions_df[["item_id", "timestamp_normalized", "predicted"]],
            on=["item_id", "timestamp_normalized"],
            how="inner",
        )

    merged["residual"] = merged["predicted"] - merged["target"]
    merged["model_name"] = model_name

    return merged[
        ["item_id", "timestamp", "target", "predicted", "residual", "model_name"]
    ]


def compute_per_sensor_metrics(predictions_df, test_df, model_name):
    """Compute evaluation metrics for each sensor individually."""
    from sklearn.metrics import (
        mean_absolute_error,
        mean_squared_error,
        mean_absolute_percentage_error,
    )

    residuals_df = compute_residuals(predictions_df, test_df, model_name)

    per_sensor_results = []

    for item_id in residuals_df["item_id"].unique():
        sensor_data = residuals_df[residuals_df["item_id"] == item_id]

        y_true = sensor_data["target"].values
        y_pred = sensor_data["predicted"].values

        mae = mean_absolute_error(y_true, y_pred)
        mse = mean_squared_error(y_true, y_pred)
        rmse = np.sqrt(mse)

        # MAPE (filter near-zero values)
        non_zero_mask = np.abs(y_true) >= 0.1
        if np.sum(non_zero_mask) > 0:
            mape = mean_absolute_percentage_error(
                y_true[non_zero_mask], y_pred[non_zero_mask]
            )
        else:
            mape = np.nan

        # MASE
        if len(y_true) > 1:
            naive_forecast = y_true[:-1]
            mae_naive = mean_absolute_error(y_true[1:], naive_forecast)
            mase = mae / mae_naive if mae_naive != 0 else mae
        else:
            mase = np.nan

        # SMAPE
        smape = (
            100
            * (
                2 * np.abs(y_true - y_pred) / (np.abs(y_true) + np.abs(y_pred) + 1e-10)
            ).mean()
        )

        per_sensor_results.append(
            {
                "item_id": item_id,
                "model_name": model_name,
                "MAE": mae,
                "RMSE": rmse,
                "MAPE": mape,
                "MASE": mase,
                "SMAPE": smape,
                "num_predictions": len(y_true),
            }
        )

    return pd.DataFrame(per_sensor_results)


def generate_comparison_report(
    ag_metrics, lstm_metrics, xgboost_metrics, ag_leaderboard, output_dir
):
    """Generate comparison report for AutoGluon vs LSTM vs XGBoost."""
    print("COMPARISON REPORT")

    os.makedirs(output_dir, exist_ok=True)

    comparison_data = []
    for metric_name in ["MAE", "RMSE", "MAPE", "MASE", "SMAPE"]:
        ag_value = ag_metrics[metric_name]
        lstm_value = lstm_metrics[metric_name]
        xgboost_value = xgboost_metrics[metric_name]

        # Convert back to positive for display (higher negative is better → lower positive is better)
        ag_abs = abs(ag_value)
        lstm_abs = abs(lstm_value)
        xgboost_abs = abs(xgboost_value)

        # Determine winner (higher negative value is better -- i.e., lower absolute error)
        values = {"AutoGluon": ag_value, "LSTM": lstm_value, "XGBoost": xgboost_value}
        winner = max(values.items(), key=lambda x: x[1])[0]

        comparison_data.append(
            {
                "Metric": metric_name,
                "AutoGluon": ag_abs,
                "LSTM": lstm_abs,
                "XGBoost": xgboost_abs,
                "Winner": winner,
            }
        )

    comparison_df = pd.DataFrame(comparison_data)

    print("\nSide-by-Side Comparison:")
    print(comparison_df.to_string(index=False))

    ag_wins = sum(1 for row in comparison_data if row["Winner"] == "AutoGluon")
    lstm_wins = sum(1 for row in comparison_data if row["Winner"] == "LSTM")
    xgboost_wins = sum(1 for row in comparison_data if row["Winner"] == "XGBoost")

    print(f"\nOverall Performance:")
    print(f"  AutoGluon wins: {ag_wins}/5 metrics")
    print(f"  LSTM wins: {lstm_wins}/5 metrics")
    print(f"  XGBoost wins: {xgboost_wins}/5 metrics")

    wins = {"AutoGluon": ag_wins, "LSTM": lstm_wins, "XGBoost": xgboost_wins}
    max_wins = max(wins.values())
    winners = [model for model, count in wins.items() if count == max_wins]

    if len(winners) == 1:
        print(f"\nOverall Winner: {winners[0]}")
    else:
        print(f"\nOverall Result: TIE between {', '.join(winners)}")

    comparison_path = os.path.join(output_dir, "comparison_report.csv")
    comparison_df.to_csv(comparison_path, index=False)
    print(f"\nComparison report saved to: {comparison_path}")

    leaderboard_path = os.path.join(output_dir, "autogluon_leaderboard.csv")
    ag_leaderboard.to_csv(leaderboard_path, index=False)
    print(f"AutoGluon leaderboard saved to: {leaderboard_path}")

    combined_metrics = {
        "AutoGluon": {k: abs(v) for k, v in ag_metrics.items()},
        "LSTM": {k: abs(v) for k, v in lstm_metrics.items()},
        "XGBoost": {k: abs(v) for k, v in xgboost_metrics.items()},
    }
    metrics_df = pd.DataFrame(combined_metrics).T
    metrics_path = os.path.join(output_dir, "combined_metrics.csv")
    metrics_df.to_csv(metrics_path)
    print(f"Combined metrics saved to: {metrics_path}")

    return comparison_df


def main():
    print("=" * 80)
    print("AUTOGLUON vs LSTM vs XGBOOST COMPARISON")
    print("=" * 80)
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # 1. Load data
        df = load_shell_data(DATA_PATH, top_n_sensors=TOP_N_SENSORS, sample_ratio=None)

        # 2. Split data
        train_df, val_df, test_df = split_timeseries_data(
            df, train_ratio=TRAIN_RATIO, val_ratio=VAL_RATIO, test_ratio=TEST_RATIO
        )

        # 3. Train AutoGluon
        ag_start_time = datetime.now()
        ag_predictor = train_autogluon(
            train_df,
            prediction_length=PREDICTION_LENGTH,
            eval_metric=EVAL_METRIC,
            time_limit=TIME_LIMIT,
            preset=PRESET,
            freq=FREQ,
            verbosity=2,
        )
        ag_training_duration = (datetime.now() - ag_start_time).total_seconds()

        # 4. Train LSTM
        lstm_start_time = datetime.now()
        lstm_model = train_lstm(train_df, val_df, prediction_length=PREDICTION_LENGTH)
        lstm_training_duration = (datetime.now() - lstm_start_time).total_seconds()

        # 5. Train XGBoost
        xgboost_start_time = datetime.now()
        xgboost_model = train_xgboost(train_df, prediction_length=PREDICTION_LENGTH)
        xgboost_training_duration = (
            datetime.now() - xgboost_start_time
        ).total_seconds()

        # 6. Evaluate AutoGluon
        ag_metrics, ag_leaderboard, ag_predictions_df = evaluate_autogluon(
            ag_predictor, test_df, freq=FREQ
        )

        # 7. Evaluate LSTM
        lstm_metrics, lstm_predictions_df = evaluate_lstm(lstm_model, test_df)

        if lstm_metrics is None:
            print("\ ERROR: LSTM evaluation failed. Cannot generate comparison report.")
            return 1

        # 8. Evaluate XGBoost
        xgboost_metrics, xgboost_predictions_df = evaluate_xgboost(
            xgboost_model, test_df
        )

        if xgboost_metrics is None:
            print(
                "\n ERROR: XGBoost evaluation failed. Cannot generate comparison report."
            )
            return 1

        # 9. Save test set actual values for visualization
        print("SAVING DATA FOR VISUALIZATION")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        test_actuals_path = os.path.join(OUTPUT_DIR, "test_actuals.parquet")
        test_df.to_parquet(test_actuals_path, index=False)
        print(f"Test actuals saved to: {test_actuals_path}")

        # 10. Save AutoGluon predictions (future forecasts)
        ag_pred_path = os.path.join(OUTPUT_DIR, "autogluon_predictions.parquet")
        ag_predictions_df.to_parquet(ag_pred_path, index=False)
        print(f"AutoGluon predictions saved to: {ag_pred_path}")
        print(
            f"  Note: AutoGluon predictions are future forecasts, not aligned with test timestamps"
        )

        # 11. Save LSTM predictions (future forecasts)
        lstm_pred_path = os.path.join(OUTPUT_DIR, "lstm_predictions.parquet")
        lstm_predictions_df.to_parquet(lstm_pred_path, index=False)
        print(f"LSTM predictions saved to: {lstm_pred_path}")
        print(
            f"  Note: LSTM predictions are future forecasts, not aligned with test timestamps"
        )

        # 12. Save XGBoost predictions (future forecasts)
        xgboost_pred_path = os.path.join(OUTPUT_DIR, "xgboost_predictions.parquet")
        xgboost_predictions_df.to_parquet(xgboost_pred_path, index=False)
        print(f"XGBoost predictions saved to: {xgboost_pred_path}")
        print(
            f"  Note: XGBoost predictions are future forecasts, not aligned with test timestamps"
        )

        # 13. Save training metadata
        metadata = {
            "run_timestamp": datetime.now().isoformat(),
            "autogluon_training_time_seconds": ag_training_duration,
            "lstm_training_time_seconds": lstm_training_duration,
            "xgboost_training_time_seconds": xgboost_training_duration,
            "num_sensors": train_df["item_id"].nunique(),
            "prediction_length": PREDICTION_LENGTH,
            "train_ratio": TRAIN_RATIO,
            "val_ratio": VAL_RATIO,
            "test_ratio": TEST_RATIO,
            "freq": FREQ,
            "eval_metric": EVAL_METRIC,
            "data_split": {
                "train_start": train_df["timestamp"].min().isoformat(),
                "train_end": train_df["timestamp"].max().isoformat(),
                "val_start": val_df["timestamp"].min().isoformat(),
                "val_end": val_df["timestamp"].max().isoformat(),
                "test_start": test_df["timestamp"].min().isoformat(),
                "test_end": test_df["timestamp"].max().isoformat(),
                "train_samples": len(train_df),
                "val_samples": len(val_df),
                "test_samples": len(test_df),
            },
            "model_configs": {
                "lstm": {
                    "lookback_window": 72,
                    "lstm_units": 48,
                    "num_lstm_layers": 2,
                    "batch_size": 512,
                    "epochs": 10,
                    "patience": 5,
                },
                "xgboost": {
                    "max_depth": 5,
                    "learning_rate": 0.05,
                    "n_estimators": 150,
                    "lag_features": [1, 6, 12, 24, 48],
                    "rolling_windows": [12, 24],
                },
            },
            "notes": {
                "models": "Comparison includes AutoGluon (ensemble), LSTM (single model with sensor embeddings), and XGBoost (gradient boosting with feature engineering)",
                "predictions": "All models generate future forecasts (prediction_length steps beyond test set).",
                "autogluon_predictions": "AutoGluon predictions are future forecasts from ensemble.",
                "lstm_predictions": "LSTM predictions are future forecasts from single model with sensor embeddings.",
                "xgboost_predictions": "XGBoost predictions are recursive forecasts with engineered lag features.",
            },
        }
        metadata_path = os.path.join(OUTPUT_DIR, "training_metadata.json")
        with open(metadata_path, "w") as f:
            json.dump(metadata, f, indent=2)
        print(f"\nTraining metadata saved to: {metadata_path}")

        # 13b. Save LSTM training history
        if hasattr(lstm_model, "training_history") and lstm_model.training_history:
            history_df = pd.DataFrame(lstm_model.training_history)
            history_path = os.path.join(OUTPUT_DIR, "lstm_training_history.csv")
            history_df.to_csv(history_path, index=True)
            print(f"LSTM training history saved to: {history_path}")

        # 13c. Save sensor list
        sensor_list = {
            "sensors": sorted(train_df["item_id"].unique().tolist()),
            "num_sensors": train_df["item_id"].nunique(),
        }
        sensor_path = os.path.join(OUTPUT_DIR, "sensor_list.json")
        with open(sensor_path, "w") as f:
            json.dump(sensor_list, f, indent=2)
        print(f"Sensor list saved to: {sensor_path}")

        # 14. Generate comparison report
        comparison_df = generate_comparison_report(
            ag_metrics, lstm_metrics, xgboost_metrics, ag_leaderboard, OUTPUT_DIR
        )

        print(f"\nEnd time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"\nAll results saved to: {OUTPUT_DIR}")
        print("\nComparison completed")

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
