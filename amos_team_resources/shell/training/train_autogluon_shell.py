"""
REQUIREMENTS:
- pip install autogluon.timeseries
- pip install pandas pyarrow
"""

import sys
import os
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "src", "sdk", "python"))

from autogluon.timeseries import TimeSeriesDataFrame, TimeSeriesPredictor


DATA_PATH = "ShellData_preprocessed_final.parquet"
OUTPUT_DIR = "autogluon_results"
MODEL_SAVE_PATH = os.path.join(OUTPUT_DIR, "autogluon_model")

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

    Args:
        data_path: Path to preprocessed parquet file
        top_n_sensors: Number of top sensors by data volume to use
        sample_ratio: Optional sampling ratio (0.01 = 1% of data)

    Returns:
        DataFrame ready for AutoGluon
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

    # Remove any null values in target
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
    Split time series data into train/val/test sets (time-aware).

    Args:
        df: DataFrame with columns [item_id, timestamp, target]
        train_ratio: Proportion for training
        val_ratio: Proportion for validation
        test_ratio: Proportion for testing

    Returns:
        train_df, val_df, test_df
    """
    print("SPLITTING DATA")

    assert (
        abs(train_ratio + val_ratio + test_ratio - 1.0) < 0.001
    ), "Split ratios must sum to 1.0"

    n = len(df)
    train_end = int(n * train_ratio)
    val_end = int(n * (train_ratio + val_ratio))

    train_df = df.iloc[:train_end].copy()
    val_df = df.iloc[train_end:val_end].copy()
    test_df = df.iloc[val_end:].copy()

    print(f"Split ratios: {train_ratio:.0%} / {val_ratio:.0%} / {test_ratio:.0%}")
    print(f"Train set: {len(train_df):,} rows ({len(train_df)/n:.1%})")
    print(
        f"  Time range: {train_df['timestamp'].min()} to {train_df['timestamp'].max()}"
    )
    print(f"Val set:   {len(val_df):,} rows ({len(val_df)/n:.1%})")
    print(f"  Time range: {val_df['timestamp'].min()} to {val_df['timestamp'].max()}")
    print(f"Test set:  {len(test_df):,} rows ({len(test_df)/n:.1%})")
    print(f"  Time range: {test_df['timestamp'].min()} to {test_df['timestamp'].max()}")

    return train_df, val_df, test_df


def create_timeseries_dataframe(df, freq="h"):
    """
    Convert pandas DataFrame to AutoGluon TimeSeriesDataFrame with regular frequency.

    Args:
        df: DataFrame with columns [item_id, timestamp, target]
        freq: Time frequency ('h' = hourly, 'D' = daily, 'T' or 'min' = minutely)

    Returns:
        TimeSeriesDataFrame with regular time index
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
    """
    Train AutoGluon time series predictor.

    Args:
        train_df: Training data
        prediction_length: Number of steps to forecast
        eval_metric: Metric to optimize
        time_limit: Training time limit in seconds
        preset: Quality preset
        freq: Time frequency for resampling
        verbosity: Verbosity level (0-4)

    Returns:
        Trained TimeSeriesPredictor
    """
    print("TRAINING AUTOGLUON MODELS")
    print(f"Prediction length: {prediction_length} time steps")
    print(f"Time frequency: {freq}")
    print(f"Evaluation metric: {eval_metric}")
    print(f"Time limit: {time_limit} seconds ({time_limit/60:.1f} minutes)")
    print(f"Quality preset: {preset}")
    print(f"Verbosity: {verbosity}")

    train_data = create_timeseries_dataframe(train_df, freq=freq)
    print(f"\nCreated TimeSeriesDataFrame with {len(train_data)} rows")

    # Initialize predictor with frequency
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


def evaluate_model(predictor, test_df, freq="h"):
    """
    Evaluate trained model on test data.

    Args:
        predictor: Trained TimeSeriesPredictor
        test_df: Test data
        freq: Time frequency for resampling

    Returns:
        Dictionary of evaluation metrics
    """
    print("EVALUATING MODEL")

    test_data = create_timeseries_dataframe(test_df, freq=freq)

    print("Evaluating")
    metrics = predictor.evaluate(
        test_data, metrics=["MAE", "RMSE", "MAPE", "MASE", "SMAPE"]
    )

    # Display metrics
    print("\nEvaluation Metrics:")
    for metric_name, metric_value in metrics.items():
        print(f"{metric_name:20s}: {metric_value:.4f}")

    return metrics


def get_leaderboard(predictor):
    """
    Display model leaderboard.

    Args:
        predictor: Trained TimeSeriesPredictor

    Returns:
        Leaderboard DataFrame
    """
    print("MODEL LEADERBOARD")

    leaderboard = predictor.leaderboard()

    print("\nAll trained models ranked by performance:")
    print("-" * 80)
    print(leaderboard.to_string())

    print("\nBest Model:")
    print("-" * 80)
    best_model = leaderboard.iloc[0]["model"]
    best_score = leaderboard.iloc[0]["score_val"]
    print(f"Model: {best_model}")
    print(f"Validation Score: {best_score:.4f}")

    return leaderboard


def generate_predictions(predictor, test_df, freq="h"):
    """
    Generate predictions on test data.

    Args:
        predictor: Trained TimeSeriesPredictor
        test_df: Test data
        freq: Time frequency for resampling

    Returns:
        DataFrame with predictions
    """
    print("PREDICTIONS")

    test_data = create_timeseries_dataframe(test_df, freq=freq)

    print("Generating forecasts")
    predictions = predictor.predict(test_data)

    print(f"Generated predictions for {len(predictions)} time steps")
    print(f"\nPrediction sample:")
    print(predictions.head(10))

    return predictions


def save_results(
    predictor, predictions, metrics, leaderboard, output_dir, model_save_path
):
    """
    Save all results to disk.

    Args:
        predictor: Trained predictor
        predictions: Prediction DataFrame
        metrics: Evaluation metrics
        leaderboard: Model leaderboard
        output_dir: Output directory
        model_save_path: Path to save model (not used - AutoGluon manages its own path)
    """
    print("SAVING RESULTS")

    os.makedirs(output_dir, exist_ok=True)

    print(f"Model already saved during training at: {predictor.path}")

    predictions_path = os.path.join(output_dir, "predictions.parquet")
    print(f"Saving predictions to: {predictions_path}")
    predictions.reset_index().to_parquet(predictions_path)
    print("Predictions saved")

    metrics_path = os.path.join(output_dir, "metrics.csv")
    print(f"Saving metrics to: {metrics_path}")
    pd.DataFrame([metrics]).to_csv(metrics_path, index=False)
    print("Metrics saved")

    # Save leaderboard
    leaderboard_path = os.path.join(output_dir, "leaderboard.csv")
    print(f"Saving leaderboard to: {leaderboard_path}")
    leaderboard.to_csv(leaderboard_path, index=False)
    print("Leaderboard saved")

    print(f"\nAll results saved to: {output_dir}")


def main():
    print("AUTOGLUON TIME SERIES FORECASTING")
    print(f"Start time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    try:
        # 1. Load data
        df = load_shell_data(DATA_PATH, top_n_sensors=TOP_N_SENSORS, sample_ratio=None)

        # 2. Split data
        train_df, val_df, test_df = split_timeseries_data(
            df, train_ratio=TRAIN_RATIO, val_ratio=VAL_RATIO, test_ratio=TEST_RATIO
        )

        # 3. Train model
        predictor = train_autogluon(
            train_df,
            prediction_length=PREDICTION_LENGTH,
            eval_metric=EVAL_METRIC,
            time_limit=TIME_LIMIT,
            preset=PRESET,
            freq=FREQ,
            verbosity=2,
        )

        # 4. Get leaderboard
        leaderboard = get_leaderboard(predictor)

        # 5. Evaluate on test set
        metrics = evaluate_model(predictor, test_df, freq=FREQ)

        # 6. Generate predictions
        predictions = generate_predictions(predictor, test_df, freq=FREQ)

        # 7. Save results
        save_results(
            predictor, predictions, metrics, leaderboard, OUTPUT_DIR, MODEL_SAVE_PATH
        )

        print(f"End time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"\nResults saved to: {OUTPUT_DIR}")
        print(f"Model automatically saved to: {predictor.path}")
        print(f"\nTo load Model:")
        print(f"from autogluon.timeseries import TimeSeriesPredictor")
        print(f"predictor = TimeSeriesPredictor.load('{predictor.path}')")

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback

        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
