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


SCRIPT_DIR = Path(__file__).parent
DEFAULT_DATA_PATH = SCRIPT_DIR / ".." / "preprocessing" / "ShellData_preprocessed_filtered.parquet"
DATA_PATH = os.environ.get("SHELL_DATA_PATH", str(DEFAULT_DATA_PATH))
OUTPUT_DIR = "autogluon_results"
MODEL_SAVE_PATH = os.path.join(OUTPUT_DIR, "autogluon_model")

TOP_N_SENSORS = 10
PREDICTION_LENGTH = 72  # Forecast horizon in hours (24 = 1 day, 48 = 2 days, 168 = 1 week)
TRAIN_RATIO = 0.7
VAL_RATIO = 0.15
TEST_RATIO = 0.15
TIME_LIMIT = 600
EVAL_METRIC = "MAE"
PRESET = "medium_quality"
FREQ = "h"  # Frequency: 'h' = hourly, '30min' = 30 minutes, 'D' = daily


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

    # Convert timestamps and get basic stats
    df['EventTime'] = pd.to_datetime(df['EventTime'], format='mixed')
    overall_min = df['EventTime'].min()
    overall_max = df['EventTime'].max()
    print(f"\nOverall time range: {overall_min} to {overall_max}")

    # Calculate sensor statistics
    print("\nAnalyzing sensors by data volume...")
    sensor_time_stats = df.groupby('TagName')['EventTime'].agg(['min', 'max', 'count'])
    sensor_time_stats = sensor_time_stats.rename(columns={'count': 'data_points'})
    sensor_time_stats['duration_days'] = (sensor_time_stats['max'] - sensor_time_stats['min']).dt.total_seconds() / 86400

    # Select top N sensors by data volume
    sensor_counts = df["TagName"].value_counts()
    top_sensors = sensor_counts.head(top_n_sensors).index.tolist()

    df_filtered = df[df["TagName"].isin(top_sensors)].copy()
    print(f"\nSelected {len(df_filtered):,} rows from {len(top_sensors)} sensors")

    print("\nSelected sensors (by data volume):")
    for i, sensor in enumerate(top_sensors, 1):
        count = sensor_counts[sensor]
        duration = sensor_time_stats.loc[sensor, 'duration_days']
        time_range = f"{sensor_time_stats.loc[sensor, 'min'].strftime('%Y-%m-%d')} to {sensor_time_stats.loc[sensor, 'max'].strftime('%Y-%m-%d')}"
        print(f"  {i}. {sensor}: {count:,} points over {duration:.1f} days, {time_range}")
    ts_data = pd.DataFrame(
        {
            "item_id": df_filtered["TagName"],
            "timestamp": df_filtered["EventTime"],
            "target": df_filtered["Value"],
        }
    )
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

    # Filter sensors by minimum data points (preserves irregular high-resolution data)
    print("\nFiltering sensors by data volume...")
    min_data_points = 100  # Require at least 100 data points per sensor

    sensor_point_counts = ts_data.groupby('item_id').size()
    valid_sensors = sensor_point_counts[sensor_point_counts >= min_data_points].index.tolist()

    ts_data = ts_data[ts_data['item_id'].isin(valid_sensors)].copy()

    print(f"  Kept {len(valid_sensors)} sensors with >= {min_data_points} data points")
    print(f"  Total irregular data points: {len(ts_data):,}")
    print(f"  Note: AutoGluon will resample to hourly frequency during training")

    print(f"\nFinal dataset: {len(ts_data):,} rows")
    print(f"Time range: {ts_data['timestamp'].min()} to {ts_data['timestamp'].max()}")
    print(
        f"Target range: [{ts_data['target'].min():.2f}, {ts_data['target'].max():.2f}]"
    )

    return ts_data


def split_timeseries_data(df, train_ratio=0.7, val_ratio=0.15, test_ratio=0.15):
    """
    Split time series data into train/val/test sets (time-aware, per-sensor).

    Each sensor's data is split individually based on its own time range.
    This ensures all sensors can generate predictions regardless of when they operated.

    Args:
        df: DataFrame with columns [item_id, timestamp, target]
        train_ratio: Proportion for training
        val_ratio: Proportion for validation
        test_ratio: Proportion for testing

    Returns:
        train_df, val_df, test_df
    """
    print("SPLITTING DATA (per-sensor time-based splitting)")

    assert (
        abs(train_ratio + val_ratio + test_ratio - 1.0) < 0.001
    ), "Split ratios must sum to 1.0"

    train_dfs = []
    val_dfs = []
    test_dfs = []

    sensors = df['item_id'].unique()
    print(f"Splitting {len(sensors)} sensors individually...")

    for sensor in sensors:
        sensor_df = df[df['item_id'] == sensor].sort_values('timestamp').reset_index(drop=True)
        n = len(sensor_df)

        # Skip sensors with insufficient data
        if n < 10:
            continue

        train_end = int(n * train_ratio)
        val_end = int(n * (train_ratio + val_ratio))

        train_dfs.append(sensor_df.iloc[:train_end])
        val_dfs.append(sensor_df.iloc[train_end:val_end])
        test_dfs.append(sensor_df.iloc[val_end:])

    train_df = pd.concat(train_dfs, ignore_index=True)
    val_df = pd.concat(val_dfs, ignore_index=True)
    test_df = pd.concat(test_dfs, ignore_index=True)

    n_total = len(df)
    print(f"\nSplit ratios: {train_ratio:.0%} / {val_ratio:.0%} / {test_ratio:.0%}")
    print(f"Train set: {len(train_df):,} rows ({len(train_df)/n_total:.1%}) across {len(train_dfs)} sensors")
    print(
        f"  Time range: {train_df['timestamp'].min()} to {train_df['timestamp'].max()}"
    )
    print(f"Val set:   {len(val_df):,} rows ({len(val_df)/n_total:.1%}) across {len(val_dfs)} sensors")
    print(f"  Time range: {val_df['timestamp'].min()} to {val_df['timestamp'].max()}")
    print(f"Test set:  {len(test_df):,} rows ({len(test_df)/n_total:.1%}) across {len(test_dfs)} sensors")
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
    predictor, predictions, metrics, leaderboard, output_dir, model_save_path, test_df=None
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
        test_df: Test data with actual values (optional)
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

    # Save test actuals for validation visualization
    if test_df is not None:
        test_actuals_path = os.path.join(output_dir, "test_actuals.parquet")
        print(f"Saving test actuals to: {test_actuals_path}")
        test_df.to_parquet(test_actuals_path, index=False)
        print("Test actuals saved")

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
            predictor, predictions, metrics, leaderboard, OUTPUT_DIR, MODEL_SAVE_PATH, test_df=test_df
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
