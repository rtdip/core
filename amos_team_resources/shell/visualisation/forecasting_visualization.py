"""
Time Series Forecasting Visualization
Shows historical data with forecast predictions and confidence intervals.

Displays:
- Historical data (dark blue) - actual sensor readings up to forecast start
- Forecast (green) - predictions for the next 24 hours
- Confidence intervals (shaded green) - uncertainty bounds
- Red dashed line - marks where the future begins
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import argparse
import warnings

warnings.filterwarnings('ignore')
plt.style.use('seaborn-v0_8-darkgrid')
sns.set_palette("husl")

SCRIPT_DIR = Path(__file__).parent
DEFAULT_DATA_PATH = SCRIPT_DIR / ".." / "training" / "autogluon_results"
DEFAULT_PREPROCESSED_DATA = SCRIPT_DIR / ".." / "preprocessing" / "ShellData_preprocessed_filtered.parquet"
DEFAULT_OUTPUT_DIR = SCRIPT_DIR / "output_images"


def load_data(data_path, preprocessed_data_path):
    """
    Load predictions and historical data.

    Args:
        data_path: Directory containing predictions.parquet
        preprocessed_data_path: Path to preprocessed data parquet file

    Returns:
        predictions_df, historical_df
    """
    print("LOADING DATA")

    predictions_path = Path(data_path) / "predictions.parquet"
    if not predictions_path.exists():
        raise FileNotFoundError(f"Predictions not found at {predictions_path}")

    predictions_df = pd.read_parquet(predictions_path)
    predictions_df['timestamp'] = pd.to_datetime(predictions_df['timestamp'])
    print(f"Loaded {len(predictions_df)} predictions")
    print(f"  Sensors: {predictions_df['item_id'].nunique()}")
    print(f"  Forecast period: {predictions_df['timestamp'].min()} to {predictions_df['timestamp'].max()}")

    preprocessed_data_path = Path(preprocessed_data_path)
    if not preprocessed_data_path.exists():
        raise FileNotFoundError(f"Preprocessed data not found at {preprocessed_data_path}")

    historical_df = pd.read_parquet(preprocessed_data_path)
    historical_df['EventTime'] = pd.to_datetime(historical_df['EventTime'], format='mixed')

    pred_sensors = predictions_df['item_id'].unique()
    historical_df = historical_df[historical_df['TagName'].isin(pred_sensors)].copy()

    print(f"Loaded {len(historical_df):,} historical values")
    print(f"  Historical period: {historical_df['EventTime'].min()} to {historical_df['EventTime'].max()}")
    print()

    return predictions_df, historical_df


def prepare_sensor_data(sensor_id, predictions_df, historical_df, lookback_hours=168):
    """
    Prepare data for a single sensor.

    Args:
        sensor_id: Sensor identifier
        predictions_df: Predictions dataframe
        historical_df: Historical data dataframe
        lookback_hours: Hours of historical data to show before forecast (default: 168 = 1 week)

    Returns:
        historical_data, forecast_data, forecast_start
    """
    sensor_preds = predictions_df[predictions_df['item_id'] == sensor_id].sort_values('timestamp')

    if len(sensor_preds) == 0:
        return None, None, None

    forecast_start = sensor_preds['timestamp'].min()

    sensor_hist = historical_df[historical_df['TagName'] == sensor_id].copy()
    sensor_hist = sensor_hist.sort_values('EventTime')
    cutoff_time = forecast_start - pd.Timedelta(hours=lookback_hours)
    sensor_hist = sensor_hist[sensor_hist['EventTime'] >= cutoff_time]
    sensor_hist = sensor_hist[sensor_hist['EventTime'] < forecast_start]

    historical_data = pd.DataFrame({
        'timestamp': sensor_hist['EventTime'],
        'value': sensor_hist['Value']
    })

    forecast_data = sensor_preds.copy()
    return historical_data, forecast_data, forecast_start


def create_overview_plot(predictions_df, historical_df, output_dir, lookback_hours=168):
    """Create multi-sensor overview plot."""
    print("Creating all sensors overview plot")

    sensors = predictions_df['item_id'].unique()
    n_sensors = len(sensors)
    
    n_cols = min(3, n_sensors)
    n_rows = (n_sensors + n_cols - 1) // n_cols

    fig, axes = plt.subplots(n_rows, n_cols, figsize=(18, 5 * n_rows))
    if n_sensors == 1:
        axes = np.array([axes])
    axes = axes.flatten()

    for idx, sensor in enumerate(sensors):
        ax = axes[idx]

        historical_data, forecast_data, forecast_start = prepare_sensor_data(
            sensor, predictions_df, historical_df, lookback_hours
        )

        if historical_data is None or len(historical_data) == 0:
            ax.text(0.5, 0.5, f'No data for {sensor}',
                   ha='center', va='center', transform=ax.transAxes)
            ax.set_title(sensor[:40], fontsize=10, fontweight='bold')
            continue

        ax.plot(historical_data['timestamp'], historical_data['value'],
               'o-', color='darkblue', label='Historical', linewidth=2, markersize=3, alpha=0.7)
        ax.plot(forecast_data['timestamp'], forecast_data['mean'],
               's-', color='green', label='Forecast', linewidth=2, markersize=3, alpha=0.8)

        if '0.1' in forecast_data.columns and '0.9' in forecast_data.columns:
            ax.fill_between(forecast_data['timestamp'],
                           forecast_data['0.1'], forecast_data['0.9'],
                           color='green', alpha=0.2, label='80% CI')

        ax.axvline(forecast_start, color='red', linestyle='--', linewidth=2,
                  alpha=0.7, label='Forecast Start')

        ax.set_xlabel('Time', fontsize=9)
        ax.set_ylabel('Value', fontsize=9)
        ax.set_title(sensor[:40], fontsize=10, fontweight='bold')
        ax.legend(loc='best', fontsize=8)
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45, labelsize=8)

    for idx in range(n_sensors, len(axes)):
        axes[idx].axis('off')

    plt.suptitle('AutoGluon Forecasts - All Sensors', fontsize=16, fontweight='bold', y=1.0)
    plt.tight_layout()

    output_path = output_dir / "all_sensors_forecast.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"  Saved: {output_path.name}")

def create_detailed_plots(predictions_df, historical_df, output_dir, lookback_hours=168):
    """Create detailed plots with confidence intervals."""
    print("\nCreating detailed confidence interval plots")

    sensors = predictions_df['item_id'].unique()

    detail_sensors = sensors[:min(4, len(sensors))]
    n_detail = len(detail_sensors)

    fig, axes = plt.subplots(n_detail, 1, figsize=(16, 5 * n_detail))
    if n_detail == 1:
        axes = [axes]

    for idx, sensor in enumerate(detail_sensors):
        ax = axes[idx]

        historical_data, forecast_data, forecast_start = prepare_sensor_data(
            sensor, predictions_df, historical_df, lookback_hours
        )

        if historical_data is None or len(historical_data) == 0:
            ax.text(0.5, 0.5, f'No data for {sensor}',
                   ha='center', va='center', transform=ax.transAxes)
            ax.set_title(sensor[:40], fontsize=12, fontweight='bold')
            continue

        ax.plot(historical_data['timestamp'], historical_data['value'],
               'o-', color='darkblue', label='Historical Data',
               linewidth=2.5, markersize=4, alpha=0.8)
        ax.plot(forecast_data['timestamp'], forecast_data['mean'],
               's-', color='green', label='Forecast (mean)',
               linewidth=2.5, markersize=4, alpha=0.9)

        if '0.1' in forecast_data.columns and '0.9' in forecast_data.columns:
            ax.fill_between(forecast_data['timestamp'],
                           forecast_data['0.2'], forecast_data['0.8'],
                           color='green', alpha=0.3, label='60% Confidence Interval')
            ax.fill_between(forecast_data['timestamp'],
                           forecast_data['0.1'], forecast_data['0.9'],
                           color='green', alpha=0.15, label='80% Confidence Interval')

        ax.axvline(forecast_start, color='red', linestyle='--', linewidth=2.5,
                  alpha=0.8, label='Forecast Start')

        ax.set_xlabel('Time', fontsize=12, fontweight='bold')
        ax.set_ylabel('Value', fontsize=12, fontweight='bold')
        ax.set_title(f'{sensor} - Historical Data + 24h Forecast',
                    fontsize=13, fontweight='bold', pad=15)
        ax.legend(loc='best', fontsize=10)
        ax.grid(True, alpha=0.3)
        ax.tick_params(axis='x', rotation=45)

    plt.tight_layout()
    output_path = output_dir / "detailed_forecasts_with_confidence.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path.name}")


def create_forecast_statistics(predictions_df, output_dir):
    """Create forecast statistics heatmap."""
    print("\nCreating forecast statistics")

    sensors = predictions_df['item_id'].unique()
    stats_data = []
    for sensor in sensors:
        sensor_data = predictions_df[predictions_df['item_id'] == sensor]
        stats_data.append({
            'Sensor': sensor[:30],
            'Mean': sensor_data['mean'].mean(),
            'Std': sensor_data['mean'].std(),
            'Min': sensor_data['mean'].min(),
            'Max': sensor_data['mean'].max(),
            'Range': sensor_data['mean'].max() - sensor_data['mean'].min()
        })

    stats_df = pd.DataFrame(stats_data)

    fig, ax = plt.subplots(figsize=(10, max(6, len(sensors) * 0.5)))

    heatmap_data = stats_df.set_index('Sensor')[['Mean', 'Std', 'Min', 'Max', 'Range']]
    sns.heatmap(heatmap_data, annot=True, fmt='.2f', cmap='YlGnBu',
                ax=ax, cbar_kws={'label': 'Value'})
    ax.set_title('Forecast Statistics by Sensor', fontsize=14, fontweight='bold', pad=15)
    ax.set_xlabel('Statistic', fontsize=12, fontweight='bold')
    ax.set_ylabel('Sensor', fontsize=12, fontweight='bold')

    plt.tight_layout()
    output_path = output_dir / "forecast_statistics.png"
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    plt.close()
    print(f"Saved: {output_path.name}")


def print_summary(predictions_df, historical_df):
    """Print forecast summary."""
    print("FORECAST SUMMARY")
    sensors = predictions_df['item_id'].unique()

    print(f"\nData Overview:")
    print(f"Sensors: {len(sensors)}")
    print(f"Prediction horizon: 24 hours per sensor")
    print(f"Total predictions: {len(predictions_df)}")

    print(f"\nTime Ranges:")
    print(f"Historical data: {historical_df['EventTime'].min()} to {historical_df['EventTime'].max()}")
    print(f"Forecast period: {predictions_df['timestamp'].min()} to {predictions_df['timestamp'].max()}")

    print("\nForecast Statistics by Sensor:")
    print(f"{'Sensor':<40} {'Mean':>10} {'Std':>10} {'Min':>10} {'Max':>10}")

    for sensor in sorted(sensors):
        sensor_data = predictions_df[predictions_df['item_id'] == sensor]
        mean_val = sensor_data['mean'].mean()
        std_val = sensor_data['mean'].std()
        min_val = sensor_data['mean'].min()
        max_val = sensor_data['mean'].max()

        print(f"{sensor[:40]:<40} {mean_val:>10.2f} {std_val:>10.4f} {min_val:>10.2f} {max_val:>10.2f}")

    print("All visualizations saved")
    print("\nGenerated files:")
    print("  - all_sensors_forecast.png (overview of all sensors)")
    print("  - detailed_forecasts_with_confidence.png (detailed view with CI)")
    print("  - forecast_statistics.png (statistics heatmap)")

def main():
    parser = argparse.ArgumentParser(
        description='Visualize AutoGluon forecasts with historical context'
    )
    parser.add_argument(
        '--data-path', '-d',
        default=DEFAULT_DATA_PATH,
        help=f'Path to directory containing predictions (default: {DEFAULT_DATA_PATH})'
    )
    parser.add_argument(
        '--preprocessed-data', '-p',
        default=DEFAULT_PREPROCESSED_DATA,
        help=f'Path to preprocessed data file (default: {DEFAULT_PREPROCESSED_DATA})'
    )
    parser.add_argument(
        '--output-dir', '-o',
        default=DEFAULT_OUTPUT_DIR,
        help=f'Output directory for visualizations (default: {DEFAULT_OUTPUT_DIR})'
    )
    parser.add_argument(
        '--lookback-hours', '-l',
        type=int,
        default=168,
        help='Hours of historical data to show before forecast (default: 168 = 1 week)'
    )

    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    try:
        predictions_df, historical_df = load_data(args.data_path, args.preprocessed_data)
        print("CREATING VISUALIZATIONS")

        create_overview_plot(predictions_df, historical_df, output_dir, args.lookback_hours)
        create_detailed_plots(predictions_df, historical_df, output_dir, args.lookback_hours)
        create_forecast_statistics(predictions_df, output_dir)

        print_summary(predictions_df, historical_df)

    except Exception as e:
        print(f"\nERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
