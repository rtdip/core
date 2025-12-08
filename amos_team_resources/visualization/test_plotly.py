"""
Example: Using Plotly Interactive Visualizations

This script demonstrates how to create interactive Plotly visualizations
for AutoGluon forecasting results.

Usage:
    cd amos_team_resources/visualization
    python example_plotly_usage.py
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from visualization import forecasting, forecasting_plotly

# Paths to AutoGluon results
PREDICTIONS_PATH = Path('../shell/training/autogluon_results/predictions.parquet')
ACTUALS_PATH = Path('../shell/training/autogluon_results/test_actuals.parquet')
HISTORICAL_PATH = Path('../shell/preprocessing/ShellData_preprocessed_filtered.parquet')

# Output directory
OUTPUT_DIR = Path('plotly_visualizations')
OUTPUT_DIR.mkdir(exist_ok=True)

print("=" * 80)
print("Plotly Interactive Visualization Example")
print("=" * 80)

# Load data
print("\n1. Loading data...")
predictions_df = pd.read_parquet(PREDICTIONS_PATH)
predictions_df['timestamp'] = pd.to_datetime(predictions_df['timestamp'])

actuals_df = pd.read_parquet(ACTUALS_PATH)
actuals_df['timestamp'] = pd.to_datetime(actuals_df['timestamp'])

historical_df = pd.read_parquet(HISTORICAL_PATH)
historical_df['EventTime'] = pd.to_datetime(historical_df['EventTime'], format='mixed')

sensors = predictions_df['item_id'].unique()
print(f"   Found {len(sensors)} sensors")

# Select first sensor for demonstration
sensor_id = sensors[0]
print(f"   Using sensor: {sensor_id}")

# Prepare data using existing utility
print("\n2. Preparing data...")
historical_data, forecast_data, forecast_start = forecasting.prepare_sensor_data(
    sensor_id=sensor_id,
    predictions_df=predictions_df,
    historical_df=historical_df,
    lookback_hours=168  # 1 week
)

# Get actuals for this sensor
actual_data = actuals_df[actuals_df['item_id'] == sensor_id].copy()
actual_data = actual_data.rename(columns={'target': 'value'})
actual_data = actual_data[['timestamp', 'value']].sort_values('timestamp')

# Create interactive visualizations
print("\n3. Creating interactive Plotly visualizations...")

# Example 1: Forecast with confidence intervals (HTML)
print("   - Forecast with confidence intervals (HTML)...")
fig1 = forecasting_plotly.plot_forecast_with_confidence(
    historical_data=historical_data,
    forecast_data=forecast_data,
    forecast_start=forecast_start,
    sensor_id=sensor_id,
    ci_levels=[60, 80]
)
forecasting_plotly.save_plotly_figure(fig1, OUTPUT_DIR / 'forecast_with_ci.html', format='html')

# Example 2: Forecast vs actual (HTML)
print("   - Forecast vs actual (HTML)...")
fig2 = forecasting_plotly.plot_forecast_with_actual(
    historical_data=historical_data,
    forecast_data=forecast_data,
    actual_data=actual_data,
    forecast_start=forecast_start,
    sensor_id=sensor_id
)
forecasting_plotly.save_plotly_figure(fig2, OUTPUT_DIR / 'forecast_vs_actual.html', format='html')

# Merge for error analysis
merged = pd.merge(
    forecast_data[['timestamp', 'mean']],
    actual_data[['timestamp', 'value']],
    on='timestamp',
    how='inner'
)

if len(merged) > 0:
    # Example 3: Residuals plot (HTML)
    print("   - Residuals over time (HTML)...")
    fig3 = forecasting_plotly.plot_residuals_over_time(
        actual=merged['value'],
        predicted=merged['mean'],
        timestamps=merged['timestamp'],
        sensor_id=sensor_id
    )
    forecasting_plotly.save_plotly_figure(fig3, OUTPUT_DIR / 'residuals.html', format='html')

    # Example 4: Error distribution (HTML)
    print("   - Error distribution (HTML)...")
    fig4 = forecasting_plotly.plot_error_distribution(
        actual=merged['value'],
        predicted=merged['mean'],
        sensor_id=sensor_id
    )
    forecasting_plotly.save_plotly_figure(fig4, OUTPUT_DIR / 'error_distribution.html', format='html')

    # Example 5: Scatter plot (HTML)
    print("   - Actual vs predicted scatter (HTML)...")
    fig5 = forecasting_plotly.plot_scatter_actual_vs_predicted(
        actual=merged['value'],
        predicted=merged['mean'],
        sensor_id=sensor_id
    )
    forecasting_plotly.save_plotly_figure(fig5, OUTPUT_DIR / 'scatter_plot.html', format='html')

    print("\n4. Creating static PNG exports (requires kaleido)...")
    try:
        # Example: Export one figure as PNG for reports
        forecasting_plotly.save_plotly_figure(fig1, OUTPUT_DIR / 'forecast_with_ci_static.png', format='png')
        print("   - Exported forecast with CI as PNG")
    except Exception as e:
        print(f"   [WARNING] PNG export failed: {e}")
        print("   Install kaleido for PNG export: pip install kaleido")

else:
    print("   [WARNING] No overlapping timestamps for error analysis")

# Summary
print("\n" + "=" * 80)
print("VISUALIZATION COMPLETE")
print("=" * 80)

generated_files = sorted(OUTPUT_DIR.glob('*'))
print(f"\nGenerated {len(generated_files)} files in:")
print(f"  {OUTPUT_DIR.absolute()}")

print("\nFiles created:")
for i, filepath in enumerate(generated_files, 1):
    print(f"  {i}. {filepath.name}")

print("\nOpen the HTML files in your browser for interactive exploration!")
print("  - Zoom, pan, hover for details")
print("  - Toggle series on/off in legend")
print("  - Export to PNG from the browser")

print("\n" + "=" * 80)
