"""
Test script for the visualization module.

This script demonstrates how to use the visualization module with synthetic data.
Run this to verify the module is working correctly.

Usage:
    python test_visualization.py
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

from visualization import forecasting, comparison, utils, config

output_dir = Path(__file__).parent / "test_output"
output_dir.mkdir(exist_ok=True)
print(f"\nOutput directory: {output_dir}")


print("\n1. Generating synthetic test data")

np.random.seed(42)
n_historical = 168 

historical_data = pd.DataFrame({
    'timestamp': pd.date_range('2025-01-01', periods=n_historical, freq='H'),
    'value': 50 + 10 * np.sin(np.arange(n_historical) * 2 * np.pi / 24) + np.random.randn(n_historical) * 2
})

n_forecast = 24
forecast_start = historical_data['timestamp'].max() + pd.Timedelta(hours=1)

forecast_timestamps = pd.date_range(forecast_start, periods=n_forecast, freq='H')
true_values = 50 + 10 * np.sin(np.arange(n_historical, n_historical + n_forecast) * 2 * np.pi / 24)

forecast_data = pd.DataFrame({
    'timestamp': forecast_timestamps,
    'mean': true_values + np.random.randn(n_forecast) * 1.5,
    '0.1': true_values + np.random.randn(n_forecast) * 1.5 - 3,
    '0.2': true_values + np.random.randn(n_forecast) * 1.5 - 1.5,
    '0.8': true_values + np.random.randn(n_forecast) * 1.5 + 1.5,
    '0.9': true_values + np.random.randn(n_forecast) * 1.5 + 3
})

actual_data = pd.DataFrame({
    'timestamp': forecast_timestamps,
    'value': true_values + np.random.randn(n_forecast) * 1.0
})

print(f"   [OK] Historical data: {len(historical_data)} points")
print(f"   [OK] Forecast data: {len(forecast_data)} points")
print(f"   [OK] Actual data: {len(actual_data)} points")


print("\n2. Testing forecasting visualizations")

utils.setup_plot_style()

print("   Testing: plot_forecast_with_confidence()", end=" ")
ax = forecasting.plot_forecast_with_confidence(
    historical_data=historical_data,
    forecast_data=forecast_data,
    forecast_start=forecast_start,
    sensor_id='TEST_SENSOR_001'
)
utils.save_plot(ax.figure, 'test_01_forecast_with_ci.png', output_dir=output_dir)
print("[OK]")

print("   Testing: plot_forecast_with_actual()", end=" ")
ax = forecasting.plot_forecast_with_actual(
    historical_data=historical_data,
    forecast_data=forecast_data,
    actual_data=actual_data,
    forecast_start=forecast_start,
    sensor_id='TEST_SENSOR_001'
)
utils.save_plot(ax.figure, 'test_02_forecast_vs_actual.png', output_dir=output_dir)


print("\n3. Testing error analysis visualizations")

merged = pd.merge(
    forecast_data[['timestamp', 'mean']],
    actual_data[['timestamp', 'value']],
    on='timestamp'
)

print("   Testing: plot_residuals_over_time()", end=" ")
ax = forecasting.plot_residuals_over_time(
    actual=merged['value'],
    predicted=merged['mean'],
    timestamps=merged['timestamp'],
    sensor_id='TEST_SENSOR_001'
)
utils.save_plot(ax.figure, 'test_03_residuals.png', output_dir=output_dir)
print("[OK]")

print("   Testing: plot_error_distribution()", end=" ")
ax = forecasting.plot_error_distribution(
    actual=merged['value'],
    predicted=merged['mean'],
    sensor_id='TEST_SENSOR_001'
)
utils.save_plot(ax.figure, 'test_04_error_distribution.png', output_dir=output_dir)
print("[OK]")

print("   Testing: plot_scatter_actual_vs_predicted()", end=" ")
ax = forecasting.plot_scatter_actual_vs_predicted(
    actual=merged['value'],
    predicted=merged['mean'],
    sensor_id='TEST_SENSOR_001'
)
utils.save_plot(ax.figure, 'test_05_scatter.png', output_dir=output_dir)
print("[OK]")


print("\n4. Testing comprehensive dashboard")

print("   Testing: create_forecast_dashboard()", end=" ")
fig = forecasting.create_forecast_dashboard(
    historical_data=historical_data,
    forecast_data=forecast_data,
    actual_data=actual_data,
    forecast_start=forecast_start,
    sensor_id='TEST_SENSOR_001',
    output_path=output_dir / 'test_06_dashboard.png'
)
plt.close(fig)
print("[OK]")


print("\n5. Testing model comparison visualizations")

models_predictions = {
    'AutoGluon': forecast_data.copy(),
    'LSTM': forecast_data.copy(),
    'XGBoost': forecast_data.copy()
}

models_predictions['LSTM']['mean'] = forecast_data['mean'] + np.random.randn(n_forecast) * 0.5
models_predictions['XGBoost']['mean'] = forecast_data['mean'] + np.random.randn(n_forecast) * 0.8

from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

metrics_dict = {}
for model_name, pred_df in models_predictions.items():
    merged_model = pd.merge(pred_df[['timestamp', 'mean']], actual_data, on='timestamp')

    mae = mean_absolute_error(merged_model['value'], merged_model['mean'])
    mse = mean_squared_error(merged_model['value'], merged_model['mean'])
    rmse = np.sqrt(mse)
    mape = np.mean(np.abs((merged_model['value'] - merged_model['mean']) / merged_model['value'])) * 100
    r2 = r2_score(merged_model['value'], merged_model['mean'])

    metrics_dict[model_name] = {
        'mae': mae,
        'mse': mse,
        'rmse': rmse,
        'mape': mape,
        'r2': r2
    }

print("   Testing: plot_model_performance_comparison()", end=" ")
ax = comparison.plot_model_performance_comparison(metrics_dict)
utils.save_plot(ax.figure, 'test_07_model_comparison.png', output_dir=output_dir)
print("[OK]")

print("   Testing: plot_model_metrics_table()", end=" ")
ax = comparison.plot_model_metrics_table(metrics_dict, highlight_best=True)
utils.save_plot(ax.figure, 'test_08_metrics_table.png', output_dir=output_dir)
print("[OK]")

print("   Testing: plot_models_overlay()", end=" ")

predictions_dict_multi = {}
for model_name, pred_df in models_predictions.items():
    df = pred_df.copy()
    df['item_id'] = 'TEST_SENSOR_001'
    predictions_dict_multi[model_name] = df

actual_data_multi = actual_data.copy()
actual_data_multi['item_id'] = 'TEST_SENSOR_001'

ax = comparison.plot_models_overlay(
    predictions_dict=predictions_dict_multi,
    sensor_id='TEST_SENSOR_001',
    actual_data=actual_data_multi
)
utils.save_plot(ax.figure, 'test_09_models_overlay.png', output_dir=output_dir)
print("[OK]")

print("   Testing: plot_forecast_distributions()", end=" ")
ax = comparison.plot_forecast_distributions(predictions_dict_multi)
utils.save_plot(ax.figure, 'test_10_distributions.png', output_dir=output_dir)
print("[OK]")

print("\n6. Testing multi-sensor overview")

n_sensors = 3
predictions_multi = []
historical_multi = []

for i in range(n_sensors):
    sensor_id = f'SENSOR_{i+1:03d}'

    hist_df = historical_data.copy()
    hist_df['TagName'] = sensor_id
    hist_df['EventTime'] = hist_df['timestamp']
    hist_df['Value'] = hist_df['value'] + np.random.randn(len(hist_df)) * 5
    historical_multi.append(hist_df)

    pred_df = forecast_data.copy()
    pred_df['item_id'] = sensor_id
    predictions_multi.append(pred_df)

predictions_df = pd.concat(predictions_multi, ignore_index=True)
historical_df = pd.concat(historical_multi, ignore_index=True)

print("   Testing: plot_multi_sensor_overview()", end=" ")
fig = forecasting.plot_multi_sensor_overview(
    predictions_df=predictions_df,
    historical_df=historical_df,
    lookback_hours=168,
    max_sensors=3,
    output_path=output_dir / 'test_11_multi_sensor.png'
)
plt.close(fig)
print("[OK]")

# Summary

print("TEST SUMMARY")

print("\n[OK] All tests passed successfully!")
print(f"\nGenerated {len(list(output_dir.glob('*.png')))} test visualizations in:")
print(f"  {output_dir.absolute()}")

print("\nGenerated files:")
for i, filepath in enumerate(sorted(output_dir.glob('*.png')), 1):
    print(f"  {i:2d}. {filepath.name}")
