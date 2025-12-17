#!/usr/bin/env python3
"""
Time Series Forecasting Results Visualization
Generates visualizations for AutoGluon, LSTM, and XGBoost models.
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# Setup plotting style
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 6)
plt.rcParams['font.size'] = 10

# Create output directory
Path("output_images").mkdir(exist_ok=True)

# Load data
print("Loading data...")
combined_metrics_raw = pd.read_csv('comparison_results/combined_metrics.csv', index_col=0)
comparison_report = pd.read_csv('comparison_results/comparison_report.csv')
autogluon_leaderboard = pd.read_csv('comparison_results/autogluon_leaderboard.csv')

combined_metrics = combined_metrics_raw.reset_index().melt(
    id_vars='index', var_name='Metric', value_name='Value'
).rename(columns={'index': 'Model'})

autogluon_preds = pd.read_parquet('comparison_results/autogluon_predictions.parquet')
lstm_preds = pd.read_parquet('comparison_results/lstm_predictions.parquet')
xgboost_preds = pd.read_parquet('comparison_results/xgboost_predictions.parquet')
test_actuals = pd.read_parquet('comparison_results/test_actuals.parquet')
lstm_history = pd.read_csv('comparison_results/lstm_training_history.csv')

# Standardize prediction columns
autogluon_preds['prediction'] = autogluon_preds['mean']
lstm_preds['prediction'] = lstm_preds['mean']
xgboost_preds['prediction'] = xgboost_preds['predicted']

print(f"✓ Data loaded: {len(autogluon_preds)} forecast points, {test_actuals['item_id'].nunique()} sensors\n")

# 1. Model Performance Comparison
print("Creating performance comparison...")
fig, ax = plt.subplots(figsize=(12, 7))
metrics_pivot = combined_metrics.pivot(index='Metric', columns='Model', values='Value')
metrics_pivot = metrics_pivot[['AutoGluon', 'LSTM', 'XGBoost']]
x = np.arange(len(metrics_pivot))
width = 0.25

colors = ['#2ecc71', '#e74c3c', '#3498db']
for i, (model, color) in enumerate(zip(metrics_pivot.columns, colors)):
    ax.bar(x + i*width, metrics_pivot[model], width, label=model, color=color, alpha=0.8)

ax.set_xlabel('Metric', fontweight='bold', fontsize=12)
ax.set_ylabel('Value (lower is better)', fontweight='bold', fontsize=12)
ax.set_title('Model Performance Comparison', fontweight='bold', fontsize=14)
ax.set_xticks(x + width)
ax.set_xticklabels(metrics_pivot.index)
ax.legend()
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig('output_images/performance_comparison.png', dpi=300, bbox_inches='tight')
plt.close()

# 2. AutoGluon Leaderboard
print("Creating AutoGluon leaderboard...")
fig, ax = plt.subplots(figsize=(10, 6))
top_models = autogluon_leaderboard.head(10)
bars = ax.barh(top_models['model'], top_models['score_val'], color='#2ecc71', alpha=0.7)
bars[0].set_color('#27ae60')
ax.set_xlabel('Validation Score (higher is better)', fontweight='bold', fontsize=12)
ax.set_title('AutoGluon Model Leaderboard', fontweight='bold', fontsize=14)
ax.invert_yaxis()
ax.grid(axis='x', alpha=0.3)
plt.tight_layout()
plt.savefig('output_images/autogluon_leaderboard.png', dpi=300, bbox_inches='tight')
plt.close()

# 3. Sample Sensor Forecasts
print("Creating sensor forecasts...")
sensors = autogluon_preds['item_id'].unique()[:3]
fig, axes = plt.subplots(1, 3, figsize=(18, 5))

for idx, sensor in enumerate(sensors):
    ax = axes[idx]
    ag_data = autogluon_preds[autogluon_preds['item_id'] == sensor].sort_values('timestamp')
    lstm_data = lstm_preds[lstm_preds['item_id'] == sensor].sort_values('timestamp')
    xg_data = xgboost_preds[xgboost_preds['item_id'] == sensor].sort_values('timestamp')
    
    ax.plot(ag_data['timestamp'], ag_data['prediction'], 'o-', label='AutoGluon', color='#2ecc71', linewidth=2, markersize=4)
    ax.plot(lstm_data['timestamp'], lstm_data['prediction'], 's-', label='LSTM', color='#e74c3c', linewidth=2, markersize=4)
    ax.plot(xg_data['timestamp'], xg_data['prediction'], '^-', label='XGBoost', color='#3498db', linewidth=2, markersize=4)
    
    ax.set_title(f'{sensor}', fontweight='bold', fontsize=11)
    ax.set_xlabel('Time', fontsize=10)
    ax.set_ylabel('Predicted Value', fontsize=10)
    ax.legend(fontsize=9)
    ax.grid(alpha=0.3)
    ax.tick_params(axis='x', rotation=45)

plt.suptitle('24-Hour Forecast Comparison', fontweight='bold', fontsize=14, y=1.02)
plt.tight_layout()
plt.savefig('output_images/sensor_forecasts.png', dpi=300, bbox_inches='tight')
plt.close()

# 4. LSTM Training History
print("Creating LSTM training history...")
fig, ax = plt.subplots(figsize=(10, 6))
epochs = range(1, len(lstm_history) + 1)
ax.plot(epochs, lstm_history['loss'], 'o-', linewidth=2, markersize=6, color='#e74c3c', label='Training Loss')
if 'val_loss' in lstm_history.columns:
    ax.plot(epochs, lstm_history['val_loss'], 's-', linewidth=2, markersize=6, color='#3498db', label='Validation Loss')

improvement = ((lstm_history['loss'].iloc[0] - lstm_history['loss'].iloc[-1]) / lstm_history['loss'].iloc[0]) * 100
ax.text(0.98, 0.98, f'Improvement: {improvement:.1f}%', transform=ax.transAxes, 
        ha='right', va='top', fontsize=11, bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

ax.set_xlabel('Epoch', fontweight='bold', fontsize=12)
ax.set_ylabel('Loss', fontweight='bold', fontsize=12)
ax.set_title('LSTM Training Convergence', fontweight='bold', fontsize=14)
ax.legend()
ax.grid(alpha=0.3)
plt.tight_layout()
plt.savefig('output_images/lstm_training.png', dpi=300, bbox_inches='tight')
plt.close()

# 5. Forecast Distributions
print("Creating forecast distributions...")
fig, ax = plt.subplots(figsize=(12, 6))
data = [autogluon_preds['prediction'], lstm_preds['prediction'], xgboost_preds['prediction']]
bp = ax.boxplot(data, labels=['AutoGluon', 'LSTM', 'XGBoost'], patch_artist=True, showmeans=True)

colors = ['#2ecc71', '#e74c3c', '#3498db']
for patch, color in zip(bp['boxes'], colors):
    patch.set_facecolor(color)
    patch.set_alpha(0.6)

ax.set_ylabel('Predicted Value', fontweight='bold', fontsize=12)
ax.set_title('Forecast Distribution Comparison', fontweight='bold', fontsize=14)
ax.grid(axis='y', alpha=0.3)
plt.tight_layout()
plt.savefig('output_images/forecast_distributions.png', dpi=300, bbox_inches='tight')
plt.close()

# Summary Report
print("\n" + "="*80)
print("FORECASTING RESULTS SUMMARY")
print("="*80)

winners = comparison_report['Winner'].value_counts()
print(f"\nBest Model: {winners.idxmax()}")
print(f"   Won {winners.max()}/5 metrics")
print(f"   MAE: {comparison_report[comparison_report['Metric']=='MAE'][winners.idxmax()].values[0]:.4f}")

best_ag = autogluon_leaderboard.iloc[0]
print(f"\nAutoGluon Top Model: {best_ag['model']}")
print(f"   Score: {best_ag['score_val']:.4f}")
print(f"   Training time: {best_ag['fit_time_marginal']:.2f}s")

print(f"\nLSTM Training:")
print(f"   Epochs: {len(lstm_history)}")
print(f"   Final loss: {lstm_history['loss'].iloc[-1]:.4f}")
print(f"   Improvement: {improvement:.1f}%")

print(f"\nForecasts:")
print(f"   Horizon: 24 hours")
print(f"   Sensors: {len(sensors)}")
print(f"   Total predictions: {len(autogluon_preds)} points")

print("\n" + "="*80)
print("✓ All visualizations saved to output_images/")
print("="*80)
