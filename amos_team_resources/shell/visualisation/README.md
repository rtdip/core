# Forecasting Visualization

Interactive notebook for analyzing AutoGluon, LSTM, and XGBoost forecasting results.

## Quick Setup

1. **Folder structure:**
```bash
Visualization/
├── forecasting_visualization.ipynb
├── comparison_results/
└── output_images/   
```

2. **The following data files in `comparison_results/`:**
   - combined_metrics.csv
   - comparison_report.csv
   - autogluon_leaderboard.csv
   - autogluon_predictions.parquet
   - lstm_predictions.parquet
   - xgboost_predictions.parquet
   - test_actuals.parquet
   - lstm_training_history.csv

3. **Run the notebook:**
```bash
jupyter notebook forecasting_visualization.ipynb
```

## Output

All visualizations are automatically saved to `output_images/`:
- model_comparison.png
- autogluon_leaderboard.png
- forecast_*.png (per sensor)
- lstm_training.png
- forecast_distributions.png
etc

## Results

AutoGluon won all 5 metrics with MAE: 0.395 (LSTM: 1.76, XGBoost: 0.50)
