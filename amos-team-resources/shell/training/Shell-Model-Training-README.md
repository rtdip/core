# Time Series Forecasting Model Comparison on SHell Dataset

## Overview

1. **AutoGluon** - Automated ensemble learning
2. **LSTM** - Deep learning with sensor embeddings
3. **XGBoost** - Gradient boosting with engineered features

---

### Prerequisites

pip install autogluon.timeseries tensorflow xgboost pandas pyarrow scikit-learn pyspark

### Run Comparison

python train_comparison.py

**Output:** "comparison_results/" directory with metrics, predictions, and visualizations data

---

## Data Requirements

### Input Data

**File:** "ShellData_preprocessed_final.parquet", can be changed as needed
**Location:** Same directory as script
**Format:** Parquet file with columns:
- item_id (string) - Sensor identifier
- timestamp (datetime) - Observation time
- target (float) - Sensor value

## Configuration

Edit "train_comparison.py" to modify:

```python
# Data
DATA_PATH = "ShellData_preprocessed_final.parquet"
TOP_N_SENSORS = 10  # Number of sensors to use

# Train/val/test split
TRAIN_RATIO = 0.7
VAL_RATIO = 0.15
TEST_RATIO = 0.15

# Forecasting
PREDICTION_LENGTH = 24  # Hours ahead to forecast

# AutoGluon
TIME_LIMIT = 600  # Training time limit (seconds)
PRESET = "medium_quality"

# LSTM
lookback_window = 72  # Hours of history
lstm_units = 48
num_lstm_layers = 2
batch_size = 512
epochs = 10
patience = 5

# XGBoost
max_depth = 5
learning_rate = 0.05
n_estimators = 150
lag_features = [1, 6, 12, 24, 48]  # Hours
rolling_windows = [12, 24]  # Hours
```

---

## Output Files

All results saved to comparison_results/:

### Metrics
- combined_metrics.csv - Overall performance comparison
- comparison_report.csv - Winner per metric
- autogluon_leaderboard.csv - AutoGluon ensemble breakdown

### Predictions
- autogluon_predictions.parquet - AutoGluon forecasts
- lstm_predictions.parquet - LSTM forecasts
- xgboost_predictions.parquet - XGBoost forecasts
- test_actuals.parquet - Ground truth values

### Training Info
- lstm_training_history.csv - LSTM training curves
- training_metadata.json - Complete configuration
- sensor_list.json - Sensors used

---

## Models used

### AutoGluon

**Type:** Automated ensemble learning
**Approach:** Trains multiple models (DeepAR, Naive, ETS, TFT, Chronos) and combines them


### LSTM

**Type:** Deep learning
**Approach:** Single LSTM with sensor embeddings

---

### XGBoost

**Type:** Gradient boosting
**Approach:** Recursive forecasting with engineered features

---

## Evaluation Metrics

| Metric | Full Name | Description | Better |
|--------|-----------|-------------|--------|
| MAE | Mean Absolute Error | Average absolute error | Lower |
| RMSE | Root Mean Squared Error | Penalizes large errors | Lower |
| MAPE | Mean Absolute Percentage Error | Error as percentage | Lower |
| MASE | Mean Absolute Scaled Error | Error vs naive baseline | <1 better than naive |
| SMAPE | Symmetric Mean Absolute Percentage Error | Symmetric MAPE | Lower |

## Future Improvements/Notes

- Need per-sensor normalization while pre-processing for LSTM/XGBoost. AutoGluon handles our pre-processed data fine, LSTM/XGBoost cant handle partly flat sensors. Seperate models per sensor would also be possible if focusing on just a few sensors.

