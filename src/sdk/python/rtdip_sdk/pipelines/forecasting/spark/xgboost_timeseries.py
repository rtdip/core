# Copyright 2025 RTDIP
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
XGBoost Time Series Forecasting for RTDIP

Implements gradient boosting for multi-sensor time series forecasting with feature engineering.
"""

import pandas as pd
import numpy as np
from pyspark.sql import DataFrame
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    mean_absolute_percentage_error,
)
import xgboost as xgb
from typing import Dict, List, Optional

from ..interfaces import MachineLearningInterface
from ..._pipeline_utils.models import Libraries, SystemType, PyPiLibrary


class XGBoostTimeSeries(MachineLearningInterface):
    """
    XGBoost-based time series forecasting with feature engineering.

    Uses gradient boosting with engineered lag features, rolling statistics,
    and time-based features for multi-step forecasting across multiple sensors.

    Architecture:
    - Single XGBoost model for all sensors
    - Sensor ID as categorical feature
    - Lag features (1, 24, 168 hours)
    - Rolling statistics (mean, std over 24h window)
    - Time features (hour, day_of_week)
    - Recursive multi-step forecasting

    Args:
        target_col: Column name for target values
        timestamp_col: Column name for timestamps
        item_id_col: Column name for sensor/item IDs
        prediction_length: Number of steps to forecast
        max_depth: Maximum tree depth
        learning_rate: Learning rate for gradient boosting
        n_estimators: Number of boosting rounds
        n_jobs: Number of parallel threads (-1 = all cores)
    """

    def __init__(
        self,
        target_col: str = "target",
        timestamp_col: str = "timestamp",
        item_id_col: str = "item_id",
        prediction_length: int = 24,
        max_depth: int = 6,
        learning_rate: float = 0.1,
        n_estimators: int = 100,
        n_jobs: int = -1,
    ):
        self.target_col = target_col
        self.timestamp_col = timestamp_col
        self.item_id_col = item_id_col
        self.prediction_length = prediction_length
        self.max_depth = max_depth
        self.learning_rate = learning_rate
        self.n_estimators = n_estimators
        self.n_jobs = n_jobs

        self.model = None
        self.label_encoder = LabelEncoder()
        self.item_ids = None
        self.feature_cols = None

    @staticmethod
    def system_type():
        return SystemType.PYTHON

    @staticmethod
    def libraries():
        """Defines the required libraries for XGBoost TimeSeries."""
        libraries = Libraries()
        libraries.add_pypi_library(PyPiLibrary(name="xgboost", version=">=1.7.0"))
        libraries.add_pypi_library(PyPiLibrary(name="scikit-learn", version=">=1.0.0"))
        libraries.add_pypi_library(PyPiLibrary(name="pandas", version=">=1.3.0"))
        libraries.add_pypi_library(PyPiLibrary(name="numpy", version=">=1.21.0"))
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def _create_time_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create time-based features from timestamp."""
        df = df.copy()
        df[self.timestamp_col] = pd.to_datetime(df[self.timestamp_col])

        df["hour"] = df[self.timestamp_col].dt.hour
        df["day_of_week"] = df[self.timestamp_col].dt.dayofweek
        df["day_of_month"] = df[self.timestamp_col].dt.day
        df["month"] = df[self.timestamp_col].dt.month

        return df

    def _create_lag_features(self, df: pd.DataFrame, lags: List[int]) -> pd.DataFrame:
        """Create lag features for each sensor."""
        df = df.copy()
        df = df.sort_values([self.item_id_col, self.timestamp_col])

        for lag in lags:
            df[f"lag_{lag}"] = df.groupby(self.item_id_col)[self.target_col].shift(lag)

        return df

    def _create_rolling_features(
        self, df: pd.DataFrame, windows: List[int]
    ) -> pd.DataFrame:
        """Create rolling statistics features for each sensor."""
        df = df.copy()
        df = df.sort_values([self.item_id_col, self.timestamp_col])

        for window in windows:
            # Rolling mean
            df[f"rolling_mean_{window}"] = df.groupby(self.item_id_col)[
                self.target_col
            ].transform(lambda x: x.rolling(window=window, min_periods=1).mean())

            # Rolling std
            df[f"rolling_std_{window}"] = df.groupby(self.item_id_col)[
                self.target_col
            ].transform(lambda x: x.rolling(window=window, min_periods=1).std())

        return df

    def _engineer_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply all feature engineering steps."""
        print("Engineering features")

        df = self._create_time_features(df)
        df = self._create_lag_features(df, lags=[1, 6, 12, 24, 48])
        df = self._create_rolling_features(df, windows=[12, 24])
        df["sensor_encoded"] = self.label_encoder.fit_transform(df[self.item_id_col])

        return df

    def train(self, train_df: DataFrame):
        """
        Train XGBoost model on time series data.

        Args:
            train_df: Spark DataFrame with columns [item_id, timestamp, target]
        """
        print("TRAINING XGBOOST MODEL")

        pdf = train_df.toPandas()
        print(
            f"Training data: {len(pdf):,} rows, {pdf[self.item_id_col].nunique()} sensors"
        )

        pdf = self._engineer_features(pdf)

        self.item_ids = self.label_encoder.classes_.tolist()

        self.feature_cols = [
            "sensor_encoded",
            "hour",
            "day_of_week",
            "day_of_month",
            "month",
            "lag_1",
            "lag_6",
            "lag_12",
            "lag_24",
            "lag_48",
            "rolling_mean_12",
            "rolling_std_12",
            "rolling_mean_24",
            "rolling_std_24",
        ]

        pdf_clean = pdf.dropna(subset=self.feature_cols)
        print(f"After removing NaN rows: {len(pdf_clean):,} rows")

        X_train = pdf_clean[self.feature_cols]
        y_train = pdf_clean[self.target_col]

        print(f"\nTraining XGBoost with {len(X_train):,} samples")
        print(f"Features: {self.feature_cols}")
        print(f"Model parameters:")
        print(f"  max_depth: {self.max_depth}")
        print(f"  learning_rate: {self.learning_rate}")
        print(f"  n_estimators: {self.n_estimators}")
        print(f"  n_jobs: {self.n_jobs}")

        self.model = xgb.XGBRegressor(
            max_depth=self.max_depth,
            learning_rate=self.learning_rate,
            n_estimators=self.n_estimators,
            n_jobs=self.n_jobs,
            tree_method="hist",
            random_state=42,
            enable_categorical=True,
        )

        self.model.fit(X_train, y_train, verbose=False)

        print("\nTraining completed")

        feature_importance = pd.DataFrame(
            {
                "feature": self.feature_cols,
                "importance": self.model.feature_importances_,
            }
        ).sort_values("importance", ascending=False)

        print("\nTop 5 Most Important Features:")
        print(feature_importance.head(5).to_string(index=False))

    def predict(self, test_df: DataFrame) -> DataFrame:
        """
        Generate future forecasts for test period.

        Uses recursive forecasting strategy: predict one step, update features, repeat.

        Args:
            test_df: Spark DataFrame with test data

        Returns:
            Spark DataFrame with predictions [item_id, timestamp, predicted]
        """
        print("GENERATING XGBOOST PREDICTIONS")

        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")

        pdf = test_df.toPandas()
        spark = test_df.sql_ctx.sparkSession

        # Get the last known values from training for each sensor
        # (used as starting point for recursive forecasting)
        predictions_list = []

        for item_id in pdf[self.item_id_col].unique():
            sensor_data = pdf[pdf[self.item_id_col] == item_id].copy()
            sensor_data = sensor_data.sort_values(self.timestamp_col)

            if len(sensor_data) == 0:
                continue
            last_timestamp = sensor_data[self.timestamp_col].max()

            sensor_data = self._engineer_features(sensor_data)

            current_data = sensor_data.copy()

            for step in range(self.prediction_length):
                last_row = current_data.dropna(subset=self.feature_cols).iloc[-1:]

                if len(last_row) == 0:
                    print(
                        f"Warning: No valid features for sensor {item_id} at step {step}"
                    )
                    break

                X = last_row[self.feature_cols]

                pred = self.model.predict(X)[0]

                next_timestamp = last_timestamp + pd.Timedelta(hours=step + 1)

                predictions_list.append(
                    {
                        self.item_id_col: item_id,
                        self.timestamp_col: next_timestamp,
                        "predicted": pred,
                    }
                )

                new_row = {
                    self.item_id_col: item_id,
                    self.timestamp_col: next_timestamp,
                    self.target_col: pred,
                }

                current_data = pd.concat(
                    [current_data, pd.DataFrame([new_row])], ignore_index=True
                )
                current_data = self._engineer_features(current_data)

        predictions_df = pd.DataFrame(predictions_list)

        print(f"\nGenerated {len(predictions_df)} predictions")
        print(f"  Sensors: {predictions_df[self.item_id_col].nunique()}")
        print(f"  Steps per sensor: {self.prediction_length}")

        return spark.createDataFrame(predictions_df)

    def evaluate(self, test_df: DataFrame) -> Dict[str, float]:
        """
        Evaluate model on test data using rolling window prediction.

        Args:
            test_df: Spark DataFrame with test data

        Returns:
            Dictionary of metrics (MAE, RMSE, MAPE, MASE, SMAPE)
        """
        print("EVALUATING XGBOOST MODEL")

        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")

        pdf = test_df.toPandas()

        pdf = self._engineer_features(pdf)

        pdf_clean = pdf.dropna(subset=self.feature_cols)

        if len(pdf_clean) == 0:
            print("ERROR: No valid test samples after feature engineering")
            return None

        print(f"Test samples: {len(pdf_clean):,}")

        X_test = pdf_clean[self.feature_cols]
        y_test = pdf_clean[self.target_col]

        y_pred = self.model.predict(X_test)

        print(f"Evaluated on {len(y_test)} predictions")

        mae = mean_absolute_error(y_test, y_pred)
        mse = mean_squared_error(y_test, y_pred)
        rmse = np.sqrt(mse)

        # MAPE (filter near-zero values)
        non_zero_mask = np.abs(y_test) >= 0.1
        if np.sum(non_zero_mask) > 0:
            mape = mean_absolute_percentage_error(
                y_test[non_zero_mask], y_pred[non_zero_mask]
            )
        else:
            mape = np.nan

        # MASE (Mean Absolute Scaled Error)
        if len(y_test) > 1:
            naive_forecast = y_test.iloc[:-1].values
            mae_naive = mean_absolute_error(y_test.iloc[1:], naive_forecast)
            mase = mae / mae_naive if mae_naive != 0 else mae
        else:
            mase = np.nan

        # SMAPE (Symmetric Mean Absolute Percentage Error)
        smape = (
            100
            * (
                2 * np.abs(y_test - y_pred) / (np.abs(y_test) + np.abs(y_pred) + 1e-10)
            ).mean()
        )

        # AutoGluon uses negative metrics (higher is better)
        metrics = {
            "MAE": -mae,
            "RMSE": -rmse,
            "MAPE": -mape,
            "MASE": -mase,
            "SMAPE": -smape,
        }

        print("\nXGBoost Metrics:")
        print("-" * 80)
        for metric_name, metric_value in metrics.items():
            print(f"{metric_name:20s}: {abs(metric_value):.4f}")

        return metrics
