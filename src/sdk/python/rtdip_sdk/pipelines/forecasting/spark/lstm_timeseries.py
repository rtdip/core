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
LSTM-based time series forecasting implementation for RTDIP.

This module provides an LSTM neural network implementation for multivariate
time series forecasting using TensorFlow/Keras with sensor embeddings.
"""

import numpy as np
import pandas as pd
from typing import Dict, Optional, Any
from pyspark.sql import DataFrame, SparkSession
from sklearn.preprocessing import StandardScaler, LabelEncoder
from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    mean_absolute_percentage_error,
)

# TensorFlow imports
import tensorflow as tf
from tensorflow import keras
from tensorflow.keras import layers, Model
from tensorflow.keras.callbacks import EarlyStopping, ReduceLROnPlateau

from ..interfaces import MachineLearningInterface
from ..._pipeline_utils.models import Libraries, SystemType, PyPiLibrary


class LSTMTimeSeries(MachineLearningInterface):
    """
    LSTM-based time series forecasting model with sensor embeddings.

    This class implements a single LSTM model that handles multiple sensors using
    embeddings, allowing knowledge transfer across sensors while maintaining
    sensor-specific adaptations.

    Parameters:
        target_col (str): Name of the target column to predict
        timestamp_col (str): Name of the timestamp column
        item_id_col (str): Name of the column containing unique identifiers for each time series
        prediction_length (int): Number of time steps to forecast
        lookback_window (int): Number of historical time steps to use as input
        lstm_units (int): Number of LSTM units in each layer
        num_lstm_layers (int): Number of stacked LSTM layers
        embedding_dim (int): Dimension of sensor ID embeddings
        dropout_rate (float): Dropout rate for regularization
        learning_rate (float): Learning rate for Adam optimizer
        batch_size (int): Batch size for training
        epochs (int): Maximum number of training epochs
        patience (int): Early stopping patience (epochs without improvement)

    """

    def __init__(
        self,
        target_col: str = "target",
        timestamp_col: str = "timestamp",
        item_id_col: str = "item_id",
        prediction_length: int = 24,
        lookback_window: int = 168,  # 1 week for hourly data
        lstm_units: int = 64,
        num_lstm_layers: int = 2,
        embedding_dim: int = 8,
        dropout_rate: float = 0.2,
        learning_rate: float = 0.001,
        batch_size: int = 32,
        epochs: int = 100,
        patience: int = 10,
    ) -> None:
        self.target_col = target_col
        self.timestamp_col = timestamp_col
        self.item_id_col = item_id_col
        self.prediction_length = prediction_length
        self.lookback_window = lookback_window
        self.lstm_units = lstm_units
        self.num_lstm_layers = num_lstm_layers
        self.embedding_dim = embedding_dim
        self.dropout_rate = dropout_rate
        self.learning_rate = learning_rate
        self.batch_size = batch_size
        self.epochs = epochs
        self.patience = patience

        self.model = None
        self.scaler = StandardScaler()
        self.label_encoder = LabelEncoder()
        self.item_ids = []
        self.num_sensors = 0
        self.training_history = None
        self.spark = SparkSession.builder.getOrCreate()

    @staticmethod
    def system_type():
        return SystemType.PYTHON

    @staticmethod
    def libraries():
        """Defines the required libraries for LSTM TimeSeries."""
        libraries = Libraries()
        libraries.add_pypi_library(PyPiLibrary(name="tensorflow", version=">=2.10.0"))
        libraries.add_pypi_library(PyPiLibrary(name="scikit-learn", version=">=1.0.0"))
        libraries.add_pypi_library(PyPiLibrary(name="pandas", version=">=1.3.0"))
        libraries.add_pypi_library(PyPiLibrary(name="numpy", version=">=1.21.0"))
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def _create_sequences(
        self,
        data: np.ndarray,
        sensor_ids: np.ndarray,
        lookback: int,
        forecast_horizon: int,
    ):
        """Create sequences for LSTM training with sensor IDs."""
        X_values, X_sensors, y = [], [], []

        unique_sensors = np.unique(sensor_ids)

        for sensor_id in unique_sensors:
            sensor_mask = sensor_ids == sensor_id
            sensor_data = data[sensor_mask]

            for i in range(len(sensor_data) - lookback - forecast_horizon + 1):
                X_values.append(sensor_data[i : i + lookback])
                X_sensors.append(sensor_id)
                y.append(sensor_data[i + lookback : i + lookback + forecast_horizon])

        return np.array(X_values), np.array(X_sensors), np.array(y)

    def _build_model(self):
        """Build LSTM model with sensor embeddings."""
        values_input = layers.Input(
            shape=(self.lookback_window, 1), name="values_input"
        )

        sensor_input = layers.Input(shape=(1,), name="sensor_input")

        sensor_embedding = layers.Embedding(
            input_dim=self.num_sensors,
            output_dim=self.embedding_dim,
            name="sensor_embedding",
        )(sensor_input)
        sensor_embedding = layers.Flatten()(sensor_embedding)

        sensor_embedding_repeated = layers.RepeatVector(self.lookback_window)(
            sensor_embedding
        )

        combined = layers.Concatenate(axis=-1)(
            [values_input, sensor_embedding_repeated]
        )
        x = combined
        for i in range(self.num_lstm_layers):
            return_sequences = i < self.num_lstm_layers - 1
            x = layers.LSTM(self.lstm_units, return_sequences=return_sequences)(x)
            x = layers.Dropout(self.dropout_rate)(x)

        output = layers.Dense(self.prediction_length)(x)

        model = Model(inputs=[values_input, sensor_input], outputs=output)

        model.compile(
            optimizer=tf.keras.optimizers.Adam(learning_rate=self.learning_rate),
            loss="mse",
            metrics=["mae"],
        )

        return model

    def train(self, train_df: DataFrame):
        """
        Train LSTM model on all sensors with embeddings.

        Args:
            train_df: Spark DataFrame containing training data with columns:
                      [item_id, timestamp, target]
        """
        print("TRAINING LSTM MODEL (SINGLE MODEL WITH EMBEDDINGS)")

        pdf = train_df.toPandas()
        pdf[self.timestamp_col] = pd.to_datetime(pdf[self.timestamp_col])
        pdf = pdf.sort_values([self.item_id_col, self.timestamp_col])

        pdf["sensor_encoded"] = self.label_encoder.fit_transform(pdf[self.item_id_col])
        self.item_ids = self.label_encoder.classes_.tolist()
        self.num_sensors = len(self.item_ids)

        print(f"Training single model for {self.num_sensors} sensors")
        print(f"Total training samples: {len(pdf)}")
        print(
            f"Configuration: {self.num_lstm_layers} LSTM layers, {self.lstm_units} units each"
        )
        print(f"Sensor embedding dimension: {self.embedding_dim}")
        print(
            f"Lookback window: {self.lookback_window}, Forecast horizon: {self.prediction_length}"
        )

        values = pdf[self.target_col].values.reshape(-1, 1)
        values_scaled = self.scaler.fit_transform(values)
        sensor_ids = pdf["sensor_encoded"].values

        print("\nCreating training sequences")
        X_values, X_sensors, y = self._create_sequences(
            values_scaled.flatten(),
            sensor_ids,
            self.lookback_window,
            self.prediction_length,
        )

        if len(X_values) == 0:
            print("ERROR: Not enough data to create sequences")
            return

        X_values = X_values.reshape(X_values.shape[0], X_values.shape[1], 1)
        X_sensors = X_sensors.reshape(-1, 1)

        print(f"Created {len(X_values)} training sequences")
        print(
            f"Input shape: {X_values.shape}, Sensor IDs shape: {X_sensors.shape}, Output shape: {y.shape}"
        )

        print("\nBuilding model")
        self.model = self._build_model()
        print(self.model.summary())

        callbacks = [
            EarlyStopping(
                monitor="val_loss",
                patience=self.patience,
                restore_best_weights=True,
                verbose=1,
            ),
            ReduceLROnPlateau(
                monitor="val_loss", factor=0.5, patience=5, min_lr=1e-6, verbose=1
            ),
        ]

        print("\nTraining model")
        history = self.model.fit(
            [X_values, X_sensors],
            y,
            batch_size=self.batch_size,
            epochs=self.epochs,
            validation_split=0.2,
            callbacks=callbacks,
            verbose=1,
        )

        self.training_history = history.history

        final_loss = history.history["val_loss"][-1]
        final_mae = history.history["val_mae"][-1]
        print(f"\nTraining completed!")
        print(f"Final validation loss: {final_loss:.4f}")
        print(f"Final validation MAE: {final_mae:.4f}")

    def predict(self, predict_df: DataFrame) -> DataFrame:
        """
        Generate predictions using trained LSTM model.

        Args:
            predict_df: Spark DataFrame containing data to predict on

        Returns:
            Spark DataFrame with predictions
        """
        if self.model is None:
            raise ValueError("Model not trained. Call train() first.")

        pdf = predict_df.toPandas()
        pdf[self.timestamp_col] = pd.to_datetime(pdf[self.timestamp_col])
        pdf = pdf.sort_values([self.item_id_col, self.timestamp_col])

        all_predictions = []

        pdf["sensor_encoded"] = self.label_encoder.transform(pdf[self.item_id_col])

        for item_id in self.item_ids:
            item_data = pdf[pdf[self.item_id_col] == item_id].copy()

            if len(item_data) < self.lookback_window:
                print(f"Warning: Not enough data for {item_id} to generate predictions")
                continue

            values = (
                item_data[self.target_col]
                .values[-self.lookback_window :]
                .reshape(-1, 1)
            )
            values_scaled = self.scaler.transform(values)

            sensor_id = item_data["sensor_encoded"].iloc[0]

            X_values = values_scaled.reshape(1, self.lookback_window, 1)
            X_sensor = np.array([[sensor_id]])

            pred_scaled = self.model.predict([X_values, X_sensor], verbose=0)
            pred = self.scaler.inverse_transform(pred_scaled.reshape(-1, 1)).flatten()

            last_timestamp = item_data[self.timestamp_col].iloc[-1]
            pred_timestamps = pd.date_range(
                start=last_timestamp + pd.Timedelta(hours=1),
                periods=self.prediction_length,
                freq="h",
            )

            pred_df = pd.DataFrame(
                {
                    self.item_id_col: item_id,
                    self.timestamp_col: pred_timestamps,
                    "mean": pred,
                }
            )

            all_predictions.append(pred_df)

        if not all_predictions:
            return self.spark.createDataFrame(
                [],
                schema=f"{self.item_id_col} string, {self.timestamp_col} timestamp, mean double",
            )

        result_pdf = pd.concat(all_predictions, ignore_index=True)
        return self.spark.createDataFrame(result_pdf)

    def evaluate(self, test_df: DataFrame) -> Optional[Dict[str, float]]:
        """
        Evaluate the trained LSTM model.

        Args:
            test_df: Spark DataFrame containing test data

        Returns:
            Dictionary of evaluation metrics
        """
        if self.model is None:
            return None

        pdf = test_df.toPandas()
        pdf[self.timestamp_col] = pd.to_datetime(pdf[self.timestamp_col])
        pdf = pdf.sort_values([self.item_id_col, self.timestamp_col])
        pdf["sensor_encoded"] = self.label_encoder.transform(pdf[self.item_id_col])

        all_predictions = []
        all_actuals = []

        print("\nGenerating rolling predictions for evaluation")

        batch_values = []
        batch_sensors = []
        batch_actuals = []

        for item_id in self.item_ids:
            item_data = pdf[pdf[self.item_id_col] == item_id].copy()
            sensor_id = item_data["sensor_encoded"].iloc[0]

            if len(item_data) < self.lookback_window + self.prediction_length:
                continue

            # (sample every 24 hours to speed up)
            step_size = self.prediction_length
            for i in range(
                0,
                len(item_data) - self.lookback_window - self.prediction_length + 1,
                step_size,
            ):
                input_values = (
                    item_data[self.target_col]
                    .iloc[i : i + self.lookback_window]
                    .values.reshape(-1, 1)
                )
                input_scaled = self.scaler.transform(input_values)

                actual_values = (
                    item_data[self.target_col]
                    .iloc[
                        i
                        + self.lookback_window : i
                        + self.lookback_window
                        + self.prediction_length
                    ]
                    .values
                )

                batch_values.append(input_scaled.reshape(self.lookback_window, 1))
                batch_sensors.append(sensor_id)
                batch_actuals.append(actual_values)

        if len(batch_values) == 0:
            return None

        print(f"Making batch predictions for {len(batch_values)} samples")
        X_values_batch = np.array(batch_values)
        X_sensors_batch = np.array(batch_sensors).reshape(-1, 1)

        pred_scaled_batch = self.model.predict(
            [X_values_batch, X_sensors_batch], verbose=0, batch_size=256
        )

        for pred_scaled, actual_values in zip(pred_scaled_batch, batch_actuals):
            pred = self.scaler.inverse_transform(pred_scaled.reshape(-1, 1)).flatten()
            all_predictions.extend(pred[: len(actual_values)])
            all_actuals.extend(actual_values)

        if len(all_predictions) == 0:
            return None

        y_true = np.array(all_actuals)
        y_pred = np.array(all_predictions)

        print(f"Evaluated on {len(y_true)} predictions")

        mae = mean_absolute_error(y_true, y_pred)
        rmse = np.sqrt(mean_squared_error(y_true, y_pred))

        # MAPE with filtering
        non_zero_mask = np.abs(y_true) >= 0.1
        if np.sum(non_zero_mask) > 0:
            mape = mean_absolute_percentage_error(
                y_true[non_zero_mask], y_pred[non_zero_mask]
            )
        else:
            mape = np.nan

        # MASE - calculate naive baseline per sensor
        naive_errors = []
        for item_id in self.item_ids:
            item_data = pdf[pdf[self.item_id_col] == item_id]
            if len(item_data) > 1:
                item_true = item_data[self.target_col].values
                naive_forecast = item_true[:-1]
                naive_error = mean_absolute_error(item_true[1:], naive_forecast)
                naive_errors.append(naive_error)

        mae_naive = sum(naive_errors) / len(naive_errors) if naive_errors else 1.0
        mase = mae / mae_naive if mae_naive != 0 else mae

        # SMAPE
        smape = (
            100
            * (
                2 * np.abs(y_true - y_pred) / (np.abs(y_true) + np.abs(y_pred) + 1e-10)
            ).mean()
        )

        # Return in AutoGluon format (negative is better)
        return {
            "MAE": -mae,
            "RMSE": -rmse,
            "MAPE": -mape,
            "MASE": -mase,
            "SMAPE": -smape,
        }

    def save(self, path: str):
        """Save trained model."""
        import joblib
        import os

        os.makedirs(path, exist_ok=True)

        model_path = os.path.join(path, "lstm_model.keras")
        self.model.save(model_path)

        scaler_path = os.path.join(path, "scaler.pkl")
        joblib.dump(self.scaler, scaler_path)

        encoder_path = os.path.join(path, "label_encoder.pkl")
        joblib.dump(self.label_encoder, encoder_path)

        metadata = {
            "item_ids": self.item_ids,
            "num_sensors": self.num_sensors,
            "config": {
                "lookback_window": self.lookback_window,
                "prediction_length": self.prediction_length,
                "lstm_units": self.lstm_units,
                "num_lstm_layers": self.num_lstm_layers,
                "embedding_dim": self.embedding_dim,
            },
        }
        metadata_path = os.path.join(path, "metadata.pkl")
        joblib.dump(metadata, metadata_path)

    def load(self, path: str):
        """Load trained model."""
        import joblib
        import os

        model_path = os.path.join(path, "lstm_model.keras")
        self.model = keras.models.load_model(model_path)

        scaler_path = os.path.join(path, "scaler.pkl")
        self.scaler = joblib.load(scaler_path)

        encoder_path = os.path.join(path, "label_encoder.pkl")
        self.label_encoder = joblib.load(encoder_path)

        metadata_path = os.path.join(path, "metadata.pkl")
        metadata = joblib.load(metadata_path)
        self.item_ids = metadata["item_ids"]
        self.num_sensors = metadata["num_sensors"]

    def get_model_info(self) -> Dict[str, Any]:
        """Get information about trained model."""
        return {
            "model_type": "Single LSTM with sensor embeddings",
            "num_sensors": self.num_sensors,
            "item_ids": self.item_ids,
            "lookback_window": self.lookback_window,
            "prediction_length": self.prediction_length,
            "lstm_units": self.lstm_units,
            "num_lstm_layers": self.num_lstm_layers,
            "embedding_dim": self.embedding_dim,
            "total_parameters": self.model.count_params() if self.model else 0,
        }
