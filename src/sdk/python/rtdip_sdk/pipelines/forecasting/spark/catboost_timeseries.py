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

from typing import Dict, List, Optional
from catboost import CatBoostRegressor
from sktime.forecasting.compose import make_reduction
from sktime.forecasting.base import ForecastingHorizon
from ..interfaces import MachineLearningInterface
from ..._pipeline_utils.models import Libraries, SystemType, PyPiLibrary


class CatboostTimeSeries(MachineLearningInterface):

    def __init__(
        self,
        target_col: str,
        window_length: int = 144,
        strategy: str = "recursive",
        random_state: int = 42,
        loss_function: str = "RMSE",
        iterations: int = 250,
        learning_rate: float = 0.05,
        depth: int = 8,
        verbose: bool = True,
    ):
        self.model = self.build_catboost_forecaster(
            window_length=window_length,
            strategy=strategy,
            random_state=random_state,
            loss_function=loss_function,
            iterations=iterations,
            learning_rate=learning_rate,
            depth=depth,
            verbose=verbose,
        )

        self.is_trained = False
        self.target_col = target_col

    @staticmethod
    def system_type():
        return SystemType.PYTHON

    @staticmethod
    def libraries():
        """Defines the required libraries for XGBoost TimeSeries."""
        libraries = Libraries()
        libraries.add_pypi_library(PyPiLibrary(name="catboost", version="==1.2.8"))
        libraries.add_pypi_library(PyPiLibrary(name="scikit-learn", version=">=1.0.0"))
        libraries.add_pypi_library(PyPiLibrary(name="pandas", version=">=1.3.0"))
        libraries.add_pypi_library(PyPiLibrary(name="sktime", version="==0.40.1"))
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def build_catboost_forecaster(
        self,
        window_length: int = 144,
        strategy: str = "recursive",
        random_state: int = 42,
        loss_function: str = "RMSE",
        iterations: int = 250,
        learning_rate: float = 0.05,
        depth: int = 8,
        verbose: bool = True,
    ):
        # CatBoost is a tabular regressor; reduction turns it into a time series forecaster
        cb = CatBoostRegressor(
            loss_function=loss_function,
            iterations=iterations,
            learning_rate=learning_rate,
            depth=depth,
            random_seed=random_state,
            verbose=verbose,  # keep training silent
        )

        # strategy="recursive" is usually fast; "direct" can be stronger but slower
        forecaster = make_reduction(
            estimator=cb,
            strategy=strategy,  # "recursive" or "direct"
            window_length=window_length,
        )
        return forecaster

    def train(self, train_df: DataFrame):
        pdf = train_df.toPandas()
        self.model.fit(y=pdf[self.target_col], X=pdf.drop(columns=[self.target_col]))
        self.is_trained = True

    def predict(self, predict_df: DataFrame, forecasting_horizon: ForecastingHorizon) -> DataFrame:
        if not self.is_trained:
            raise ValueError("The model is not trained yet. Please train it first.")

        predict_pdf = predict_df.toPandas()
        prediction = self.model.predict(fh=forecasting_horizon, X=predict_pdf)

        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        predictions_df = spark.createDataFrame(prediction)

        return predictions_df

    def evaluate(self, test_df: DataFrame):
        """
        Evaluates the trained model using various metrics.

        Args:
            test_df (DataFrame): DataFrame containing the test data.

        Returns:
            dict: Dictionary of evaluation metrics.

        Raises:
            ValueError: If the model has not been trained.
        """
        if not self.is_trained:
            raise ValueError("The model is not trained yet. Please train it first.")

        test_pdf = test_df.toPandas()

        prediction = self.predict(
            predict_df=test_df.drop(self.target_col),
            forecasting_horizon=ForecastingHorizon(test_pdf.index, is_relative=False),
        )
        prediction = prediction.toPandas()

        y_test = test_pdf[self.target_col].values
        y_pred = prediction.values

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
            naive_forecast = y_test[:-1]
            mae_naive = mean_absolute_error(y_test[1:], naive_forecast)
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

        print("\nCatBoost Metrics:")
        print("-" * 80)
        for metric_name, metric_value in metrics.items():
            print(f"{metric_name:20s}: {abs(metric_value):.4f}")

        return metrics
