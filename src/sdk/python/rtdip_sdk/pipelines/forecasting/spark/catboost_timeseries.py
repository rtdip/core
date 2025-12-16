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
CatBoost Time Series Forecasting for RTDIP

Implements gradient boosting for time series forecasting using CatBoost and sktime's
reduction approach (tabular regressor -> forecaster). Designed for multi-sensor
setups where additional columns act as exogenous features.
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
    """
    Class for forecasting time series using CatBoost via sktime reduction.

    Args:
        target_col (str): Name of the target column.
        timestamp_col (str): Name of the timestamp column.
        window_length (int): Number of past observations used to create lag features.
        strategy (str): Reduction strategy ("recursive" or "direct").
        random_state (int): Random seed used by CatBoost.
        loss_function (str): CatBoost loss function (e.g., "RMSE").
        iterations (int): Number of boosting iterations.
        learning_rate (float): Learning rate.
        depth (int): Tree depth.
        verbose (bool): Whether CatBoost should log training progress.

    Notes:
        - CatBoost is a tabular regressor. sktime's make_reduction wraps it into a forecaster.
        - The input DataFrame is expected to contain a timestamp column and a target column.
        - All remaining columns are treated as exogenous regressors (X).
    """
    def __init__(
        self,
        target_col: str = "target",
        timestamp_col: str = "timestamp",
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

        self.target_col = target_col
        self.timestamp_col = timestamp_col

        self.is_trained = False

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
        """
        Builds a CatBoost-based time series forecaster using sktime reduction.

        Args:
            window_length (int): Number of lags used to create supervised features.
            strategy (str): Reduction strategy ("recursive" or "direct").
            random_state (int): Random seed.
            loss_function (str): CatBoost loss function.
            iterations (int): Number of boosting iterations.
            learning_rate (float): Learning rate.
            depth (int): Tree depth.
            verbose (bool): Training verbosity.

        Returns:
            object: An sktime forecaster created via make_reduction.
        """

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
        """
        Trains the CatBoost forecaster on the provided training data.

        Args:
            train_df (DataFrame): DataFrame containing the training data.

        Raises:
            ValueError: If required columns are missing, the DataFrame is empty,
                        or training data contains missing values.
        """
        pdf = self.convert_spark_to_pandas(train_df)

        if pdf.empty:
            raise ValueError("train_df is empty after conversion to pandas.")
        if self.target_col not in pdf.columns:
            raise ValueError(
                f"Required column {self.target_col} is missing in the training DataFrame."
            )

        # CatBoost generally cannot handle NaN in y; be strict to avoid silent issues.
        if pdf[[self.target_col]].isnull().values.any():
            raise ValueError(
                f"The target column '{self.target_col}' contains NaN/None values."
            )

        self.model.fit(y=pdf[self.target_col], X=pdf.drop(columns=[self.target_col]))
        self.is_trained = True

    def predict(self, predict_df: DataFrame, forecasting_horizon: ForecastingHorizon) -> DataFrame:
        """
        Makes predictions using the trained CatBoost forecaster.

        Args:
            predict_df (DataFrame): DataFrame containing the data to predict (features only).
            forecasting_horizon (ForecastingHorizon): Absolute forecasting horizon aligned to the index.

        Returns:
            DataFrame: Spark DataFrame containing predictions

        Raises:
            ValueError: If the model has not been trained, the input is empty,
                        forecasting_horizon is invalid, or required columns are missing.
        """

        predict_pdf = self.convert_spark_to_pandas(predict_df)

        if not self.is_trained:
            raise ValueError("The model is not trained yet. Please train it first.")

        if forecasting_horizon is None:
            raise ValueError("forecasting_horizon must not be None.")

        if predict_pdf.empty:
            raise ValueError("predict_df is empty after conversion to pandas.")

        # Ensure no accidental target leakage (the caller is expected to pass features only).
        if self.target_col in predict_pdf.columns:
            raise ValueError(
                f"predict_df must not contain the target column '{self.target_col}'. "
                "Please drop it before calling predict()."
            )

        prediction = self.model.predict(fh=forecasting_horizon, X=predict_pdf)

        pred_pdf = prediction.to_frame(name=self.target_col)

        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

        predictions_df = spark.createDataFrame(pred_pdf)
        return predictions_df

    def evaluate(self, test_df: DataFrame):
        """
        Evaluates the trained model using various metrics.

        Args:
            test_df (DataFrame): DataFrame containing the test data.

        Returns:
            dict: Dictionary of evaluation metrics.

        Raises:
            ValueError: If the model has not been trained, required columns are missing,
                        the test set is empty, or prediction shape does not match targets.
        """
        if not self.is_trained:
            raise ValueError("The model is not trained yet. Please train it first.")

        test_pdf = self.convert_spark_to_pandas(test_df)

        if test_pdf.empty:
            raise ValueError("test_df is empty after conversion to pandas.")
        if self.target_col not in test_pdf.columns:
            raise ValueError(
                f"Required column {self.target_col} is missing in the test DataFrame."
            )
        if test_pdf[[self.target_col]].isnull().values.any():
            raise ValueError(
                f"The target column '{self.target_col}' contains NaN/None values in test_df."
            )

        prediction = self.predict(
            predict_df=test_df.drop(self.target_col),
            forecasting_horizon=ForecastingHorizon(test_pdf.index, is_relative=False),
        )
        prediction = prediction.toPandas()

        y_test = test_pdf[self.target_col].values
        y_pred = prediction.values

        # Basic shape guard to avoid misleading metrics on misaligned outputs.
        if len(y_test) != len(y_pred):
            raise ValueError(
                f"Prediction length ({len(y_pred)}) does not match test length ({len(y_test)}). "
                "Please check timestamp alignment and forecasting horizon."
            )

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

    def convert_spark_to_pandas(self, df:DataFrame):
        """
        Converts a PySpark DataFrame to a Pandas DataFrame with a DatetimeIndex.

        Args:
            df (DataFrame): PySpark DataFrame.

        Returns:
            pd.DataFrame: Pandas DataFrame indexed by the timestamp column and sorted.

        Raises:
            ValueError: If required columns are missing, the dataframe is empty
        """

        pdf = df.toPandas()

        if self.timestamp_col not in pdf:
            raise ValueError(
                f"Required column {self.timestamp_col} is missing in the DataFrame."
            )

        if pdf.empty:
            raise ValueError("Input DataFrame is empty.")


        pdf[self.timestamp_col] = pd.to_datetime(pdf[self.timestamp_col])
        pdf = pdf.set_index("timestamp").sort_index()

        return pdf


