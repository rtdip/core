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

from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error,
    mean_absolute_percentage_error,
)
from prophet import Prophet
from pyspark.sql import DataFrame
import pandas as pd
import numpy as np
from ..interfaces import MachineLearningInterface
from ..._pipeline_utils.models import Libraries, SystemType, PyPiLibrary

import sys

# Hide polars from cmdstanpy/prophet import path
# so cmdstanpy can't import it and should fall back to other parsers.
sys.modules["polars"] = None


class ProphetForecaster(MachineLearningInterface):
    """
    Class for forecasting time series using Prophet.

    Args:
        use_only_timestamp_and_target (bool): Whether to use only the timestamp and target columns for training.
        target_col (str): Name of the target column.
        timestamp_col (str): Name of the timestamp column.
        growth (str): Type of growth ("linear" or "logistic").
        n_changepoints (int): Number of changepoints to consider.
        changepoint_range (float): Proportion of data used to estimate changepoint locations.
        yearly_seasonality (str): Type of yearly seasonality ("auto", "True", or "False").
        weekly_seasonality (str): Type of weekly seasonality ("auto").
        daily_seasonality (str): Type of daily seasonality ("auto").
        seasonality_mode (str): Mode for seasonality ("additive" or "multiplicative").
        seasonality_prior_scale (float): Scale for seasonality prior.
        scaling (str): Scaling method ("absmax" or "minmax").

    Example:
    --------
    ```python
    from pyspark.sql import SparkSession
    from rtdip_sdk.pipelines.forecasting.spark.prophet import ProphetForecaster
        from sktime.forecasting.model_selection import temporal_train_test_split

    spark = SparkSession.builder.master("local[2]").appName("ProphetExample").getOrCreate()

    # Sample time series data
    data = [
        ("2024-01-01", 100.0),
        ("2024-01-02", 102.0),
        ("2024-01-03", 105.0),
        ("2024-01-04", 103.0),
        ("2024-01-05", 107.0),
    ]
    columns = ["ds", "y"]
    pdf = pd.DataFrame(data, columns=columns)

    # Split data into train and test sets
    train_set, test_set = temporal_train_test_split(pdf_turbine1_no_NaN, test_size=0.2)

    spark_trainset = spark.createDataFrame(train_set)
    spark_testset = spark.createDataFrame(test_set)

    pf = ProphetForecaster(scaling="absmax")
    pf.train(scada_spark_trainset)
    metrics = pf.evaluate(scada_spark_testset, "D")

    """

    def __init__(
        self,
        use_only_timestamp_and_target: bool = True,
        target_col: str = "y",
        timestamp_col: str = "ds",
        growth: str = "linear",
        n_changepoints: int = 25,
        changepoint_range: float = 0.8,
        yearly_seasonality: str = "auto",  # can be "auto", "True" or "False"
        weekly_seasonality: str = "auto",
        daily_seasonality: str = "auto",
        seasonality_mode: str = "additive",  # can be "additive" or "multiplicative"
        seasonality_prior_scale: float = 10,
        scaling: str = "absmax",  # can be "absmax" or "minmax"
    ) -> None:

        self.use_only_timestamp_and_target = use_only_timestamp_and_target
        self.target_col = target_col
        self.timestamp_col = timestamp_col

        self.prophet = Prophet(
            growth=growth,
            n_changepoints=n_changepoints,
            changepoint_range=changepoint_range,
            yearly_seasonality=yearly_seasonality,
            weekly_seasonality=weekly_seasonality,
            daily_seasonality=daily_seasonality,
            seasonality_mode=seasonality_mode,
            seasonality_prior_scale=seasonality_prior_scale,
            scaling=scaling,
        )

        self.is_trained = False

    @staticmethod
    def system_type():
        return SystemType.PYTHON

    @staticmethod
    def settings() -> dict:
        return {}

    def train(self, train_df: DataFrame):
        """
        Trains the Prophet model on the provided training data.

        Args:
            train_df (DataFrame): DataFrame containing the training data.

        Raises:
            ValueError: If the input DataFrame contains any missing values (NaN or None).
                        Prophet requires the data to be complete without any missing values.
        """
        pdf = self.convert_spark_to_pandas(train_df)

        if pdf.isnull().values.any():
            raise ValueError(
                "The dataframe contains NaN values. Prophet doesn't allow any NaN or None values"
            )

        self.prophet.fit(pdf)

        self.is_trained = True

    def evaluate(self, test_df: DataFrame, freq: str):
        """
        Evaluates the trained model using various metrics.

        Args:
            test_df (DataFrame): DataFrame containing the test data.
            freq (str): Frequency of the data (e.g., 'D', 'H').

        Returns:
            dict: Dictionary of evaluation metrics.

        Raises:
            ValueError: If the model has not been trained.
        """
        if not self.is_trained:
            raise ValueError("The model is not trained yet. Please train it first.")

        test_pdf = self.convert_spark_to_pandas(test_df)
        prediction = self.predict(predict_df=test_df, periods=len(test_pdf), freq=freq)
        prediction = prediction.toPandas()

        actual_prediction = prediction.tail(len(test_pdf))

        y_test = test_pdf[self.target_col].values
        y_pred = actual_prediction["yhat"].values

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

        print("\nProphet Metrics:")
        print("-" * 80)
        for metric_name, metric_value in metrics.items():
            print(f"{metric_name:20s}: {abs(metric_value):.4f}")

        return metrics

    def predict(self, predict_df: DataFrame, periods: int, freq: str) -> DataFrame:
        """
        Makes predictions using the trained Prophet model.

        Args:
            predict_df (DataFrame): DataFrame containing the data to predict.
            periods (int): Number of periods to forecast.
            freq (str): Frequency of the data (e.g., 'D', 'H').

        Returns:
            DataFrame: DataFrame containing the predictions.

        Raises:
            ValueError: If the model has not been trained.
        """
        if not self.is_trained:
            raise ValueError("The model is not trained yet. Please train it first.")

        future = self.prophet.make_future_dataframe(periods=periods, freq=freq)
        prediction = self.prophet.predict(future)

        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        predictions_pdf = prediction.reset_index()
        predictions_df = spark.createDataFrame(predictions_pdf)

        return predictions_df

    def convert_spark_to_pandas(self, df: DataFrame):
        """
        Converts a PySpark DataFrame to a Pandas DataFrame compatible with Prophet.

        Args:
            df (DataFrame): PySpark DataFrame.

        Returns:
            DataFrame: Pandas DataFrame formatted for Prophet.

        Raises:
            ValueError: If required columns are missing from the DataFrame.
        """
        pdf = df.toPandas()
        if self.use_only_timestamp_and_target:
            if self.timestamp_col not in pdf or self.target_col not in pdf:
                raise ValueError(
                    f"Required columns {self.timestamp_col} or {self.target_col} are missing in the DataFrame."
                )
            pdf = pdf[[self.timestamp_col, self.target_col]]

        pdf[self.timestamp_col] = pd.to_datetime(pdf[self.timestamp_col])
        pdf.rename(
            columns={self.target_col: "y", self.timestamp_col: "ds"}, inplace=True
        )

        return pdf
