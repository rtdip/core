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

from pyspark.sql import DataFrame
import pandas as pd
from autogluon.timeseries import TimeSeriesDataFrame, TimeSeriesPredictor
from ..interfaces import MachineLearningInterface
from ..._pipeline_utils.models import Libraries, SystemType, PyPiLibrary
from typing import Optional, Dict, List, Tuple


class AutoGluonTimeSeries(MachineLearningInterface):
    """
    This class uses AutoGluon's TimeSeriesPredictor to automatically train and select
    the best time series forecasting models from an ensemble including ARIMA, ETS,
    DeepAR, Temporal Fusion Transformer, and more.

    Args:
        target_col (str): Name of the column containing the target variable to forecast. Default is 'target'.
        timestamp_col (str): Name of the column containing timestamps. Default is 'timestamp'.
        item_id_col (str): Name of the column containing item/series identifiers. Default is 'item_id'.
        prediction_length (int): Number of time steps to forecast into the future. Default is 24.
        eval_metric (str): Metric to optimize during training. Options include 'MAPE', 'RMSE', 'MAE', 'SMAPE', 'MASE'. Default is 'MAPE'.
        time_limit (int): Time limit in seconds for training. Default is 600 (10 minutes).
        preset (str): Quality preset for training. Options: 'fast_training', 'medium_quality', 'good_quality', 'high_quality', 'best_quality'. Default is 'medium_quality'.
        freq (str): Time frequency for resampling irregular time series. Options: 'h' (hourly), 'D' (daily), 'T' or 'min' (minutely), 'W' (weekly), 'MS' (monthly). Default is 'h'.
        verbosity (int): Verbosity level (0-4). Default is 2.

    Example:
    --------
    ```python
    from pyspark.sql import SparkSession
    from rtdip_sdk.pipelines.forecasting.spark.autogluon_timeseries import AutoGluonTimeSeries

    spark = SparkSession.builder.master("local[2]").appName("AutoGluonExample").getOrCreate()

    # Sample time series data
    data = [
        ("A", "2024-01-01", 100.0),
        ("A", "2024-01-02", 102.0),
        ("A", "2024-01-03", 105.0),
        ("A", "2024-01-04", 103.0),
        ("A", "2024-01-05", 107.0),
    ]
    columns = ["item_id", "timestamp", "target"]
    df = spark.createDataFrame(data, columns)

    # Initialize and train
    ag = AutoGluonTimeSeries(
        target_col="target",
        timestamp_col="timestamp",
        item_id_col="item_id",
        prediction_length=2,
        eval_metric="MAPE",
        preset="medium_quality"
    )

    train_df, test_df = ag.split_data(df, train_ratio=0.8)
    ag.train(train_df)
    predictions = ag.predict(test_df)
    metrics = ag.evaluate(predictions)
    print(f"Metrics: {metrics}")

    # Get model leaderboard
    leaderboard = ag.get_leaderboard()
    print(leaderboard)
    ```

    """

    def __init__(
        self,
        target_col: str = "target",
        timestamp_col: str = "timestamp",
        item_id_col: str = "item_id",
        prediction_length: int = 24,
        eval_metric: str = "MAE",
        time_limit: int = 600,
        preset: str = "medium_quality",
        freq: str = "h",
        verbosity: int = 2,
    ) -> None:
        self.target_col = target_col
        self.timestamp_col = timestamp_col
        self.item_id_col = item_id_col
        self.prediction_length = prediction_length
        self.eval_metric = eval_metric
        self.time_limit = time_limit
        self.preset = preset
        self.freq = freq
        self.verbosity = verbosity
        self.predictor = None
        self.model = None

    @staticmethod
    def system_type():
        return SystemType.PYTHON

    @staticmethod
    def libraries():
        """
        Defines the required libraries for AutoGluon TimeSeries.
        """
        libraries = Libraries()
        libraries.add_pypi_library(
            PyPiLibrary(name="autogluon.timeseries", version="1.1.1", repo=None)
        )
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def split_data(
        self, df: DataFrame, train_ratio: float = 0.8
    ) -> Tuple[DataFrame, DataFrame]:
        """
        Splits the dataset into training and testing sets based on time order.

        Args:
            df (DataFrame): The PySpark DataFrame to split.
            train_ratio (float): The ratio of the data to be used for training. Default is 0.8 (80% for training).

        Returns:
            Tuple[DataFrame, DataFrame]: Returns the training and testing datasets.
        """
        pdf = df.orderBy(self.timestamp_col).toPandas()
        split_idx = int(len(pdf) * train_ratio)

        train_pdf = pdf.iloc[:split_idx]
        test_pdf = pdf.iloc[split_idx:]

        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        train_df = spark.createDataFrame(train_pdf)
        test_df = spark.createDataFrame(test_pdf)

        return train_df, test_df

    def _prepare_timeseries_dataframe(self, df: DataFrame) -> TimeSeriesDataFrame:
        """
        Converts PySpark DataFrame to AutoGluon TimeSeriesDataFrame format with regular frequency.

        Args:
            df (DataFrame): PySpark DataFrame with time series data.

        Returns:
            TimeSeriesDataFrame: AutoGluon-compatible time series dataframe with regular time index.
        """
        pdf = df.toPandas()

        pdf[self.timestamp_col] = pd.to_datetime(pdf[self.timestamp_col])

        ts_df = TimeSeriesDataFrame.from_data_frame(
            pdf,
            id_column=self.item_id_col,
            timestamp_column=self.timestamp_col,
        )

        ts_df = ts_df.convert_frequency(freq=self.freq)

        return ts_df

    def train(self, train_df: DataFrame):
        """
        Trains AutoGluon time series models on the provided data.

        Args:
            train_df (DataFrame): PySpark DataFrame containing training data.

        Returns:
            self: Returns the instance for method chaining.
        """
        train_data = self._prepare_timeseries_dataframe(train_df)

        self.predictor = TimeSeriesPredictor(
            prediction_length=self.prediction_length,
            eval_metric=self.eval_metric,
            freq=self.freq,
            verbosity=self.verbosity,
        )

        self.predictor.fit(
            train_data=train_data,
            time_limit=self.time_limit,
            presets=self.preset,
        )

        self.model = self.predictor

        return self

    def predict(self, prediction_df: DataFrame) -> DataFrame:
        """
        Generates predictions for the time series data.

        Args:
            prediction_df (DataFrame): PySpark DataFrame to generate predictions for.

        Returns:
            DataFrame: PySpark DataFrame with predictions added.
        """
        if self.predictor is None:
            raise ValueError("Model has not been trained yet. Call train() first.")
        pred_data = self._prepare_timeseries_dataframe(prediction_df)

        predictions = self.predictor.predict(pred_data)

        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()

        predictions_pdf = predictions.reset_index()
        predictions_df = spark.createDataFrame(predictions_pdf)

        return predictions_df

    def evaluate(self, test_df: DataFrame) -> Optional[Dict[str, float]]:
        """
        Evaluates the trained model using multiple metrics.

        Args:
            test_df (DataFrame): The PySpark DataFrame containing test data with actual values.

        Returns:
            Optional[Dict[str, float]]: Dictionary containing evaluation metrics (MAPE, RMSE, MAE, etc.)
                                       or None if evaluation fails.
        """
        if self.predictor is None:
            print("Error: Model has not been trained yet. Call train() first.")
            return None

        try:
            test_data = self._prepare_timeseries_dataframe(test_df)

            metrics = self.predictor.evaluate(
                test_data, metrics=["MAE", "RMSE", "MAPE", "MASE", "SMAPE"]
            )

            return metrics
        except Exception as e:
            print(f"Error during evaluation: {str(e)}")
            return None

    def get_leaderboard(self) -> Optional[pd.DataFrame]:
        """
        Returns the leaderboard showing performance of all trained models.

        Returns:
            Optional[pd.DataFrame]: DataFrame with model performance metrics,
                                   or None if no models have been trained.
        """
        if self.predictor is None:
            print("Error: Model has not been trained yet. Call train() first.")
            return None

        return self.predictor.leaderboard()

    def get_best_model(self) -> Optional[str]:
        """
        Returns the name of the best performing model.

        Returns:
            Optional[str]: Name of the best model or None if no models trained.
        """
        if self.predictor is None:
            print("Error: Model has not been trained yet. Call train() first.")
            return None

        leaderboard = self.get_leaderboard()
        if leaderboard is not None and len(leaderboard) > 0:
            return leaderboard.iloc[0]["model"]

        return None

    def save_model(self, path: str = None) -> str:
        """
        Returns the path where the model is saved (AutoGluon saves automatically during training).

        Args:
            path (str): Optional - not used. AutoGluon manages model paths automatically.

        Returns:
            str: Path where the model is saved.
        """
        if self.predictor is None:
            raise ValueError("Model has not been trained yet. Call train() first.")

        model_path = self.predictor.path
        if path:
            print(f"Note: AutoGluon models are auto-saved. Model is at: {model_path}")
        return model_path

    def load_model(self, path: str):
        """
        Loads a previously trained predictor from disk.

        Args:
            path (str): Directory path from where the model should be loaded.

        Returns:
            self: Returns the instance for method chaining.
        """
        self.predictor = TimeSeriesPredictor.load(path)
        self.model = self.predictor
        print(f"Model loaded from {path}")

        return self
