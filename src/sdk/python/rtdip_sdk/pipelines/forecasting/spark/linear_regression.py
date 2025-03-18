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
import pyspark.ml as ml
from pyspark.ml.evaluation import RegressionEvaluator
from ..interfaces import MachineLearningInterface
from ..._pipeline_utils.models import Libraries, SystemType
from typing import Optional


class LinearRegression(MachineLearningInterface):
    """
    This class uses pyspark.ml.LinearRegression to train a linear regression model on time data
    and then uses the model to predict next values in the time series.

    Args:
        features_col (str): Name of the column containing the features (the input). Default is 'features'.
        label_col (str): Name of the column containing the label (the input). Default is 'label'.
        prediction_col (str): Name of the column to which the prediction will be written. Default is 'prediction'.

    Example:
    --------
    ```python
    from pyspark.sql import SparkSession
    from pyspark.ml.feature import VectorAssembler
    from rtdip_sdk.pipelines.forecasting.spark.linear_regression import LinearRegression

    spark = SparkSession.builder.master("local[2]").appName("LinearRegressionExample").getOrCreate()

    data = [
        (1, 2.0, 3.0),
        (2, 3.0, 4.0),
        (3, 4.0, 5.0),
        (4, 5.0, 6.0),
        (5, 6.0, 7.0),
    ]
    columns = ["id", "feature1", "label"]
    df = spark.createDataFrame(data, columns)

    assembler = VectorAssembler(inputCols=["feature1"], outputCol="features")
    df = assembler.transform(df)

    lr = LinearRegression(features_col="features", label_col="label", prediction_col="prediction")
    train_df, test_df = lr.split_data(df, train_ratio=0.8)
    lr.train(train_df)
    predictions = lr.predict(test_df)
    rmse, r2 = lr.evaluate(predictions)
    print(f"RMSE: {rmse}, R²: {r2}")
    ```

    """

    def __init__(
        self,
        features_col: str = "features",
        label_col: str = "label",
        prediction_col: str = "prediction",
    ) -> None:
        self.features_col = features_col
        self.label_col = label_col
        self.prediction_col = prediction_col

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK
        """
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def split_data(
        self, df: DataFrame, train_ratio: float = 0.8
    ) -> tuple[DataFrame, DataFrame]:
        """
        Splits the dataset into training and testing sets.

        Args:
            train_ratio (float): The ratio of the data to be used for training. Default is 0.8 (80% for training).

        Returns:
            tuple[DataFrame, DataFrame]: Returns the training and testing datasets.
        """
        train_df, test_df = df.randomSplit([train_ratio, 1 - train_ratio], seed=42)
        return train_df, test_df

    def train(self, train_df: DataFrame):
        """
        Trains a linear regression model on the provided data.
        """
        linear_regression = ml.regression.LinearRegression(
            featuresCol=self.features_col,
            labelCol=self.label_col,
            predictionCol=self.prediction_col,
        )

        self.model = linear_regression.fit(train_df)
        return self

    def predict(self, prediction_df: DataFrame):
        """
        Predicts the next values in the time series.
        """

        return self.model.transform(
            prediction_df,
        )

    def evaluate(self, test_df: DataFrame) -> Optional[float]:
        """
        Evaluates the trained model using RMSE.

        Args:
            test_df (DataFrame): The testing dataset to evaluate the model.

        Returns:
            Optional[float]: The Root Mean Squared Error (RMSE) of the model or None if the prediction columnd doesn't exist.
        """

        if self.prediction_col not in test_df.columns:
            print(
                f"Error: '{self.prediction_col}' column is missing in the test DataFrame."
            )
            return None

        # Evaluator for RMSE
        evaluator_rmse = RegressionEvaluator(
            labelCol=self.label_col,
            predictionCol=self.prediction_col,
            metricName="rmse",
        )
        rmse = evaluator_rmse.evaluate(test_df)

        # Evaluator for R²
        evaluator_r2 = RegressionEvaluator(
            labelCol=self.label_col, predictionCol=self.prediction_col, metricName="r2"
        )
        r2 = evaluator_r2.evaluate(test_df)

        return rmse, r2
