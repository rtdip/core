# Copyright 2022 RTDIP
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


class LinearRegression(MachineLearningInterface):
    """
    This function uses pyspark.ml.LinearRegression to train a linear regression model on time data.
    And the uses the model to predict next values in the time series.

    Args:
        df (pyspark.sql.Dataframe): DataFrame containing the features and labels.
        features_col (str): Name of the column containing the features (the input). Default is 'features'.
        label_col (str): Name of the column containing the label (the input). Default is 'label'.
        prediction_col (str): Name of the column to which the prediction will be written. Default is 'prediction'.
    Returns:
        PySparkDataFrame: Returns the original PySpark DataFrame without changes.
    """

    def __init__(
        self,
        df: DataFrame,
        features_col: str = "features",
        label_col: str = "label",
        prediction_col: str = "prediction",
    ) -> None:
        self.df = df
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

    def split_data(self, train_ratio: float = 0.8):
        """
        Splits the dataset into training and testing sets.

        Args:
            train_ratio (float): The ratio of the data to be used for training. Default is 0.8 (80% for training).

        Returns:
            DataFrame: Returns the training and testing datasets.
        """
        train_df, test_df = self.df.randomSplit([train_ratio, 1 - train_ratio], seed=42)
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

    def evaluate(self, test_df: DataFrame):
        """
        Evaluates the trained model using RMSE.

        Args:
            test_df (DataFrame): The testing dataset to evaluate the model.

        Returns:
            float: The Root Mean Squared Error (RMSE) of the model.
        """
        # Check the columns of the test DataFrame
        print(f"Columns in test_df: {test_df.columns}")
        test_df.show(5)

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
