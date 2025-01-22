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
from pyspark.sql.functions import col,udf
from pyspark.sql.types import DoubleType
from ..interfaces import MachineLearningInterface
from ..._pipeline_utils.models import Libraries, SystemType
import numpy as np


class KNearestNeighbors(MachineLearningInterface):
    """
    Implements the K-Nearest Neighbors (KNN) algorithm to predict missing values in a dataset.
    This component allows for both weighted and unweighted averaging for prediction.

    Example
    --------
    ```python
    from src.sdk.python.rtdip_sdk.pipelines.machine_learning.spark.k_nearest_neighbors import KNearestNeighbors
    from pyspark.ml.feature import StandardScaler, VectorAssembler
    from pyspark.sql import SparkSession
    spark = ... # SparkSession
    raw_df = ... # Get a PySpark DataFrame
    assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="assembled_features")
    df = assembler.transform(raw_df)
    scaler = StandardScaler(inputCol="assembled_features", outputCol="features", withStd=True, withMean=True)
    scaled_df = scaler.fit(df).transform(df)
    knn = KNearestNeighbors(
        df=scaled_df,
        features_col="features",
        label_col="label",
        k=3,
        weighted=True
        )

    train_df, test_df = knn.split_data(train_ratio=0.8)
    knn.train(train_df)
    result = knn.predict(test_df)
    ```
    Parameters:
        df (pyspark.sql.Dataframe): DataFrame containing the features and labels.
        features_col (str): Name of the column containing the features (the input). Default is 'features'.
        label_col (str): Name of the column containing the label (the input). Default is 'label'.
        k (int): The number of neighbors to consider in the KNN algorithm. Default is 3.
        weighted (bool): Whether to use weighted averaging based on distance. Default is False (unweighted averaging).
    """

    def __init__(self, df: DataFrame, features_col, label_col, k=3, weighted=False):
        self.df = df
        self.features_col = features_col  # Should be a single column (e.g., "features")
        self.label_col = label_col
        self.k = k
        self.weighted = weighted
        self.train_features = None
        self.train_labels = None

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


    def train(self, train_df: DataFrame):
        """
        Sets up the training DataFrame.

        Parameters:
        train_df (DataFrame): The DataFrame to use for training.
        """
        df = train_df.select(self.features_col, self.label_col).collect()
        self.train_features = np.array([row[self.features_col] for row in df])
        self.train_labels = np.array([row[self.label_col] for row in df])
        return self
    def predict(self, test_df: DataFrame) -> DataFrame:
        """
        Predicts the labels for the test DataFrame based on the trained model.

        Parameters:
        test_df (DataFrame): The DataFrame containing test data.

        Returns:
        DataFrame: The test DataFrame with an additional column for predictions.
        """
        train_features = self.train_features
        train_labels = self.train_labels
        k = self.k
        weighted = self.weighted

        def knn_predict(features):
            test_point = np.array(features)
            distances = np.sqrt(np.sum((train_features - test_point) ** 2, axis=1))
            k_nearest_indices = np.argsort(distances)[:k]
            k_nearest_labels = train_labels[k_nearest_indices]

            if weighted:
                k_distances = distances[k_nearest_indices]
                weights = 1 / (k_distances + 1e-10)
                weights /= np.sum(weights)
                unique_labels = np.unique(k_nearest_labels)
                weighted_votes = {
                    label: np.sum(weights[k_nearest_labels == label])
                    for label in unique_labels
                }
                return float(max(weighted_votes.items(), key=lambda x: x[1])[0])
            else:
                return float(max(set(k_nearest_labels), key=list(k_nearest_labels).count))

        predict_udf = udf(knn_predict, DoubleType())

        return test_df.withColumn("prediction", predict_udf(col(self.features_col)))