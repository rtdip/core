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
from pyspark.sql.functions import col, udf
from pyspark.sql.types import DoubleType
from ..interfaces import MachineLearningInterface
from ..._pipeline_utils.models import Libraries, SystemType
import numpy as np


class KNearestNeighbors(MachineLearningInterface):
    """
    Implements the K-Nearest Neighbors (KNN) algorithm to predict missing values in a dataset.
    This component is compatible with time series data and supports customizable weighted or unweighted averaging for predictions.

    Example:
    ```python
    from pyspark.ml.feature import StandardScaler, VectorAssembler
    from pyspark.sql import SparkSession
    from src.sdk.python.rtdip_sdk.pipelines.forecasting.spark.k_nearest_neighbors import KNearestNeighbors
    spark_session = SparkSession.builder.master("local[2]").appName("KNN").getOrCreate()
    data = [
        ("A2PS64V0J.:ZUX09R", "2024-01-02 03:49:45.000", "Good", 25.0),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 07:53:11.000", "Good", -5.0),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 11:56:42.000", "Good", 50.0),
        ("B3TS64V0K.:ZUX09R", "2024-01-02 16:00:12.000", "Good", 80.0),
        ("A2PS64V0J.:ZUX09R", "2024-01-02 20:03:46.000", "Good", 100.0),
    ]
    columns = ["TagName", "EventTime", "Status", "Value"]
    raw_df = = spark.createDataFrame(data, columns)
    assembler = VectorAssembler(inputCols=["feature1", "feature2"], outputCol="assembled_features")
    df = assembler.transform(raw_df)
    scaler = StandardScaler(inputCol="assembled_features", outputCol="features", withStd=True, withMean=True)
    scaled_df = scaler.fit(df).transform(df)
    knn = KNearestNeighbors(
        features_col="features",
        label_col="label",
        timestamp_col="timestamp",
        k=3,
        weighted=True,
        distance_metric="combined",  # Options: "euclidean", "temporal", "combined"
        temporal_weight=0.3  # Weight for temporal distance when using combined metric
    )
    train_df, test_df = knn.randomSplit([0.8, 0.2], seed=42)
    knn.train(train_df)
    predictions = knn.predict(test_df)
    ```

    Parameters:
        features_col (str): Name of the column containing the features (the input). Default is 'features'
        label_col (str): Name of the column containing the label (the input). Default is 'label'
        timestamp_col (str, optional): Name of the column containing timestamps
        k (int): The number of neighbors to consider in the KNN algorithm. Default is 3
        weighted (bool): Whether to use weighted averaging based on distance. Default is False (unweighted averaging)
        distance_metric (str): Type of distance calculation ("euclidean", "temporal", or "combined")
        temporal_weight (float): Weight for temporal distance in combined metric (0 to 1)
    """

    def __init__(
        self,
        features_col,
        label_col,
        timestamp_col=None,
        k=3,
        weighted=False,
        distance_metric="euclidean",
        temporal_weight=0.5,
    ):
        self.features_col = features_col
        self.label_col = label_col
        self.timestamp_col = timestamp_col
        self.k = k
        self.weighted = weighted
        self.distance_metric = distance_metric
        self.temporal_weight = temporal_weight
        self.train_features = None
        self.train_labels = None
        self.train_timestamps = None

        if distance_metric not in ["euclidean", "temporal", "combined"]:
            raise ValueError(
                "distance_metric must be 'euclidean', 'temporal', or 'combined'"
            )

        if distance_metric in ["temporal", "combined"] and timestamp_col is None:
            raise ValueError(
                "timestamp_col must be provided when using temporal or combined distance metrics"
            )

    @staticmethod
    def system_type():
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
        Sets up the training DataFrame including temporal information if specified.
        """
        if self.timestamp_col:
            df = train_df.select(
                self.features_col, self.label_col, self.timestamp_col
            ).collect()
            self.train_timestamps = np.array(
                [row[self.timestamp_col].timestamp() for row in df]
            )
        else:
            df = train_df.select(self.features_col, self.label_col).collect()

        self.train_features = np.array([row[self.features_col] for row in df])
        self.train_labels = np.array([row[self.label_col] for row in df])
        return self

    def predict(self, test_df: DataFrame) -> DataFrame:
        """
        Predicts labels using the specified distance metric.
        """
        train_features = self.train_features
        train_labels = self.train_labels
        train_timestamps = self.train_timestamps
        k = self.k
        weighted = self.weighted
        distance_metric = self.distance_metric
        temporal_weight = self.temporal_weight

        def calculate_distances(features, timestamp=None):
            test_point = np.array(features)

            if distance_metric == "euclidean":
                return np.sqrt(np.sum((train_features - test_point) ** 2, axis=1))

            elif distance_metric == "temporal":
                return np.abs(train_timestamps - timestamp)

            else:  # combined
                feature_distances = np.sqrt(
                    np.sum((train_features - test_point) ** 2, axis=1)
                )
                temporal_distances = np.abs(train_timestamps - timestamp)

                # Normalize distances to [0, 1] range
                feature_distances = (feature_distances - feature_distances.min()) / (
                    feature_distances.max() - feature_distances.min() + 1e-10
                )
                temporal_distances = (temporal_distances - temporal_distances.min()) / (
                    temporal_distances.max() - temporal_distances.min() + 1e-10
                )

                # Combine distances with weights
                return (
                    1 - temporal_weight
                ) * feature_distances + temporal_weight * temporal_distances

        def knn_predict(features, timestamp=None):
            distances = calculate_distances(features, timestamp)
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
                return float(
                    max(set(k_nearest_labels), key=list(k_nearest_labels).count)
                )

        if self.distance_metric in ["temporal", "combined"]:
            predict_udf = udf(
                lambda features, timestamp: knn_predict(
                    features, timestamp.timestamp()
                ),
                DoubleType(),
            )
            return test_df.withColumn(
                "prediction",
                predict_udf(col(self.features_col), col(self.timestamp_col)),
            )
        else:
            predict_udf = udf(lambda features: knn_predict(features), DoubleType())
            return test_df.withColumn("prediction", predict_udf(col(self.features_col)))
