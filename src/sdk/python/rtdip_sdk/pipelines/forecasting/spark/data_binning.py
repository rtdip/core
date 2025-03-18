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

import pyspark.ml.clustering as clustering
from pyspark.sql import DataFrame
from ..interfaces import MachineLearningInterface
from ..._pipeline_utils.models import Libraries, SystemType


class DataBinning(MachineLearningInterface):
    """
    Data binning using clustering methods. This method partitions the data points into a specified number of clusters (bins)
    based on the specified column. Each data point is assigned to the nearest cluster center.

    Example
    --------
    ```python
    from src.sdk.python.rtdip_sdk.pipelines.forecasting.spark.data_binning import DataBinning

    df = ... # Get a PySpark DataFrame with features column

    binning = DataBinning(
        column_name="features",
        bins=3,
        output_column_name="bin",
        method="kmeans"
    )
    binned_df = binning.train(df).predict(df)
    binned_df.show()
    ```

    Parameters:
        column_name (str): The name of the input column to be binned (default: "features").
        bins (int): The number of bins/clusters to create (default: 2).
        output_column_name (str): The name of the output column containing bin assignments (default: "bin").
        method (str): The binning method to use. Currently only supports "kmeans".
    """

    def __init__(
        self,
        column_name: str = "features",
        bins: int = 2,
        output_column_name: str = "bin",
        method: str = "kmeans",
    ) -> None:
        self.column_name = column_name

        if method == "kmeans":
            self.method = clustering.KMeans(
                featuresCol=column_name, predictionCol=output_column_name, k=bins
            )
        else:
            raise ValueError("Unknown method: {}".format(method))

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

    def train(self, train_df):
        """
        Filter anomalies based on the k-sigma rule
        """
        self.model = self.method.fit(train_df)
        return self

    def predict(self, predict_df):
        return self.model.transform(predict_df)
