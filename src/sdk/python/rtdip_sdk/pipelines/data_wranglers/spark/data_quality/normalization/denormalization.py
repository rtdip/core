# Copyright 2024 RTDIP
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
from pyspark.sql import DataFrame as PySparkDataFrame
from rtdip_sdk.pipelines.data_wranglers.interfaces import WranglerBaseInterface
from rtdip_sdk.pipelines._pipeline_utils.models import Libraries, SystemType
from rtdip_sdk.pipelines.data_wranglers.spark.data_quality.normalization.normalization import NormalizationBaseClass

class Denormalization(WranglerBaseInterface):
    """
    #TODO
    Applies denormalization to multiple columns in a PySpark DataFrame using Z-Score, Min-Max or Mean normalization.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.monitoring.spark.data_quality.normalization import Normalization
    from pyspark.sql import SparkSession
    from pyspark.sql.dataframe import DataFrame

    normalization = Normalization(df, "z-score", ["value_column_1", "value_column_2"])
    normalized_df = normalization.normalize()
    ```

    Parameters:
        df (DataFrame): PySpark DataFrame to be normalized.
        method (str): Normalization method, either "z-score" or "min-max" or "mean".
        column_names (List[str]): List of columns in the DataFrame to be normalized.
    """

    df: PySparkDataFrame
    normalization_to_revert: NormalizationBaseClass

    def __init__(self, df: PySparkDataFrame, normalization_to_revert: NormalizationBaseClass) -> None:
        self.df = df
        self.normalization_to_revert = normalization_to_revert

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

    def filter(self) -> PySparkDataFrame:
        return self.normalization_to_revert.denormalize(self.df)
