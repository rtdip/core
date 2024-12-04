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

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import mean, stddev, abs, col
from ..interfaces import DataManipulationBaseInterface
from ...input_validator import InputValidator
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)


class KSigmaAnomalyDetection(DataManipulationBaseInterface, InputValidator):
    """
    Anomaly detection with the k-sigma method. This method either computes the mean and standard deviation, or the median and the median absolute deviation (MAD) of the data.
    The k-sigma method then filters out all data points that are k times the standard deviation away from the mean, or k times the MAD away from the median.
    Assuming a normal distribution, this method keeps around 99.7% of the data points when k=3 and use_median=False.

    Example
    --------
    ```python
    from src.sdk.python.rtdip_sdk.pipelines.data_wranglers.spark.data_manipulation.k_sigma_anomaly_detection import KSigmaAnomalyDetection

    spark = ... # SparkSession
    df = ... # Get a PySpark DataFrame

    filtered_df = KSigmaAnomalyDetection(
        spark, df, ["<column to filter>"]
    ).filter()

    filtered_df.show()
    ```

    Parameters:
        spark (SparkSession): A SparkSession object.
        df (DataFrame): Dataframe containing the raw data.
        column_names (list[str]): The names of the columns to be filtered (currently only one column is supported).
        k_value (float): The number of deviations to build the threshold.
        use_median (book): If True the median and the median absolute deviation (MAD) are used, instead of the mean and standard deviation.
    """

    def __init__(
        self,
        spark: SparkSession,
        df: DataFrame,
        column_names: list[str],
        k_value: float = 3.0,
        use_median: bool = False,
    ) -> None:
        if len(column_names) == 0:
            raise Exception("You must provide at least one column name")
        if len(column_names) > 1:
            raise NotImplemented("Multiple columns are not supported yet")
        self.column_names = column_names

        self.use_median = use_median
        self.spark = spark
        self.df = df
        self.k_value = k_value

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

    def filter(self) -> DataFrame:
        """
        Filter anomalies based on the k-sigma rule
        """

        column_name = self.column_names[0]
        mean_value, deviation = 0, 0

        if self.use_median:
            mean_value = self.df.approxQuantile(column_name, [0.5], 0.0)[0]
            if mean_value is None:
                raise Exception("Failed to calculate the mean value")

            df_with_deviation = self.df.withColumn(
                "absolute_deviation", abs(col(column_name) - mean_value)
            )
            deviation = df_with_deviation.approxQuantile(
                "absolute_deviation", [0.5], 0.0
            )[0]
            if deviation is None:
                raise Exception("Failed to calculate the deviation value")
        else:
            stats = self.df.select(
                mean(column_name), stddev(self.column_names[0])
            ).first()
            if stats is None:
                raise Exception(
                    "Failed to calculate the mean value and the standard deviation value"
                )

            mean_value = stats[0]
            deviation = stats[1]

        shift = self.k_value * deviation
        lower_bound = mean_value - shift
        upper_bound = mean_value + shift

        return self.df.filter(
            (self.df[column_name] >= lower_bound)
            & (self.df[column_name] <= upper_bound)
        )
