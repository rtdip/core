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
from pyspark.sql.functions import mean, stddev, median, abs, col
from ...interfaces import MonitoringBaseInterface
from ...._pipeline_utils.models import Libraries, SystemType


class KSigmaAnomalyDetection(MonitoringBaseInterface):
    """
    Anomaly detection with the k-sigma method. This method either computes the mean and standard deviation, or the median and the median absolute deviation (MAD) of the data.
    The k-sigma method then filters out all data points that are k times the standard deviation away from the mean, or k times the MAD away from the median.
    Assuming a normal distribution, this method keeps around 99.7% of the data points when k=3 and use_median=False.
    """

    def __init__(
        self,
        spark: SparkSession,
        df: DataFrame,
        column_names: list[str],
        k_value: int = 3,
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

    def filter_anomalies(self) -> DataFrame:
        """
        Filter outliers based on the k-sigma rule
        """

        column_name = self.column_names[0]
        mean_value, deviation = 0, 0

        if mean_value is None:
            raise Exception("Couldn't calculate mean value")

        if self.use_median:
            mean_value = self.df.select(median(column_name)).first()
            if mean_value is None:
                raise Exception("Couldn't calculate median value")
            mean_value = mean_value[0]

            deviation = self.df.agg(median(abs(col(column_name) - mean_value))).first()
            if deviation is None:
                raise Exception("Couldn't calculate mean value")
            deviation = deviation[0]
        else:
            stats = self.df.select(
                mean(column_name), stddev(self.column_names[0])
            ).first()
            if stats is None:
                raise Exception("Couldn't calculate mean value and standard deviation")

            mean_value = stats[0]
            deviation = stats[1]

        shift = self.k_value * deviation
        lower_bound = mean_value - shift
        upper_bound = mean_value + shift

        return self.df.filter(
            (self.df[column_name] >= lower_bound)
            & (self.df[column_name] <= upper_bound)
        )
