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
from pandas import DataFrame

from rtdip_sdk.pipelines._pipeline_utils.models import SystemType, Libraries
from ...interfaces import WranglerBaseInterface

class IntervallDetection(WranglerBaseInterface):
    """
       The Intervall Detection cleanses a PySpark DataFrame from entries



       Parameters:
           spark (SparkSession): A SparkSession object.
           df (DataFrame): Dataframe containing the raw data.
           column_names (list[str]): The names of the columns to be filtered (currently only one column is supported).
           k_value (float): The number of deviations to build the threshold.
           use_median (book): If True the median and the median absolute deviation (MAD) are used, instead of the mean and standard deviation.
       """
    df: DataFrame


    def __init__(self, df: DataFrame) -> None:
        self.df = df

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




