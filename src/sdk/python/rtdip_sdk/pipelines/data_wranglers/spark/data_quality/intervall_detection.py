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
from ...._pipeline_utils.models import Libraries, SystemType
from ...interfaces import WranglerBaseInterface

class IntervallDetection(WranglerBaseInterface):
    """
       The Intervall Detection cleanses a PySpark DataFrame from entries specified by a passed interval.



       Parameters:
           spark (SparkSession): A SparkSession object.
           df (DataFrame): PySpark DataFrame to be converted
           interval (float): The interval to be used for the detection in seconds

       """


    def __init__(self, spark: SparkSession,  df: DataFrame, interval: float) -> None:
        self.spark = spark
        self.df = df
        self.interval = interval

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
        Filters the DataFrame based on the interval
        """
        return self.df.filter(f"timestamp % {self.interval} == 0")




