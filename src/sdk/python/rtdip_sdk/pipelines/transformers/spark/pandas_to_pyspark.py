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

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as PySparkDataFrame
from pandas import DataFrame as PandasDataFrame

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType

class PandasToPySparkTransformer(TransformerInterface):
    '''
    Converts a Pandas DataFrame to a PySpark DataFrame.

    Args:
        spark (SparkSession): Spark Session required to convert DataFrame
        df (DataFrame): Pandas DataFrame to be converted
    ''' 
    spark: SparkSession
    df: PandasDataFrame

    def __init__(self, spark: SparkSession, df: PandasDataFrame) -> None:
        self.spark = spark
        self.df = df

    @staticmethod
    def system_type():
        '''
        Attributes:
            SystemType (Environment): Requires PYSPARK
        '''            
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries
    
    @staticmethod
    def settings() -> dict:
        return {}
    
    def pre_transform_validation(self):
        return True
    
    def post_transform_validation(self):
        return True

    def transform(self) -> PySparkDataFrame:
        '''
        Returns:
            DataFrame: A PySpark dataframe converted from a Pandas DataFrame.
        '''
        df = self.spark.createDataFrame(self.df)
        return df