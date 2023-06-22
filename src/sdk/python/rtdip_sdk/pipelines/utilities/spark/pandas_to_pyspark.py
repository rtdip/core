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

import logging
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError
from pandas.core.frame import DataFrame

from ..interfaces import UtilitiesInterface
from .configuration import SparkConfigurationUtility
from ..._pipeline_utils.models import Libraries, SystemType

class PandasToPySparkDFUtility(UtilitiesInterface):
    '''
    Converts a Pandas DataFrame to a PySpark DataFrame

    Args:
        spark (SparkSession): Spark Session required to convert DataFrame
        df (DataFrame): Pandas DataFrame to be converted
    ''' 
    spark: SparkSession
    df: DataFrame

    def __init__(self, spark: SparkSession, df: DataFrame) -> None:
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

    def execute(self) -> bool:
        try:
            df = self.spark.createDataFrame(self.df)
            return df
        
        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e