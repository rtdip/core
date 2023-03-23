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

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType

class EventhubBodyBinaryToString(TransformerInterface):
    '''
    Converts the Eventhub dataframe body column from a binary to a string.
    ''' 
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

    def transform(self, df: DataFrame) -> DataFrame:
        '''
        Args:
            df (DataFrame): A dataframe based on the structure of the Spark Eventhub connector.

        Returns:
            DataFrame: A dataframe with the body column converted to string.
        '''
        return (
            df
            .withColumn("body", df["body"].cast("string"))
        )