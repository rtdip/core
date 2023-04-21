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
from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import ArrayType, StringType

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.spark import OPCUA_SCHEMA

class JsonToOPCUA(TransformerInterface):
    '''
    Converts a Spark Dataframe column from a json string to OPC UA.

    Args:
        source_column_name (str): Spark Dataframe column containing the Json OPC UA data
        target_column_name (str): Column name to be used for the structyred OPC UA data.
        multiple_rows_per_message (bool): Each Dataframe Row contains an array of/multiple OPC UA messages. The list of Json will be exploded into rows in the Dataframe.
    '''

    source_column_name: str
    target_column_name: str
    multiple_rows_per_message: bool

    def __init__(self, source_column_name: str, target_column_name: str = "OPCUA", multiple_rows_per_message: bool = True) -> None:
        self.source_column_name = source_column_name
        self.target_column_name = target_column_name
        self.multiple_rows_per_message = multiple_rows_per_message

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
            df (DataFrame): A dataframe containing the column with Json OPC UA data

        Returns:
            DataFrame: A dataframe with the specified column converted to OPC UA
        '''
        if self.multiple_rows_per_message:
            df = (df
                  .withColumn(self.source_column_name, from_json(col(self.source_column_name), ArrayType(StringType())))
                  .withColumn(self.source_column_name, explode(self.source_column_name))
            )

        return df.withColumn(self.target_column_name, from_json(col(self.source_column_name), OPCUA_SCHEMA))