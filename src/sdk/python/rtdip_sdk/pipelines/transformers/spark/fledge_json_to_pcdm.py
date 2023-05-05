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
from pyspark.sql.functions import from_json, col, explode, when, lit

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.spark import FLEDGE_SCHEMA

class FledgeJsonToPCDMTransformer(TransformerInterface):
    '''
    Converts a Spark Dataframe column containing a json string created by Fledge to the Process Control Data Model

    Args:
        data (DataFrame): Dataframe containing the column with Json Fledge data
        status_null_value (str): If populated, will replace 'Good' in the Status column with the specified value.
    '''
    data: DataFrame
    status_null_value: str

    def __init__(self, data: DataFrame, status_null_value: str = "Good") -> None: # NOSONAR
        self.data = data
        self.status_null_value = status_null_value

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
    
    def transform(self) -> DataFrame:
        '''
        Returns:
            DataFrame: A dataframe with the specified column converted to OPC UA
        '''
        df = (self.data
              .withColumn("body", from_json("body", FLEDGE_SCHEMA))
              .selectExpr("inline(body)").select(explode("readings"), "timestamp")
              .withColumnRenamed("timestamp", "EventTime")
              .withColumnRenamed("key", "TagName")
              .withColumnRenamed("value", "Value")
              .withColumn("Status", lit(self.status_null_value))
              .withColumn("DataType", when(col("value").cast("float").isNotNull(), "float").when(col("value").cast("float").isNull(), "string")))
        return df
