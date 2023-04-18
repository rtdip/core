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
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, lit

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType

class OPCUAToProcessControlDataModel(TransformerInterface):
    '''
    Converts a Spark Dataframe column from a structured OPC UA schema to the RTDIP Process Control Data Model.

    Args:
        source_column_name (str): Spark Dataframe column containing the OPC UA data. Defaults to OPCUA
    '''

    source_column_name: str

    def __init__(self, source_column_name: str = "OPCUA") -> None:
        self.source_column_name = source_column_name

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

        return (df
            .withColumn("TagName", (col("{}.DisplayName".format(self.source_column_name)))) # Will be in payload directly
            .withColumn("EventTime", to_timestamp(regexp_replace(col("{}.Value.SourceTimestamp".format(self.source_column_name)).substr(0, 23), 'T|Z', ''), "yyyy-MM-ddHH:mm:ss.SSS"))
            .withColumn("EventTime", when(col("EventTime").isNull(), to_timestamp(regexp_replace(col("{}.Value.SourceTimestamp".format(self.source_column_name)).substr(0, 20), 'T|Z', ''), "yyyy-MM-ddHH:mm:ss"))
                                    .otherwise(col("EventTime")))
            .withColumn("EventDate", col("EventTime").cast("date"))
            .withColumn("Value", col("{}.Value.Value".format(self.source_column_name)))
            .withColumn("Status", when(col("{}.Value.StatusCode.Symbol".format(self.source_column_name)).isNotNull(), col("{}.Value.StatusCode.Symbol".format(self.source_column_name))).otherwise(lit("Good")))
            .select("EventDate", "TagName", "EventTime", "Status", "Value")
        )