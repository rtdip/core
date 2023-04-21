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
from pyspark.sql.functions import to_timestamp, col, regexp_replace, when, lit, coalesce

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType

class OPCUAToProcessControlDataModel(TransformerInterface):
    '''
    Converts a Spark Dataframe column from a structured OPC UA schema to the RTDIP Process Control Data Model.

    Args:
        source_column_name (str): Spark Dataframe column containing the OPC UA data.
        status_null_value (str): If populated, will replace null values in the Status column with the specified value.
        timestamp_formats (list[str]): Specifies the timestamp formats to be used for converting the timestamp string to a Timestamp Type. For more information on formats, refer to this [documentation.](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)
    '''

    source_column_name: str
    status_null_value: str
    timestamp_formats: list

    def __init__(self, source_column_name: str = "OPCUA", status_null_value: str = None, timestamp_formats: list = ["yyyy-MM-dd'T'HH:mm:ss.SSSX", "yyyy-MM-dd'T'HH:mm:ssX"]) -> None: # NOSONAR
        self.source_column_name = source_column_name
        self.status_null_value = status_null_value
        self.timestamp_formats = timestamp_formats

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

        df = (df
            .withColumn("TagName", (col("{}.DisplayName".format(self.source_column_name)))) # Will be in payload directly
            .withColumn("EventTime", coalesce(*[to_timestamp(col("{}.Value.SourceTimestamp".format(self.source_column_name)), f) for f in self.timestamp_formats]))
            .withColumn("EventDate", col("EventTime").cast("date"))     
            .withColumn("Value", col("{}.Value.Value".format(self.source_column_name)))
        )

        status_col_name = "{}.Value.StatusCode.Symbol".format(self.source_column_name)
        if self.status_null_value != None:
            df = df.withColumn("Status", when(col(status_col_name).isNotNull(), col(status_col_name)).otherwise(lit(self.status_null_value)))
        else:
            df = df.withColumn("Status", col(status_col_name))

        return df.select("EventDate", "TagName", "EventTime", "Status", "Value")