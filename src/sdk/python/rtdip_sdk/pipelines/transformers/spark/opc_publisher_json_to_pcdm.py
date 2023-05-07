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
from pyspark.sql.functions import from_json, col, explode, to_timestamp, when, lit, coalesce
from pyspark.sql.types import ArrayType, StringType

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.spark import OPC_PUBLISHER_SCHEMA

class OPCPublisherJsonToPCDMTransformer(TransformerInterface):
    '''
    Converts a Spark Dataframe column containing a json string created by OPC Publisher to the Process Control Data Model

    Args:
        data (DataFrame): Dataframe containing the column with Json OPC UA data
        source_column_name (str): Spark Dataframe column containing the OPC Publisher Json OPC UA data
        multiple_rows_per_message (bool): Each Dataframe Row contains an array of/multiple OPC UA messages. The list of Json will be exploded into rows in the Dataframe.
        status_null_value (str): If populated, will replace null values in the Status column with the specified value.
        timestamp_formats (list[str]): Specifies the timestamp formats to be used for converting the timestamp string to a Timestamp Type. For more information on formats, refer to this [documentation.](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)

    '''
    data: DataFrame
    source_column_name: str
    multiple_rows_per_message: bool
    status_null_value: str
    timestamp_formats: list

    def __init__(self, data: DataFrame, source_column_name: str, multiple_rows_per_message: bool = True, status_null_value: str = None, timestamp_formats: list = ["yyyy-MM-dd'T'HH:mm:ss.SSSX", "yyyy-MM-dd'T'HH:mm:ssX"]) -> None: # NOSONAR
        self.data = data
        self.source_column_name = source_column_name
        self.multiple_rows_per_message = multiple_rows_per_message
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

    def transform(self) -> DataFrame:
        '''
        Returns:
            DataFrame: A dataframe with the specified column converted to PCDM
        '''
        if self.multiple_rows_per_message:
            df = (self.data
                .withColumn(self.source_column_name, from_json(col(self.source_column_name), ArrayType(StringType())))
                .withColumn(self.source_column_name, explode(self.source_column_name))
            )
        else:
            df = (self.data
                .withColumn(self.source_column_name, from_json(col(self.source_column_name), StringType()))
            )

        df = (df
            .withColumn("OPCUA", from_json(col(self.source_column_name), OPC_PUBLISHER_SCHEMA))
            .withColumn("TagName", (col("OPCUA.DisplayName")))
            .withColumn("EventTime", coalesce(*[to_timestamp(col("OPCUA.Value.SourceTimestamp"), f) for f in self.timestamp_formats]))   
            .withColumn("Value", col("OPCUA.Value.Value"))
            .withColumn("DataType", when(col("Value").cast("float").isNotNull(), "float")
                                    .when(col("Value").cast("float").isNull(), "string")
                                    .otherwise("unknown"))                                  
        )

        status_col_name = "OPCUA.Value.StatusCode.Symbol"
        if self.status_null_value != None:
            df = df.withColumn("Status", when(col(status_col_name).isNotNull(), col(status_col_name)).otherwise(lit(self.status_null_value)))
        else:
            df = df.withColumn("Status", col(status_col_name))

        return df.select("TagName", "EventTime", "Status", "Value", "DataType")    