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
from pyspark.sql.functions import (
    from_json,
    col,
    explode,
    when,
    lit,
    coalesce,
    to_timestamp,
)

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.spark import FLEDGE_SCHEMA


class FledgeOPCUAJsonToPCDMTransformer(TransformerInterface):
    """
    Converts a Spark Dataframe column containing a json string created by Fledge to the Process Control Data Model.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.transformers import FledgeOPCUAJsonToPCDMTransformer

    fledge_opcua_json_to_pcdm_transfromer = FledgeOPCUAJsonToPCDMTransformer(
        data=df,
        souce_column_name="body",
        status_null_value="Good",
        change_type_value="insert",
        timestamp_formats=[
            "yyyy-MM-dd'T'HH:mm:ss.SSSX",
            "yyyy-MM-dd'T'HH:mm:ssX",
        ]
    )

    result = fledge_opcua_json_to_pcdm_transfromer.transform()
    ```

    Parameters:
        data (DataFrame): Dataframe containing the column with Json Fledge data
        source_column_name (str): Spark Dataframe column containing the OPC Publisher Json OPC UA data
        status_null_value (str): If populated, will replace 'Good' in the Status column with the specified value.
        change_type_value (optional str): If populated, will replace 'insert' in the ChangeType column with the specified value.
        timestamp_formats (list[str]): Specifies the timestamp formats to be used for converting the timestamp string to a Timestamp Type. For more information on formats, refer to this [documentation.](https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html)
    """

    data: DataFrame
    source_column_name: str
    status_null_value: str
    change_type_value: str
    timestamp_formats: list

    def __init__(
        self,
        data: DataFrame,
        source_column_name: str,
        status_null_value: str = "Good",
        change_type_value: str = "insert",
        timestamp_formats: list = [
            "yyyy-MM-dd'T'HH:mm:ss.SSSX",
            "yyyy-MM-dd'T'HH:mm:ssX",
        ],
    ) -> None:  # NOSONAR
        self.data = data
        self.source_column_name = source_column_name
        self.status_null_value = status_null_value
        self.change_type_value = change_type_value
        self.timestamp_formats = timestamp_formats

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

    def pre_transform_validation(self):
        return True

    def post_transform_validation(self):
        return True

    def transform(self) -> DataFrame:
        """
        Returns:
            DataFrame: A dataframe with the specified column converted to PCDM
        """
        df = (
            self.data.withColumn(
                self.source_column_name,
                from_json(self.source_column_name, FLEDGE_SCHEMA),
            )
            .selectExpr("inline({})".format(self.source_column_name))
            .select(explode("readings"), "timestamp")
            .withColumn(
                "EventTime",
                coalesce(
                    *[to_timestamp(col("timestamp"), f) for f in self.timestamp_formats]
                ),
            )
            .withColumnRenamed("key", "TagName")
            .withColumnRenamed("value", "Value")
            .withColumn("Status", lit(self.status_null_value))
            .withColumn(
                "ValueType",
                when(col("value").cast("float").isNotNull(), "float").when(
                    col("value").cast("float").isNull(), "string"
                ),
            )
            .withColumn("ChangeType", lit(self.change_type_value))
        )

        return df.select(
            "TagName", "EventTime", "Status", "Value", "ValueType", "ChangeType"
        )
