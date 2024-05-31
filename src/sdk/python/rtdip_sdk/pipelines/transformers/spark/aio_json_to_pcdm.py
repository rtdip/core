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
from pyspark.sql.functions import from_json, col, explode, when, lit, expr

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.spark import AIO_SCHEMA


class AIOJsonToPCDMTransformer(TransformerInterface):
    """
    Converts a Spark Dataframe column containing a json string created by AIO to the Process Control Data Model.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.transformers import AIOJsonToPCDMTransformer

    aio_json_to_pcdm_transfromer = AIOJsonToPCDMTransformer(
        data=df,
        souce_column_name="body",
        status_null_value="Good",
        change_type_value="insert"
    )

    result = aio_json_to_pcdm_transfromer.transform()
    ```

    Parameters:
        data (DataFrame): Dataframe containing the column with Json AIO data
        source_column_name (str): Spark Dataframe column containing the Json AIO data
        status_null_value (str): If populated, will replace 'Good' in the Status column with the specified value.
        change_type_value (optional str): If populated, will replace 'insert' in the ChangeType column with the specified value.
    """

    data: DataFrame
    source_column_name: str
    status_null_value: str
    change_type_value: str

    def __init__(
        self,
        data: DataFrame,
        source_column_name: str,
        status_null_value: str = "Good",
        change_type_value: str = "insert",
    ) -> None:  # NOSONAR
        self.data = data
        self.source_column_name = source_column_name
        self.status_null_value = status_null_value
        self.change_type_value = change_type_value

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
            self.data.select(
                from_json(col(self.source_column_name), "Payload STRING").alias("body")
            )
            .select(from_json(expr("body.Payload"), AIO_SCHEMA).alias("body"))
            .select(explode("body"))
            .select(col("key").alias("TagName"), "value.*")
            .select(col("SourceTimestamp").alias("EventTime"), "TagName", "Value")
            .withColumn("Status", lit(self.status_null_value))
            .withColumn(
                "ValueType",
                when(col("Value").cast("float").isNotNull(), "float").otherwise(
                    "string"
                ),
            )
            .withColumn("ChangeType", lit(self.change_type_value))
        )

        return df.select(
            "EventTime", "TagName", "Status", "Value", "ValueType", "ChangeType"
        )
