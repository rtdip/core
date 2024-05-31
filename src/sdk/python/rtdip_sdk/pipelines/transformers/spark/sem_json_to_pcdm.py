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
import logging
from pyspark.sql.functions import (
    from_json,
    col,
    posexplode,
    regexp_replace,
    to_timestamp,
    concat,
    lit,
    udf,
    map_from_arrays,
    split,
    expr,
)
from ...._sdk_utils.compare_versions import (
    _package_version_meets_minimum,
)
from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.spark import SEM_SCHEMA
from ..._pipeline_utils import obc_field_mappings


class SEMJsonToPCDMTransformer(TransformerInterface):
    """
    Converts a Spark Dataframe column containing a json string created by SEM to the Process Control Data Model.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.transformers import SEMJsonToPCDMTransformer

    sem_json_to_pcdm_transformer = SEMJsonToPCDMTransformer(
        data=df
        source_column_name="body",
        version=10,
        status_null_value="Good",
        change_type_value="insert"
    )

    result = sem_json_to_pcdm_transformer.transform()
    ```

    Parameters:
        data (DataFrame): Dataframe containing the column with SEM data
        source_column_name (str): Spark Dataframe column containing the Json SEM data
        version (int): The version for the OBC field mappings. The latest version is 10.
        status_null_value (optional str): If populated, will replace 'Good' in the Status column with the specified value.
        change_type_value (optional str): If populated, will replace 'insert' in the ChangeType column with the specified value.
    """

    data: DataFrame
    source_column_name: str
    version: int
    status_null_value: str
    change_type_value: str

    def __init__(
        self,
        data: DataFrame,
        source_column_name: str,
        version: int,
        status_null_value: str = "Good",
        change_type_value: str = "insert",
    ) -> None:
        _package_version_meets_minimum("pyspark", "3.4.0")
        self.data = data
        self.source_column_name = source_column_name
        self.version = version
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
        if self.version == 10:
            mapping = obc_field_mappings.OBC_FIELD_MAPPINGS_V10
            df = (
                self.data.withColumn(
                    self.source_column_name,
                    from_json(self.source_column_name, SEM_SCHEMA),
                )
                .select(self.source_column_name + ".readings")
                .melt(
                    ids=["readings.resourceName"],
                    values=["readings.value"],
                    variableColumnName="var",
                    valueColumnName="value",
                )
                .drop("var")
                .select(map_from_arrays("resourceName", "value").alias("resourceName"))
                .select("resourceName.dID", "resourceName.d", "resourceName.t")
                .select(
                    regexp_replace(col("t").cast("string"), "(\d{10})(\d+)", "$1.$2")
                    .cast("double")
                    .alias("timestamp"),
                    "dID",
                    posexplode(split(expr("substring(d, 2, length(d)-2)"), ",")),
                )
                .select(
                    to_timestamp("timestamp").alias("EventTime"),
                    col("dID"),
                    col("pos").cast("string"),
                    col("col").alias("Value"),
                )
                .withColumn(
                    "TagName",
                    concat(
                        col("dID"),
                        lit(":"),
                        udf(lambda row: mapping[row]["TagName"])(col("pos")),
                    ),
                )
                .withColumn(
                    "ValueType", udf(lambda row: mapping[row]["ValueType"])(col("pos"))
                )
                .withColumn("Status", lit(self.status_null_value))
                .withColumn("ChangeType", lit(self.change_type_value))
            )
            return df.select(
                "EventTime", "TagName", "Status", "Value", "ValueType", "ChangeType"
            )
        else:
            return logging.exception(
                "The wrong version was specified. Please use the latest version"
            )
