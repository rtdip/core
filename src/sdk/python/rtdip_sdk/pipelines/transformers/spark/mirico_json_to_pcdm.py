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
    lit,
    udf,
    map_from_arrays,
    map_keys,
    map_values,
    concat_ws,
    to_timestamp,
    upper,
    when,
)
from ...._sdk_utils.compare_versions import (
    _package_version_meets_minimum,
)
from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils import mirico_field_mappings


class MiricoJsonToPCDMTransformer(TransformerInterface):
    """
    Converts a Spark Dataframe column containing a json string created from Mirico to the Process Control Data Model.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.transformers import MiricoJsonToPCDMTransformer

    mirico_json_to_pcdm_transformer = MiricoJsonToPCDMTransformer(
        data=df
        source_column_name="body",
        status_null_value="Good",
        change_type_value="insert"
        tagname_field="test"
    )

    result = mirico_json_to_pcdm_transformer.transform()
    ```

    Parameters:
        data (DataFrame): Dataframe containing the column with Mirico data
        source_column_name (str): Spark Dataframe column containing the Json Mirico data
        status_null_value (optional str): If populated, will replace 'Good' in the Status column with the specified value.
        change_type_value (optional str): If populated, will replace 'insert' in the ChangeType column with the specified value.
        tagname_field (optional str): If populated, will add the specified field to the TagName column.
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
        tagname_field: str = None,
    ) -> None:
        _package_version_meets_minimum("pyspark", "3.4.0")
        self.data = data
        self.source_column_name = source_column_name
        self.status_null_value = status_null_value
        self.change_type_value = change_type_value
        self.tagname_field = tagname_field

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

        mapping = mirico_field_mappings.MIRICO_FIELD_MAPPINGS
        df = (
            self.data.withColumn(
                self.source_column_name,
                from_json(self.source_column_name, "map<string,string>"),
            )
            .withColumn("TagName", map_keys("body"))
            .withColumn("Value", map_values("body"))
            .select(
                map_from_arrays("TagName", "Value").alias("x"),
                to_timestamp(col("x.timeStamp")).alias("EventTime"),
                col("x.siteName").alias("siteName"),
                col("x.gasType").alias("gasType"),
                col("x.retroName").alias("retroName"),
            )
            .select("EventTime", "siteName", "gasType", "retroName", posexplode("x"))
            .withColumn(
                "ValueType", udf(lambda row: mapping[row]["ValueType"])(col("pos"))
            )
            .withColumn("Status", lit("Good"))
            .withColumn("ChangeType", lit("insert"))
            .withColumn(
                "TagName",
                when(
                    lit(self.tagname_field).isNotNull(),
                    concat_ws(
                        ":",
                        *[
                            upper(lit(self.tagname_field)),
                            concat_ws(
                                "_",
                                *[
                                    upper(col("siteName")),
                                    upper(col("retroName")),
                                    when(
                                        upper(col("key")) == "GASPPM",
                                        concat_ws(
                                            "_",
                                            *[upper(col("key")), upper(col("gasType"))]
                                        ),
                                    ).otherwise(upper(col("key"))),
                                ]
                            ),
                        ]
                    ),
                ).otherwise(
                    concat_ws(
                        "_",
                        *[
                            upper(col("siteName")),
                            upper(col("retroName")),
                            when(
                                upper(col("key")) == "GASPPM",
                                concat_ws(
                                    "_", *[upper(col("key")), upper(col("gasType"))]
                                ),
                            ).otherwise(upper(col("key"))),
                        ]
                    )
                ),
            )
            .filter(
                ~col("key").isin(
                    "timeStamp",
                    "gasType",
                    "retroLongitude",
                    "retroLatitude",
                    "retroAltitude",
                    "sensorLongitude",
                    "sensorLatitude",
                    "sensorAltitude",
                    "siteName",
                    "siteKey",
                    "retroName",
                )
            )
        )
        return df.select(
            "EventTime", "TagName", "Status", "Value", "ValueType", "ChangeType"
        )
