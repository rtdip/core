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

import sys

sys.path.insert(0, ".")
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.mirico_json_to_metadata import (
    MiricoJsonToMetadataTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, FloatType
import pytest
from src.sdk.python.rtdip_sdk._sdk_utils.compare_versions import (
    _package_version_meets_minimum,
)


def test_mirico_json_to_metadata(spark_session: SparkSession):
    mirico_json_data = '{"gasType": "test", "retroLongitude": 123.45, "retroLatitude": 123.45 , "sensorAltitude": 123.45, "sensorLongitude": 123.45, "sensorLatitude": 123.45, "retroName": "test", "siteName": "test", "retroAltitude": 123.45}'
    mirico_df: DataFrame = spark_session.createDataFrame([{"body": mirico_json_data}])

    expected_schema = StructType(
        [
            StructField("TagName", StringType(), False),
            StructField("Description", StringType(), False),
            StructField("UoM", StringType(), False),
            StructField(
                "Properties",
                StructType(
                    [
                        StructField("retroAltitude", FloatType(), True),
                        StructField("retroLongitude", FloatType(), True),
                        StructField("retroLatitude", FloatType(), True),
                        StructField("sensorAltitude", FloatType(), True),
                        StructField("sensorLongitude", FloatType(), True),
                        StructField("sensorLatitude", FloatType(), True),
                    ]
                ),
                False,
            ),
        ]
    )

    expected_data = [
        {
            "TagName": "TEST_TEST_TEST",
            "Description": "",
            "UoM": "",
            "Properties": {
                "retroAltitude": 123.45,
                "retroLongitude": 123.45,
                "retroLatitude": 123.45,
                "sensorAltitude": 123.45,
                "sensorLongitude": 123.45,
                "sensorLatitude": 123.45,
            },
        }
    ]

    expected_df: DataFrame = spark_session.createDataFrame(
        schema=expected_schema, data=expected_data
    )

    try:
        if _package_version_meets_minimum("pyspark", "3.4.0"):
            mirico_json_to_metadata_transformer = MiricoJsonToMetadataTransformer(
                data=mirico_df, source_column_name="body"
            )
            actual_df = mirico_json_to_metadata_transformer.transform()

            assert (
                mirico_json_to_metadata_transformer.system_type() == SystemType.PYSPARK
            )
            assert isinstance(
                mirico_json_to_metadata_transformer.libraries(), Libraries
            )

            assert expected_schema == actual_df.schema
            assert expected_df.collect() == actual_df.collect()
    except:
        with pytest.raises(Exception):
            mirico_json_to_metadata_transformer = MiricoJsonToMetadataTransformer(
                data=mirico_df, source_column_name="body"
            )
