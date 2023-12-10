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

import os

from pyspark.sql import SparkSession, DataFrame

from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.iso import ERCOT_SCHEMA
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.mdm import (
    MDM_USAGE_SCHEMA,
    MDM_META_SCHEMA,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from src.sdk.python.rtdip_sdk.pipelines.transformers import ERCOTToMDMTransformer
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.base_raw_to_mdm import (
    BaseRawToMDMTransformer,
)

parent_base_path: str = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "test_data"
)


def compare_dataframes(transformer, actual_df, expected_df, order_cols):
    actual_df = actual_df.orderBy(*order_cols)
    expected_df = expected_df.orderBy(*order_cols)

    assert transformer.system_type() == SystemType.PYSPARK
    assert isinstance(transformer.libraries(), Libraries)
    assert transformer.settings() == dict()
    assert str(actual_df.schema) == str(expected_df.schema)
    assert str(actual_df.collect()) == str(expected_df.collect())


def ercot_to_mdm_test(
    spark_session: SparkSession,
    output_type: str,
    order_cols: list,
    expected_df: DataFrame,
    base_path: str,
):
    input_df: DataFrame = spark_session.read.csv(
        f"{base_path}/input.csv", header=True, schema=ERCOT_SCHEMA
    )

    transformer: BaseRawToMDMTransformer = ERCOTToMDMTransformer(
        spark_session, input_df, output_type=output_type
    )
    actual_df = transformer.transform()

    compare_dataframes(transformer, actual_df, expected_df, order_cols)


def test_ercot_to_mdm_usage(spark_session: SparkSession):
    base_path: str = os.path.join(parent_base_path, "ercot_usage")

    expected_df: DataFrame = spark_session.read.csv(
        f"{base_path}/output.csv", header=True, schema=MDM_USAGE_SCHEMA
    )

    ercot_to_mdm_test(
        spark_session=spark_session,
        output_type="usage",
        order_cols=["Uid", "Timestamp"],
        expected_df=expected_df,
        base_path=base_path,
    )


def test_ercot_to_mdm_meta(spark_session: SparkSession):
    base_path: str = os.path.join(parent_base_path, "ercot_meta")

    expected_df: DataFrame = spark_session.read.json(
        f"{base_path}/output.json", schema=MDM_META_SCHEMA
    )

    ercot_to_mdm_test(
        spark_session=spark_session,
        output_type="meta",
        order_cols=["Uid", "TimestampStart"],
        expected_df=expected_df,
        base_path=base_path,
    )
