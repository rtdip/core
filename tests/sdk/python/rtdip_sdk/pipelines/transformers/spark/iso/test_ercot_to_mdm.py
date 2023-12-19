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

parent_base_path: str = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "test_data"
)


def ercot_to_mdm_test(
    spark_session: SparkSession,
    output_type: str,
    expected_df: DataFrame,
    base_path: str,
):
    input_df: DataFrame = spark_session.read.csv(
        f"{base_path}/input.csv", header=True, schema=ERCOT_SCHEMA
    )

    transformer = ERCOTToMDMTransformer(
        spark_session, input_df, output_type=output_type
    )
    assert transformer.system_type() == SystemType.PYSPARK
    assert isinstance(transformer.libraries(), Libraries)
    assert transformer.settings() == dict()

    actual_df = transformer.transform()

    cols = list(
        filter(
            lambda column: column in expected_df.columns,
            ["Uid", "Timestamp", "TimestampStart"],
        )
    )
    assert actual_df.orderBy(cols).collect() == expected_df.orderBy(cols).collect()


def test_ercot_to_mdm_usage(spark_session: SparkSession):
    usage_path = os.path.join(parent_base_path, "ercot_usage")
    usage_expected_df: DataFrame = spark_session.read.csv(
        f"{usage_path}/output.csv", header=True, schema=MDM_USAGE_SCHEMA
    )

    ercot_to_mdm_test(spark_session, "usage", usage_expected_df, usage_path)


def test_ercot_to_mdm_meta(spark_session: SparkSession):
    meta_path: str = os.path.join(parent_base_path, "ercot_meta")

    meta_expected_df: DataFrame = spark_session.read.json(
        f"{meta_path}/output.json", schema=MDM_META_SCHEMA
    )

    ercot_to_mdm_test(spark_session, "meta", meta_expected_df, meta_path)
