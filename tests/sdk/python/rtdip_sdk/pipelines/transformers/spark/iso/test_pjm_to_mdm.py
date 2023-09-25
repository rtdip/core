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
from pyspark.sql.functions import expr, lit

from src.sdk.python.rtdip_sdk.data_models.timeseries import ValueType
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.base_raw_to_mdm import (
    BaseRawToMDMTransformer,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.mdm import (
    MDM_USAGE_SCHEMA,
    MDM_META_SCHEMA,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.iso import PJM_SCHEMA
from src.sdk.python.rtdip_sdk.pipelines.transformers import PJMToMDMTransformer
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from pyspark.sql import SparkSession, DataFrame

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


def test_pjm_to_mdm_usage(spark_session: SparkSession):
    base_path: str = os.path.join(parent_base_path, "pjm_usage")

    expected_df: DataFrame = spark_session.read.csv(
        f"{base_path}/output.csv", header=True, schema=MDM_USAGE_SCHEMA
    )
    input_df: DataFrame = spark_session.read.csv(
        f"{base_path}/input.csv", header=True, schema=PJM_SCHEMA
    )

    transformer: BaseRawToMDMTransformer = PJMToMDMTransformer(
        spark_session, input_df, output_type="usage"
    )
    actual_df = transformer.transform()

    order_cols = ["Uid", "Timestamp"]
    compare_dataframes(transformer, actual_df, expected_df, order_cols)


def test_pjm_to_mdm_meta(spark_session: SparkSession):
    base_path: str = os.path.join(parent_base_path, "pjm_meta")

    expected_df: DataFrame = spark_session.read.json(
        f"{base_path}/output.json", schema=MDM_META_SCHEMA
    )
    input_df: DataFrame = spark_session.read.csv(
        f"{base_path}/input.csv", header=True, schema=PJM_SCHEMA
    )

    transformer: BaseRawToMDMTransformer = PJMToMDMTransformer(
        spark_session, input_df, output_type="meta"
    )

    actual_df = transformer.transform()

    order_cols = ["Uid", "TimestampStart"]
    compare_dataframes(transformer, actual_df, expected_df, order_cols)


def test_pjm_to_mdm_meta_with_params(spark_session: SparkSession):
    base_path: str = os.path.join(parent_base_path, "pjm_meta")

    expected_df: DataFrame = spark_session.read.json(
        f"{base_path}/output.json", schema=MDM_META_SCHEMA
    )
    input_df: DataFrame = spark_session.read.csv(
        f"{base_path}/input.csv", header=True, schema=PJM_SCHEMA
    )

    version = "'v2'"
    series_id = "'new_series'"
    series_parent_id = "'new_parent'"
    name = "'New Data'"
    description = "'Pulled from API'"
    value_type = ValueType.Usage

    transformer: BaseRawToMDMTransformer = PJMToMDMTransformer(
        spark_session,
        input_df,
        output_type="meta",
        version=version,
        series_id=series_id,
        series_parent_id=series_parent_id,
        name=name,
        description=description,
        value_type=value_type,
    )

    expected_df = (
        expected_df.withColumn("Version", expr(version))
        .withColumn("SeriesId", expr(series_id))
        .withColumn("SeriesParentId", expr(series_parent_id))
        .withColumn("Name", expr(name))
        .withColumn("Description", expr(description))
        .withColumn("ValueType", lit(value_type.value))
    )

    expected_df = spark_session.createDataFrame(expected_df.rdd, schema=MDM_META_SCHEMA)

    actual_df = transformer.transform()

    order_cols = ["Uid", "TimestampStart"]
    compare_dataframes(transformer, actual_df, expected_df, order_cols)
