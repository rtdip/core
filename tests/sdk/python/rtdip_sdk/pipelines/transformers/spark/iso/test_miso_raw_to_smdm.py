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
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.iso import SMDM_USAGE_SCHEMA, MISO_SCHEMA
from src.sdk.python.rtdip_sdk.pipelines.transformers import MISORawToSMDMTransformer
from src.sdk.python.rtdip_sdk.pipelines.transformers.spark.binary_to_string import BinaryToStringTransformer
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import Libraries, SystemType
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session
from pyspark.sql import SparkSession, DataFrame


base_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "test_data", "miso_raw_to_smdm")


def test_miso_raw_to_smdm_transformation(spark_session: SparkSession):
    expected_df: DataFrame = spark_session.read.csv(f"{base_path}/output.csv", header=True, schema=SMDM_USAGE_SCHEMA)
    input_df: DataFrame = spark_session.read.csv(f"{base_path}/input.csv", header=True, schema=MISO_SCHEMA)

    transformer = MISORawToSMDMTransformer(spark_session, input_df)
    actual_df = transformer.transform()

    actual_df = actual_df.orderBy("uid", "timestamp")
    expected_df = expected_df.orderBy("uid", "timestamp")

    assert transformer.system_type() == SystemType.PYSPARK
    assert isinstance(transformer.libraries(), Libraries)
    assert transformer.settings() == dict()
    assert transformer.pre_transform_validation()
    assert transformer.post_transform_validation()
    assert str(actual_df.schema) == str(expected_df.schema)
    assert str(actual_df.collect()) == str(expected_df.collect())
