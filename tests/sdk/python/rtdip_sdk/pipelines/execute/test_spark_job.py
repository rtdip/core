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
sys.path.insert(0, '.')
from pytest_mock import MockerFixture
from pyspark.sql import SparkSession
import pytest

from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.constants import EVENTHUB_SCHEMA
from src.sdk.python.rtdip_sdk.pipelines.execute.job import PipelineJobExecute
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.pipeline_job_templates import get_spark_pipeline_job

def test_pipeline_job_execute(spark_session: SparkSession, mocker: MockerFixture):
    pipeline_job = get_spark_pipeline_job()

    expected_df = spark_session.createDataFrame(data=[], schema=EVENTHUB_SCHEMA)
    mocker.patch("src.sdk.python.rtdip_sdk.pipelines.sources.spark.eventhub.SparkEventhubSource.read_batch", return_value=expected_df)

    pipeline = PipelineJobExecute(pipeline_job)

    result = pipeline.run()
    
    assert result

def test_pipeline_job_execute_fails(mocker: MockerFixture):
    pipeline_job = get_spark_pipeline_job()
    pipeline = PipelineJobExecute(pipeline_job)

    mocker.patch("src.sdk.python.rtdip_sdk.pipelines.sources.spark.eventhub.SparkEventhubSource.read_batch", side_effect=Exception)


    with pytest.raises(Exception):
        pipeline.run()