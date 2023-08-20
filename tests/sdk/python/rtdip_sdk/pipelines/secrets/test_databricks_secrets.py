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
from pytest_mock import MockerFixture
from pyspark.sql import SparkSession

from src.sdk.python.rtdip_sdk.pipelines.secrets.models import PipelineSecret
from src.sdk.python.rtdip_sdk.pipelines.secrets.databricks import DatabricksSecrets
from src.sdk.python.rtdip_sdk.pipelines.execute.job import PipelineJobExecute
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark import EVENTHUB_SCHEMA

from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.pipeline_job_templates import (
    get_spark_pipeline_job,
)
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import (
    DBUtilsSecretsFixture,
)


def test_databricks_secret_scopes(spark_session: SparkSession, mocker: MockerFixture):
    pipeline_job = get_spark_pipeline_job()

    pipeline_job.task_list[0].step_list[0].component_parameters["options"][
        "eventhubs.connectionString"
    ] = PipelineSecret(type=DatabricksSecrets, vault="test_vault", key="test_key")

    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.secrets.databricks.get_dbutils",
        return_value=DBUtilsSecretsFixture(
            secret_value="Endpoint=sb://test.servicebus.windows.net/;SharedAccessKeyName=test;SharedAccessKey=test;EntityPath=test"
        ),
    )

    expected_df = spark_session.createDataFrame(data=[], schema=EVENTHUB_SCHEMA)
    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.sources.spark.eventhub.SparkEventhubSource.read_batch",
        return_value=expected_df,
    )

    pipeline = PipelineJobExecute(pipeline_job)

    result = pipeline.run()

    assert result
