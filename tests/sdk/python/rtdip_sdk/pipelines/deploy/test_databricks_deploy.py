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
import pytest

from src.sdk.python.rtdip_sdk.pipelines.deploy.databricks import DatabricksDBXDeploy
from src.sdk.python.rtdip_sdk.pipelines.deploy.models.databricks import DatabricksCluster, DatabricksJobCluster, DatabricksJobForPipelineJob, DatabricksTaskForPipelineTask

from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.spark_configuration_constants import spark_session
from tests.sdk.python.rtdip_sdk.pipelines._pipeline_utils.pipeline_job_templates import get_spark_pipeline_job

def test_pipeline_job_deploy(mocker: MockerFixture):
    pipeline_job = get_spark_pipeline_job()

    databricks_job_cluster = DatabricksJobCluster(
        job_cluster_key="test_job_cluster", 
        new_cluster=DatabricksCluster(
            spark_version = "11.3.x-scala2.12",
            virtual_cluster_size = "VirtualSmall",
            enable_serverless_compute = True
        )
    )

    databricks_task = DatabricksTaskForPipelineTask(name="test_task", job_cluster_key="test_job_cluster")

    databricks_job = DatabricksJobForPipelineJob(
        job_clusters=[databricks_job_cluster],
        databricks_task_for_pipeline_task_list=[databricks_task]
    )

    databricks_job = DatabricksDBXDeploy(pipeline_job=pipeline_job, databricks_job_for_pipeline_job=databricks_job, host="https://test.databricks.net", token="test_token")

    mocker.patch("src.sdk.python.rtdip_sdk.pipelines.deploy.databricks.dbx_deploy", return_value=None)
    deploy_result = databricks_job.deploy()
    assert deploy_result

    mocker.patch("src.sdk.python.rtdip_sdk.pipelines.deploy.databricks.dbx_launch", return_value=None)
    launch_result = databricks_job.launch()
    assert launch_result
    
def test_pipeline_job_deploy_fails(mocker: MockerFixture):
    pipeline_job = get_spark_pipeline_job()

    databricks_job_cluster = DatabricksJobCluster(
        job_cluster_key="test_job_cluster", 
        new_cluster=DatabricksCluster(
            spark_version = "11.3.x-scala2.12",
            virtual_cluster_size = "VirtualSmall",
            enable_serverless_compute = True
        )
    )

    databricks_task = DatabricksTaskForPipelineTask(name="test_task", job_cluster_key="test_job_cluster")

    databricks_job = DatabricksJobForPipelineJob(
        job_clusters=[databricks_job_cluster],
        databricks_task_for_pipeline_task_list=[databricks_task]
    )

    databricks_job = DatabricksDBXDeploy(pipeline_job=pipeline_job, databricks_job_for_pipeline_job=databricks_job, host="https://test.databricks.net", token="test_token")

    mocker.patch("src.sdk.python.rtdip_sdk.pipelines.deploy.databricks.dbx_deploy", side_effect=Exception)
    with pytest.raises(Exception):
        databricks_job.deploy()

    mocker.patch("src.sdk.python.rtdip_sdk.pipelines.deploy.databricks.dbx_launch", side_effect=Exception)
    with pytest.raises(Exception):
        databricks_job.launch()
