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
import pytest

from src.sdk.python.rtdip_sdk.pipelines.deploy import (
    DatabricksSDKDeploy,
    CreateJob,
    JobCluster,
    ClusterSpec,
    Task,
    NotebookTask,
    AutoScale,
    RuntimeEngine,
    DataSecurityMode,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    PyPiLibrary,
    MavenLibrary,
    PythonWheelLibrary,
)


class DummyModule:
    def __init__(self):
        return None

    def __name__(self):
        return "test_module"


class DummyJob:
    jobs = []

    def __init__(
        self,
    ):
        return None

    def create(self, **kwargs):
        return None

    def reset(self, job_id=None, new_settings=None):
        return None

    def run_now(self, job_id=None):
        return None

    def cancel_all_runs(self, job_id=None):
        return None

    def list(self, name=None, job_id=None):
        return [self]

    job_id = 1


class DummyWorkspace:
    def __init__(self):
        return None

    def mkdirs(self, path=None):
        return None

    def upload(self, path=None, overwrite=True, content=None):
        return None


class DummyWorkspaceClient:
    def __init__(self):
        return None

    workspace = DummyWorkspace()

    jobs = DummyJob()


class DummyConfig:
    def __init__(self, product=None):
        return None


default_version = "0.0.0rc0"
default_list_package = "databricks.sdk.service.jobs.JobsAPI.list"


def test_pipeline_job_deploy(mocker: MockerFixture):
    cluster_list = []
    cluster_list.append(
        JobCluster(
            job_cluster_key="test_cluster",
            new_cluster=ClusterSpec(
                node_type_id="Standard_E4ds_v5",
                autoscale=AutoScale(min_workers=1, max_workers=3),
                spark_version="13.2.x-scala2.12",
                data_security_mode=DataSecurityMode.SINGLE_USER,
                runtime_engine=RuntimeEngine.PHOTON,
            ),
        )
    )

    task_list = []
    task_list.append(
        Task(
            task_key="test_task",
            job_cluster_key="test_cluster",
            notebook_task=NotebookTask(notebook_path="/directory/to/pipeline.py"),
        )
    )

    job = CreateJob(name="test_job_rtdip", job_clusters=cluster_list, tasks=task_list)

    databricks_job = DatabricksSDKDeploy(
        databricks_job=job, host="https://test.databricks.net", token="test_token"
    )

    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.deploy.databricks.DatabricksSDKDeploy._load_module",
        return_value=DummyModule(),
    )
    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.deploy.databricks.DatabricksSDKDeploy._convert_file_to_binary",
        return_value=None,
    )
    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.deploy.databricks.WorkspaceClient",
        return_value=DummyWorkspaceClient(),
    )
    # mocker.patch(
    #     "databricks.sdk.mixins.workspace.WorkspaceExt.mkdirs", return_value=None
    # )
    # mocker.patch(
    #     "databricks.sdk.mixins.workspace.WorkspaceExt.upload", return_value=None
    # )
    libraries = Libraries(
        pypi_libraries=[PyPiLibrary(name="rtdip-sdk", version=default_version)],
        maven_libraries=[
            MavenLibrary(
                group_id="rtdip", artifact_id="rtdip-sdk", version=default_version
            )
        ],
        python_wheel_libraries=[PythonWheelLibrary(path="test_wheel.whl")],
    )
    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.utilities.pipeline_components.PipelineComponentsGetUtility.execute",
        return_value=(libraries, {"config": "test_config"}),
    )
    mocker.patch(default_list_package, return_value=[])
    mocker.patch("databricks.sdk.service.jobs.JobsAPI.create", return_value=None)
    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.deploy.databricks.Config",
        return_value=DummyConfig(),
    )
    deploy_result = databricks_job.deploy()
    assert deploy_result

    mocker.patch(default_list_package, return_value=[DummyJob()])
    mocker.patch("databricks.sdk.service.jobs.JobsAPI.run_now", return_value=None)
    launch_result = databricks_job.launch()
    assert launch_result

    mocker.patch(default_list_package, return_value=[DummyJob()])
    mocker.patch(
        "databricks.sdk.service.jobs.JobsAPI.cancel_all_runs", return_value=None
    )
    launch_result = databricks_job.stop()
    assert launch_result


def test_pipeline_job_deploy_fails(mocker: MockerFixture):
    cluster_list = []
    cluster_list.append(
        JobCluster(
            job_cluster_key="test_cluster",
            new_cluster=ClusterSpec(
                node_type_id="Standard_E4ds_v5",
                autoscale=AutoScale(min_workers=1, max_workers=3),
                spark_version="13.2.x-scala2.12",
                data_security_mode=DataSecurityMode.SINGLE_USER,
                runtime_engine=RuntimeEngine.PHOTON,
            ),
        )
    )

    task_list = []
    task_list.append(
        Task(
            task_key="test_task",
            new_cluster=cluster_list[0].new_cluster,
            notebook_task=NotebookTask(notebook_path="/directory/to/pipeline.py"),
        )
    )

    job = CreateJob(name="test_job_rtdip", job_clusters=cluster_list, tasks=task_list)

    databricks_job = DatabricksSDKDeploy(
        databricks_job=job, host="https://test.databricks.net", token="test_token"
    )

    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.deploy.databricks.DatabricksSDKDeploy._load_module",
        return_value=DummyModule(),
    )
    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.deploy.databricks.DatabricksSDKDeploy._convert_file_to_binary",
        return_value=None,
    )
    mocker.patch(
        "databricks.sdk.mixins.workspace.WorkspaceExt.mkdirs", return_value=None
    )
    mocker.patch(
        "databricks.sdk.mixins.workspace.WorkspaceExt.upload", return_value=None
    )
    libraries = Libraries(
        pypi_libraries=[PyPiLibrary(name="rtdip-sdk", version=default_version)],
        maven_libraries=[
            MavenLibrary(
                group_id="rtdip", artifact_id="rtdip-sdk", version=default_version
            )
        ],
        python_wheel_libraries=[PythonWheelLibrary(path="test_wheel.whl")],
    )
    mocker.patch(
        "src.sdk.python.rtdip_sdk.pipelines.utilities.pipeline_components.PipelineComponentsGetUtility.execute",
        return_value=(libraries, {"config": "test_config"}),
    )
    mocker.patch(default_list_package, return_value=[])
    mocker.patch("databricks.sdk.service.jobs.JobsAPI.create", side_effect=Exception)
    with pytest.raises(Exception):
        databricks_job.deploy()

    mocker.patch(default_list_package, return_value=[DummyJob()])
    mocker.patch("databricks.sdk.service.jobs.JobsAPI.run_now", side_effect=Exception)
    with pytest.raises(Exception):
        databricks_job.launch()

    mocker.patch(default_list_package, return_value=[DummyJob()])
    mocker.patch(
        "databricks.sdk.service.jobs.JobsAPI.cancel_all_runs", side_effect=Exception
    )
    with pytest.raises(Exception):
        databricks_job.launch()
