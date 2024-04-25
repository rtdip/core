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
from typing import Union
from importlib_metadata import PackageNotFoundError, version
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from io import BytesIO

from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.service.jobs import CreateJob, JobSettings
from databricks.sdk.service.compute import Library, PythonPyPiLibrary, MavenLibrary
from .interfaces import DeployInterface
from ..utilities.pipeline_components import PipelineComponentsGetUtility

__name__: str
__version__: str
__description__: str


class DatabricksSDKDeploy(DeployInterface):
    """
    Deploys an RTDIP Pipeline to Databricks Workflows leveraging the Databricks [SDK.](https://docs.databricks.com/dev-tools/sdk-python.html)

    Deploying an RTDIP Pipeline to Databricks requires only a few additional pieces of information to ensure the RTDIP Pipeline Job can be run in Databricks. This information includes:

    - **Cluster**: This can be defined a the Job or Task level and includes the size of the cluster to be used for the job
    - **Task**: The cluster to be used to execute the task, as well as any task scheduling information, if required.

    All options available in the [Databricks Jobs REST API v2.1](https://docs.databricks.com/dev-tools/api/latest/jobs.html) can be configured in the Databricks classes that have been defined in `rtdip_sdk.pipelines.deploy.models.databricks`, enabling full control of the configuration of the Databricks Workflow :

    - `CreateJob`
    - `Task`

    RTDIP Pipeline Components provide Databricks with all the required Python packages and JARs to execute each component and these will be setup on the Workflow automatically during the Databricks Workflow creation.

    Example:
        This example assumes that a PipelineJob has already been defined by a variable called `pipeline_job`

        ```python
        from rtdip_sdk.pipelines.deploy import DatabricksSDKDeploy, CreateJob, JobCluster, ClusterSpec, Task, NotebookTask, ComputeSpecKind, AutoScale, RuntimeEngine, DataSecurityMode

        cluster_list = []
        cluster_list.append(JobCluster(
            job_cluster_key="test_cluster",
            new_cluster=ClusterSpec(
                node_type_id="Standard_E4ds_v5",
                autoscale=AutoScale(min_workers=1, max_workers=3),
                spark_version="13.2.x-scala2.12",
                data_security_mode=DataSecurityMode.SINGLE_USER,
                runtime_engine=RuntimeEngine.PHOTON
            )
        ))

        task_list = []
        task_list.append(Task(
            task_key="test_task",
            job_cluster_key="test_cluster",
            notebook_task=NotebookTask(
                notebook_path="/path/to/pipeline/rtdip_pipeline.py"
            )
        ))

        job = CreateJob(
            name="test_job_rtdip",
            job_clusters=cluster_list,
            tasks=task_list
        )

        databricks_job = DatabricksSDKDeploy(databricks_job=job, host="https://test.databricks.net", token="test_token")

        # Execute the deploy method to create a Workflow in the specified Databricks Environment
        deploy_result = databricks_job.deploy()

        # If the job should be executed immediately, execute the `launch` method
        launch_result = databricks_job.launch()
        ```

    Parameters:
        databricks_job (DatabricksJob): Contains Databricks specific information required for deploying the RTDIP Pipeline Job to Databricks, such as cluster and workflow scheduling information. This can be any field in the [Databricks Jobs REST API v2.1](https://docs.databricks.com/dev-tools/api/latest/jobs.html)
        host (str): Databricks URL
        token (str): Token for authenticating with Databricks such as a Databricks PAT Token or Azure AD Token
        workspace_directory (str, optional): Determines the folder location in the Databricks Workspace. Defaults to /rtdip
    """

    def __init__(
        self,
        databricks_job: CreateJob,
        host: str,
        token: str,
        workspace_directory: str = "/rtdip",
    ) -> None:
        if databricks_job.name is None or databricks_job.name == "":
            raise ValueError("databricks_job.name cannot be empty")
        self.databricks_job = databricks_job
        self.host = host
        self.token = token
        self.workspace_directory = workspace_directory

    def _convert_file_to_binary(self, path) -> BytesIO:
        with open(path, "rb") as f:
            return BytesIO(f.read())

    def _load_module(self, module_name, path):
        spec = spec_from_file_location(module_name, path)
        module = module_from_spec(spec)
        spec.loader.exec_module(module)
        sys.modules[module.__name__] = module
        return module

    def deploy(self) -> Union[bool, ValueError]:
        """
        Deploys an RTDIP Pipeline Job to Databricks Workflows. The deployment is managed by the Job Name and therefore will overwrite any existing workflow in Databricks with the same name.
        """
        # Add libraries to Databricks Job
        workspace_client = WorkspaceClient(
            config=Config(
                product="RTDIP",
                host=self.host,
                token=self.token,
                auth_type="pat",
            )
        )
        for task in self.databricks_job.tasks:
            if task.notebook_task is None and task.spark_python_task is None:
                return ValueError(
                    "A Notebook or Spark Python Task must be populated for each task in the Databricks Job"
                )  # NOSONAR
            if task.notebook_task is not None:
                module = self._load_module(
                    task.task_key + "file_upload", task.notebook_task.notebook_path
                )
                (task_libraries, spark_configuration) = PipelineComponentsGetUtility(
                    module.__name__
                ).execute()
                workspace_client.workspace.mkdirs(path=self.workspace_directory)
                path = "{}/{}".format(
                    self.workspace_directory,
                    Path(task.notebook_task.notebook_path).name,
                )
                workspace_client.workspace.upload(
                    path=path,
                    overwrite=True,
                    content=self._convert_file_to_binary(
                        task.notebook_task.notebook_path
                    ),
                )
                task.notebook_task.notebook_path = path
            else:
                module = self._load_module(
                    task.task_key + "file_upload", task.spark_python_task.python_file
                )
                (task_libraries, spark_configuration) = PipelineComponentsGetUtility(
                    module
                ).execute()
                workspace_client.workspace.mkdirs(path=self.workspace_directory)
                path = "{}/{}".format(
                    self.workspace_directory,
                    Path(task.spark_python_task.python_file).name,
                )
                workspace_client.workspace.upload(
                    path=path,
                    overwrite=True,
                    content=self._convert_file_to_binary(
                        task.spark_python_task.python_file
                    ),
                )
                task.spark_python_task.python_file = path

            task.libraries = []
            for pypi_library in task_libraries.pypi_libraries:
                task.libraries.append(
                    Library(
                        pypi=PythonPyPiLibrary(
                            package=pypi_library.to_string(), repo=pypi_library.repo
                        )
                    )
                )
            for maven_library in task_libraries.maven_libraries:
                if not maven_library.group_id in ["io.delta", "org.apache.spark"]:
                    task.libraries.append(
                        Library(
                            maven=MavenLibrary(
                                coordinates=maven_library.to_string(),
                                repo=maven_library.repo,
                            )
                        )
                    )
            for wheel_library in task_libraries.pythonwheel_libraries:
                task.libraries.append(Library(whl=wheel_library))

            try:
                rtdip_version = version("rtdip-sdk")
                task.libraries.append(
                    Library(
                        pypi=PythonPyPiLibrary(
                            package="rtdip-sdk[pipelines]=={}".format(rtdip_version)
                        )
                    )
                )
            except PackageNotFoundError as e:
                task.libraries.append(
                    Library(pypi=PythonPyPiLibrary(package="rtdip-sdk[pipelines]"))
                )

            # Add Spark Configuration to Databricks Job
            if (
                task.new_cluster is None
                and task.job_cluster_key is None
                and task.compute_key is None
            ):
                return ValueError(
                    "A Cluster or Compute must be specified for each task in the Databricks Job"
                )
            if task.new_cluster is not None:
                if spark_configuration is not None:
                    if task.new_cluster.spark_conf is None:
                        task.new_cluster.spark_conf = {}
                    task.new_cluster.spark_conf.update(spark_configuration)
            elif task.job_cluster_key is not None:
                for job_cluster in self.databricks_job.job_clusters:
                    if job_cluster.job_cluster_key == task.job_cluster_key:
                        if spark_configuration is not None:
                            if job_cluster.new_cluster.spark_conf is None:
                                job_cluster.new_cluster.spark_conf = {}
                            job_cluster.new_cluster.spark_conf.update(
                                spark_configuration
                            )
                        break
            elif task.compute_key is not None:
                for compute in self.databricks_job.compute:
                    if compute.compute_key == task.compute_key:
                        # TODO : Add spark config for compute. Does not seem to be currently available in the Databricks SDK # NOSONAR
                        # compute.spark_conf.update(spark_configuration)
                        break

        # Create Databricks Job
        job_found = False
        for existing_job in workspace_client.jobs.list(name=self.databricks_job.name):
            new_settings = JobSettings()
            for key, value in self.databricks_job.__dict__.items():
                if key in new_settings.__dict__:
                    setattr(new_settings, key, value)
            workspace_client.jobs.reset(
                job_id=existing_job.job_id, new_settings=new_settings
            )
            job_found = True
            break

        if job_found == False:
            workspace_client.jobs.create(**self.databricks_job.__dict__)

        return True

    def launch(self):
        """
        Launches an RTDIP Pipeline Job in Databricks Workflows. This will perform the equivalent of a `Run Now` in Databricks Workflows
        """
        workspace_client = WorkspaceClient(
            config=Config(
                product="RTDIP",
                host=self.host,
                token=self.token,
                auth_type="pat",
            )
        )
        job_found = False
        for existing_job in workspace_client.jobs.list(name=self.databricks_job.name):
            workspace_client.jobs.run_now(job_id=existing_job.job_id)
            job_found = True
            break

        if job_found == False:
            raise ValueError("Job not found in Databricks Workflows")

        return True

    def stop(self):
        """
        Cancels an RTDIP Pipeline Job in Databricks Workflows. This will perform the equivalent of a `Cancel All Runs` in Databricks Workflows
        """
        workspace_client = WorkspaceClient(
            config=Config(
                product="RTDIP",
                host=self.host,
                token=self.token,
                auth_type="pat",
            )
        )
        job_found = False
        for existing_job in workspace_client.jobs.list(name=self.databricks_job.name):
            workspace_client.jobs.cancel_all_runs(job_id=existing_job.job_id)
            job_found = True
            break

        if job_found == False:
            raise ValueError("Job not found in Databricks Workflows")

        return True
