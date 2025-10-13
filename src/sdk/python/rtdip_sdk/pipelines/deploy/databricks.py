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
from dataclasses import dataclass
import sys
from typing import List, Optional, Union
from importlib_metadata import PackageNotFoundError, version
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from io import BytesIO
from enum import Enum
from typing import Any, Callable, Dict, Iterator, List, Optional
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.service.jobs import (
    JobSettings,
    Continuous,
    JobAccessControlRequest,
    JobDeployment,
    JobEditMode,
    JobEmailNotifications,
    JobEnvironment,
    Format,
    GitSource,
    JobsHealthRules,
    JobCluster,
    JobNotificationSettings,
    JobParameterDefinition,
    PerformanceTarget,
    QueueSettings,
    JobRunAs,
    CronSchedule,
    Task,
    WebhookNotifications,
    TriggerSettings,
)
from databricks.sdk.service.compute import Library, PythonPyPiLibrary, MavenLibrary
from .interfaces import DeployInterface
from ..utilities.pipeline_components import PipelineComponentsGetUtility

__name__: str
__version__: str
__description__: str


@dataclass
class CreateJob:
    access_control_list: Optional[List[JobAccessControlRequest]] = None
    """List of permissions to set on the job."""

    budget_policy_id: Optional[str] = None
    """The id of the user specified budget policy to use for this job. If not specified, a default
    budget policy may be applied when creating or modifying the job. See
    `effective_budget_policy_id` for the budget policy used by this workload."""

    continuous: Optional[Continuous] = None
    """An optional continuous property for this job. The continuous property will ensure that there is
    always one run executing. Only one of `schedule` and `continuous` can be used."""

    deployment: Optional[JobDeployment] = None
    """Deployment information for jobs managed by external sources."""

    description: Optional[str] = None
    """An optional description for the job. The maximum length is 27700 characters in UTF-8 encoding."""

    edit_mode: Optional[JobEditMode] = None
    """Edit mode of the job.
    
    * `UI_LOCKED`: The job is in a locked UI state and cannot be modified. * `EDITABLE`: The job is
    in an editable state and can be modified."""

    email_notifications: Optional[JobEmailNotifications] = None
    """An optional set of email addresses that is notified when runs of this job begin or complete as
    well as when this job is deleted."""

    environments: Optional[List[JobEnvironment]] = None
    """A list of task execution environment specifications that can be referenced by serverless tasks
    of this job. An environment is required to be present for serverless tasks. For serverless
    notebook tasks, the environment is accessible in the notebook environment panel. For other
    serverless tasks, the task environment is required to be specified using environment_key in the
    task settings."""

    format: Optional[Format] = None
    """Used to tell what is the format of the job. This field is ignored in Create/Update/Reset calls.
    When using the Jobs API 2.1 this value is always set to `"MULTI_TASK"`."""

    git_source: Optional[GitSource] = None
    """An optional specification for a remote Git repository containing the source code used by tasks.
    Version-controlled source code is supported by notebook, dbt, Python script, and SQL File tasks.
    
    If `git_source` is set, these tasks retrieve the file from the remote repository by default.
    However, this behavior can be overridden by setting `source` to `WORKSPACE` on the task.
    
    Note: dbt and SQL File tasks support only version-controlled sources. If dbt or SQL File tasks
    are used, `git_source` must be defined on the job."""

    health: Optional[JobsHealthRules] = None

    job_clusters: Optional[List[JobCluster]] = None
    """A list of job cluster specifications that can be shared and reused by tasks of this job.
    Libraries cannot be declared in a shared job cluster. You must declare dependent libraries in
    task settings."""

    max_concurrent_runs: Optional[int] = None
    """An optional maximum allowed number of concurrent runs of the job. Set this value if you want to
    be able to execute multiple runs of the same job concurrently. This is useful for example if you
    trigger your job on a frequent schedule and want to allow consecutive runs to overlap with each
    other, or if you want to trigger multiple runs which differ by their input parameters. This
    setting affects only new runs. For example, suppose the job’s concurrency is 4 and there are 4
    concurrent active runs. Then setting the concurrency to 3 won’t kill any of the active runs.
    However, from then on, new runs are skipped unless there are fewer than 3 active runs. This
    value cannot exceed 1000. Setting this value to `0` causes all new runs to be skipped."""

    name: Optional[str] = None
    """An optional name for the job. The maximum length is 4096 bytes in UTF-8 encoding."""

    notification_settings: Optional[JobNotificationSettings] = None
    """Optional notification settings that are used when sending notifications to each of the
    `email_notifications` and `webhook_notifications` for this job."""

    parameters: Optional[List[JobParameterDefinition]] = None
    """Job-level parameter definitions"""

    performance_target: Optional[PerformanceTarget] = None
    """The performance mode on a serverless job. This field determines the level of compute performance
    or cost-efficiency for the run.
    
    * `STANDARD`: Enables cost-efficient execution of serverless workloads. *
    `PERFORMANCE_OPTIMIZED`: Prioritizes fast startup and execution times through rapid scaling and
    optimized cluster performance."""

    queue: Optional[QueueSettings] = None
    """The queue settings of the job."""

    run_as: Optional[JobRunAs] = None

    schedule: Optional[CronSchedule] = None
    """An optional periodic schedule for this job. The default behavior is that the job only runs when
    triggered by clicking “Run Now” in the Jobs UI or sending an API request to `runNow`."""

    tags: Optional[Dict[str, str]] = None
    """A map of tags associated with the job. These are forwarded to the cluster as cluster tags for
    jobs clusters, and are subject to the same limitations as cluster tags. A maximum of 25 tags can
    be added to the job."""

    tasks: Optional[List[Task]] = None
    """A list of task specifications to be executed by this job. It supports up to 1000 elements in
    write endpoints (:method:jobs/create, :method:jobs/reset, :method:jobs/update,
    :method:jobs/submit). Read endpoints return only 100 tasks. If more than 100 tasks are
    available, you can paginate through them using :method:jobs/get. Use the `next_page_token` field
    at the object root to determine if more results are available."""

    timeout_seconds: Optional[int] = None
    """An optional timeout applied to each run of this job. A value of `0` means no timeout."""

    trigger: Optional[TriggerSettings] = None
    """A configuration to trigger a run when certain conditions are met. The default behavior is that
    the job runs only when triggered by clicking “Run Now” in the Jobs UI or sending an API
    request to `runNow`."""

    webhook_notifications: Optional[WebhookNotifications] = None
    """A collection of system notification IDs to notify when runs of this job begin or complete."""

    def as_dict(self) -> dict:  # pragma: no cover
        """Serializes the CreateJob into a dictionary suitable for use as a JSON request body."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = [
                v.as_dict() for v in self.access_control_list
            ]
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.continuous:
            body["continuous"] = self.continuous.as_dict()
        if self.deployment:
            body["deployment"] = self.deployment.as_dict()
        if self.description is not None:
            body["description"] = self.description
        if self.edit_mode is not None:
            body["edit_mode"] = self.edit_mode.value
        if self.email_notifications:
            body["email_notifications"] = self.email_notifications.as_dict()
        if self.environments:
            body["environments"] = [v.as_dict() for v in self.environments]
        if self.format is not None:
            body["format"] = self.format.value
        if self.git_source:
            body["git_source"] = self.git_source.as_dict()
        if self.health:
            body["health"] = self.health.as_dict()
        if self.job_clusters:
            body["job_clusters"] = [v.as_dict() for v in self.job_clusters]
        if self.max_concurrent_runs is not None:
            body["max_concurrent_runs"] = self.max_concurrent_runs
        if self.name is not None:
            body["name"] = self.name
        if self.notification_settings:
            body["notification_settings"] = self.notification_settings.as_dict()
        if self.parameters:
            body["parameters"] = [v.as_dict() for v in self.parameters]
        if self.performance_target is not None:
            body["performance_target"] = self.performance_target.value
        if self.queue:
            body["queue"] = self.queue.as_dict()
        if self.run_as:
            body["run_as"] = self.run_as.as_dict()
        if self.schedule:
            body["schedule"] = self.schedule.as_dict()
        if self.tags:
            body["tags"] = self.tags
        if self.tasks:
            body["tasks"] = [v.as_dict() for v in self.tasks]
        if self.timeout_seconds is not None:
            body["timeout_seconds"] = self.timeout_seconds
        if self.trigger:
            body["trigger"] = self.trigger.as_dict()
        if self.webhook_notifications:
            body["webhook_notifications"] = self.webhook_notifications.as_dict()
        return body

    def as_shallow_dict(self) -> dict:  # pragma: no cover
        """Serializes the CreateJob into a shallow dictionary of its immediate attributes."""
        body = {}
        if self.access_control_list:
            body["access_control_list"] = self.access_control_list
        if self.budget_policy_id is not None:
            body["budget_policy_id"] = self.budget_policy_id
        if self.continuous:
            body["continuous"] = self.continuous
        if self.deployment:
            body["deployment"] = self.deployment
        if self.description is not None:
            body["description"] = self.description
        if self.edit_mode is not None:
            body["edit_mode"] = self.edit_mode
        if self.email_notifications:
            body["email_notifications"] = self.email_notifications
        if self.environments:
            body["environments"] = self.environments
        if self.format is not None:
            body["format"] = self.format
        if self.git_source:
            body["git_source"] = self.git_source
        if self.health:
            body["health"] = self.health
        if self.job_clusters:
            body["job_clusters"] = self.job_clusters
        if self.max_concurrent_runs is not None:
            body["max_concurrent_runs"] = self.max_concurrent_runs
        if self.name is not None:
            body["name"] = self.name
        if self.notification_settings:
            body["notification_settings"] = self.notification_settings
        if self.parameters:
            body["parameters"] = self.parameters
        if self.performance_target is not None:
            body["performance_target"] = self.performance_target
        if self.queue:
            body["queue"] = self.queue
        if self.run_as:
            body["run_as"] = self.run_as
        if self.schedule:
            body["schedule"] = self.schedule
        if self.tags:
            body["tags"] = self.tags
        if self.tasks:
            body["tasks"] = self.tasks
        if self.timeout_seconds is not None:
            body["timeout_seconds"] = self.timeout_seconds
        if self.trigger:
            body["trigger"] = self.trigger
        if self.webhook_notifications:
            body["webhook_notifications"] = self.webhook_notifications
        return body


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

    def _convert_file_to_binary(self, path) -> BytesIO:  # pragma: no cover
        with open(path, "rb") as f:
            return BytesIO(f.read())

    def _load_module(self, module_name, path):  # pragma: no cover
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
        for task in self.databricks_job.tasks:  # pragma: no cover
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
