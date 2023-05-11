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
import json
import shutil
from importlib_metadata import PackageNotFoundError, version
from pathlib import Path

from dbx.commands.deploy import deploy as dbx_deploy
from dbx.commands.launch import launch as dbx_launch
from dbx.api.auth import ProfileEnvConfigProvider

from .interfaces import DeployInterface
from .models.databricks import DatabricksJob, DatabricksJobForPipelineJob, DatabricksSparkPythonTask, DatabricksTask, DatabricksLibraries, DatabricksLibrariesMaven, DatbricksLibrariesPypi, DatabricksDBXProject
from ..execute.job import PipelineJob
from ..converters.pipeline_job_json import PipelineJobToJsonConverter

__name__: str
__version__: str
__description__: str

class DatabricksDBXDeploy(DeployInterface):
    '''
    Deploys an RTDIP Pipeline to Databricks Worflows leveraging Databricks DBX.  For more information about Databricks DBX, please click [here.](https://dbx.readthedocs.io/en/latest/)

    Deploying an RTDIP Pipeline to Databricks requires only a few additional pieces of information to ensure the RTDIP Pipeline Job can be run in Databricks. This information includes:

    - **Cluster**: This can be defined a the Job or Task level and includes the size of the cluster to be used for the job
    - **Task**: The cluster to be used to execute the task, as well as any task scheduling information, if required.

    All options available in the [Databricks Jobs REST API v2.1](https://docs.databricks.com/dev-tools/api/latest/jobs.html) can be configured in the Databricks classes that have been defined in `rtdip_sdk.pipelines.deploy.models.databricks`, enabling full control of the configuration of the Databricks Workflow :

    - `DatabricksJob`
    - `DatabricksTask`

    RTDIP Pipeline Components provide Databricks with all the required Python packages and JARs to execute each component and these will be setup on the Worflow automatically during the Databricks Workflow creation.

    Example:
        This example assumes that a PipelineJob has already been defined by a variable called `pipeline_job`

        ```python
        from rtdip_sdk.pipelines.deploy.databricks import DatabricksDBXDeploy

        # Setup a job cluster for Databricks
        databricks_job_cluster = DatabricksJobCluster(
            job_cluster_key="test_job_cluster", 
            new_cluster=DatabricksCluster(
                spark_version = "11.3.x-scala2.12",
                node_type_id = "Standard_D3_v2",
                num_workers = 2
            )
        )

        # Define the cluster to be leveraged for the Pipeline Task
        databricks_task = DatabricksTaskForPipelineTask(
            name="test_task", 
            job_cluster_key="test_job_cluster"
        )

        # Create the Databricks Job for the PipelineJob
        databricks_job = DatabricksJobForPipelineJob(
            job_clusters=[databricks_job_cluster],
            databricks_task_for_pipeline_task_list=[databricks_task]
        )

        # Create an instance of `DatabricksDBXDeploy` and pass the relevant arguments to the class
        databricks_job = DatabricksDBXDeploy(
            pipeline_job=pipeline_job, 
            databricks_job_for_pipeline_job=databricks_job, 
            host="https://test.databricks.net", 
            token="test_token"
        )

        # Execute the deploy method to create a Workflow in the specified Databricks Environment
        deploy_result = databricks_job.deploy()

        # If the job should be executed immediately, excute the `launch` method
        launch_result = databricks_job.launch()        
        ```

    Args:
        pipeline_job (PipelineJob): Pipeline Job containing tasks and steps that are to be deployed
        databricks_job_for_pipeline_job (DatabricksJobForPipelineJob): Contains Databricks specific information required for deploying the RTDIP Pipeline Job to Databricks, such as cluster and workflow scheduling information. This can be any field in the [Databricks Jobs REST API v2.1](https://docs.databricks.com/dev-tools/api/latest/jobs.html)
        host (str): Databricks URL
        token (str): Token for authenticating with Databricks such as a Databricks PAT Token or Azure AD Token
        workspace_directory (str, optional): Determines the folder location in the Databricks Workspace. Defaults to /rtdip 
        artifacts_directory (str, optional): Determines the folder location in the Databricks Workspace. Defaults to dbfs:/rtdip/projects
    '''
    pipeline_job: PipelineJob
    databricks_job_for_pipeline_job: DatabricksJobForPipelineJob
    host: str
    token: str
    workspace_directory: str
    artifacts_directory: str

    def __init__(self, pipeline_job: PipelineJob, databricks_job_for_pipeline_job: DatabricksJobForPipelineJob, host: str, token: str, workspace_directory: str = "/rtdip", artifacts_directory: str = "dbfs:/rtdip/projects") -> None:
        self.pipeline_job = pipeline_job
        self.databricks_job_for_pipeline_job = databricks_job_for_pipeline_job
        self.host = host
        self.token = token
        self.workspace_directory = workspace_directory
        self.artifacts_directory = artifacts_directory

    
    def deploy(self) -> bool:
        '''
        Deploys an RTDIP Pipeline Job to Databricks Workflows. The deployment is managed by the Pipeline Job Name and therefore will overwrite any existing workflow in Databricks with the same name.

        DBX packages the pipeline job into a python wheel that is uploaded as an artifact in the dbfs and creates the relevant tasks as specified by the Databricks Jobs REST API. 
        '''

        # Setup folder 
        current_dir = os.getcwd()
        dbx_path = os.path.dirname(os.path.abspath(__file__)) + "/dbx"
        project_path = os.path.dirname(os.path.abspath(__file__)) + "/dbx/.dbx"
        build_path = os.path.dirname(os.path.abspath(__file__)) + "/dbx/build"
        dist_path = os.path.dirname(os.path.abspath(__file__)) + "/dbx/dist"
        egg_path = os.path.dirname(os.path.abspath(__file__)) + "/dbx/{}.egg-info".format(self.pipeline_job.name)
        if os.path.exists(project_path):
            shutil.rmtree(project_path, ignore_errors=True)
        if os.path.exists(build_path):
            shutil.rmtree(build_path, ignore_errors=True)
        if os.path.exists(dist_path):
            shutil.rmtree(dist_path, ignore_errors=True)
        if os.path.exists(egg_path):
            shutil.rmtree(egg_path, ignore_errors=True)

        os.chdir(dbx_path)

        # create Databricks Job Tasks
        databricks_tasks = []
        for task in self.pipeline_job.task_list:
            databricks_job_task = DatabricksTask(task_key=task.name, libraries=[], depends_on=[])
            if self.databricks_job_for_pipeline_job.databricks_task_for_pipeline_task_list is not None:
                databricks_task_for_pipeline_task = next(x for x in self.databricks_job_for_pipeline_job.databricks_task_for_pipeline_task_list if x.name == task.name)
                if databricks_task_for_pipeline_task is not None:
                    databricks_job_task.__dict__.update(databricks_task_for_pipeline_task.__dict__)
            
            databricks_job_task.name = task.name
            databricks_job_task.depends_on = task.depends_on_task

            # get libraries
            for step in task.step_list:
                libraries = step.component.libraries()
                for pypi_library in libraries.pypi_libraries:
                    databricks_job_task.libraries.append(DatabricksLibraries(pypi=DatbricksLibrariesPypi(package=pypi_library.to_string(), repo=pypi_library.repo)))
                for maven_library in libraries.maven_libraries:
                    if not maven_library.group_id in ["io.delta", "org.apache.spark"]:
                        databricks_job_task.libraries.append(DatabricksLibraries(maven=DatabricksLibrariesMaven(coordinates=maven_library.to_string(), repo=maven_library.repo)))
                for wheel_library in libraries.pythonwheel_libraries:
                    databricks_job_task.libraries.append(DatabricksLibraries(whl=wheel_library))

            try:
                rtdip_version = version("rtdip-sdk")
                databricks_job_task.libraries.append(DatabricksLibraries(pypi=DatbricksLibrariesPypi(package="rtdip-sdk[pipelines]=={}".format(rtdip_version))))
            except PackageNotFoundError as e:
                databricks_job_task.libraries.append(DatabricksLibraries(pypi=DatbricksLibrariesPypi(package="rtdip-sdk[pipelines]")))

            databricks_job_task.spark_python_task = DatabricksSparkPythonTask(
                python_file="file://{}".format("rtdip/tasks/pipeline_task.py"),
                parameters=[PipelineJobToJsonConverter(self.pipeline_job).convert()]
            )
            databricks_tasks.append(databricks_job_task)

        databricks_job = DatabricksJob(name=self.pipeline_job.name, tasks=databricks_tasks)
        databricks_job.__dict__.update(self.databricks_job_for_pipeline_job.__dict__)
        databricks_job.__dict__.pop("databricks_task_for_pipeline_task_list", None)

        # Setup Project 
        environment = {
            "profile": "rtdip",
            "storage_type": "mlflow",
            "properties": {
                "workspace_directory": "{}/{}/".format(self.workspace_directory, self.pipeline_job.name.lower()),
                "artifact_location": "{}/{}".format(self.artifacts_directory, self.pipeline_job.name.lower())
            }            
        }
        project = DatabricksDBXProject(
            environments={"rtdip": environment},
            inplace_jinja_support=True,
            failsafe_cluster_reuse_with_assets=False,
            context_based_upload_for_execute=False
        )

        # create project file
        if not os.path.exists(project_path):
            os.mkdir(project_path)

        with open(project_path + "/project.json", "w") as f:
            json.dump(project.dict(), f)

        # create Databricks DBX Environment
        os.environ[ProfileEnvConfigProvider.DBX_PROFILE_ENV] = json.dumps(environment)

        os.environ["RTDIP_DEPLOYMENT_CONFIGURATION"] = json.dumps({
            "environments": { 
                "rtdip": {"workflows": [databricks_job.dict(exclude_none=True)]}
            }
        })

        # set authentication environment variables
        os.environ["DATABRICKS_HOST"] = self.host
        os.environ["DATABRICKS_TOKEN"] = self.token

        os.environ["RTDIP_PACKAGE_NAME"] = self.pipeline_job.name.lower()
        os.environ["RTDIP_PACKAGE_DESCRIPTION"] = self.pipeline_job.description
        os.environ["RTDIP_PACKAGE_VERSION"] = self.pipeline_job.version

        # Create Databricks DBX Job
        dbx_deploy(
            workflow_name=self.pipeline_job.name,
            workflow_names=None,
            job_names=None,
            deployment_file=Path("conf/deployment.json"),
            environment_name="rtdip",
            requirements_file=None,
            jinja_variables_file=None,
            branch_name=None,
            tags=[],
            headers=[],
            no_rebuild=False,
            no_package=False,
            files_only=False,
            assets_only=False,
            write_specs_to_file=None,
            debug=False,
        )
        if os.path.exists(build_path):
            shutil.rmtree(build_path, ignore_errors=True)
        if os.path.exists(dist_path):
            shutil.rmtree(dist_path, ignore_errors=True)
        if os.path.exists(egg_path):
            shutil.rmtree(egg_path, ignore_errors=True)
        os.chdir(current_dir)

        return True
                            
    def launch(self):
        '''
        Launches an RTDIP Pipeline Job in Databricks Workflows. This will perform the equivalent of a `Run Now` in Databricks Workflows
        '''        
        # set authentication environment variables
        os.environ["DATABRICKS_HOST"] = self.host
        os.environ["DATABRICKS_TOKEN"] = self.token

        #launch job        
        current_dir = os.getcwd()
        dbx_path = os.path.dirname(os.path.abspath(__file__)) + "/dbx"
        os.chdir(dbx_path)
        dbx_launch(
            workflow_name=self.pipeline_job.name,
            environment_name="rtdip",
            job_name=None,
            is_pipeline=False,
            trace=False,
            kill_on_sigterm=False,
            existing_runs="pass",
            as_run_submit=False,
            from_assets=False,
            tags=[],
            branch_name=None,
            include_output=None,
            headers=None,
            parameters=None,
            debug=None
        )
        os.chdir(current_dir)

        return True