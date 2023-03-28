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
from databricks_cli.sdk.api_client import ApiClient
from databricks_cli.jobs.api import JobsApi

from .interfaces import DeployInterface
from .models.databricks import DatabricksJob, DatabricksJobForPipelineJob, DatabricksSparkPythonTask, DatabricksTask, DatabricksLibraries, DatabricksLibrariesMaven, DatbricksLibrariesPypi, DatabricksDBXProject
from ..execute.job import PipelineJob

__name__: str
__version__: str
__description__: str

class DatabricksDBXDeploy(DeployInterface):
    '''

    '''
    pipeline_job: PipelineJob
    databricks_job_for_pipeline_job: DatabricksJobForPipelineJob
    host: str
    token: str

    def __init__(self, pipeline_job: PipelineJob, databricks_job_for_pipeline_job: DatabricksJobForPipelineJob, host: str, token: str) -> None:
        self.pipeline_job = pipeline_job
        self.databricks_job_for_pipeline_job = databricks_job_for_pipeline_job
        self.host = host
        self.token = token
        __name__ = pipeline_job.name
        __version__ = pipeline_job.version
        __description__ = pipeline_job.description
    
    def deploy(self) -> bool:

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
                    databricks_job_task.libraries.append(DatabricksLibraries(maven=DatabricksLibrariesMaven(coordinates=maven_library.to_string(), repo=maven_library.repo)))
                for wheel_library in libraries.pythonwheel_libraries:
                    databricks_job_task.libraries.append(DatabricksLibraries(whl=wheel_library))

            try:
                rtdip_version = version("rtdip-sdk")
                databricks_job_task.libraries.append(DatabricksLibraries(pypi=DatbricksLibrariesPypi(package="rtdip-sdk[pipelines]=={}".format(rtdip_version))))
            except PackageNotFoundError as e:
                databricks_job_task.libraries.append(DatabricksLibraries(pypi=DatbricksLibrariesPypi(package="rtdip-sdk[pipelines]")))

            databricks_job_task.spark_python_task = DatabricksSparkPythonTask(
                python_file="file://{}".format("rtdip/tasks/pipeline_task.py"), #.format(task_python_file_location),
                parameters=[self.pipeline_job.json(exclude_none=True)]
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
                "workspace_directory": "/rtdip/{}/".format(self.pipeline_job.name.lower()),
                "artifact_location": "dbfs:/rtdip/projects/{}".format(self.pipeline_job.name.lower())
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
            deployment_file=Path("conf/deployment.json.j2"),
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