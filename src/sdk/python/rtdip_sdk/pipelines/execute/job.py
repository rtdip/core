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
import json
from dependency_injector import containers, providers

from .container import Clients, Configs
from .models import PipelineJob, PipelineTask, PipelineStep
from .._pipeline_utils.models import Libraries, SystemType
from ..sources.interfaces import SourceInterface
from ..transformers.interfaces import TransformerInterface
from ..destinations.interfaces import DestinationInterface
from ..utilities.interfaces import UtilitiesInterface

class PipelineJobExecute():
    '''
    Executes Pipeline components in their intended order as a complete data pipeline. It ensures that components dependencies are injected as needed.

    Args:
        job (PipelineJob): Contains the steps and tasks of a PipelineJob to be executed
        batch_job (bool): Specifies if the job is to be executed as a batch job
    '''
    job: PipelineJob

    def __init__(self, job: PipelineJob, batch_job: bool = False):
        self.job = job

    def _tasks_order(self, task_list: list[PipelineTask]):
        '''
        Orders tasks within a job
        '''
        ordered_task_list = []
        temp_task_list = task_list.copy()
        while len(temp_task_list) > 0:
            for task in temp_task_list:
                if task.depends_on_task is None:
                    ordered_task_list.append(task)
                    temp_task_list.remove(task)
                else:
                    for ordered_task in ordered_task_list:
                        if task.depends_on_task == ordered_task.name:
                            ordered_task_list.append(task)
                            temp_task_list.remove(task)
                            break
        return ordered_task_list
    
    def _steps_order(self, step_list: list[PipelineStep]):
        '''
        Orders steps within a task
        '''        
        ordered_step_list = []
        temp_step_list = step_list.copy()
        while len(temp_step_list) > 0:
            for step in temp_step_list:
                if step.depends_on_step is None:
                    ordered_step_list.append(step)
                    temp_step_list.remove(step)
                else:
                    ordered_step_names = [x.name for x in ordered_step_list]
                    if all(item in ordered_step_names for item in step.depends_on_step):
                        ordered_step_list.append(step)
                        temp_step_list.remove(step)
                        break
        return ordered_step_list

    def _task_setup_dependency_injection(self, step_list: list[PipelineStep]):
        '''
        Determines the dependencies to be injected into each component 
        '''
        container = containers.DynamicContainer()
        task_libraries = Libraries()
        task_step_configuration = {}
        task_spark_configuration = {}
        # setup container configuration
        for step in step_list:
            if step.component.system_type() == SystemType.PYSPARK or step.component.system_type() == SystemType.PYSPARK_DATABRICKS:

                # set spark configuration
                task_spark_configuration = {**task_spark_configuration, **step.component.settings()}
                        
                # set spark libraries
                component_libraries = step.component.libraries()
                for library in component_libraries.pypi_libraries:
                    task_libraries.pypi_libraries.append(library)
                for library in component_libraries.maven_libraries:
                    task_libraries.maven_libraries.append(library)
                for library in component_libraries.pythonwheel_libraries:
                    task_libraries.pythonwheel_libraries.append(library)

        Configs.spark_configuration.override(task_spark_configuration)
        Configs.step_configuration.override(task_step_configuration)
        Configs.spark_libraries.override(task_libraries)

        # setup container provider factories
        for step in step_list:
            # setup factory provider for component
            provider = providers.Factory(step.component)
            attributes = getattr(step.component, '__annotations__', {}).items()
            # add spark session, if needed
            for key, value in attributes:
                # if isinstance(value, SparkSession): # TODO: fix this as value does not seem to be an instance of SparkSession
                if key == "spark":
                    provider.add_kwargs(spark=Clients.spark_client().spark_session)
            # add parameters
            if isinstance(step.component, DestinationInterface):
                step.component_parameters["query_name"] = step.name
            provider.add_kwargs(**step.component_parameters)
            # set provider
            container.set_provider(
                step.name,
                provider
            )
        return container

    def run(self) -> bool:
        '''
        Executes all the steps and tasks in a pipeline job as per the job definition.
        '''
        ordered_task_list = self._tasks_order(self.job.task_list)

        for task in ordered_task_list:
            container = self._task_setup_dependency_injection(task.step_list)
            ordered_step_list = self._steps_order(task.step_list)
            task_results = {}
            for step in ordered_step_list:
                factory = container.providers.get(step.name)

                # source components
                if isinstance(factory(), SourceInterface):
                    if task.batch_task:
                        result = factory().read_batch()
                    else:
                        result = factory().read_stream()
                        
                # transformer components
                elif isinstance(factory(), TransformerInterface):
                    result = factory().transform(task_results[step.name])

                # destination components
                elif isinstance(factory(), DestinationInterface):
                    if task.batch_task:
                        result = factory().write_batch(task_results[step.name])
                    else:
                        result = factory().write_stream(task_results[step.name])

                # utilities components
                elif isinstance(factory(), UtilitiesInterface):
                    result = factory().execute()  

                # store results for steps that need it as input
                if step.provide_output_to_step is not None:
                    for step in step.provide_output_to_step:
                        task_results[step] = result

        return True
                    
class PipelineJobFromJson():
    '''
    Converts a json string into a Pipeline Jobs

    Args:
        pipeline_json: Json representing PipelineJob information, including tasks and related steps 
    '''
    pipeline_json: str

    def init(self, pipeline_json: str):
        self.pipeline_json = pipeline_json

    def convert(self) -> PipelineJob:
        pipeline_job_dict = json.loads(self.pipeline_json)

        # convert string component to class
        for task in pipeline_job_dict["task_list"]:
            for step in task["step_list"]:
                step["component"] = getattr(sys.modules[__name__], step["component"])

        return PipelineJob(**pipeline_job_dict)
    