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

from typing import List
from dependency_injector import containers, providers
from .container import Clients, Configs
from .models import PipelineJob, PipelineTask, PipelineStep
from .._pipeline_utils.models import Libraries, SystemType
from ..sources.interfaces import SourceInterface
from ..transformers.interfaces import TransformerInterface
from ..destinations.interfaces import DestinationInterface
from ..utilities.interfaces import UtilitiesInterface
from ..secrets.models import PipelineSecret


class PipelineJobExecute:
    """
    Executes Pipeline components in their intended order as a complete data pipeline. It ensures that components dependencies are injected as needed.

    Parameters:
        job (PipelineJob): Contains the steps and tasks of a PipelineJob to be executed
        batch_job (bool): Specifies if the job is to be executed as a batch job
    """

    job: PipelineJob

    def __init__(self, job: PipelineJob, batch_job: bool = False):
        self.job = job

    def _get_provider_attributes(
        self, provider: providers.Factory, component: object
    ) -> providers.Factory:
        attributes = getattr(component, "__annotations__", {}).items()
        # add spark session, if needed
        for key, value in attributes:
            # if isinstance(value, SparkSession): # TODO: fix this as value does not seem to be an instance of SparkSession
            if key == "spark":
                provider.add_kwargs(spark=Clients.spark_client().spark_session)
            if key == "data":
                provider.add_kwargs(data=None)
        return provider

    def _get_secret_provider_attributes(
        self, pipeline_secret: PipelineSecret
    ) -> providers.Factory:
        secret_provider = providers.Factory(pipeline_secret.type)
        secret_provider = self._get_provider_attributes(
            secret_provider, pipeline_secret.type
        )
        secret_provider.add_kwargs(vault=pipeline_secret.vault, key=pipeline_secret.key)
        return secret_provider

    def _tasks_order(self, task_list: List[PipelineTask]):
        """
        Orders tasks within a job
        """
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

    def _steps_order(self, step_list: List[PipelineStep]):
        """
        Orders steps within a task
        """
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

    def _task_setup_dependency_injection(self, step_list: List[PipelineStep]):
        """
        Determines the dependencies to be injected into each component
        """
        container = containers.DynamicContainer()
        task_libraries = Libraries()
        task_step_configuration = {}
        task_spark_configuration = {}
        # setup container configuration
        for step in step_list:
            if (
                step.component.system_type() == SystemType.PYSPARK
                or step.component.system_type() == SystemType.PYSPARK_DATABRICKS
            ):
                # set spark configuration
                task_spark_configuration = {
                    **task_spark_configuration,
                    **step.component.settings(),
                }

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
            provider = self._get_provider_attributes(provider, step.component)

            # get secrets
            for param_key, param_value in step.component_parameters.items():
                if isinstance(param_value, PipelineSecret):
                    step.component_parameters[param_key] = (
                        self._get_secret_provider_attributes(param_value)().get()
                    )
                if isinstance(param_value, dict):
                    for key, value in param_value.items():
                        if isinstance(value, PipelineSecret):
                            step.component_parameters[param_key][key] = (
                                self._get_secret_provider_attributes(value)().get()
                            )

            provider.add_kwargs(**step.component_parameters)

            # set provider
            container.set_provider(step.name, provider)
        return container

    def run(self) -> bool:
        """
        Executes all the steps and tasks in a pipeline job as per the job definition.
        """
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
                    result = factory(data=task_results[step.name]).transform()

                # destination components
                elif isinstance(factory(), DestinationInterface):
                    if task.batch_task:
                        result = factory(
                            data=task_results[step.name], query_name=step.name
                        ).write_batch()
                    else:
                        result = factory(
                            data=task_results[step.name], query_name=step.name
                        ).write_stream()

                # utilities components
                elif isinstance(factory(), UtilitiesInterface):
                    result = factory().execute()

                # store results for steps that need it as input
                if step.provide_output_to_step is not None:
                    for step in step.provide_output_to_step:
                        task_results[step] = result

        return True
