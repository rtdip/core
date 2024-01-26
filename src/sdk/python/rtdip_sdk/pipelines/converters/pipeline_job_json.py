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

from .interfaces import ConverterInterface
from ..secrets.models import PipelineSecret
from ..execute.models import PipelineJob
from ..sources import *  # NOSONAR
from ..transformers import *  # NOSONAR
from ..destinations import *  # NOSONAR
from ..secrets import *  # NOSONAR
from ..utilities import *  # NOSONAR


class PipelineJobFromJsonConverter(ConverterInterface):
    """
    Converts a json string into a Pipeline Job.

    Example
    -------
    ```python
    from rtdip_sdk.pipelines.secrets import PipelineJobFromJsonConverter

    convert_json_string_to_pipline_job = PipelineJobFromJsonConverter(
        pipeline_json = "{JSON-STRING}"
    )

    convert_json_string_to_pipline_job.convert()
    ```

    Parameters:
        pipeline_json (str): Json representing PipelineJob information, including tasks and related steps
    """

    pipeline_json: str

    def __init__(self, pipeline_json: str):
        self.pipeline_json = pipeline_json

    def _try_convert_to_pipeline_secret(self, value):
        try:
            if "pipeline_secret" in value:
                value["pipeline_secret"]["type"] = getattr(
                    sys.modules[__name__], value["pipeline_secret"]["type"]
                )
            return PipelineSecret.parse_obj(value["pipeline_secret"])
        except:  # NOSONAR
            return value

    def convert(self) -> PipelineJob:
        """
        Converts a json string to a Pipeline Job
        """
        pipeline_job_dict = json.loads(self.pipeline_json)

        # convert string component to class
        for task in pipeline_job_dict["task_list"]:
            for step in task["step_list"]:
                step["component"] = getattr(sys.modules[__name__], step["component"])
                for param_key, param_value in step["component_parameters"].items():
                    step["component_parameters"][param_key] = (
                        self._try_convert_to_pipeline_secret(param_value)
                    )
                    if not isinstance(
                        step["component_parameters"][param_key], PipelineSecret
                    ) and isinstance(param_value, dict):
                        for key, value in param_value.items():
                            step["component_parameters"][param_key][key] = (
                                self._try_convert_to_pipeline_secret(value)
                            )

        return PipelineJob(**pipeline_job_dict)


class PipelineJobToJsonConverter(ConverterInterface):
    """
    Converts a Pipeline Job into a json string.

    Example
    -------
    ```python
    from rtdip_sdk.pipelines.secrets import PipelineJobToJsonConverter

    convert_pipeline_job_to_json_string = PipelineJobFromJsonConverter(
        pipeline_json = PipelineJob
    )

    convert_pipeline_job_to_json_string.convert()
    ```

    Parameters:
        pipeline_job (PipelineJob): A Pipeline Job consisting of tasks and steps
    """

    pipeline_job: PipelineJob

    def __init__(self, pipeline_job: PipelineJob):
        self.pipeline_job = pipeline_job

    def convert(self):
        """
        Converts a Pipeline Job to a json string
        """
        # required because pydantic does not use encoders in subclasses
        for task in self.pipeline_job.task_list:
            step_dict_list = []
            for step in task.step_list:
                step_dict_list.append(
                    json.loads(step.json(models_as_dict=False, exclude_none=True))
                )
            task.step_list = step_dict_list

        pipeline_job_json = self.pipeline_job.json(exclude_none=True)
        return pipeline_job_json
