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

from typing import Optional, List, Type, Union
from pydantic import BaseModel

from ..sources.interfaces import SourceInterface
from ..transformers.interfaces import TransformerInterface
from ..destinations.interfaces import DestinationInterface
from ..utilities.interfaces import UtilitiesInterface

class PipelineStep(BaseModel):
    name: str
    description: str
    depends_on_step: Optional[list[str]]
    component: Union[Type[SourceInterface], Type[TransformerInterface], Type[DestinationInterface], Type[UtilitiesInterface]]
    component_parameters: Optional[dict]
    provide_output_to_step: Optional[list[str]]

    class Config:
        json_encoders = {
            Union[Type[SourceInterface], Type[TransformerInterface], Type[DestinationInterface], Type[UtilitiesInterface]]: lambda x: x.__name__
        }

class PipelineTask(BaseModel):
    name: str
    description: str
    depends_on_task: Optional[list[str]]
    step_list: list[PipelineStep]
    batch_task: Optional[bool]

class PipelineJob(BaseModel):
    name: str
    description: str
    version: str
    task_list: list[PipelineTask]