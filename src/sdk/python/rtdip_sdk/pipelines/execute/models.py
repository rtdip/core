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

from typing import List, Optional, Type, Union, Dict
import re
from pydantic.v1 import BaseConfig, BaseModel, validator
from abc import ABCMeta
from ..sources.interfaces import SourceInterface
from ..transformers.interfaces import TransformerInterface
from ..destinations.interfaces import DestinationInterface
from ..secrets.models import PipelineSecret
from ..utilities.interfaces import UtilitiesInterface

BaseConfig.json_encoders = {
    ABCMeta: lambda x: x.__name__,
    PipelineSecret: lambda x: {"pipeline_secret": x.dict()},
}


def validate_name(name: str) -> str:
    if re.match("^[a-z0-9_]*$", name) is None:
        raise ValueError("Can only contain lower case letters, numbers and underscores")
    else:
        return name


class PipelineStep(BaseModel):
    name: str
    description: str
    depends_on_step: Optional[List[str]]
    component: Union[
        Type[SourceInterface],
        Type[TransformerInterface],
        Type[DestinationInterface],
        Type[UtilitiesInterface],
    ]
    component_parameters: Optional[dict]
    provide_output_to_step: Optional[List[str]]

    class Config:
        json_encoders = {
            ABCMeta: lambda x: x.__name__,
            PipelineSecret: lambda x: {"pipeline_secret": x.dict()},
        }

    # validators
    _validate_name = validator("name", allow_reuse=True, always=True)(validate_name)
    _validate_depends_on_step = validator(
        "depends_on_step", allow_reuse=True, each_item=True
    )(validate_name)
    _validate_provide_output_to_step = validator(
        "provide_output_to_step", allow_reuse=True, each_item=True
    )(validate_name)


class PipelineTask(BaseModel):
    name: str
    description: str
    depends_on_task: Optional[List[str]]
    step_list: List[PipelineStep]
    batch_task: Optional[bool]

    class Config:
        json_encoders = {
            ABCMeta: lambda x: x.__name__,
            PipelineSecret: lambda x: {"pipeline_secret": x.dict()},
        }

    # validators
    _validate_name = validator("name", allow_reuse=True)(validate_name)
    _validate_depends_on_step = validator(
        "depends_on_task", allow_reuse=True, each_item=True
    )(validate_name)


class PipelineJob(BaseModel):
    name: str
    description: str
    version: str
    task_list: List[PipelineTask]

    class Config:
        json_encoders = {
            ABCMeta: lambda x: x.__name__,
            PipelineSecret: lambda x: {"pipeline_secret": x.dict()},
        }

    # validators
    _validate_name = validator("name", allow_reuse=True)(validate_name)
