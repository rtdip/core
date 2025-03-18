# Copyright 2025 RTDIP
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
from abc import abstractmethod

from great_expectations.compatibility.pyspark import DataFrame

from ..interfaces import PipelineComponentBaseInterface


class MachineLearningInterface(PipelineComponentBaseInterface):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def train(self, train_df: DataFrame):
        return self

    @abstractmethod
    def predict(self, predict_df: DataFrame, *args, **kwargs) -> DataFrame:
        pass
