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

from pyspark.sql import DataFrame as PySparkDataFrame
from pyspark.ml.stat import Correlation
from pyspark.sql.functions import col
from pyspark.ml.feature import VectorAssembler

from ...interfaces import DataManipulationBaseInterface
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)


class GaussianSmoothing(DataManipulationBaseInterface):


    df: PySparkDataFrame
    sigma: float

    def __init__(
        self,
        df: PySparkDataFrame,
        sigma: float,
    ) -> None:
        # Validate input


        self.df = df


    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK
        """
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}



    def filter(self) -> PySparkDataFrame:
        return self.df
