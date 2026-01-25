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

from pyspark.sql import DataFrame as SparkDataFrame
from pandas import DataFrame as PandasDataFrame
from ..interfaces import PipelineComponentBaseInterface


class DecompositionBaseInterface(PipelineComponentBaseInterface):
    """
    Base interface for PySpark-based time series decomposition components.
    """

    @abstractmethod
    def decompose(self) -> SparkDataFrame:
        """
        Perform time series decomposition on the input data.

        Returns:
            SparkDataFrame: DataFrame containing the original data plus
                           decomposed components (trend, seasonal, residual)
        """
        pass


class PandasDecompositionBaseInterface(PipelineComponentBaseInterface):
    """
    Base interface for Pandas-based time series decomposition components.
    """

    @abstractmethod
    def decompose(self) -> PandasDataFrame:
        """
        Perform time series decomposition on the input data.

        Returns:
            PandasDataFrame: DataFrame containing the original data plus
                            decomposed components (trend, seasonal, residual)
        """
        pass
