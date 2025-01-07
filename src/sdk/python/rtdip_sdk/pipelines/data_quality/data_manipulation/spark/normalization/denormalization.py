# Copyright 2024 RTDIP
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
from ....input_validator import InputValidator
from src.sdk.python.rtdip_sdk.pipelines.data_quality.data_manipulation.interfaces import (
    DataManipulationBaseInterface,
)
from src.sdk.python.rtdip_sdk.pipelines._pipeline_utils.models import (
    Libraries,
    SystemType,
)
from .normalization import (
    NormalizationBaseClass,
)


class Denormalization(DataManipulationBaseInterface, InputValidator):
    """
    #TODO
    Applies the appropriate denormalization method to revert values to their original scale.

    Example
    --------
    ```python
    from src.sdk.python.rtdip_sdk.pipelines.data_wranglers import Denormalization
    from pyspark.sql import SparkSession
    from pyspark.sql.dataframe import DataFrame

    denormalization = Denormalization(normalized_df, normalization)
    denormalized_df = denormalization.filter()
    ```

    Parameters:
        df (DataFrame): PySpark DataFrame to be reverted to its original scale.
        normalization_to_revert (NormalizationBaseClass): An instance of the specific normalization subclass (NormalizationZScore, NormalizationMinMax, NormalizationMean) that was originally used to normalize the data.
    """

    df: PySparkDataFrame
    normalization_to_revert: NormalizationBaseClass

    def __init__(
        self, df: PySparkDataFrame, normalization_to_revert: NormalizationBaseClass
    ) -> None:
        self.df = df
        self.normalization_to_revert = normalization_to_revert

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
        return self.normalization_to_revert.denormalize(self.df)
