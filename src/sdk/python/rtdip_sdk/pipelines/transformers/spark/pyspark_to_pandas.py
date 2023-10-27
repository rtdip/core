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

from pyspark.sql.dataframe import DataFrame as PySparkDataFrame
from pandas import DataFrame as PandasDataFrame

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType


class PySparkToPandasTransformer(TransformerInterface):
    """
    Converts a PySpark DataFrame to a Pandas DataFrame.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.transformers import PySparkToPandasTransformer

    pyspark_to_pandas = PySparkToPandasTransformer(
        df=df
    )

    result = pyspark_to_pandas.transform()
    ```

    Parameters:
        df (DataFrame): PySpark DataFrame to be converted
    """

    df: PySparkDataFrame

    def __init__(self, df: PySparkDataFrame) -> None:
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

    def pre_transform_validation(self):
        return True

    def post_transform_validation(self):
        return True

    def transform(self) -> PandasDataFrame:
        """
        Returns:
            DataFrame: A Pandas dataframe converted from a PySpark DataFrame.
        """
        df = self.df.toPandas()
        return df
