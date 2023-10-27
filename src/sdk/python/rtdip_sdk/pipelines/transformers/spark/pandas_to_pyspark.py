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

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame as PySparkDataFrame
from pandas import DataFrame as PandasDataFrame

from ..interfaces import TransformerInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ...._sdk_utils.pandas import _prepare_pandas_to_convert_to_spark


class PandasToPySparkTransformer(TransformerInterface):
    """
    Converts a Pandas DataFrame to a PySpark DataFrame.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.transformers import PandasToPySparkTransformer
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    pandas_to_pyspark = PandasToPySparkTransformer(
        spark=spark,
        df=df,
    )

    result = pandas_to_pyspark.transform()
    ```

    Parameters:
        spark (SparkSession): Spark Session required to convert DataFrame
        df (DataFrame): Pandas DataFrame to be converted
    """

    spark: SparkSession
    df: PandasDataFrame

    def __init__(self, spark: SparkSession, df: PandasDataFrame) -> None:
        self.spark = spark
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

    def transform(self) -> PySparkDataFrame:
        """
        Returns:
            DataFrame: A PySpark dataframe converted from a Pandas DataFrame.
        """

        self.df = _prepare_pandas_to_convert_to_spark(self.df)
        df = self.spark.createDataFrame(self.df)

        return df
