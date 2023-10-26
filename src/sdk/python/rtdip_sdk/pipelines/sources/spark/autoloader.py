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

import logging
from pyspark.sql import DataFrame, SparkSession

from ..interfaces import SourceInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package


class DataBricksAutoLoaderSource(SourceInterface):
    """
    The Spark Auto Loader is used to read new data files as they arrive in cloud storage. Further information on Auto Loader is available [here](https://docs.databricks.com/ingestion/auto-loader/index.html)

    Example
    --------
    === "ADLS Gen2"

        ```python
        from rtdip_sdk.pipelines.sources import DataBricksAutoLoaderSource
        from rtdip_sdk.pipelines.utilities import SparkSessionUtility

        # Not required if using Databricks
        spark = SparkSessionUtility(config={}).execute()

        options = {}
        path = "abfss://{FILE-SYSTEM}@{ACCOUNT-NAME}.dfs.core.windows.net/{PATH}/{FILE-NAME}
        format = "{DESIRED-FILE-FORMAT}"

        DataBricksAutoLoaderSource(spark, options, path, format).read_stream()

        OR

        DataBricksAutoLoaderSource(spark, options, path, format).read_batch()
        ```
    === "AWS S3"

        ```python
        from rtdip_sdk.pipelines.sources import DataBricksAutoLoaderSource
        from rtdip_sdk.pipelines.utilities import SparkSessionUtility

        # Not required if using Databricks
        spark = SparkSessionUtility(config={}).execute()

        options = {}
        path = "https://s3.{REGION-CODE}.amazonaws.com/{BUCKET-NAME}/{KEY-NAME}"
        format = "{DESIRED-FILE-FORMAT}"

        DataBricksAutoLoaderSource(spark, options, path, format).read_stream()

        OR

        DataBricksAutoLoaderSource(spark, options, path, format).read_batch()
        ```
    === "GCS"

        ```python
        from rtdip_sdk.pipelines.sources import DataBricksAutoLoaderSource
        from rtdip_sdk.pipelines.utilities import SparkSessionUtility

        # Not required if using Databricks
        spark = SparkSessionUtility(config={}).execute()

        options = {}
        path = "gs://{BUCKET-NAME}/{FILE-PATH}"
        format = "{DESIRED-FILE-FORMAT}"

        DataBricksAutoLoaderSource(spark, options, path, format).read_stream()

        OR

        DataBricksAutoLoaderSource(spark, options, path, format).read_batch()
        ```

    Parameters:
        spark (SparkSession): Spark Session required to read data from cloud storage
        options (dict): Options that can be specified for configuring the Auto Loader. Further information on the options available are [here](https://docs.databricks.com/ingestion/auto-loader/options.html)
        path (str): The cloud storage path
        format (str): Specifies the file format to be read. Supported formats are available [here](https://docs.databricks.com/ingestion/auto-loader/options.html#file-format-options)
    """

    spark: SparkSession
    options: dict
    path: str

    def __init__(
        self, spark: SparkSession, options: dict, path: str, format: str
    ) -> None:
        self.spark = spark
        self.options = options
        self.path = path
        self.options["cloudFiles.format"] = format

    @staticmethod
    def system_type():
        """
        Attributes:
            SystemType (Environment): Requires PYSPARK on Databricks
        """
        return SystemType.PYSPARK_DATABRICKS

    @staticmethod
    def libraries():
        libraries = Libraries()
        libraries.add_maven_library(get_default_package("spark_delta_core"))
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def pre_read_validation(self):
        return True

    def post_read_validation(self, df: DataFrame):
        return True

    def read_batch(self):
        """
        Raises:
            NotImplementedError: Auto Loader only supports streaming reads. To perform a batch read, use the read_stream method of this component and specify the Trigger on the write_stream to be `availableNow` to perform batch-like reads of cloud storage files.
        """
        raise NotImplementedError(
            "Auto Loader only supports streaming reads. To perform a batch read, use the read_stream method and specify Trigger on the write_stream as `availableNow`"
        )

    def read_stream(self) -> DataFrame:
        """
        Performs streaming reads of files in cloud storage.
        """
        try:
            return (
                self.spark.readStream.format("cloudFiles")
                .options(**self.options)
                .load(self.path)
            )

        except Exception as e:
            logging.exception(str(e))
            raise e
