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
from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField
from py4j.protocol import Py4JJavaError

from ..interfaces import UtilitiesInterface
from ..._pipeline_utils.models import Libraries, SystemType


class SparkConfigurationUtility(UtilitiesInterface):
    """
    Sets configuration key value pairs to a Spark Session

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.sources import SparkConfigurationUtility
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    # Not required if using Databricks
    spark = SparkSessionUtility(config={}).execute()

    configuration_utility = SparkConfigurationUtility(
        spark=spark,
        config={}
    )

    result = configuration_utility.execute()
    ```

    Parameters:
        spark (SparkSession): Spark Session required to read data from cloud storage
        config (dict): Dictionary of spark configuration to be applied to the spark session
    """

    spark: SparkSession
    config: dict
    columns: List[StructField]
    partitioned_by: List[str]
    location: str
    properties: dict
    comment: str

    def __init__(self, spark: SparkSession, config: dict) -> None:
        self.spark = spark
        self.config = config

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

    def execute(self) -> bool:
        """Executes configuration key value pairs to a Spark Session"""
        try:
            for configuration in self.config.items():
                self.spark.conf.set(configuration[0], configuration[1])
            return True

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e
