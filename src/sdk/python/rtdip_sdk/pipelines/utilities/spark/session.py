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
import sys
import inspect
from typing import List
from pyspark.sql import SparkSession

from ..interfaces import UtilitiesInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.spark import SparkClient
from ..pipeline_components import PipelineComponentsGetUtility


class SparkSessionUtility(UtilitiesInterface):
    """
    Creates or Gets a Spark Session and uses settings and libraries of the imported RTDIP components to populate the spark configuration and jars in the spark session.

    Call this component after all imports of the RTDIP components to ensure that the spark session is configured correctly.

    Example
    --------
    ```python
    from rtdip_sdk.pipelines.utilities import SparkSessionUtility

    spark_session_utility = SparkSessionUtility(
        config={},
        module=None,
        remote=None
    )

    result = spark_session_utility.execute()
    ```

    Parameters:
        config (optional dict): Dictionary of spark configuration to be applied to the spark session
        module (optional str): Provide the module to use for imports of rtdip-sdk components. If not populated, it will use the calling module to check for imports
        remote (optional str): Specify the remote parameters if intending to use Spark Connect
    """

    spark: SparkSession
    config: dict
    module: str

    def __init__(
        self, config: dict = None, module: str = None, remote: str = None
    ) -> None:
        self.config = config
        if module == None:
            frm = inspect.stack()[1]
            mod = inspect.getmodule(frm[0])
            self.module = mod.__name__
        else:
            self.module = module
        self.remote = remote

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

    def execute(self) -> SparkSession:
        """To execute"""
        try:
            (task_libraries, spark_configuration) = PipelineComponentsGetUtility(
                self.module, self.config
            ).execute()
            self.spark = SparkClient(
                spark_configuration=spark_configuration,
                spark_libraries=task_libraries,
                spark_remote=self.remote,
            ).spark_session
            return self.spark

        except Exception as e:
            logging.exception(str(e))
            raise e
