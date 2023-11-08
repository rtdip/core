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
from typing import Optional
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError
from delta.tables import DeltaTable

from ..interfaces import UtilitiesInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package


class DeltaTableVacuumUtility(UtilitiesInterface):
    """
    [Vacuums](https://docs.delta.io/latest/delta-utility.html#-delta-vacuum) a Delta Table.

    Example
    -------
    ```python
    from rtdip_sdk.pipelines.utilities.spark.delta_table_vacuum import DeltaTableVacuumUtility

    table_vacuum_utility =  DeltaTableVacuumUtility(
        spark=spark_session,
        table_name="delta_table",
        retention_hours="168"
    )

    result = table_vacuum_utility.execute()
    ```

    Parameters:
        spark (SparkSession): Spark Session required to read data from cloud storage
        table_name (str): Name of the table, including catalog and schema if table is to be created in Unity Catalog
        retention_hours (int, optional): Sets the retention threshold in hours.
    """

    spark: SparkSession
    table_name: str
    retention_hours: Optional[int]

    def __init__(
        self, spark: SparkSession, table_name: str, retention_hours: int = None
    ) -> None:
        self.spark = spark
        self.table_name = table_name
        self.retention_hours = retention_hours

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
        libraries.add_maven_library(get_default_package("spark_delta_core"))
        return libraries

    @staticmethod
    def settings() -> dict:
        return {}

    def execute(self) -> bool:
        try:
            delta_table = DeltaTable.forName(self.spark, self.table_name)

            delta_table.vacuum(retentionHours=self.retention_hours)

            return True

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e
