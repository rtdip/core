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
from typing import List, Optional
from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError
from delta.tables import DeltaTable

from ..interfaces import UtilitiesInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import get_default_package


class DeltaTableOptimizeUtility(UtilitiesInterface):
    """
    [Optimizes](https://docs.delta.io/latest/optimizations-oss.html) a Delta Table.

    Example
    -------
    ```python
    from rtdip_sdk.pipelines.utilities.spark.delta_table_optimize import DeltaTableOptimizeUtility

    table_optimize_utility = DeltaTableOptimizeUtility(
        spark=spark_session,
        table_name="delta_table",
        where="EventDate<=current_date()",
        zorder_by=["EventDate"]
    )

    result = table_optimize_utility.execute()
    ```

    Parameters:
        spark (SparkSession): Spark Session required to read data from cloud storage
        table_name (str): Name of the table, including catalog and schema if table is to be created in Unity Catalog
        where (str, optional): Apply a partition filter to limit optimize to specific partitions. Example, "date='2021-11-18'" or "EventDate<=current_date()"
        zorder_by (list[str], optional): List of column names to zorder the table by. For more information, see [here.](https://docs.delta.io/latest/optimizations-oss.html#optimize-performance-with-file-management&language-python)
    """

    spark: SparkSession
    table_name: str
    where: Optional[str]
    zorder_by: Optional[List[str]]

    def __init__(
        self,
        spark: SparkSession,
        table_name: str,
        where: str = None,
        zorder_by: List[str] = None,
    ) -> None:
        self.spark = spark
        self.table_name = table_name
        self.where = where
        self.zorder_by = zorder_by

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
            delta_table = DeltaTable.forName(self.spark, self.table_name).optimize()

            if self.where is not None:
                delta_table = delta_table.where(self.where)

            if self.zorder_by is not None:
                delta_table = delta_table.executeZOrderBy(self.zorder_by)
            else:
                delta_table.executeCompaction()

            return True

        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e
