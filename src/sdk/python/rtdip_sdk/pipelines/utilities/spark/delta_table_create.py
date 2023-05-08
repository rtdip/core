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
from pyspark.sql import SparkSession
from pyspark.sql.types import StructField
from py4j.protocol import Py4JJavaError
from delta.tables import DeltaTable

from ..interfaces import UtilitiesInterface
from ..._pipeline_utils.models import Libraries, SystemType
from ..._pipeline_utils.constants import DEFAULT_PACKAGES

class DeltaTableCreateUtility(UtilitiesInterface):
    '''
    Creates a Delta Table in a Hive Metastore or in Databricks Unity Catalog.

    Args:
        spark (SparkSession): Spark Session required to read data from cloud storage
        table_name (str): Name of the table, including catalog and schema if table is to be created in Unity Catalog
        columns (list[StructField]): List of columns and their related column properties
        partitioned_by (list[str], optional): List of column names to partition the table by
        location (str, optional): Path to storage location
        properties (dict, optional): Propoerties that can be specified for a Delta Table. Further information on the options available are [here](https://docs.databricks.com/delta/table-properties.html#delta-table-properties)
        comment (str, optional): Provides a comment on the table metadata
    ''' 
    spark: SparkSession
    table_name: str
    columns: list[StructField]
    partitioned_by: list[str]
    location: str
    properties: dict
    comment: str

    def __init__(self, spark: SparkSession, table_name: str, columns: list[StructField], partitioned_by: list[str] = None, location: str = None, properties: dict = None, comment: str = None) -> None:
        self.spark = spark
        self.table_name = table_name
        self.columns = columns
        self.partitioned_by = partitioned_by
        self.location = location
        self.properties = properties
        self.comment = comment

    @staticmethod
    def system_type():
        '''
        Attributes:
            SystemType (Environment): Requires PYSPARK
        '''            
        return SystemType.PYSPARK

    @staticmethod
    def libraries():
        libraries = Libraries()
        libraries.add_maven_library(DEFAULT_PACKAGES["spark_delta_core"])
        return libraries
    
    @staticmethod
    def settings() -> dict:
        return {}

    def execute(self) -> bool:
        try:
            delta_table = (
                DeltaTable
                .createIfNotExists(self.spark)
                .tableName(self.table_name)
                .addColumns(self.columns)
            )

            if self.partitioned_by is not None:
                delta_table = delta_table.partitionedBy(self.partitioned_by)

            if self.location is not None:
                delta_table = delta_table.location(self.location)

            if self.properties is not None:
                for key, value in self.properties.items():
                    delta_table = delta_table.property(key, value)
            
            if self.comment is not None:
                delta_table = delta_table.comment(self.comment)

            delta_table.execute()
            return True
        
        except Py4JJavaError as e:
            logging.exception(e.errmsg)
            raise e
        except Exception as e:
            logging.exception(str(e))
            raise e